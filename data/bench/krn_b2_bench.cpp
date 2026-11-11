// krn_b2_bench — B2, rows/sec through the DAG, one data point per step (ENC-1487;
// the mbo data point, ENC-1628).
//
// Design: krn specs/2026-10-14-krn-data-layer/SPEC.md D3, D9, D13;
//         docs/data-layer.md §6, §14.
//
// WHAT IT MEASURES
// ----------------
// The real sample's rows, decoded from one retained DBN file through krn-data's
// own DatabentoAdapter (no ClickHouse, no CSV), driven through the
// engine's Dispatcher into a NON-WINDOWED graph built by the engine's own
// TreeBuilder (D9: nothing here waits on a clock, so nothing needs event time).
// Each STEP is one way of getting a record into the Dispatcher. Steps 1 and 2
// exist at the pinned engine commit:
//
//   1. json-ontick   today's JSON path: a rapidjson DOM synthesized per record
//                    (the equivalent tick RecordFieldSource defines), then
//                    `Dispatcher::onTick`. DOM construction is INSIDE the timed
//                    region, because that is the cost the step exists to show.
//   2. onrecord      `Dispatcher::onRecord` over a `RecordFieldSource`; no DOM.
//
// ONE DATA POINT PER FILE, chosen by what the file decodes to (ENC-1628):
//
//   trades.dbn -> TickRecords, through RecordFieldSource.
//   mbo.dbn    -> BookRecords, through BookRecordFieldSource. EVERY decoded row
//                 is offered on both steps: a print carries price and size, a
//                 book mutation carries no field (its equivalent DOM is `{}`),
//                 per the mapping in BookRecordFieldSource.hpp. The run prints
//                 the split, because fieldless rows are cheaper per row and the
//                 rows/sec of a mixed stream is not a per-print figure.
//
// A file that decodes to both, or to neither, is refused (exit 2). The two data
// points are separate runs with separate references and are never summed:
// docs/data-layer.md §C13, the same execution is in both files.
//
// Steps 3-5 (inline DAG drive, interned symbols, store-write elision) are
// engine changes in krn and are NOT implemented here. Each plugs in as one more
// entry in `steps()` below: a `configure` hook for whatever engine knob the
// step introduces, an `ingest` call, and an `OutputContract` saying what it
// must reproduce byte-for-byte. Nothing else in the harness changes.
//
// THE BIT-IDENTICAL ASSERTION (SPEC D3)
// ------------------------------------
// Every iteration of every step captures its output as BYTES: each terminal's
// delivery stream in order (symbol, the ArgType's raw bytes, bucketStartMs) and
// the AtomicStore's sorted final contents. Step 1's first measured iteration is
// the reference; every other iteration of every step — step 1's own included,
// which is what catches nondeterminism — must equal it exactly, or the run
// exits 1. Doubles are compared as their 8-byte representation, so a one-ULP
// difference is a mismatch. `--falsify=<kind>` perturbs step 2 and must turn
// the verdict red; `run_b2.sh` asserts that each one applied and did.
//
// It is refused, not passed, when the comparison would be vacuous: a terminal
// that received nothing, an empty store, or no rows decoded all exit 2.
//
// Exit: 0 identical, 1 a mismatch (a finding), 2 could not run.
//
// WHAT IT DOES NOT CLAIM
// ----------------------
// No B2 >= B1 verdict. The sample is 1000 trades rows and 1000 mbo messages;
// fixed costs dominate at that size (SPEC §5, the T5 notes). Iterations replay the same real rows over
// a FRESH engine each time — never duplicated or synthesized rows — and the
// cache is warm by construction: the rows are decoded into memory once, before
// any timing, and no I/O is in the timed region.

#include "krn/AtomicStore.hpp"
#include "krn/Dispatcher.hpp"
#include "krn/Event.hpp"
#include "krn/FunctionRegistry.hpp"
#include "krn/NodeRegistry.hpp"
#include "krn/StreamValue.hpp"
#include "krn/TreeBuilder.hpp"
#include "krn/data/BookRecord.hpp"
#include "krn/data/BookRecordFieldSource.hpp"
#include "krn/data/DatabentoAdapter.hpp"
#include "krn/data/RecordFieldSource.hpp"
#include "krn/data/TickRecord.hpp"
#include "krn/engine/FieldSource.hpp"
#include "krn/engine/SymbolTable.hpp"
#include "krn/nodes/INode.hpp"
#include "krn/rt/ThreadPool.hpp"
#include "krn/util/Config.hpp"

#include <rapidjson/document.h>

#include <sched.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace {

using namespace krn;

constexpr int kExitIdentical = 0;
constexpr int kExitMismatch  = 1;
constexpr int kExitCouldNot  = 2;

// ---------------------------------------------------------------------------
// The graph. Non-windowed (D9): Listener, Worker, Aggregate, AtomicAccessor
// only — no BucketTime, TumblingWindow or Interval, so no wall clock is read.
// `%S` is replaced with the sample's ticker, resolved from DBN symbology.
//
//   raw     the head Listener straight into the terminal: carries the record's
//           price unchanged, so a one-ULP input difference reaches the output.
//   mean    a Listener on the Dispatcher-computed builtin `mean`. The
//           Dispatcher computes a builtin only when something listens on its
//           name (computeAndStoreAtomics), so this and `stddev` are what make
//           the store keys `pull` reads exist at all.
//   stddev  likewise, for `stddev`.
//   pull    clocked by `price`: the two builtins sampled from the AtomicStore
//           at the clock (AtomicAccessor::bindAtClock) and fanned into an
//           Aggregate — the pull nodes step 5's store-write elision must keep
//           fed.
//   volume  push on the second field: Worker(sum) over `size`.
//
// With no namespace flag (the default, atomicKeyNamespaceByField=false) the
// builtins are recomputed for EVERY listened field of the symbol — price and
// size — into the same bare key, and `mean`/`stddev` receive both. That is the
// engine's behaviour at the pin (ENC-1008), measured as-is.
// ---------------------------------------------------------------------------
struct RequestSpec {
  const char* name;
  const char* json;
};

const std::vector<RequestSpec>& requests() {
  static const std::vector<RequestSpec> r = {
      {"raw",    R"({"key":1,"streamKey":"%S","field":"price"})"},
      {"mean",   R"({"key":2,"streamKey":"%S","field":"mean"})"},
      {"stddev", R"({"key":3,"streamKey":"%S","field":"stddev"})"},
      {"pull",   R"({"key":4,"streamKey":"%S","field":"price",
                     "node":{"type":"Aggregate","arity":2,"inputs":[
                       {"type":"AtomicAccessor","streamKey":"%S","field":"mean"},
                       {"type":"AtomicAccessor","streamKey":"%S","field":"stddev"}]}})"},
      {"volume", R"({"key":5,"streamKey":"%S","field":"size",
                     "pipeline":[{"type":"Worker","fn":"sum"}]})"},
  };
  return r;
}

std::string substitute(std::string s, const std::string& ticker) {
  for (std::size_t p = s.find("%S"); p != std::string::npos; p = s.find("%S", p)) {
    s.replace(p, 2, ticker);
    p += ticker.size();
  }
  return s;
}

// ---------------------------------------------------------------------------
// Exact byte encoding of what the graph emits. Tag byte + raw representation;
// a double is its 8 bytes, so equality here is bit equality (NaN payloads and
// signed zeros included), never a tolerance.
// ---------------------------------------------------------------------------
void putBytes(std::string& out, const void* p, std::size_t n) {
  out.append(static_cast<const char*>(p), n);
}
template <typename T> void putPod(std::string& out, const T& v) { putBytes(out, &v, sizeof v); }
void putStr(std::string& out, const std::string& s) {
  putPod<std::uint64_t>(out, s.size());
  out += s;
}

void encode(std::string& out, const ArgType& v) {
  out.push_back(static_cast<char>(v.index()));
  std::visit([&out](auto&& x) {
    using T = std::decay_t<decltype(x)>;
    if constexpr (std::is_same_v<T, bool>) out.push_back(x ? 1 : 0);
    else if constexpr (std::is_same_v<T, int> || std::is_same_v<T, double>) putPod(out, x);
    else if constexpr (std::is_same_v<T, std::string>) putStr(out, x);
    else if constexpr (std::is_same_v<T, std::vector<int>> ||
                       std::is_same_v<T, std::vector<double>>) {
      putPod<std::uint64_t>(out, x.size());
      for (const auto& e : x) putPod(out, e);
    } else if constexpr (std::is_same_v<T, std::vector<ArgValue>>) {
      putPod<std::uint64_t>(out, x.size());
      for (const auto& e : x) encode(out, e.value);
    } else {  // Record
      putPod<std::uint64_t>(out, x.fields.size());
      for (const auto& f : x.fields) { putStr(out, f.name); encode(out, f.value.value); }
    }
  }, v);
}

// A readable rendering for mismatch reports only; never used for equality.
std::string render(const ArgType& v) {
  return std::visit([](auto&& x) -> std::string {
    using T = std::decay_t<decltype(x)>;
    char b[64];
    if constexpr (std::is_same_v<T, double>) { std::snprintf(b, sizeof b, "%.17g (%a)", x, x); return b; }
    else if constexpr (std::is_same_v<T, int>) return std::to_string(x);
    else if constexpr (std::is_same_v<T, bool>) return x ? "true" : "false";
    else if constexpr (std::is_same_v<T, std::string>) return "\"" + x + "\"";
    else if constexpr (std::is_same_v<T, std::vector<ArgValue>>) {
      std::string s = "[";
      for (const auto& e : x) s += render(e.value) + ", ";
      return s + "]";
    } else return "<" + std::to_string(sizeof(T)) + "-byte composite>";
  }, v);
}

// The graph's terminal. Appends each delivery's exact bytes; the values are
// also kept so a mismatch can name the first differing one in human terms.
class CaptureSink final : public INode {
public:
  void onValue(const StreamValue& sv) override {
    std::lock_guard<std::mutex> lk(mx_);
    const std::size_t at = bytes_.size();
    putStr(bytes_, sv.symbol);
    encode(bytes_, sv.value);
    putPod(bytes_, sv.bucketStartMs);
    offsets_.push_back(at);
    values_.push_back(sv.value);
  }
  void shutdown() noexcept override {}

  struct Capture {
    std::string            bytes;
    std::vector<std::size_t> offsets;
    std::vector<ArgType>   values;
  };
  Capture take() {
    std::lock_guard<std::mutex> lk(mx_);
    return Capture{std::move(bytes_), std::move(offsets_), std::move(values_)};
  }

private:
  std::mutex               mx_;
  std::string              bytes_;
  std::vector<std::size_t> offsets_;
  std::vector<ArgType>     values_;
};

struct RunOutput {
  std::vector<CaptureSink::Capture> perRequest;  // indexed like requests()
  std::string                       storeBytes;  // sorted AtomicStore snapshot
  std::size_t                       storeEntries = 0;
};

std::string encodeStore(const AtomicStore& store, std::size_t& entries) {
  auto snap = store.snapshot();
  std::sort(snap.begin(), snap.end(), [](const auto& a, const auto& b) {
    return std::tie(std::get<0>(a), std::get<1>(a)) < std::tie(std::get<0>(b), std::get<1>(b));
  });
  entries = snap.size();
  std::string out;
  for (const auto& [key, field, value] : snap) { putStr(out, key); putStr(out, field); encode(out, value); }
  return out;
}

// ---------------------------------------------------------------------------
// One engine instance: a pool, a store, a Dispatcher and the built graph.
// Constructed fresh for every iteration, OUTSIDE the timed region, so every
// iteration replays the sample from the same cold engine state.
// ---------------------------------------------------------------------------
struct Rig {
  Rig(unsigned threads, const util::Config& cfg, const std::string& ticker,
      const data::SymbolTable& symbols)
    : pool(std::make_shared<rt::ThreadPool>(threads)),
      disp(pool.get(), &store, cfg) {
    gThreadPool = pool;  // the global some nodes resolve their executor through
    disp.setSymbolTable(&symbols);
    tree::Deps deps;
    deps.store      = &store;
    deps.pool       = pool.get();
    deps.dispatcher = &disp;
    for (const auto& rq : requests()) {
      rapidjson::Document d;
      const std::string text = substitute(rq.json, ticker);
      d.Parse(text.c_str());
      if (d.HasParseError()) throw std::runtime_error(std::string("request does not parse: ") + rq.name);
      auto sink = std::make_shared<CaptureSink>();
      chains.push_back(tree::buildForRequest(d, deps, sink));
      sinks.push_back(sink);
    }
  }

  ~Rig() {
    pool->drain();
    for (auto& c : chains) {
      for (auto& n : c.keepAlive) if (n) n->shutdown();
      if (c.head) c.head->shutdown();
    }
    pool->drain();
    pool->shutdown();
    if (gThreadPool == pool) gThreadPool.reset();
  }

  void quiesce() { pool->drain(); }

  RunOutput collect() {
    RunOutput out;
    for (auto& s : sinks) out.perRequest.push_back(s->take());
    out.storeBytes = encodeStore(store, out.storeEntries);
    return out;
  }

  std::shared_ptr<rt::ThreadPool>          pool;
  AtomicStore                              store;
  Dispatcher                               disp;
  std::vector<tree::BuiltChain>            chains;
  std::vector<std::shared_ptr<CaptureSink>> sinks;
};

// ---------------------------------------------------------------------------
// Falsification. Applied to step 2 only; each must turn the verdict red.
// ---------------------------------------------------------------------------
enum class Falsify { None, Ulp, Drop, Swap, Store };

struct FalsifyState {
  Falsify     kind = Falsify::None;
  std::size_t index = 0;     // the record it targets
  std::size_t applied = 0;   // how many times it actually changed something
  std::string note;
};

// Serves one record's `price` one ULP up; everything else passes through.
class UlpPrice final : public engine::FieldSource {
public:
  UlpPrice(const engine::FieldSource& in, FalsifyState& st) : in_(in), st_(st) {}
  bool isObject() const noexcept override { return in_.isObject(); }
  std::size_t fieldCountHint() const noexcept override { return in_.fieldCountHint(); }
  bool has(const std::string& n) const override { return in_.has(n); }
  bool tryGetNumber(const std::string& n, double& out) const override {
    if (!in_.tryGetNumber(n, out)) return false;
    if (n == engine::recordfields::kPrice) bump(out);
    return true;
  }
  bool tryGetUint64(const std::string& n, std::uint64_t& out) const override { return in_.tryGetUint64(n, out); }
  void forEachNumber(engine::NumericFieldSink& sink) const override {
    struct Fwd final : engine::NumericFieldSink {
      engine::NumericFieldSink* inner; const UlpPrice* self;
      void field(const std::string& n, double v) override {
        if (n == engine::recordfields::kPrice) self->bump(v);
        inner->field(n, v);
      }
    } fwd;
    fwd.inner = &sink; fwd.self = this;
    in_.forEachNumber(fwd);
  }
private:
  void bump(double& v) const {
    const double was = v;
    v = std::nextafter(v, INFINITY);
    if (std::memcmp(&was, &v, sizeof v) != 0) {
      if (st_.applied++ == 0) {
        char b[160];
        std::snprintf(b, sizeof b, "record %zu price %.17g -> %.17g", st_.index, was, v);
        st_.note = b;
      }
    }
  }
  const engine::FieldSource& in_;
  FalsifyState&              st_;
};

// ---------------------------------------------------------------------------
// The two record types a data point can be made of. `Source` is the engine
// field view of one row; everything below is written once over `Rec`.
// ---------------------------------------------------------------------------
template <typename Rec> struct RowKind;
template <> struct RowKind<data::TickRecord> {
  using Source = engine::RecordFieldSource;
  static constexpr const char* kName   = "TickRecords";
  static constexpr const char* kSchema = "trades";
};
template <> struct RowKind<data::BookRecord> {
  using Source = engine::BookRecordFieldSource;
  static constexpr const char* kName   = "BookRecords";
  static constexpr const char* kSchema = "mbo";
};

// One row's fields, in emission order: what "distinct" and "carries a price"
// mean for the falsification targets, for either record type.
template <typename Rec>
std::vector<std::pair<std::string, double>> fieldsOf(const Rec& r) {
  struct Collect final : engine::NumericFieldSink {
    std::vector<std::pair<std::string, double>> out;
    void field(const std::string& n, double v) override { out.emplace_back(n, v); }
  } c;
  typename RowKind<Rec>::Source(r).forEachNumber(c);
  return std::move(c.out);
}

// ---------------------------------------------------------------------------
// Steps. THE PLUG POINT for steps 3-5: add an entry. `configure` sets whatever
// engine knob the step introduces on the Dispatcher's Config; `ingest` drives
// one record; `contract` says what it must reproduce. A step whose engine
// support does not exist at the pin is simply not listed (it cannot be
// "skipped" into a green table).
// ---------------------------------------------------------------------------
struct OutputContract {
  // Every terminal's delivery stream, in order, byte-for-byte. Steps 1-5 all.
  bool deliveries = true;
  // The AtomicStore's final contents, byte-for-byte. True for steps 1-4.
  // Step 5 (store-write elision) legitimately writes fewer keys; its contract
  // will be "every key a pull node reads is identical", which needs the
  // engine's notion of which keys those are.
  bool store = true;
};

template <typename Rec>
struct StepSpec {
  int         number;
  const char* id;
  const char* what;
  std::function<void(util::Config&)> configure;
  // Drives the whole sample through `rig`, in order. Takes the full span so a
  // step that batches (none yet) can.
  std::function<void(Rig&, std::span<const Rec>, const std::string& ticker,
                     FalsifyState*)> drive;
  OutputContract contract;
};

// Today's JSON path: the DOM a JSON feed would have produced for this record,
// built per record. Its members are exactly the record's FieldSource fields in
// its order — the "equivalent tick" that RecordFieldSource.hpp and
// BookRecordFieldSource.hpp define and RecordIngressParityTest pins.
template <typename Rec>
Event synthesizeEvent(const std::string& ticker, const Rec& rec) {
  auto doc = std::make_shared<rapidjson::Document>();
  doc->SetObject();
  struct Add final : engine::NumericFieldSink {
    rapidjson::Document* doc = nullptr;
    void field(const std::string& name, double value) override {
      auto& a = doc->GetAllocator();
      doc->AddMember(rapidjson::Value(name.c_str(), static_cast<rapidjson::SizeType>(name.size()), a),
                     rapidjson::Value(value), a);
    }
  } add;
  add.doc = doc.get();
  typename RowKind<Rec>::Source(rec).forEachNumber(add);
  return Event{ticker, std::move(doc)};
}

template <typename Rec>
const std::vector<StepSpec<Rec>>& steps() {
  using Source = typename RowKind<Rec>::Source;
  static const std::vector<StepSpec<Rec>> s = {
      {1, "json-ontick", "onTick + synthesized DOM (today's JSON path)",
       [](util::Config&) {},
       [](Rig& rig, std::span<const Rec> rows, const std::string& ticker, FalsifyState*) {
         for (const auto& r : rows) rig.disp.onTick(synthesizeEvent(ticker, r));
       },
       {}},
      {2, "onrecord", "onRecord, no DOM",
       [](util::Config&) {},
       [](Rig& rig, std::span<const Rec> rows, const std::string&, FalsifyState* f) {
         if (!f || f->kind == Falsify::None || f->kind == Falsify::Store) {
           for (const auto& r : rows) {
             const Source fields(r);
             rig.disp.onRecord(r.header.symbolId, fields);
           }
           return;
         }
         for (std::size_t i = 0; i < rows.size(); ++i) {
           std::size_t j = i;
           if (f->kind == Falsify::Drop && i == f->index) { ++f->applied; continue; }
           if (f->kind == Falsify::Swap && i == f->index)     { j = i + 1; ++f->applied; }
           if (f->kind == Falsify::Swap && i == f->index + 1) { j = i - 1; }
           const Source fields(rows[j]);
           if (f->kind == Falsify::Ulp && i == f->index) {
             const UlpPrice bumped(fields, *f);
             rig.disp.onRecord(rows[j].header.symbolId, bumped);
           } else {
             rig.disp.onRecord(rows[j].header.symbolId, fields);
           }
         }
       },
       {}},
      // 3 inline-dag  — needs krn: a Dispatcher/Listener mode with no
      //                 ThreadPool::post between nodes (see ENC-1487 report).
      // 4 interned    — needs krn: StreamValue carrying a SymbolId.
      // 5 store-elide — needs krn: the set of keys pull nodes read; contract.store=false.
  };
  return s;
}

// ---------------------------------------------------------------------------
// Comparison against the reference. Returns an empty string when identical.
// ---------------------------------------------------------------------------
std::string compare(const RunOutput& ref, const RunOutput& got, const OutputContract& c) {
  std::ostringstream os;
  if (c.deliveries) {
    for (std::size_t q = 0; q < ref.perRequest.size(); ++q) {
      const auto& a = ref.perRequest[q];
      const auto& b = got.perRequest[q];
      if (a.bytes == b.bytes) continue;
      os << "terminal '" << requests()[q].name << "': " << a.values.size() << " vs "
         << b.values.size() << " deliveries";
      const std::size_t n = std::min(a.values.size(), b.values.size());
      for (std::size_t i = 0; i < n; ++i) {
        const std::size_t ea = (i + 1 < a.offsets.size()) ? a.offsets[i + 1] : a.bytes.size();
        const std::size_t eb = (i + 1 < b.offsets.size()) ? b.offsets[i + 1] : b.bytes.size();
        if (a.bytes.compare(a.offsets[i], ea - a.offsets[i], b.bytes, b.offsets[i], eb - b.offsets[i]) != 0) {
          os << "; first difference at delivery " << i << ": " << render(a.values[i])
             << " vs " << render(b.values[i]);
          break;
        }
      }
      os << "\n";
    }
  }
  if (c.store && ref.storeBytes != got.storeBytes) {
    os << "AtomicStore contents differ (" << ref.storeEntries << " vs " << got.storeEntries
       << " entries, " << ref.storeBytes.size() << " vs " << got.storeBytes.size() << " bytes)\n";
  }
  return os.str();
}

// ---------------------------------------------------------------------------
// Input: the retained DBN file, through krn-data's adapter.
// ---------------------------------------------------------------------------
struct Sample {
  std::vector<data::TickRecord> trades;
  std::vector<data::BookRecord> book;
  std::string                   ticker;
  std::uint64_t                 bytes = 0;
  std::uint64_t                 fnv1a32 = 0;
};

// Every row must name one symbol; returns it, or 0 after printing why not.
template <typename Rec>
data::SymbolId singleSymbol(const std::vector<Rec>& rows) {
  const data::SymbolId sid = rows.front().header.symbolId;
  for (const auto& r : rows) {
    if (r.header.symbolId != sid) {
      std::cerr << "krn_b2_bench: sample spans more than one symbol; refused\n";
      return 0;
    }
  }
  return sid;
}

int loadSample(const std::string& path, data::SymbolTable& symbols, Sample& out) {
  std::ifstream f(path, std::ios::binary);
  if (!f) { std::cerr << "krn_b2_bench: cannot open " << path << "\n"; return kExitCouldNot; }
  std::vector<char> raw((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
  out.bytes = raw.size();
  std::uint32_t h = 2166136261u;
  for (char c : raw) { h ^= static_cast<unsigned char>(c); h *= 16777619u; }
  out.fnv1a32 = h;

  data::SymbolTable venues;
  data::DatabentoAdapter::Config cfg;   // defaults: venue from metadata, ticker from symbology (D7)
  cfg.sessionId = h != 0 ? h : 1;      // the ingest job's ruling: FNV-1a-32 of the file
  data::DatabentoAdapter ad(cfg, symbols, venues);
  auto st = ad.feed(std::span<const std::byte>(reinterpret_cast<const std::byte*>(raw.data()), raw.size()));
  if (st.ok()) st = ad.finish();
  if (!st.ok()) { std::cerr << "krn_b2_bench: decode refused: " << st.ToString() << "\n"; return kExitCouldNot; }
  out.trades = ad.takeTrades();
  out.book   = ad.takeBookEvents();
  if (out.trades.empty() == out.book.empty()) {
    std::cerr << "krn_b2_bench: " << path << " decoded to " << out.trades.size() << " TickRecords and "
              << out.book.size() << " BookRecords; a data point is one schema, so exactly one must"
                 " be non-empty\n";
    return kExitCouldNot;
  }
  const data::SymbolId sid = out.trades.empty() ? singleSymbol(out.book) : singleSymbol(out.trades);
  if (sid == 0) return kExitCouldNot;
  out.ticker = symbols.name(sid);
  if (out.ticker.empty()) { std::cerr << "krn_b2_bench: symbolId resolves to no ticker\n"; return kExitCouldNot; }
  return kExitIdentical;
}

std::string readFirstLine(const char* path, const char* key = nullptr) {
  std::ifstream f(path);
  std::string line;
  while (std::getline(f, line)) {
    if (!key) return line;
    if (line.rfind(key, 0) == 0) {
      const auto p = line.find(':');
      return p == std::string::npos ? line : line.substr(p + 2);
    }
  }
  return "?";
}

struct Args {
  std::string dbn;
  int         iterations = 50;
  int         warmup = 3;
  unsigned    threads = std::thread::hardware_concurrency();
  Falsify     falsify = Falsify::None;
};

int usage() {
  std::cerr <<
      "usage: krn_b2_bench --dbn <trades.dbn|mbo.dbn> [--iterations N] [--warmup W] [--threads T]\n"
      "                    [--falsify=ulp|drop|swap|store]\n"
      "  exit 0: every iteration of every step byte-identical to step 1\n"
      "  exit 1: a mismatch (a finding)\n"
      "  exit 2: could not run\n";
  return kExitCouldNot;
}

double percentile(std::vector<double> v, double p) {
  std::sort(v.begin(), v.end());
  const double idx = p * static_cast<double>(v.size() - 1);
  const auto lo = static_cast<std::size_t>(idx);
  const auto hi = std::min(lo + 1, v.size() - 1);
  return v[lo] + (v[hi] - v[lo]) * (idx - static_cast<double>(lo));
}

// One data point: steps 1-2 over `rows`, compared byte-for-byte to step 1.
template <typename Rec>
int measure(const Args& a, const Sample& sample, const std::vector<Rec>& all,
            const data::SymbolTable& symbols) {
  using Kind = RowKind<Rec>;
  const std::span<const Rec> rows(all);

  // Split by what the engine sees, for the header and for the targets below.
  std::size_t fieldless = 0;
  for (const auto& r : rows) if (fieldsOf(r).empty()) ++fieldless;

  // Falsification targets. A target must be a row whose mutation CHANGES what
  // the engine sees: on mbo a mutation row carries no field, so a ULP bump or a
  // drop of one is a no-op and a swap of one with a print reorders nothing a
  // terminal records. So each target is searched for from the middle onward.
  const auto hasPrice = [&](std::size_t i) {
    for (const auto& [n, v] : fieldsOf(rows[i])) if (n == engine::recordfields::kPrice) return true;
    return false;
  };
  FalsifyState fstate;
  fstate.kind = a.falsify;
  fstate.index = rows.size() / 2;
  if (a.falsify == Falsify::Ulp || a.falsify == Falsify::Drop) {
    while (fstate.index < rows.size() && !hasPrice(fstate.index)) ++fstate.index;
    if (fstate.index >= rows.size()) { std::cerr << "no row carrying a price to falsify\n"; return kExitCouldNot; }
  }
  if (a.falsify == Falsify::Swap) {
    // A swap of two rows the engine sees identically is a no-op mutation; find
    // an adjacent pair that both carry fields and differ.
    while (fstate.index + 1 < rows.size() &&
           (fieldsOf(rows[fstate.index]).empty() ||
            fieldsOf(rows[fstate.index]) == fieldsOf(rows[fstate.index + 1]) ||
            fieldsOf(rows[fstate.index + 1]).empty())) ++fstate.index;
    if (fstate.index + 1 >= rows.size()) { std::cerr << "no distinct adjacent pair to swap\n"; return kExitCouldNot; }
    fstate.note = "records " + std::to_string(fstate.index) + " and " + std::to_string(fstate.index + 1) + " swapped";
  }
  if (a.falsify == Falsify::Drop) fstate.note = "record " + std::to_string(fstate.index) + " dropped";

  std::printf("krn_b2_bench (ENC-1487, ENC-1628) — B2 steps over the real sample, %s data point\n",
              Kind::kSchema);
  std::printf("  sample        : %s\n", a.dbn.c_str());
  std::printf("  sample bytes  : %llu, FNV-1a-32 %08llx\n",
              static_cast<unsigned long long>(sample.bytes), static_cast<unsigned long long>(sample.fnv1a32));
  std::printf("  rows          : %zu %s (ticker %s, from DBN symbology): %zu carry fields, %zu fieldless\n",
              rows.size(), Kind::kName, sample.ticker.c_str(), rows.size() - fieldless, fieldless);
  std::printf("  graph         : %zu requests, non-windowed (D9):", requests().size());
  for (const auto& r : requests()) std::printf(" %s", r.name);
  std::printf("\n");
  cpu_set_t set;
  CPU_ZERO(&set);
  const int affinity = sched_getaffinity(0, sizeof set, &set) == 0 ? CPU_COUNT(&set) : -1;
  std::printf("  confound      : %u hardware threads, %d in this process's affinity mask, pool threads %u\n",
              std::thread::hardware_concurrency(), affinity, a.threads);
  std::printf("  cpu           : %s\n", readFirstLine("/proc/cpuinfo", "model name").c_str());
  std::printf("  loadavg       : %s\n", readFirstLine("/proc/loadavg").c_str());
  std::printf("  cache         : WARM. Rows decoded into memory once before timing; no I/O in the timed\n"
              "                  region; each iteration replays the same %zu real rows into a FRESH engine.\n",
              rows.size());
  std::printf("  iterations    : %d measured per step + %d warm-up discarded, steps interleaved\n",
              a.iterations, a.warmup);
  std::printf("  timed region  : first ingress call -> ThreadPool::drain() returns (graph quiesced)\n");
  if (a.falsify != Falsify::None)
    std::printf("  FALSIFY       : %s (step 2 only)\n",
                a.falsify == Falsify::Ulp ? "ulp" : a.falsify == Falsify::Drop ? "drop" :
                a.falsify == Falsify::Swap ? "swap" : "store");

  const auto& ss = steps<Rec>();
  std::vector<std::vector<double>> seconds(ss.size());
  RunOutput reference;
  bool haveReference = false;
  std::size_t comparisons = 0;
  std::vector<std::string> mismatches;

  try {
    for (int it = -a.warmup; it < a.iterations; ++it) {
      for (std::size_t si = 0; si < ss.size(); ++si) {
        const auto& step = ss[si];
        util::Config cfg;
        step.configure(cfg);
        Rig rig(a.threads, cfg, sample.ticker, symbols);
        FalsifyState* f = (step.number == 2 && a.falsify != Falsify::None) ? &fstate : nullptr;

        const auto t0 = std::chrono::steady_clock::now();
        step.drive(rig, rows, sample.ticker, f);
        rig.quiesce();
        const auto t1 = std::chrono::steady_clock::now();

        if (f && f->kind == Falsify::Store) {
          rig.store.set(sample.ticker, "__falsify__", ArgType{1.0});
          if (f->applied++ == 0) f->note = "one extra AtomicStore key written after quiesce";
        }
        RunOutput out = rig.collect();
        if (it < 0) continue;
        seconds[si].push_back(std::chrono::duration<double>(t1 - t0).count());

        if (!haveReference) {
          // Refuse a vacuous reference: an empty terminal or store would make
          // every later comparison trivially equal.
          for (std::size_t q = 0; q < out.perRequest.size(); ++q) {
            if (out.perRequest[q].values.empty()) {
              std::fprintf(stderr, "krn_b2_bench: terminal '%s' received nothing; the comparison would be vacuous\n",
                           requests()[q].name);
              return kExitCouldNot;
            }
          }
          if (out.storeEntries == 0) {
            std::fprintf(stderr, "krn_b2_bench: AtomicStore empty after the run; refused\n");
            return kExitCouldNot;
          }
          reference = std::move(out);
          haveReference = true;
          continue;
        }
        ++comparisons;
        const std::string diff = compare(reference, out, step.contract);
        if (!diff.empty() && mismatches.size() < 8) {
          mismatches.push_back("step " + std::to_string(step.number) + " iteration " +
                               std::to_string(it) + ":\n" + diff);
        } else if (!diff.empty()) {
          mismatches.push_back("");
        }
      }
    }
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "krn_b2_bench: %s\n", ex.what());
    return kExitCouldNot;
  }

  std::printf("\n  reference     : step 1, first measured iteration —");
  std::size_t deliveries = 0;
  for (std::size_t q = 0; q < reference.perRequest.size(); ++q) {
    std::printf(" %s=%zu", requests()[q].name, reference.perRequest[q].values.size());
    deliveries += reference.perRequest[q].values.size();
  }
  std::printf(" deliveries (%zu total), %zu store entries\n\n", deliveries, reference.storeEntries);

  std::printf("| step | path | rows/iter | iters | median rows/s | p10 rows/s | p90 rows/s | median us/row | vs step 1 |\n");
  std::printf("|---|---|---:|---:|---:|---:|---:|---:|---:|\n");
  double baseMedian = 0.0;
  for (std::size_t si = 0; si < ss.size(); ++si) {
    std::vector<double> rps;
    for (double t : seconds[si]) rps.push_back(static_cast<double>(rows.size()) / t);
    const double med = percentile(rps, 0.5);
    if (si == 0) baseMedian = med;
    std::printf("| %d | %s — %s | %zu | %zu | %.0f | %.0f | %.0f | %.3f | %.2fx |\n",
                ss[si].number, ss[si].id, ss[si].what, rows.size(), rps.size(), med,
                percentile(rps, 0.1), percentile(rps, 0.9), 1e6 / med, med / baseMedian);
  }
  std::printf("\n(p10 is the slow tail: 10%% of iterations ran at or below it.)\n");

  if (a.falsify != Falsify::None) {
    std::printf("\nfalsify: %s — %s\n", fstate.applied > 0 ? "applied" : "NOT APPLIED",
                fstate.note.c_str());
    if (fstate.applied == 0) return kExitCouldNot;
  }

  std::printf("\nbit-identical: %zu comparisons against the reference (deliveries + store)\n", comparisons);
  if (!mismatches.empty()) {
    std::printf("MISMATCH in %zu of %zu comparisons:\n", mismatches.size(), comparisons);
    for (const auto& m : mismatches) if (!m.empty()) std::printf("  %s", m.c_str());
    return kExitMismatch;
  }
  std::printf("IDENTICAL: every iteration of every step reproduced step 1 byte-for-byte\n");
  return kExitIdentical;
}

}  // namespace

int main(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    const std::string s = argv[i];
    auto val = [&](const char* flag) -> const char* {
      const std::size_t n = std::strlen(flag);
      if (s.rfind(flag, 0) == 0 && s.size() > n && s[n] == '=') return argv[i] + n + 1;
      if (s == flag && i + 1 < argc) return argv[++i];
      return nullptr;
    };
    if (const char* v = val("--dbn")) a.dbn = v;
    else if (const char* v2 = val("--iterations")) a.iterations = std::atoi(v2);
    else if (const char* v3 = val("--warmup")) a.warmup = std::atoi(v3);
    else if (const char* v4 = val("--threads")) a.threads = static_cast<unsigned>(std::atoi(v4));
    else if (const char* v5 = val("--falsify")) {
      const std::string k = v5;
      if (k == "ulp") a.falsify = Falsify::Ulp;
      else if (k == "drop") a.falsify = Falsify::Drop;
      else if (k == "swap") a.falsify = Falsify::Swap;
      else if (k == "store") a.falsify = Falsify::Store;
      else return usage();
    } else return usage();
  }
  if (a.dbn.empty() || a.iterations < 1 || a.warmup < 0 || a.threads < 1) return usage();

  // The engine bootstrap Host::run performs (src/host/Host.cpp step 3).
  krn::registerBuiltinFunctions();
  krn::registerBuiltinNodeTypes();

  data::SymbolTable symbols;
  Sample sample;
  if (int rc = loadSample(a.dbn, symbols, sample); rc != kExitIdentical) return rc;
  return sample.trades.empty() ? measure(a, sample, sample.book, symbols)
                               : measure(a, sample, sample.trades, symbols);
}
