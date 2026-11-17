//
// IngestJob — see IngestJob.hpp for the path and what is deliberately not here.
//

#include "krn/data/IngestJob.hpp"

#include "krn/data/DatabentoAdapter.hpp"
#include "krn/data/Gaps.hpp"
#include "krn/engine/SymbolTable.hpp"

#include <arrow/util/compression.h>
#include <rapidjson/document.h>

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <set>
#include <span>
#include <sstream>
#include <vector>

namespace krn::data::ingest {

// ===========================================================================
// THE TWO RULINGS THIS TICKET OWED. Text, so that the tool's output, the Linear
// comment and SPEC §5 cannot drift into three different answers.
// ===========================================================================

const char* const kSessionIdRuling =
    "A RE-INGEST REUSES ITS session_id, AND session_id IS THE IDENTITY OF THE "
    "RETAINED DBN FILE RATHER THAN A PER-RUN COUNTER.\n"
    "  session_id = FNV-1a-32 over the whole retained .dbn, forced non-zero. "
    "The same bytes give the same number on every machine, every rebuild and "
    "every reload; a CORRECTED pull is different bytes and therefore a "
    "different session, which is the one case where the distinction is real.\n"
    "  Decided this way because the alternatives break locked decisions. D6 "
    "makes L1 'a derived, rebuildable projection of the raw DBN files'; a "
    "projection whose output changes when rebuilt from unchanged input has a "
    "hidden input, and a per-run counter is exactly that. D5's re-ingest ('DROP "
    "PARTITION + reload reproduces identical rows') and ENC-1487's "
    "bit-identical assertions both fail on the session_id column alone if a "
    "reload renumbers it. And DatabentoAdapter's own note on provider_seq -- "
    "'FROM DATABENTO, NEVER SYNTHESIZED ... stable across a re-download, which "
    "is what makes re-ingest converge and the splice's dedup exact' -- is about "
    "the PAIR (session_id, provider_seq) (docs 8.3); a fresh session_id each run "
    "makes the stable half useless.\n"
    "  WHY THE FILE AND NOT THE VENDOR job_id, WHICH WAS THE FIRST ANSWER: the "
    "job_id is in metadata.json and is NOT IN DBN AT ALL. Deriving from it "
    "makes `krn_ingest reload` -- the pure D6 reload, which reads only the "
    "retained file -- produce a DIFFERENT session_id from the run that created "
    "it, so the reload's rows would differ from the originals in exactly the "
    "column that is supposed to prove they do not. Measured on this sample "
    "before the change. The job_id stays on every ingest_windows and symbology "
    "row as `source_ref`, so nothing traceable is lost; it is provenance, not "
    "identity.\n"
    "  AND A CORRECTION TO docs/data-layer.md 8.8 THAT NO ANSWER HERE RESCUES: "
    "8.8 calls (symbol, kind, from_ts, session_id) 'unique by construction'. "
    "THAT IS FALSE UNDER EVERY ANSWER, not only under reuse. ENC-1480 made the "
    "table an append-only transition log, so one window and one session write at "
    "least two rows (state=0 then state=1) and a failed run writes three -- "
    "observed on this ingest as exactly 4 rows for 2 windows. The claim dates "
    "from revision 2's one-row-per-window ReplacingMergeTree and was voided by "
    "the rewrite that replaced it. What makes the table readable is the READ "
    "RULE, never uniqueness.";

const char* const kGapsNote =
    "ZERO krn.gaps ROWS WRITTEN, AND NOT FOR LACK OF SCOPE: THE SEQ-"
    "DISCONTINUITY DETECTOR CANNOT RUN ON A SYMBOL-FILTERED PULL.\n"
    "  docs 8.8 detects a provider hole as 'a seq discontinuity'. Databento's "
    "`sequence` is the VENUE's, not the symbol's: in this sample consecutive "
    "MSFT records step 11404021 -> 11404028 -> 11423666, because every other "
    "Nasdaq instrument's messages occupy the numbers in between. provider_seq is "
    "MONOTONIC within (session_id, symbol) exactly as 8.3 says -- measured "
    "sorted, 1000/1000, both kinds -- but it is not CONTIGUOUS, and gap "
    "detection needs contiguity. A gap row per jump would have produced hundreds "
    "of phantom holes per kind, each of which a research run joining `gaps` "
    "reads as 'there was no data here': the same wrong-answer class the table "
    "exists to prevent, manufactured by the detector.\n"
    "  ENC-1506 MADE THE REFUSAL CODE (gaps::infer, Gaps.hpp): no seq-gap row is "
    "ever inferred -- from a symbol-filtered pull because a jump is proven not "
    "to be a hole, from an all-symbols pull because contiguity is unmeasured. "
    "Each kind above reports the phantom rows refused. The one vendor-declared "
    "signal in DBN, F_MAYBE_BAD_BOOK, carries no bounds, so it becomes no row "
    "either: the job refuses to record such a window complete. A real detector "
    "needs an unfiltered pull measured contiguous, a design decision rather "
    "than code.";

const char* const kSnapshotRuling =
    "THE VENDOR'S START-OF-DAY MBO SNAPSHOT IS WRITTEN TO krn.book_snapshots, "
    "NOT TO krn.book_events (ENC-1640, ENC-1663).\n"
    "  Every delivered XNAS.ITCH mbo day file opens with a Clear and one Add per "
    "order resting at midnight, all flagged F_SNAPSHOT (and F_BAD_TS_RECV: "
    "ts_recv is the day's midnight). Their ts_event is each order's ORIGINAL "
    "time, as early as the previous day's 08:00 UTC: 4116 records on NVDA "
    "2026-10-27. Written as events they would land in the PREVIOUS day's "
    "partition, so the load would DROP that partition (another window's rows), "
    "and a replay of that day ordered by ts_event would meet a Clear and a "
    "duplicate Add of every overnight order in the middle of it.\n"
    "  They are state, not events. Each is one krn.book_snapshots row whose "
    "ts_event is the instant the snapshot is AS OF (the records' ts_recv, the "
    "window's start) and whose order_ts is the order's own ts_event, so the rows "
    "sit in the partition of the day they seed and a re-ingest of that day "
    "replaces them and nothing else. The snapshot is recorded complete as its "
    "own `book_snapshots` window, written state=1 after its rows and BEFORE the "
    "day's `book_events` window, so a book_events day reads complete only when "
    "its seed is in L1 too (D11). A delivered mbo day is refused if a Cancel "
    "or Modify names an order that neither its snapshot nor its own Adds "
    "introduced (a day with no snapshot, such as the NVDA job's Mondays, "
    "passes only so), and so is a snapshot that is not one opening block (a "
    "Clear, then Adds, one as-of instant, the Clear's provider_seq below every "
    "Add's). Every decoded "
    "record is accounted for as a row, a snapshot row, an mbo action N or a "
    "skipped rtype, or the day is refused.";

namespace {

// ---------------------------------------------------------------------------
// session_id derivation — see kSessionIdRuling.
// ---------------------------------------------------------------------------
std::uint32_t deriveSessionId(const std::vector<std::byte>& dbnBytes) {
  // FNV-1a 32 over the retained file. Chosen for being a fixed, written-down
  // function of its input rather than for its distribution: the property that
  // matters is that the same bytes yield the same number on every machine,
  // every rebuild and every reload. The file already contains the dataset, the
  // schema, the requested window, the limit and the symbology in its metadata
  // block, so hashing the file subsumes hashing those.
  std::uint32_t h = 0x811C9DC5u;
  for (const std::byte b : dbnBytes) {
    h ^= static_cast<std::uint8_t>(b);
    h *= 0x01000193u;
  }
  // 0 is reserved: a zero session_id on an event row is indistinguishable from
  // a default-constructed header.
  return h == 0 ? 1u : h;
}

// CODE-REVIEW FINDING #8. Every other line in this file is scrupulous about
// returning a `Status`; four used `std::stoll`/`std::stoul`, which THROW. The
// inputs are ClickHouse response fields, where `\N`, an empty field or a
// truncated body are all reachable — and an exception escaping an
// `arrow::Status`-returning function skips every refusal path this job has,
// including the `state=2` write that records a failed window.
template <typename T>
bool parseExact(const std::string& text, T& out) {
  if (text.empty()) return false;
  const char* first = text.data();
  const char* last  = text.data() + text.size();
  const auto r = std::from_chars(first, last, out);
  return r.ec == std::errc{} && r.ptr == last;
}

arrow::Status badField(const char* what, const std::string& text) {
  return arrow::Status::Invalid(
      "ClickHouse returned \"", text, "\" for ", what,
      ", which is not an integer. This is a COULD-NOT-RUN rather than a "
      "finding: no verdict about the data was reached.");
}

std::string expandTilde(const std::string& p) {
  if (p.empty() || p[0] != '~') return p;
  const char* home = std::getenv("HOME");
  if (home == nullptr) return p;
  return std::string(home) + p.substr(1);
}

bool fileExists(const std::string& p) {
  std::FILE* f = std::fopen(p.c_str(), "rb");
  if (f == nullptr) return false;
  std::fclose(f);
  return true;
}

// ---------------------------------------------------------------------------
// Decode a DBN buffer through the adapter, in `chunk`-byte feeds.
//
// The chunk size is a parameter because it is one of the round-trip's two axes:
// the adapter buffers across `feed()` calls, so a chunk that straddles the
// metadata block, the symbology section and records of both 56 and 48 bytes
// drives the framing differently from one whole-file feed. Two chunkings
// agreeing is a real property; one chunking repeated is not.
// ---------------------------------------------------------------------------
struct Decoded {
  std::vector<TickRecord> ticks;
  std::vector<BookRecord> book;
  DatabentoAdapter::Stats stats;
  dbn::Metadata meta;
  std::string symbol;
  std::string venue;
  std::uint32_t instrumentId = 0;
};

arrow::Result<Decoded> decodeDbn(const std::vector<std::byte>& bytes,
                                 std::uint32_t sessionId, std::size_t chunk) {
  SymbolTable symbols;
  SymbolTable venues;
  DatabentoAdapter::Config cfg;
  cfg.sessionId = sessionId;
  // `venue` empty => the adapter interns the DBN metadata's `dataset`
  // ("XNAS.ITCH"), which is what makes the column checkable by hand in
  // ClickHouse. `symbolOverride` empty => the ticker is RESOLVED from the
  // metadata's symbology (SPEC D7) rather than asserted by us, which is why
  // the symbology section is written into the DBN at all.
  //
  // `fillReducesRestingOrder` IS LEFT AT ITS DEFAULT, AND THE DEFAULT IS NOW
  // MEASURED rather than assumed — see the comment on it in
  // DatabentoAdapter.hpp. 91 of 91 `F` records in this sample are accompanied
  // by a `C` for the SAME order_id at the same (ts_event, sequence), so the
  // venue emits the resting reduction itself and applying the fill as a second
  // reduction would double-reduce on 100% of fills.
  DatabentoAdapter adapter(cfg, symbols, venues);

  if (chunk == 0) return arrow::Status::Invalid("chunk size 0");
  for (std::size_t off = 0; off < bytes.size(); off += chunk) {
    const std::size_t n = std::min(chunk, bytes.size() - off);
    ARROW_RETURN_NOT_OK(
        adapter.feed(std::span<const std::byte>(bytes.data() + off, n)));
  }
  ARROW_RETURN_NOT_OK(adapter.finish());

  Decoded d;
  d.ticks = adapter.takeTrades();
  d.book  = adapter.takeBookEvents();
  d.stats = adapter.stats();
  if (!adapter.metadata().has_value()) {
    return arrow::Status::Invalid("the adapter produced no DBN metadata");
  }
  d.meta = *adapter.metadata();

  std::set<std::uint32_t> symbolIds;
  std::set<std::uint32_t> instrumentIds;
  std::uint32_t venueId = 0;
  const auto noteHeader = [&](const RecordHeader& h) {
    symbolIds.insert(h.symbolId);
    instrumentIds.insert(h.instrumentId);
    venueId = h.venueId;
  };
  for (const auto& t : d.ticks) noteHeader(t.header);
  for (const auto& b : d.book) noteHeader(b.header);
  if (symbolIds.size() != 1 || instrumentIds.size() != 1) {
    return asFinding(arrow::Status::Invalid(
        "decode yielded ", symbolIds.size(), " symbol(s) and ",
        instrumentIds.size(),
        " instrument_id(s). This slice is one symbol (SPEC 6) and the window is "
        "recorded PER SYMBOL in krn.ingest_windows, so a multi-symbol file is "
        "refused rather than recorded under one of them."));
  }
  d.symbol       = symbols.name(*symbolIds.begin());
  d.venue        = venues.name(venueId);
  d.instrumentId = *instrumentIds.begin();
  return d;
}

// ---------------------------------------------------------------------------
// Row rendering. ONE renderer, used twice: for the round-trip comparison and
// for the insert body. That is deliberate — comparing the RENDERED rows is
// what makes the round-trip a statement about what reaches L1, and a second
// renderer written for the comparison could agree with itself while disagreeing
// with the insert.
//
// Columns are NAMED in the INSERT so a DDL column reorder is a server-side
// error rather than a silently transposed table.
// ---------------------------------------------------------------------------
constexpr const char* kTradesColumns =
    "ts_event, ts_recv, provider_seq, session_id, symbol, instrument_id, venue, "
    "price, size, aggressor, book_neutral, trade_id, flags";

constexpr const char* kBookEventsColumns =
    "ts_event, ts_recv, provider_seq, session_id, symbol, instrument_id, venue, "
    "action, side, aggressor, price, size, order_id, priority, depth, epoch, flags";

constexpr const char* kBookSnapshotsColumns =
    "ts_event, ts_recv, provider_seq, session_id, symbol, instrument_id, venue, "
    "order_ts, action, side, price, size, order_id, priority, epoch, flags";

// A field this renderer omits is a field no round-trip assertion covers and no
// L1 column holds, so the two facts stay tied together. TickRecord's quote half
// (bidPx/askPx/bidSz/askSz/bidCt/askCt) is omitted because `krn.quotes` is not
// in this slice (SPEC Non-goals) and nothing populates it.
std::string renderTradeRow(const TickRecord& t, const std::string& symbol,
                           const std::string& venue) {
  std::ostringstream os;
  os << t.header.tsEventNs << '\t' << t.header.tsRecvNs << '\t'
     << t.header.providerSeq << '\t' << t.header.sessionId << '\t'
     << ch::tsvEscape(symbol) << '\t' << t.header.instrumentId << '\t'
     << ch::tsvEscape(venue) << '\t' << t.price << '\t' << t.size << '\t'
     << static_cast<unsigned>(t.aggressor) << '\t' << (t.bookNeutral ? 1 : 0)
     << '\t' << t.tradeId << '\t' << t.flags << '\n';
  return os.str();
}

std::string renderBookRow(const BookRecord& b, const std::string& symbol,
                          const std::string& venue) {
  std::ostringstream os;
  os << b.header.tsEventNs << '\t' << b.header.tsRecvNs << '\t'
     << b.header.providerSeq << '\t' << b.header.sessionId << '\t'
     << ch::tsvEscape(symbol) << '\t' << b.header.instrumentId << '\t'
     << ch::tsvEscape(venue) << '\t' << static_cast<unsigned>(b.action) << '\t'
     << static_cast<unsigned>(b.side) << '\t'
     << static_cast<unsigned>(b.aggressor) << '\t' << b.price << '\t' << b.size << '\t'
     << b.orderId << '\t' << b.priority << '\t' << b.depth << '\t' << b.epoch
     << '\t' << b.flags << '\n';
  return os.str();
}

// One vendor F_SNAPSHOT record as a krn.book_snapshots row. `asOfNs` is the
// instant the snapshot is as of and goes in ts_event, the partition key; the
// record's own ts_event goes in order_ts (kSnapshotRuling). aggressor and
// depth have no column: a snapshot record is a Clear or an Add, which carry
// Aggressor::Unknown and depth 0 by construction.
std::string renderSnapshotRow(const BookRecord& b, std::int64_t asOfNs,
                              const std::string& symbol, const std::string& venue) {
  std::ostringstream os;
  os << asOfNs << '\t' << b.header.tsRecvNs << '\t' << b.header.providerSeq << '\t'
     << b.header.sessionId << '\t' << ch::tsvEscape(symbol) << '\t'
     << b.header.instrumentId << '\t' << ch::tsvEscape(venue) << '\t'
     << b.header.tsEventNs << '\t' << static_cast<unsigned>(b.action) << '\t'
     << static_cast<unsigned>(b.side) << '\t' << b.price << '\t' << b.size << '\t'
     << b.orderId << '\t' << b.priority << '\t' << b.epoch << '\t' << b.flags
     << '\n';
  return os.str();
}

// ---------------------------------------------------------------------------
// ingest_windows writes. updated_at IS STAMPED ON EVERY ROW, including the
// state=0 one. The DDL's own measured note: with no value the column defaults
// to epoch 0, every row ties, "the single newest row" becomes arbitrary and a
// stale state=1 can win — the identical zero-valued-clock defect the table was
// rewritten to eliminate. `DEFAULT now64(3)` in the DDL is the backstop;
// writing it is the contract.
//
// `now64(3)` is a FUNCTION, and TabSeparated would store the literal text, so
// these rows go in through a SELECT. The clock is the server's either way.
// ---------------------------------------------------------------------------
arrow::Status writeWindowRow(const ch::Client& client, const std::string& symbol,
                             const std::string& kind, std::int64_t fromNs,
                             std::int64_t toNs, std::uint8_t state,
                             std::uint64_t rowsInserted, std::uint32_t sessionId,
                             const std::string& sourceRef) {
  std::ostringstream q;
  q << "INSERT INTO krn.ingest_windows (symbol, kind, from_ts, to_ts, state, "
       "rows_inserted, updated_at, completed_at, session_id, source, source_ref) "
       "SELECT "
    << ch::sqlQuote(symbol) << ", " << ch::sqlQuote(kind)
    << ", fromUnixTimestamp64Nano(toInt64(" << fromNs
    << "), 'UTC'), fromUnixTimestamp64Nano(toInt64(" << toNs << "), 'UTC'), "
    << static_cast<unsigned>(state) << ", " << rowsInserted << ", now64(3), "
    // completed_at: a COMPLETION clock. Zero unless this row IS the completion
    // — the DDL is explicit that it is zero while in flight and that ordering
    // by it is what made the revision-2 engine delete the in-flight row.
    << (state == 1 ? "now64(3)" : "fromUnixTimestamp64Milli(toInt64(0), 'UTC')")
    << ", " << sessionId << ", 'databento', " << ch::sqlQuote(sourceRef);
  return client.exec(q.str()).status();
}

std::string isoFromNs(std::int64_t ns) {
  const auto day = csv::yyyymmddUtc(static_cast<std::uint64_t>(ns));
  const auto midnight = static_cast<std::int64_t>(csv::midnightUtcNs(day));
  std::int64_t rem = ns - midnight;
  const int h = static_cast<int>(rem / 3600000000000LL);
  rem %= 3600000000000LL;
  const int m = static_cast<int>(rem / 60000000000LL);
  rem %= 60000000000LL;
  const int s = static_cast<int>(rem / 1000000000LL);
  const long long frac = rem % 1000000000LL;
  char buf[64];
  std::snprintf(buf, sizeof buf, "%04u-%02u-%02uT%02d:%02d:%02d.%09lldZ",
                day / 10000, (day / 100) % 100, day % 100, h, m, s, frac);
  return buf;
}

// The partition days a window and its rows touch.
std::set<std::uint32_t> daysOf(std::int64_t fromNs, std::int64_t toNs) {
  std::set<std::uint32_t> days;
  std::uint32_t d = csv::yyyymmddUtc(static_cast<std::uint64_t>(fromNs));
  for (;;) {
    days.insert(d);
    const auto next = csv::midnightUtcNs(d) + 86400000000000ULL;
    if (static_cast<std::int64_t>(next) >= toNs) break;
    d = csv::yyyymmddUtc(next);
  }
  return days;
}

struct Staged {
  KindReport rep;
  Decoded decoded;
  // The vendor's F_SNAPSHOT block, in file order (Clear first), out of
  // `decoded.book`. Empty on a pull with no snapshot (the MSFT CSV sample).
  std::vector<BookRecord> snapshot;
  std::vector<csv::Transcoded::Mapping> mappings;
  std::string table;
  std::string dataset;
  std::uint32_t sessionId = 0;
};

}  // namespace

// ===========================================================================
// retainOnlyIfDecodable — see IngestJob.hpp for what the old order destroyed.
// ===========================================================================

arrow::Result<csv::RetentionResult> retainOnlyIfDecodable(
    const std::string& path, const std::vector<std::byte>& bytes) {
  // The decode is done for its STATUS, not its records: the question is only
  // whether the adapter accepts these bytes end to end, including the metadata
  // block, the symbology section and `finish()`'s refusal of a stream that ends
  // mid-record. `sessionId` is irrelevant to decodability, so a fixed 1 is
  // passed rather than threading the real one in and implying it matters.
  const arrow::Status decodes = decodeDbn(bytes, 1u, bytes.size()).status();
  if (!decodes.ok()) {
    return asFinding(arrow::Status::Invalid(
        "REFUSING to write ", path,
        ": the bytes do not decode through DatabentoAdapter, and that file is "
        "the SPEC D6 baseline L1 is rebuilt from — overwriting it with "
        "undecodable bytes is what makes a later `reload` fail permanently. "
        "Nothing was written; any existing file is untouched.\n  the decoder "
        "said: ",
        decodes.message()));
  }
  return csv::retain(path, bytes);
}

std::string formatNs(std::int64_t ns) { return isoFromNs(ns); }

std::string formatWindow(std::int64_t fromNs, std::int64_t toNs) {
  return "[" + isoFromNs(fromNs) + ", " + isoFromNs(toNs) + ")";
}

// ===========================================================================
// The vendor start-of-day snapshot's gates (ENC-1663, kSnapshotRuling)
// ===========================================================================

arrow::Status splitSnapshot(std::vector<BookRecord>& book,
                            std::vector<BookRecord>& snapshot, std::int64_t& asOfNs,
                            const std::string& where) {
  // One opening block, a Clear and then Adds, one as-of instant, and the
  // Clear's provider_seq below every Add's so the table's sort key replays it
  // in the order the file gave it. A seed that is not exactly the vendor's book
  // at one instant is a torn book.
  snapshot.clear();
  asOfNs = 0;
  const auto isSnap = [](const BookRecord& b) {
    return (b.flags & dbn::kFlagSnapshot) != 0;
  };
  const auto firstEvent = std::find_if_not(book.begin(), book.end(), isSnap);
  const auto stray = std::count_if(firstEvent, book.end(), isSnap);
  const auto refuseShape = [&](auto&&... why) {
    return asFinding(arrow::Status::Invalid(
        where, ": the F_SNAPSHOT records are not one start-of-day snapshot: ",
        why..., " Nothing was written (kSnapshotRuling)."));
  };
  if (stray != 0) {
    return refuseShape(stray, " F_SNAPSHOT record(s) follow the first record "
                       "that is not one; a snapshot is the file's opening block.");
  }
  if (firstEvent == book.begin()) return arrow::Status::OK();
  const BookRecord& clear = book.front();
  if (clear.action != BookAction::Clear) {
    return refuseShape("the first is action ", static_cast<unsigned>(clear.action),
                       ", not a Clear.");
  }
  for (auto it = book.begin() + 1; it != firstEvent; ++it) {
    const std::size_t i = static_cast<std::size_t>(it - book.begin());
    if (it->action != BookAction::Add) {
      return refuseShape("record ", i, " is action ", static_cast<unsigned>(it->action),
                         ", not an Add.");
    }
    if (it->header.providerSeq <= clear.header.providerSeq) {
      return refuseShape("Add ", i, " has provider_seq ", it->header.providerSeq,
                         ", not above the Clear's ", clear.header.providerSeq,
                         "; ordered by the table's sort key it would replay "
                         "before the Clear.");
    }
  }
  for (auto it = book.begin(); it != firstEvent; ++it) {
    if (it->header.tsRecvNs != clear.header.tsRecvNs) {
      return refuseShape("ts_recv ", it->header.tsRecvNs, " and ",
                         clear.header.tsRecvNs, " are two as-of instants.");
    }
  }
  asOfNs = clear.header.tsRecvNs;
  snapshot.assign(book.begin(), firstEvent);
  book.erase(book.begin(), firstEvent);
  return arrow::Status::OK();
}

arrow::Status checkSnapshotAsOf(std::int64_t asOfNs, std::int64_t windowFromNs,
                                std::int64_t windowToNs, const std::string& where) {
  // The snapshot seeds THIS window from its first instant. As of any later
  // instant, the events between the window start and it would be replayed
  // onto a book that already contains them; earlier, it is another window's.
  if (asOfNs != windowFromNs) {
    return asFinding(arrow::Status::Invalid(
        where, ": the F_SNAPSHOT block is as of ", formatNs(asOfNs),
        ", not the start of the recorded window ",
        formatWindow(windowFromNs, windowToNs),
        ". Nothing was written (kSnapshotRuling)."));
  }
  return arrow::Status::OK();
}

arrow::Status checkDeliveredBookDay(const std::vector<BookRecord>& events,
                                    std::size_t snapshotRecords,
                                    std::uint64_t unknownOrders,
                                    std::uint64_t evictedOrders,
                                    const std::string& where) {
  // D11 FOR A DELIVERED DAY: recorded complete only if its book rebuilds from
  // L1 alone.
  //
  // A day with no snapshot must open with an unflagged Clear: that is what
  // says its book starts empty (the NVDA job's Mondays: record 0 is `R`,
  // sequence 0). Without one, nothing in the file says what rests at the start.
  if (snapshotRecords == 0 &&
      (events.empty() || events.front().action != BookAction::Clear ||
       (events.front().flags & dbn::kFlagSnapshot) != 0)) {
    return asFinding(arrow::Status::Invalid(
        where, ": no F_SNAPSHOT block, and the first book record is ",
        events.empty() ? std::string("absent")
                       : "action " + std::to_string(static_cast<unsigned>(
                                         events.front().action)),
        ", not a Clear, so nothing says what book the day's events apply to. "
        "Nothing was written (D11, kSnapshotRuling)."));
  }
  // The adapter's resting-order map is seeded by exactly the snapshot this job
  // writes, so a Cancel or Modify it could not resolve is an order L1 cannot
  // resolve either; an evicted order makes that count unreliable.
  if (unknownOrders != 0 || evictedOrders != 0) {
    return asFinding(arrow::Status::Invalid(
        where, ": ", unknownOrders, " Cancel/Modify record(s) name an order_id "
        "that neither the day's F_SNAPSHOT block (", snapshotRecords,
        " record(s)) nor an earlier Add introduced, and ", evictedOrders,
        " tracked order(s) were evicted. A book seeded from krn.book_snapshots "
        "and replayed through this day's book_events would not be the venue's "
        "book. Nothing was written (D11, kSnapshotRuling)."));
  }
  return arrow::Status::OK();
}

// ===========================================================================
// Which recorded windows a DROP PARTITION invalidates (§8.6)
// ===========================================================================

std::vector<RecordedWindow> invalidationTargets(
    const std::vector<RecordedWindow>& intersecting,
    const std::string& ownSymbol, std::int64_t ownFromNs,
    std::int64_t ownToNs) {
  // Every intersecting window except the one being re-ingested, which its own
  // state=0 and then state=1 resolve. The own symbol's OTHER windows are in:
  // the partition holds their rows too, the reload puts back only this
  // window's, and before ENC-1635 they kept state=1 over deleted rows.
  //
  // state=3 and not a re-recorded state=1: this job reloads one DBN, so a
  // state=1 on another window would claim rows nobody reinserted. Where this
  // window's range does contain the other one, the read rule's union still
  // covers it through this window, so state=3 is never short (§8.6).
  std::vector<RecordedWindow> out;
  for (const auto& w : intersecting) {
    const bool own = w.symbol == ownSymbol && w.fromTsNs == ownFromNs &&
                     w.toTsNs == ownToNs;
    if (!own) out.push_back(w);
  }
  return out;
}

// ===========================================================================
// The completeness read (D11)
// ===========================================================================

arrow::Result<Completeness> queryCompleteness(const ch::Client& client,
                                              const std::string& symbol,
                                              const std::string& kind,
                                              std::int64_t reqFromNs,
                                              std::int64_t reqToNs) {
  // Projection plus a WHERE on the sort-key prefix. No aggregate, no FINAL, no
  // argMax, no LIMIT: the version resolution and the coverage test are both
  // done by the caller, which is the DDL's own instruction and keeps this a
  // plain selection on this side of the D2 boundary.
  std::ostringstream q;
  q << "SELECT toUnixTimestamp64Nano(from_ts), toUnixTimestamp64Nano(to_ts), "
       "toUInt16(state), toUnixTimestamp64Milli(updated_at) "
       "FROM krn.ingest_windows WHERE symbol = "
    << ch::sqlQuote(symbol) << " AND kind = " << ch::sqlQuote(kind)
    << " FORMAT TabSeparated";
  ARROW_ASSIGN_OR_RAISE(const auto rows, client.select(q.str()));

  std::vector<IngestWindowRow> parsed;
  parsed.reserve(rows.size());
  for (const auto& r : rows) {
    if (r.size() != 4) {
      return arrow::Status::Invalid("krn.ingest_windows row has ", r.size(),
                                    " field(s), expected 4");
    }
    IngestWindowRow w;
    std::uint16_t state = 0;
    if (!parseExact(r[0], w.fromTsNs)) return badField("from_ts", r[0]);
    if (!parseExact(r[1], w.toTsNs)) return badField("to_ts", r[1]);
    if (!parseExact(r[2], state)) return badField("state", r[2]);
    if (!parseExact(r[3], w.updatedAtMs)) return badField("updated_at", r[3]);
    w.state = static_cast<std::uint8_t>(state);
    parsed.push_back(w);
  }
  return isComplete(parsed, reqFromNs, reqToNs);
}

arrow::Result<std::int64_t> rowCount(const ch::Client& client,
                                     const std::string& table,
                                     const std::string& symbol,
                                     std::int64_t fromNs, std::int64_t toNs) {
  std::ostringstream q;
  // `table` is NOT interpolated from user input — it is one of two string
  // literals chosen by the caller's `KindSpec` — but `symbol` is, and comes
  // from vendor DBN symbology as well as from the CLI.
  q << "SELECT count() FROM krn." << table << " WHERE symbol = "
    << ch::sqlQuote(symbol) << " AND ts_event >= fromUnixTimestamp64Nano(toInt64("
    << fromNs << "), 'UTC') AND ts_event < fromUnixTimestamp64Nano(toInt64("
    << toNs << "), 'UTC')";
  ARROW_ASSIGN_OR_RAISE(const auto text, client.scalar(q.str()));
  std::int64_t n = 0;
  if (!parseExact(text, n)) return badField("count()", text);
  return n;
}

// ===========================================================================
// A delivered vendor DBN job (ENC-1640)
// ===========================================================================

std::pair<std::int64_t, std::int64_t> dayWindow(std::uint32_t yyyymmdd,
                                                std::uint64_t queryStart,
                                                std::uint64_t queryEnd) {
  const std::uint64_t dayFrom = csv::midnightUtcNs(yyyymmdd);
  const std::uint64_t dayTo   = dayFrom + 86400000000000ULL;
  const std::uint64_t from    = std::max(dayFrom, queryStart);
  const std::uint64_t to      = std::min(dayTo, queryEnd);
  return {static_cast<std::int64_t>(from), static_cast<std::int64_t>(to)};
}

arrow::Result<std::uint64_t> decompressZstdFile(const std::string& inPath,
                                                const std::string& outPath) {
  if (!arrow::util::Codec::IsAvailable(arrow::Compression::ZSTD)) {
    return arrow::Status::NotImplemented(
        "this build's Arrow has no ZSTD codec (KRN_DATA_ARROW_IPC_CODECS=OFF?), "
        "so a delivered .dbn.zst cannot be decompressed");
  }
  ARROW_ASSIGN_OR_RAISE(auto codec,
                        arrow::util::Codec::Create(arrow::Compression::ZSTD));
  ARROW_ASSIGN_OR_RAISE(auto dec, codec->MakeDecompressor());

  std::FILE* in = std::fopen(inPath.c_str(), "rb");
  if (in == nullptr) return arrow::Status::IOError("cannot open ", inPath);
  std::FILE* out = std::fopen(outPath.c_str(), "wb");
  if (out == nullptr) {
    std::fclose(in);
    return arrow::Status::IOError("cannot create the scratch file ", outPath);
  }
  const auto fail = [&](arrow::Status st) {
    std::fclose(in);
    std::fclose(out);
    std::remove(outPath.c_str());
    return st;
  };

  std::vector<std::uint8_t> ibuf(1u << 20);
  std::vector<std::uint8_t> obuf(1u << 22);
  std::uint64_t written = 0;
  bool sawInput = false;
  for (;;) {
    const std::size_t n = std::fread(ibuf.data(), 1, ibuf.size(), in);
    if (n == 0) break;
    sawInput = true;
    std::size_t pos = 0;
    // Drain this chunk completely; a frame end inside it is followed by the
    // next frame, which ZSTD_decompressStream begins on its own.
    while (pos < n || !dec->IsFinished()) {
      auto r = dec->Decompress(static_cast<std::int64_t>(n - pos), ibuf.data() + pos,
                               static_cast<std::int64_t>(obuf.size()), obuf.data());
      if (!r.ok()) return fail(r.status());
      pos += static_cast<std::size_t>(r->bytes_read);
      if (r->bytes_written > 0) {
        if (std::fwrite(obuf.data(), 1, static_cast<std::size_t>(r->bytes_written),
                        out) != static_cast<std::size_t>(r->bytes_written)) {
          return fail(arrow::Status::IOError("short write to ", outPath));
        }
        written += static_cast<std::uint64_t>(r->bytes_written);
      }
      if (r->bytes_read == 0 && r->bytes_written == 0) break;  // needs more input
    }
  }
  if (std::ferror(in) != 0) return fail(arrow::Status::IOError("read error on ", inPath));
  if (!sawInput || !dec->IsFinished()) {
    return fail(arrow::Status::IOError(
        inPath, " ends inside a zstd frame (", written,
        " bytes decompressed). A truncated delivered file is not a day's data; "
        "re-download it and check manifest.json's sha256."));
  }
  std::fclose(in);
  if (std::fclose(out) != 0) {
    std::remove(outPath.c_str());
    return arrow::Status::IOError("cannot close ", outPath);
  }
  return written;
}

namespace {

arrow::Result<rapidjson::Document> readJson(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(const auto bytes, csv::readFile(path));
  const std::string text(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  rapidjson::Document doc;
  doc.Parse(text.c_str());
  if (doc.HasParseError()) {
    return arrow::Status::Invalid(path, " is not valid JSON");
  }
  return doc;
}

}  // namespace

arrow::Result<DbnJobPlan> planDbnJob(const std::string& dirIn,
                                     const std::vector<std::uint32_t>& onlyDays) {
  const std::string dir = expandTilde(dirIn);
  DbnJobPlan plan;
  // A missing metadata.json is an IOError from readPullMetadata: untagged, so
  // exit 2. DBN carries no job_id and the query window is only here.
  ARROW_ASSIGN_OR_RAISE(plan.md, csv::readPullMetadata(dir + "/metadata.json"));
  const auto& md = plan.md;
  const auto refuse = [&](auto&&... parts) {
    return arrow::Status::Invalid(dir, "/metadata.json: ", parts...,
                                  " This is not a delivered DBN job this mode reads; "
                                  "nothing was decoded or written.");
  };
  if (md.encoding != "dbn") {
    return refuse("encoding is `", md.encoding,
                  "`, not `dbn`. A CSV job is `krn_ingest ingest --data-root`.");
  }
  if (md.compression != "zstd") {
    return refuse("compression is `", md.compression.empty() ? "null" : md.compression,
                  "`, not `zstd`.");
  }
  if (md.splitDuration != "day") {
    return refuse("split_duration is `",
                  md.splitDuration.empty() ? "null" : md.splitDuration,
                  "`, not `day`; each file must be one UTC day for it to be one "
                  "window.");
  }
  if (md.jobId.empty()) return refuse("job_id is missing.");
  // A JOB-LEVEL `limit` is cut across the day files: the day it ends inside
  // is short, and nothing in that day's file says so (its own DBN limit is
  // 0), so it would be recorded state=1 over a prefix — SPEC D11's failure.
  if (md.limit != 0) {
    return refuse("query.limit is ", md.limit, "; a limited pull split by day "
                  "cannot say which day it cut short. Re-request with limit null.");
  }
  if (md.symbols.empty()) return refuse("query.symbols is empty.");
  if (md.schema == "trades") {
    plan.kind = "trades";
  } else if (md.schema == "mbo") {
    plan.kind = "book_events";
  } else {
    return refuse("schema `", md.schema, "` has no L1 table in this job.");
  }
  plan.dbnSchema = md.schema;

  // manifest.json: the vendor's file list for THIS job. A directory holding a
  // second job's files, or a file of the wrong size, is refused here.
  std::map<std::string, std::uint64_t> manifest;
  {
    ARROW_ASSIGN_OR_RAISE(const auto doc, readJson(dir + "/manifest.json"));
    if (!doc.IsObject() || !doc.HasMember("job_id") || !doc["job_id"].IsString() ||
        doc["job_id"].GetString() != md.jobId) {
      return refuse("manifest.json is not for job_id ", md.jobId, ".");
    }
    if (!doc.HasMember("files") || !doc["files"].IsArray()) {
      return refuse("manifest.json has no `files` array.");
    }
    for (const auto& f : doc["files"].GetArray()) {
      if (!f.IsObject() || !f.HasMember("filename") || !f["filename"].IsString() ||
          !f.HasMember("size") || !f["size"].IsUint64()) {
        return refuse("manifest.json has a file entry without filename/size.");
      }
      manifest[f["filename"].GetString()] = f["size"].GetUint64();
    }
  }
  std::map<std::uint32_t, std::string> conditions;
  {
    ARROW_ASSIGN_OR_RAISE(const auto doc, readJson(dir + "/condition.json"));
    if (!doc.IsArray()) return refuse("condition.json is not an array.");
    for (const auto& c : doc.GetArray()) {
      if (!c.IsObject() || !c.HasMember("date") || !c["date"].IsString() ||
          !c.HasMember("condition") || !c["condition"].IsString()) {
        return refuse("condition.json has an entry without date/condition.");
      }
      std::string d = c["date"].GetString();  // YYYY-MM-DD
      d.erase(std::remove(d.begin(), d.end(), '-'), d.end());
      std::uint32_t day = 0;
      if (!parseExact(d, day)) return refuse("condition.json date `", d, "`.");
      conditions[day] = c["condition"].GetString();
    }
  }

  // The day files: `<dataset-slug>-YYYYMMDD.<schema>.dbn.zst`, taken from the
  // MANIFEST so a stray file in the directory is ignored rather than ingested.
  const std::string suffix = ".dbn.zst";
  for (const auto& [name, size] : manifest) {
    if (name.size() <= suffix.size() ||
        name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
      continue;
    }
    const std::string stem = name.substr(0, name.size() - suffix.size());
    const auto dot  = stem.rfind('.');
    const auto dash = stem.rfind('-', dot);
    std::uint32_t day = 0;
    if (dot == std::string::npos || dash == std::string::npos ||
        !parseExact(stem.substr(dash + 1, dot - dash - 1), day)) {
      return refuse("cannot read a yyyymmdd day from `", name, "`.");
    }
    const std::string fileSchema = stem.substr(dot + 1);
    if (fileSchema != md.schema) {
      return refuse("query.schema is `", md.schema, "` but `", name,
                    "` is a `", fileSchema, "` file.");
    }
    DbnJobDay d;
    d.day   = day;
    d.path  = dir + "/" + name;
    const auto win = dayWindow(day, md.start, md.end);
    d.fromNs = win.first;
    d.toNs   = win.second;
    if (d.toNs <= d.fromNs) {
      return refuse("`", name, "` is outside the query [", md.start, ", ",
                    md.end, ").");
    }
    const auto c = conditions.find(day);
    d.condition = c == conditions.end() ? "" : c->second;
    if (!onlyDays.empty() &&
        std::find(onlyDays.begin(), onlyDays.end(), day) == onlyDays.end()) {
      continue;
    }
    std::FILE* f = std::fopen(d.path.c_str(), "rb");
    if (f == nullptr) return refuse("`", name, "` is in manifest.json and not on disk.");
    std::fseek(f, 0, SEEK_END);
    d.bytes = static_cast<std::uint64_t>(std::ftell(f));
    std::fclose(f);
    if (d.bytes != size) {
      return refuse("`", name, "` is ", d.bytes, " bytes and manifest.json says ",
                    size, ".");
    }
    plan.days.push_back(std::move(d));
  }
  for (const auto want : onlyDays) {
    const bool have = std::any_of(plan.days.begin(), plan.days.end(),
                                  [&](const DbnJobDay& d) { return d.day == want; });
    if (!have) return refuse("--day ", want, " has no file in this job.");
  }
  if (plan.days.empty()) return refuse("the manifest lists no .dbn.zst day file.");
  std::sort(plan.days.begin(), plan.days.end(),
            [](const DbnJobDay& a, const DbnJobDay& b) { return a.day < b.day; });
  return plan;
}

// ===========================================================================
// run
// ===========================================================================

namespace {

// The two kinds. `kind` is krn.ingest_windows' value (and the table name);
// `dbnSchema` is the vendor's. They differ for MBO — the vendor calls the
// schema `mbo` and L1's table is `book_events` — and conflating them writes a
// completeness row under a kind no reader queries.
struct KindSpec {
  const char* kind;
  const char* dbnSchema;
  const char* table;
};
constexpr KindSpec kSpecs[] = {
    KindSpec{"trades", "trades", "trades"},
    KindSpec{"book_events", "mbo", "book_events"},
};

// Whole-file and chunked decodes render the same rows. Compared row by row
// through the ONE renderer rather than by concatenating both renders, which
// for a delivered mbo day (7M rows) was two strings of ~1 GB each.
bool sameRenderedRows(const Decoded& a, const Decoded& b) {
  if (a.ticks.size() != b.ticks.size() || a.book.size() != b.book.size()) return false;
  for (std::size_t i = 0; i < a.ticks.size(); ++i) {
    if (renderTradeRow(a.ticks[i], a.symbol, a.venue) !=
        renderTradeRow(b.ticks[i], b.symbol, b.venue)) {
      return false;
    }
  }
  for (std::size_t i = 0; i < a.book.size(); ++i) {
    if (renderBookRow(a.book[i], a.symbol, a.venue) !=
        renderBookRow(b.book[i], b.symbol, b.venue)) {
      return false;
    }
  }
  return true;
}

// ---- pass 1 for ONE kind: the raw half (read, decode, round-trip) --------
//
// `job`/`day` non-null selects the delivered-DBN source (ENC-1640); otherwise
// `opts.fromDbnOnly` picks the retained DBN or the CSV, as before.
arrow::Status stageOne(const Options& opts, const std::string& root,
                       const KindSpec& spec, const DbnJobPlan* job,
                       const DbnJobDay* day, Staged& st) {
  st.rep.kind      = spec.kind;
  st.rep.dbnSchema = spec.dbnSchema;
  st.table         = spec.table;

  const bool dbnJob = job != nullptr;
  const std::string dbnPath =
      dbnJob ? day->path : root + "/xnas-itch-20220610." + spec.dbnSchema + ".dbn";
  st.rep.dbnPath = dbnPath;

  std::vector<std::byte> dbnBytes;
  std::vector<csv::Transcoded::Probe> probes;
  bool retainedMatched = false;
  // The recorded window's inputs, from whichever source this path reads
  // (metadata.json + CSV on `ingest`, the DBN on `reload` and on a delivered
  // DBN job). They meet in ONE call to `csv::deliveredWindow` below, after the
  // decode (ENC-1636).
  std::uint64_t winStart = 0, winEnd = 0, winLimit = 0, winRecords = 0,
                winLastTs = 0;

  if (dbnJob) {
    // THE DELIVERED FILE IS THE D6 RETAINED FILE. It is opened read-only and
    // decompressed into scratch; nothing here can write to `dbnPath`.
    const std::string base =
        dbnPath.substr(dbnPath.rfind('/') + 1, dbnPath.size() - dbnPath.rfind('/') - 5);
    // A UNIQUE scratch name, created exclusively: a fixed name would overwrite
    // (and then delete) a same-named file already in --scratch, and two runs
    // sharing --scratch would decode each other's bytes.
    std::string tmpl = expandTilde(opts.scratchDir) + "/" + base + ".XXXXXX";
    const int fd = ::mkstemp(tmpl.data());
    if (fd < 0) {
      return arrow::Status::IOError("cannot create a scratch file in ",
                                    opts.scratchDir, ": ", std::strerror(errno));
    }
    ::close(fd);
    const std::string scratch = tmpl;
    auto unpacked = decompressZstdFile(dbnPath, scratch);
    if (!unpacked.ok()) {
      std::remove(scratch.c_str());
      return unpacked.status();
    }
    st.rep.decompressedBytes = *unpacked;
    auto read = csv::readFile(scratch);
    std::remove(scratch.c_str());
    ARROW_ASSIGN_OR_RAISE(dbnBytes, std::move(read));
    st.rep.dbnExisted = true;
    st.rep.jobId      = job->md.jobId;
    st.rep.condition  = day->condition;
    st.dataset        = job->md.dataset;
    if (day->condition != "available") {
      // condition.json is the vendor's own word on whether the day is whole.
      return asFinding(arrow::Status::Invalid(
          "condition.json says `", day->condition.empty() ? "<absent>" : day->condition,
          "` for ", day->day, ", not `available`; that day is not recorded "
          "complete (SPEC D11)."));
    }
  } else if (opts.fromDbnOnly) {
    // THE PURE D6 RELOAD. No CSV and no metadata.json: the retained DBN
    // carries `start`, `end` and `limit` in its own metadata block, which is
    // what makes "reload must reload from something" a complete statement.
    if (!fileExists(dbnPath)) {
      return arrow::Status::IOError(
          "--from-dbn-only but ", dbnPath,
          " does not exist. SPEC D6 retains the raw DBN so a reload needs no "
          "re-pull; run once without the flag to produce it from the CSV.");
    }
    ARROW_ASSIGN_OR_RAISE(dbnBytes, csv::readFile(dbnPath));
    st.rep.dbnExisted = true;
    st.dataset        = "XNAS.ITCH";  // replaced from the DBN metadata below
  } else {
    // metadata.json: PER JOB, and this directory holds two jobs.
    //
    // Each vendor zip carries its own metadata.json, and extracting both zips
    // into one directory leaves ONE of them — silently the wrong one for one
    // of the two CSVs. So a per-schema sidecar is preferred, and a shared
    // metadata.json is accepted ONLY if its own `query.schema` matches the
    // file being read. A mismatch is refused with the command that fixes it,
    // because the fields at stake (`start`, `end`, `limit`) are exactly what
    // krn.ingest_windows forms an opinion from.
    const std::string sidecar = root + "/metadata." + spec.dbnSchema + ".json";
    const std::string shared  = root + "/metadata.json";
    const std::string mdPath  = fileExists(sidecar) ? sidecar : shared;
    ARROW_ASSIGN_OR_RAISE(const auto md, csv::readPullMetadata(mdPath));
    if (md.schema != spec.dbnSchema) {
      return arrow::Status::Invalid(
          mdPath, " describes the `", md.schema, "` job (job_id ", md.jobId,
          ") but the `", spec.dbnSchema,
          "` CSV is being read. Two batch jobs extracted into one directory "
          "leave one metadata.json, and `start`/`end`/`limit` from the wrong "
          "job would be recorded in krn.ingest_windows as this pull's window. "
          "Extract the per-job sidecars first, e.g.\n"
          "    for z in ", root,
          "/*.zip; do s=$(unzip -p \"$z\" metadata.json | "
          "python3 -c 'import json,sys; print(json.load(sys.stdin)[\"query\"]"
          "[\"schema\"])'); unzip -p \"$z\" metadata.json > ", root,
          "/metadata.$s.json; done");
    }
    const std::string csvPath =
        root + "/xnas-itch-20220610." + spec.dbnSchema + ".csv";
    st.rep.csvPath = csvPath;
    st.rep.jobId   = md.jobId;
    st.dataset     = md.dataset;

    ARROW_ASSIGN_OR_RAISE(const auto tc,
                          csv::transcodeCsv(csvPath, md, opts.dbnVersion));
    st.rep.csvRecords           = tc.records;
    st.rep.recordsAtLastTsEvent = tc.recordsAtLastTsEvent;
    st.rep.tsRecvMatchesDelta   = tc.tsRecvMatchesDelta;
    st.rep.actionCounts         = tc.actionCounts;
    winStart   = md.start;
    winEnd     = md.end;
    winLimit   = md.limit;
    winRecords = tc.records;
    winLastTs  = tc.lastTsEvent;
    st.mappings                 = tc.mappings;
    probes                      = tc.probes;

    // DECODE, THEN RETAIN (finding #3). `retainOnlyIfDecodable` proves the
    // bytes decode through the real adapter BEFORE the D6 baseline moves, so
    // a bad `--dbn-version`, a bad CSV or an encoder regression fails with
    // the previous file intact instead of destroying it.
    //
    // `--force-reencode` deletes the existing file, so the deletion also
    // waits until the replacement has been proven: the old order deleted
    // first and could leave NO file at all.
    {
      const arrow::Status decodes =
          decodeDbn(tc.dbn, 1u, tc.dbn.size()).status();
      if (!decodes.ok()) {
        return asFinding(arrow::Status::Invalid(
            "the DBN transcoded from ", csvPath,
            " does not decode through DatabentoAdapter, so it was NOT "
            "written: ",
            decodes.message()));
      }
      if (opts.forceReencode && fileExists(dbnPath)) {
        std::remove(dbnPath.c_str());
      }
    }
    ARROW_ASSIGN_OR_RAISE(const auto ret,
                          retainOnlyIfDecodable(dbnPath, tc.dbn));
    st.rep.dbnExisted   = ret.existed;
    st.rep.dbnRewritten = ret.changed;
    retainedMatched     = ret.existed && !ret.changed;

    // Read the file BACK FROM DISK rather than reusing `tc.dbn`. The insert
    // path IS the reload path; if retention wrote something other than what
    // it was handed, this is where it shows.
    ARROW_ASSIGN_OR_RAISE(dbnBytes, csv::readFile(dbnPath));
    if (dbnBytes != tc.dbn) {
      // A FINDING (exit 1), not a could-not-run: the file was written and
      // read back fine, and what came back is wrong. Was IOError, which
      // `main` mapped to exit 2 — finding #1.
      return asFinding(arrow::Status::Invalid(
          "the retained DBN at ", dbnPath,
          " does not read back as the bytes just written. L1 is a rebuildable "
          "projection of these files (SPEC D6); a file that does not read back "
          "is not a baseline."));
    }
  }
  st.rep.dbnBytes = dbnJob ? day->bytes : dbnBytes.size();
  if (!dbnJob) st.rep.decompressedBytes = dbnBytes.size();

  // On a DBN job the session is FNV-1a-32 of the DECOMPRESSED stream, which
  // is what the adapter decodes and what a `.dbn` reload of the same data
  // would hash: the same records give the same session however they were
  // compressed (kSessionIdRuling).
  st.sessionId = opts.sessionId != 0 ? opts.sessionId : deriveSessionId(dbnBytes);
  st.rep.sessionId = st.sessionId;

  // ---- decode twice, at two chunkings, and compare the RENDERED rows ---
  ARROW_ASSIGN_OR_RAISE(auto whole,
                        decodeDbn(dbnBytes, st.sessionId, dbnBytes.size()));
  std::ostringstream note;
  bool ok = true;
  {
    // 8191: a chunk that lands inside the 100-byte fixed metadata block, inside
    // the variable symbology section and inside records of both 56 and 48
    // bytes, so no boundary in the file is also a feed boundary.
    ARROW_ASSIGN_OR_RAISE(const auto chunked, decodeDbn(dbnBytes, st.sessionId, 8191));
    if (!sameRenderedRows(whole, chunked)) {
      ok = false;
      note << "the retained DBN decodes to DIFFERENT canonical rows depending on "
              "the feed chunking (whole file vs 8191-byte chunks): "
           << whole.ticks.size() << "/" << whole.book.size() << " vs "
           << chunked.ticks.size() << "/" << chunked.book.size()
           << " tick/book records. ";
    }
  }
  std::vector<std::byte>().swap(dbnBytes);  // the decoded rows are what is kept

  // The second axis, and the stronger one: the canonical rows must carry the
  // (ts_event, provider_seq, price) the CSV carried, for every record that
  // produces a row. This is what shows the CSV -> DBN -> canonical legs
  // LOSSLESS without a second CSV -> canonical mapping existing to compare
  // against (SPEC D1 forbids that second mapping). `Action::None` produces no
  // row by design — DBN's own "has no effect on the book" — so it is the one
  // action excluded.
  if (ok && !probes.empty()) {
    using Triple = std::tuple<std::int64_t, std::uint64_t, std::int64_t>;
    std::vector<Triple> want;
    // ⚠ CODE-REVIEW FINDING #11: the exclusion is MBO-ONLY. `onMbo` accepts
    // `Action::None` and deliberately produces no book row ("there is no
    // BookAction for 'no effect' and inventing one would put a no-op mutation
    // into the replay stream"), but `onTrade` emits a `TickRecord` for EVERY
    // `trades` record whatever its action. Excluding `N` for both schemas
    // therefore made a `trades` CSV containing one `action='N'` row fail the
    // round trip and abort a perfectly valid ingest.
    //
    // UNTRIGGERED ON THIS SAMPLE, and the near-miss is worth naming: our
    // `trades` action column is 1000/1000 `T`. The 546 `N` values are in
    // `side`, a DIFFERENT COLUMN with a different meaning ("no side specified
    // by the original source"), and conflating the two is exactly how this
    // bug would have been argued away as already covered.
    const bool skipActionNone = std::string(spec.dbnSchema) == "mbo";
    for (const auto& p : probes) {
      if (skipActionNone && p.action == static_cast<char>(dbn::Action::None)) {
        continue;
      }
      want.emplace_back(static_cast<std::int64_t>(p.tsEvent),
                        static_cast<std::uint64_t>(p.sequence),
                        p.price == dbn::kUndefPrice ? kNoPrice : p.price);
    }
    std::vector<Triple> got;
    for (const auto& t : whole.ticks) {
      got.emplace_back(t.header.tsEventNs, t.header.providerSeq, t.price);
    }
    for (const auto& b : whole.book) {
      got.emplace_back(b.header.tsEventNs, b.header.providerSeq, b.price);
    }
    std::sort(want.begin(), want.end());
    std::sort(got.begin(), got.end());
    if (want != got) {
      ok = false;
      note << "the (ts_event, provider_seq, price) multiset of the canonical "
              "rows does not match the CSV's: "
           << want.size() << " expected, " << got.size() << " produced. ";
    } else {
      note << "the (ts_event, provider_seq, price) multiset of all "
           << got.size()
           << " canonical rows matches the CSV's exactly; whole-file and "
              "8191-byte-chunk feeds of the retained DBN render identical "
              "rows";
      if (retainedMatched) {
        note << "; and re-transcoding the CSV reproduced the already-retained "
                "DBN byte-for-byte";
      }
    }
  } else if (ok) {
    note << "whole-file and 8191-byte-chunk feeds of the retained DBN render "
            "identical rows ("
         << whole.ticks.size() << " ticks, " << whole.book.size()
         << " book events); no CSV was read, so there is no CSV cross-check "
            "on this run";
  }
  st.rep.roundTripOk   = ok;
  st.rep.roundTripNote = note.str();
  if (!ok) {
    // A FINDING (exit 1). The header's own exit-code table names the
    // round-trip mismatch as the archetypal one; `main` nonetheless reported
    // it as could-not-run until finding #1.
    return asFinding(arrow::Status::Invalid(
        "round-trip failed for ", spec.kind, ": ", st.rep.roundTripNote));
  }

  // ⚠ CODE-REVIEW FINDING #12. The insert below renders ONLY `ticks` for
  // `trades` and ONLY `book` for `book_events`, so a file that decoded into
  // the other vector inserted ZERO rows and the window was still recorded
  // `state=1` — a completeness claim over nothing, which is the one thing
  // this table must never say. Unreachable on the CSV path (the transcoder's
  // rtype gate refuses a schema/rtype mismatch) and REACHABLE on `reload`,
  // where the DBN on disk is constrained by nothing at all: rename
  // `*.trades.dbn` over `*.mbo.dbn` and the mbo kind decodes 1000 ticks, zero
  // book events, and claims the window.
  {
    const bool wantTicks = std::string(spec.kind) == "trades";
    const std::size_t wanted = wantTicks ? whole.ticks.size() : whole.book.size();
    const std::size_t other  = wantTicks ? whole.book.size() : whole.ticks.size();
    if (other != 0 || wanted == 0) {
      return asFinding(arrow::Status::Invalid(
          "the DBN for kind `", spec.kind, "` decoded into ",
          whole.ticks.size(), " tick record(s) and ", whole.book.size(),
          " book record(s), but krn.", spec.table, " holds only the ",
          wantTicks ? "tick" : "book",
          " kind. Inserting the matching half and recording the window "
          "complete would assert coverage over rows that were silently "
          "dropped (SPEC D11). Check that ", dbnPath,
          " really is the `", spec.dbnSchema, "` pull."));
    }
  }

  // ---- the vendor's start-of-day book snapshot (kSnapshotRuling) --------
  //
  // Out of `whole.book` and into `st.snapshot` once `splitSnapshot` has proven
  // its shape; any other shape is refused before any write.
  ARROW_RETURN_NOT_OK(
      splitSnapshot(whole.book, st.snapshot, st.rep.snapshotAsOfNs, dbnPath));
  st.rep.snapshotExcluded = st.snapshot.size();
  if (std::string(spec.kind) == "book_events") {
    if (whole.book.empty()) {
      return asFinding(arrow::Status::Invalid(
          dbnPath, " decoded only F_SNAPSHOT book records; there is no event "
          "in the window to record. Nothing was written."));
    }
    st.rep.unknownOrders = whole.stats.unknownOrder;
    if (dbnJob) {
      ARROW_RETURN_NOT_OK(checkDeliveredBookDay(whole.book, st.snapshot.size(),
                                                whole.stats.unknownOrder,
                                                whole.stats.evictedOrders, dbnPath));
    }
  }
  st.rep.actionNone = static_cast<std::size_t>(whole.stats.actionNone);

  // krn.gaps (ENC-1506). No seq-derived row is inferred — see Gaps.hpp —
  // and the count of what a naive detector would have written is reported
  // so the refusal is a number, not an empty table.
  {
    const auto pts = std::string(spec.kind) == "trades"
                         ? gaps::seqPoints(whole.ticks)
                         : gaps::seqPoints(whole.book);
    const gaps::Inference inf = gaps::infer(whole.meta, pts);
    st.rep.gapPullScope     = gaps::toString(inf.scope);
    st.rep.gapNaiveSeqJumps = inf.naiveSeqJumps;
    st.rep.gapRowsWritten   = inf.rows.size();
    st.rep.gapMaybeBadBook  = inf.vendorMaybeBadBook;
    st.rep.gapRefusal       = inf.refusal;
    if (!inf.rows.empty()) {
      // Unreachable while `infer` writes nothing. Here so that a future
      // branch that starts emitting rows fails loudly until this job learns
      // to write them, rather than computing them and dropping them.
      return asFinding(arrow::Status::Invalid(
          "gaps::infer returned ", inf.rows.size(), " row(s) for `",
          spec.kind, "` and this job has no krn.gaps writer."));
    }
    if (inf.vendorMaybeBadBook != 0) {
      // The vendor says the channel lost data, and krn.gaps cannot hold it
      // (the flag carries no bounds). Recording this window complete would
      // be a completeness claim over a declared hole — SPEC D11's failure —
      // so the window is refused, before any table is touched.
      return asFinding(arrow::Status::Invalid(
          inf.vendorMaybeBadBook, " `", spec.kind, "` record(s) in ", dbnPath,
          " carry F_MAYBE_BAD_BOOK, Databento's 'unrecoverable gap detected in "
          "the channel'. The flag has no seq or event-time bounds, so no "
          "krn.gaps row can be written for it, and the window is not recorded "
          "complete (docs/data-layer.md 8.8, ENC-1506)."));
    }
  }

  st.rep.decodedRecords  = whole.stats.recordsIn;
  st.rep.canonicalRows   = whole.ticks.size() + whole.book.size();
  st.rep.dbnVersionNote  = "DBN v" + std::to_string(whole.meta.version);

  // EVERY DECODED RECORD IS ACCOUNTED FOR: a row, an excluded snapshot record,
  // an mbo action N, or a declared rtype this job does not consume. A record
  // in none of those is a row lost between the adapter and the insert.
  if (st.rep.canonicalRows + st.rep.snapshotExcluded + st.rep.actionNone +
          whole.stats.recordsSkipped !=
      whole.stats.recordsIn) {
    return asFinding(arrow::Status::Invalid(
        dbnPath, ": ", whole.stats.recordsIn, " record(s) decoded but ",
        st.rep.canonicalRows, " row(s) + ", st.rep.snapshotExcluded,
        " snapshot + ", st.rep.actionNone, " action N + ",
        whole.stats.recordsSkipped, " skipped do not add up to it."));
  }

  // ENC-1484's number, free while the records are in hand and measured HERE
  // rather than beside the insert so `--no-insert` reports it too: D5 picked
  // plain MergeTree because (symbol, ts_event, provider_seq) is NOT unique,
  // and this is by how much. One symbol, so the symbol leg is constant.
  {
    std::vector<std::pair<std::int64_t, std::uint64_t>> keys;
    keys.reserve(st.rep.canonicalRows);
    for (const auto& t : whole.ticks) {
      keys.emplace_back(t.header.tsEventNs, t.header.providerSeq);
    }
    for (const auto& b2 : whole.book) {
      keys.emplace_back(b2.header.tsEventNs, b2.header.providerSeq);
    }
    std::sort(keys.begin(), keys.end());
    const auto last = std::unique(keys.begin(), keys.end());
    st.rep.distinctSortKeys = static_cast<std::size_t>(last - keys.begin());
    st.rep.duplicateSortKeys = keys.size() - st.rep.distinctSortKeys;
  }
  st.rep.sideUnspecified = whole.stats.sideUnspecified;
  st.rep.symbol          = whole.symbol;
  st.rep.venue           = whole.venue;
  st.rep.instrumentId    = whole.instrumentId;
  st.dataset             = whole.meta.dataset;

  if (dbnJob) {
    // The ticker resolved from the DBN's own symbology must be one the job
    // asked for; anything else is metadata.json not describing its files.
    const auto& wanted = job->md.symbols;
    if (std::find(wanted.begin(), wanted.end(), whole.symbol) == wanted.end()) {
      return arrow::Status::Invalid(
          dbnPath, " decodes as symbol `", whole.symbol,
          "`, which is not in metadata.json's query.symbols. Nothing was written.");
    }
    // The job's metadata.json and the file's own DBN metadata must describe
    // the same pull. A disagreement is metadata.json not matching its files:
    // COULD-NOT-RUN, the same class as a missing metadata.json.
    const std::uint16_t wantSchema = static_cast<std::uint16_t>(
        job->dbnSchema == "mbo" ? dbn::Schema::Mbo : dbn::Schema::Trades);
    if (whole.meta.dataset != job->md.dataset || !whole.meta.schema.has_value() ||
        *whole.meta.schema != wantSchema) {
      return arrow::Status::Invalid(
          dbnPath, " is DBN dataset `", whole.meta.dataset, "` schema ",
          whole.meta.schema.has_value() ? std::to_string(*whole.meta.schema) : "mixed",
          " but metadata.json says `", job->md.dataset, "` `", job->md.schema,
          "`. Nothing was written.");
    }
    if (static_cast<std::int64_t>(whole.meta.start) != day->fromNs ||
        static_cast<std::int64_t>(whole.meta.end) != day->toNs) {
      return arrow::Status::Invalid(
          dbnPath, " declares [", whole.meta.start, ", ", whole.meta.end,
          ") in its DBN metadata, and this day of the query is ",
          formatWindow(day->fromNs, day->toNs),
          ". The window recorded is the day's; a file that disagrees is not "
          "this day. Nothing was written.");
    }
  }

  if (opts.fromDbnOnly || dbnJob) {
    // The window from the DBN's own metadata — the whole point of writing
    // `limit` into it — and from the RECORDS the adapter framed, not the
    // canonical rows: an mbo action `N` record counts toward `limit` and
    // yields no row (ENC-1636; it used to take the max over rows). On a DBN
    // job the metadata's [start, end) was just proven to be the day's window.
    winStart   = whole.meta.start;
    winEnd     = whole.meta.end;
    winLimit   = whole.meta.limit;
    winRecords = whole.stats.recordsIn;
    winLastTs  = whole.stats.lastTsEvent;
    st.rep.recordsAtLastTsEvent =
        static_cast<std::size_t>(whole.stats.recordsAtLastTsEvent);
    for (const auto& m : whole.meta.mappings) {
      for (const auto& iv : m.intervals) {
        std::uint32_t id = 0;
        if (!parseExact(iv.symbol, id)) continue;  // not an id in this interval
        st.mappings.push_back(csv::Transcoded::Mapping{
            id, m.rawSymbol, iv.startDate,
            iv.endDate > iv.startDate ? iv.endDate - 1 : iv.startDate});
      }
    }
  }

  // `lastTsEvent` is the delivered end only if ts_event never descends. The
  // CSV path refuses a descent in the transcoder; a retained DBN is checked
  // here, so `reload` refuses what `ingest` would have.
  if (whole.stats.tsEventDescents != 0) {
    return asFinding(arrow::Status::Invalid(
        dbnPath, " has ", whole.stats.tsEventDescents,
        " record(s) whose ts_event is below the previous record's. The "
        "recorded window's exclusive end is the LAST record's ts_event, which "
        "is the delivered end only in time order; refused rather than "
        "published (docs §8.8)."));
  }
  // On `ingest` the CSV's count and last ts_event must be what the decoder
  // saw in the retained DBN, or the two paths would read different inputs.
  if (!opts.fromDbnOnly && !dbnJob &&
      (winRecords != whole.stats.recordsIn || winLastTs != whole.stats.lastTsEvent)) {
    return asFinding(arrow::Status::Invalid(
        "the CSV for ", spec.kind, " has ", winRecords,
        " record(s) ending at ts_event ", winLastTs, " but its retained DBN "
        "decodes ", whole.stats.recordsIn, " ending at ",
        whole.stats.lastTsEvent, "; `reload` would record a different window."));
  }
  {
    const csv::DeliveredWindow w =
        csv::deliveredWindow(winStart, winEnd, winLimit, winRecords, winLastTs);
    st.rep.pullLimit        = winLimit;
    st.rep.truncatedByLimit = w.truncatedByLimit;
    st.rep.requestedFromNs  = static_cast<std::int64_t>(winStart);
    st.rep.requestedToNs    = static_cast<std::int64_t>(winEnd);
    st.rep.recordedFromNs   = static_cast<std::int64_t>(w.fromNs);
    st.rep.recordedToNs     = static_cast<std::int64_t>(w.toNs);
  }

  if (st.rep.recordedToNs <= st.rep.recordedFromNs) {
    return asFinding(arrow::Status::Invalid(
        "the delivered window for ", spec.kind, " came out empty (",
        formatWindow(st.rep.recordedFromNs, st.rep.recordedToNs),
        "). Writing it as an ingested window would assert coverage of "
        "nothing."));
  }

  // A ROW ON A DAY THE WINDOW DOES NOT CLAIM WOULD DROP THAT DAY'S PARTITION
  // (ENC-1640). The load drops every partition its rows land in, so one row
  // dated outside the recorded window's days empties a neighbouring day's
  // partition — another window's data — and only state=3 would say so. That
  // is what the vendor's start-of-day mbo snapshot did before it was
  // excluded: ts_event on the previous day. Refused, before any write.
  {
    const auto windowDays = daysOf(st.rep.recordedFromNs, st.rep.recordedToNs);
    std::size_t outside = 0;
    std::int64_t firstOutside = 0;
    const auto check = [&](std::int64_t ts) {
      if (windowDays.count(csv::yyyymmddUtc(static_cast<std::uint64_t>(ts))) == 0) {
        if (outside++ == 0) firstOutside = ts;
      }
    };
    for (const auto& t : whole.ticks) check(t.header.tsEventNs);
    for (const auto& b : whole.book) check(b.header.tsEventNs);
    // The snapshot's as-of instant is checked below, against the window
    // itself rather than its days.
    if (outside != 0) {
      return asFinding(arrow::Status::Invalid(
          dbnPath, ": ", outside, " ", spec.kind, " row(s) have a ts_event on "
          "a day outside the recorded window ",
          formatWindow(st.rep.recordedFromNs, st.rep.recordedToNs), " (first ",
          formatNs(firstOutside), "). Loading them would DROP that day's "
          "partition, which holds another window's rows. Nothing was written."));
    }
  }

  if (!st.snapshot.empty()) {
    ARROW_RETURN_NOT_OK(checkSnapshotAsOf(st.rep.snapshotAsOfNs, st.rep.recordedFromNs,
                                          st.rep.recordedToNs, dbnPath));
  }

  st.decoded = std::move(whole);
  return arrow::Status::OK();
}

// ---- preflight: the table holds every column the INSERT names ---------
//
// Before ANY write. An L1 created before a column was added (ENC-1494's
// `book_events.aggressor`) refuses the INSERT, but only after the DROP
// PARTITION below has emptied the day: measured, the job exited 2 with the
// window at state=2 and book_events at 0 rows. Recoverable by reload, and
// never reported complete, but it destroyed data it could have refused to
// touch. `apply.py verify` names the drift; this makes the job say so first.
arrow::Status preflightTable(const ch::Client& client, const char* columns,
                             const std::string& table) {
  std::string q = "SELECT ";
  q += columns;
  q += " FROM krn.";
  q += table;
  q += " LIMIT 0 FORMAT TabSeparated";
  const auto probe = client.select(q);
  if (!probe.ok()) {
    return arrow::Status::Invalid(
        "krn.", table, " does not hold every column this build inserts (",
        columns, "): ", probe.status().message(),
        "\nNothing has been dropped or written. The table predates this "
        "build's schema: `data/schema/apply.py verify` names the difference. "
        "clickhouse.sql is all CREATE ... IF NOT EXISTS, so re-applying it does "
        "not change an existing table: DROP TABLE krn.", table, " and re-apply "
        "(`data/schema/apply.sh`), or `apply.sh --fresh` for all of krn, then "
        "reload from the retained DBN (docs/data-layer.md §8.4). A book_snapshots "
        "table with `levels.*` columns predates ENC-1663.");
  }
  return arrow::Status::OK();
}

// An mbo load also drops and writes krn.book_snapshots (kSnapshotRuling), so
// that table is probed too, before anything is dropped.
arrow::Status preflight(const ch::Client& client, const std::string& kind,
                        const std::string& table) {
  if (kind == "trades") return preflightTable(client, kTradesColumns, table);
  ARROW_RETURN_NOT_OK(preflightTable(client, kBookEventsColumns, table));
  return preflightTable(client, kBookSnapshotsColumns, "book_snapshots");
}

// Rows per INSERT statement. The whole day was one statement while a day was
// 1000 rows; a delivered mbo day is millions, and one ~1 GB TSV body is a
// second copy of the day in memory for no gain. Same rows, same order.
constexpr std::size_t kInsertBatchRows = 1000000;

// ---- pass 2 for ONE staged kind: drop, insert, record -------------------
arrow::Status loadOne(const ch::Client& client, Staged& st, Report& rep) {
  // Day partitions this kind's rows land in, plus the recorded window's own
  // days: a window may claim a day the rows do not reach (a quiet tail), and
  // that day's partition is still being asserted over.
  std::set<std::uint32_t> days = daysOf(st.rep.recordedFromNs, st.rep.recordedToNs);
  for (const auto& t : st.decoded.ticks) {
    days.insert(csv::yyyymmddUtc(static_cast<std::uint64_t>(t.header.tsEventNs)));
  }
  for (const auto& b : st.decoded.book) {
    days.insert(csv::yyyymmddUtc(static_cast<std::uint64_t>(b.header.tsEventNs)));
  }
  if (!st.snapshot.empty()) {
    days.insert(csv::yyyymmddUtc(static_cast<std::uint64_t>(st.rep.snapshotAsOfNs)));
  }

  // An mbo load drops the same days of krn.book_snapshots as of krn.book_events
  // (kSnapshotRuling): the snapshot rows of a reloaded day are replaced, and a
  // reload whose file carries none leaves none behind. So both kinds' windows
  // on those days are invalidated, and the snapshot window is "own", and left
  // to its own state=0, only when this load writes one.
  struct TableKind {
    std::string kind;
    std::string table;
    bool writesOwnWindow;
  };
  std::vector<TableKind> tableKinds{{st.rep.kind, st.table, true}};
  if (st.rep.kind == "book_events") {
    tableKinds.push_back({"book_snapshots", "book_snapshots", !st.snapshot.empty()});
  }

  // ---- THE INVARIANT THE SCHEMA CANNOT ENFORCE -----------------------
  //
  // From the warning block in clickhouse.sql: a DROP PARTITION invalidates
  // EVERY recorded window's completeness rows for that partition, not only
  // the window being re-ingested, because partitions are day-granular while
  // completeness is per (symbol, window). Without this step another window's
  // newest row still reads state=1 over an emptied partition, and isComplete
  // returns true over nothing.
  //
  // So before dropping: every recorded window of this kind intersecting a
  // partition about to go gets a state=3 (invalidated) row, our own symbol's
  // other windows included (ENC-1635). Only the window being written is
  // left out; the state=0 below covers it. `invalidationTargets` decides.
  for (const auto& tk : tableKinds) {
    for (const std::uint32_t day : days) {
      const auto dayFrom = static_cast<std::int64_t>(csv::midnightUtcNs(day));
      const std::int64_t dayTo = dayFrom + 86400000000000LL;
      std::ostringstream q;
      q << "SELECT DISTINCT symbol, toUnixTimestamp64Nano(from_ts), "
           "toUnixTimestamp64Nano(to_ts) FROM krn.ingest_windows WHERE kind = "
        << ch::sqlQuote(tk.kind)
        << " AND from_ts < fromUnixTimestamp64Nano(toInt64(" << dayTo
        << "), 'UTC') AND to_ts > fromUnixTimestamp64Nano(toInt64(" << dayFrom
        << "), 'UTC') FORMAT TabSeparated";
      ARROW_ASSIGN_OR_RAISE(const auto found, client.select(q.str()));
      std::vector<RecordedWindow> intersecting;
      intersecting.reserve(found.size());
      for (const auto& r : found) {
        // ⚠ CODE-REVIEW FINDING #6. This was `if (r.size() != 3) continue;` —
        // a SILENT SKIP, inside the block whose entire stated purpose is
        // preventing `isComplete` from returning true over an emptied
        // partition. Skipping a row here means some window keeps a `state=1`
        // row over data this loop is about to drop, which is the exact failure
        // the block exists to prevent, reached by the code meant to prevent
        // it. It refuses now.
        if (r.size() != 3) {
          return arrow::Status::Invalid(
              "a krn.ingest_windows row came back with ", r.size(),
              " field(s) instead of 3 while collecting the recorded windows "
              "whose completeness this DROP PARTITION invalidates. Refusing "
              "rather than skipping: a skipped row leaves a window asserting "
              "state=1 over a partition about to be emptied. Nothing has been "
              "dropped yet.");
        }
        RecordedWindow w;
        w.symbol = r[0];
        if (!parseExact(r[1], w.fromTsNs)) return badField("from_ts", r[1]);
        if (!parseExact(r[2], w.toTsNs)) return badField("to_ts", r[2]);
        intersecting.push_back(std::move(w));
      }
      const auto targets =
          tk.writesOwnWindow ? invalidationTargets(intersecting, st.rep.symbol,
                                                   st.rep.recordedFromNs,
                                                   st.rep.recordedToNs)
                             : intersecting;
      for (const auto& w : targets) {
        ARROW_RETURN_NOT_OK(writeWindowRow(
            client, w.symbol, tk.kind, w.fromTsNs, w.toTsNs,
            /*state=*/3, /*rowsInserted=*/0, st.sessionId,
            "invalidated by DROP PARTITION " + std::to_string(day) + " for " +
                st.rep.symbol));
        rep.invalidationRows.push_back(
            w.symbol + "/" + tk.kind + " " + std::to_string(w.fromTsNs) +
            ".." + std::to_string(w.toTsNs) + " -> state=3 (partition " +
            std::to_string(day) + ")");
      }
    }
  }

  // ---- state=0, in flight --------------------------------------------
  for (const auto& tk : tableKinds) {
    if (!tk.writesOwnWindow) continue;
    ARROW_RETURN_NOT_OK(writeWindowRow(client, st.rep.symbol, tk.kind,
                                       st.rep.recordedFromNs, st.rep.recordedToNs,
                                       /*state=*/0, /*rowsInserted=*/0,
                                       st.sessionId, st.rep.jobId));
  }
  // state=2 for every window this load owns, on any insert failure.
  const auto failOwned = [&](const arrow::Status& cause) -> arrow::Status {
    for (const auto& tk : tableKinds) {
      if (!tk.writesOwnWindow) continue;
      ARROW_RETURN_NOT_OK(writeWindowRow(client, st.rep.symbol, tk.kind,
                                         st.rep.recordedFromNs, st.rep.recordedToNs,
                                         /*state=*/2, 0, st.sessionId, st.rep.jobId));
    }
    return cause;
  };

  // ---- DROP PARTITION, then reload (D5) ------------------------------
  for (const auto& tk : tableKinds) {
    for (const std::uint32_t day : days) {
      std::ostringstream q;
      q << "ALTER TABLE krn." << tk.table << " DROP PARTITION " << day;
      ARROW_RETURN_NOT_OK(client.exec(q.str()).status());
      rep.partitionsDropped.push_back(tk.table + ":" + std::to_string(day));
    }
  }

  // ---- the snapshot first: the book the day's events apply to ----------
  //
  // One INSERT, ordered by provider_seq (ts_event is the one as-of instant),
  // which stageOne proved puts the Clear first.
  if (!st.snapshot.empty()) {
    std::vector<std::size_t> sorder(st.snapshot.size());
    for (std::size_t i = 0; i < sorder.size(); ++i) sorder[i] = i;
    std::stable_sort(sorder.begin(), sorder.end(), [&](std::size_t a, std::size_t b) {
      return st.snapshot[a].header.providerSeq < st.snapshot[b].header.providerSeq;
    });
    std::string body = "INSERT INTO krn.book_snapshots (";
    body += kBookSnapshotsColumns;
    body += ") FORMAT TabSeparated\n";
    for (const std::size_t i : sorder) {
      body += renderSnapshotRow(st.snapshot[i], st.rep.snapshotAsOfNs,
                                st.decoded.symbol, st.decoded.venue);
    }
    const arrow::Status ins = client.exec(body).status();
    if (!ins.ok()) return failOwned(ins);
    st.rep.snapshotRowsInserted = st.snapshot.size();
  }

  // ---- sort by the physical sort key, then insert ---------------------
  //
  // docs 8.6: "Insert symbol-sorted batches per day partition — the key leads
  // with symbol, so raw DBN file order (time-ordered) forces a per-part
  // re-sort. The ingest job owns this; a background merge cannot fix it after
  // the fact." At one symbol and one day this sort is the identity, which is
  // exactly why it goes in now: getting it wrong is invisible here and
  // expensive at the universe.
  //
  // One symbol, so the symbol leg of (symbol, ts_event, provider_seq) is
  // constant and `stable_sort` on the remaining two is the same order — with
  // ties left in arrival order, which matters because the key is NOT unique.
  // Sorted as indices and rendered per batch, so the rendered day is never
  // held whole.
  const bool trades = st.rep.kind == "trades";
  const std::size_t nRows = trades ? st.decoded.ticks.size() : st.decoded.book.size();
  const auto headerOf = [&](std::size_t i) -> const RecordHeader& {
    return trades ? st.decoded.ticks[i].header : st.decoded.book[i].header;
  };
  std::vector<std::size_t> order(nRows);
  for (std::size_t i = 0; i < nRows; ++i) order[i] = i;
  std::stable_sort(order.begin(), order.end(), [&](std::size_t a, std::size_t b) {
    const auto& ha = headerOf(a);
    const auto& hb = headerOf(b);
    return ha.tsEventNs != hb.tsEventNs ? ha.tsEventNs < hb.tsEventNs
                                        : ha.providerSeq < hb.providerSeq;
  });

  std::size_t inserted = 0;
  for (std::size_t begin = 0; begin < nRows; begin += kInsertBatchRows) {
    const std::size_t end = std::min(nRows, begin + kInsertBatchRows);
    std::string body = "INSERT INTO krn.";
    body += st.table;
    body += " (";
    body += trades ? kTradesColumns : kBookEventsColumns;
    body += ") FORMAT TabSeparated\n";
    for (std::size_t k = begin; k < end; ++k) {
      const std::size_t i = order[k];
      body += trades
                  ? renderTradeRow(st.decoded.ticks[i], st.decoded.symbol, st.decoded.venue)
                  : renderBookRow(st.decoded.book[i], st.decoded.symbol, st.decoded.venue);
    }
    const arrow::Status ins = client.exec(body).status();
    if (!ins.ok()) {
      // state=2, failed. Written before returning so the window never reads as
      // anything but unloaded: the state=0 row above is already the newest, and
      // this makes the failure explicit rather than leaving it in flight
      // forever.
      return failOwned(ins);
    }
    inserted = end;
  }
  st.rep.rowsInserted = inserted;

  // ---- symbology (SPEC D7, docs 8.9) ---------------------------------
  for (const auto& m : st.mappings) {
    const std::uint64_t validFrom = csv::midnightUtcNs(m.firstDay);
    // HALF-OPEN [valid_from, valid_to): valid_to is midnight of the day AFTER
    // the last day the mapping was observed on.
    const std::uint64_t validTo =
        csv::midnightUtcNs(m.lastDay) + 86400000000000ULL;
    std::ostringstream q;
    // `dataset` and `raw_symbol` come out of the vendor DBN's own metadata,
    // and `source_ref` is either a vendor job id or a filesystem path — all
    // three are quoted rather than concatenated (finding #5).
    q << "INSERT INTO krn.symbology (dataset, instrument_id, raw_symbol, "
         "valid_from, valid_to, source_ref, ingested_at) SELECT "
      << ch::sqlQuote(st.dataset) << ", " << m.instrumentId << ", "
      << ch::sqlQuote(m.rawSymbol) << ", fromUnixTimestamp64Nano(toInt64("
      << validFrom << "), 'UTC'), fromUnixTimestamp64Nano(toInt64(" << validTo
      << "), 'UTC'), "
      << ch::sqlQuote(st.rep.jobId.empty() ? st.rep.dbnPath : st.rep.jobId)
      << ", now64(3)";
    ARROW_RETURN_NOT_OK(client.exec(q.str()).status());
    ++rep.symbologyRows;
  }

  // ---- state=1, complete ---------------------------------------------
  //
  // The snapshot's window first: the day's book_events window must never read
  // complete while the seed it is replayed from does not (D11).
  if (!st.snapshot.empty()) {
    ARROW_RETURN_NOT_OK(writeWindowRow(client, st.rep.symbol, "book_snapshots",
                                       st.rep.recordedFromNs, st.rep.recordedToNs,
                                       /*state=*/1, st.rep.snapshotRowsInserted,
                                       st.sessionId, st.rep.jobId));
  }
  ARROW_RETURN_NOT_OK(writeWindowRow(client, st.rep.symbol, st.rep.kind,
                                     st.rep.recordedFromNs, st.rep.recordedToNs,
                                     /*state=*/1, inserted, st.sessionId,
                                     st.rep.jobId));
  return arrow::Status::OK();
}

}  // namespace

arrow::Result<Report> run(const Options& opts) {
  Report rep;
  rep.sessionIdRuling = kSessionIdRuling;
  rep.gapsNote        = kGapsNote;
  ARROW_ASSIGN_OR_RAISE(rep.target, ch::resolveTarget(opts.url));
  const ch::Client client(rep.target);

  // ---- a delivered vendor DBN job (ENC-1640) -----------------------------
  //
  // ONE DAY AT A TIME: stage it (the same raw half as below), load it (the
  // same drop/insert/record), then the next. Staging the whole job first, as
  // the two-kind path does, would hold a month of mbo in memory. The cost is
  // that a bad day 20 is found after days 1-19 are loaded; each of those is a
  // complete, independently recorded window, so nothing is claimed that is
  // not there, and a re-run reloads them exactly (D5).
  if (!opts.dbnJobDir.empty()) {
    if (!opts.scratchDir.empty()) {
      std::error_code ec;
      std::filesystem::create_directories(expandTilde(opts.scratchDir), ec);
      if (ec) {
        return arrow::Status::IOError("cannot create the scratch directory ",
                                      opts.scratchDir, ": ", ec.message());
      }
    }
    if (opts.scratchDir.empty()) {
      return arrow::Status::Invalid(
          "a DBN job needs a scratch directory (--scratch) to decompress each "
          ".dbn.zst into; the delivered files are never written.");
    }
    ARROW_ASSIGN_OR_RAISE(const DbnJobPlan plan, planDbnJob(opts.dbnJobDir, opts.days));
    const KindSpec& spec = plan.dbnSchema == "mbo" ? kSpecs[1] : kSpecs[0];
    if (!opts.noInsert) ARROW_RETURN_NOT_OK(preflight(client, spec.kind, spec.table));
    for (const auto& day : plan.days) {
      Staged st;
      ARROW_RETURN_NOT_OK(stageOne(opts, "", spec, &plan, &day, st));
      if (!opts.noInsert) ARROW_RETURN_NOT_OK(loadOne(client, st, rep));
      rep.kinds.push_back(st.rep);
    }
    return rep;
  }

  const std::string root = expandTilde(opts.dataRoot);

  // ---- pass 1: the raw half (transcode, retain, decode, round-trip) ------
  //
  // Every file becomes canonical rows BEFORE anything is dropped or inserted. A
  // DROP PARTITION followed by a decode failure would leave the partition empty
  // with nothing but a state=0 able to say so; doing the whole raw half first
  // means a bad file is refused with L1 untouched.
  std::vector<Staged> staged;
  for (const auto& spec : kSpecs) {
    Staged st;
    ARROW_RETURN_NOT_OK(stageOne(opts, root, spec, nullptr, nullptr, st));
    staged.push_back(std::move(st));
  }

  if (opts.noInsert) {
    for (const auto& st : staged) rep.kinds.push_back(st.rep);
    return rep;
  }

  for (const auto& st : staged) {
    ARROW_RETURN_NOT_OK(preflight(client, st.rep.kind, st.table));
  }

  // ---- pass 2: drop, insert, record -------------------------------------
  for (auto& st : staged) {
    ARROW_RETURN_NOT_OK(loadOne(client, st, rep));
    rep.kinds.push_back(st.rep);
  }

  return rep;
}

}  // namespace krn::data::ingest
