// B1 — ClickHouse -> krn transport, native protocol vs HTTP `FORMAT ArrowStream`
// (ENC-1485).
//
// Design: krn specs/2026-10-14-krn-data-layer/SPEC.md D13; docs/data-layer.md
// §5.2, §14.
//
// WHAT IS MEASURED: rows/sec from a live ClickHouse into this process's
// address space, TRANSPORT ONLY. A run is one `SELECT <every column> FROM
// krn.<table>` — no WHERE, no aggregate, no ORDER BY — and ends when the last
// column block has been decoded into in-process columns:
//
//   native  clickhouse-cpp `Client::Select`; a block is a `clickhouse::Block`
//           whose columns the library has decoded off the socket.
//   http    one keep-alive Boost.Beast connection; the response body is fed,
//           chunk by chunk as it arrives, to `arrow::ipc::StreamDecoder`; a
//           block is a decoded `arrow::RecordBatch`.
//
// Nothing is computed on the data inside the timed window. A per-transport
// checksum (row count, sum of `price`, sum of `size`) is taken on a separate,
// UNTIMED pass and the two transports must agree, so a fast transport that
// delivered the wrong rows cannot post a number.
//
// The SQL text is identical for both arms except the `FORMAT` clause, and both
// carry `SETTINGS max_block_size=<N>, max_threads=1`. Wire compression is OFF
// on both (`CompressionMethod::None`; `output_format_arrow_compression_method=
// none`): on loopback compression is CPU for nothing, and comparing an
// uncompressed native stream with ClickHouse's lz4_frame Arrow default would
// measure the codec, not the transport.
//
// Per run it records wall time, rows, non-empty blocks, bytes received on
// sockets (recv/recvmsg are interposed below), and heap allocations (malloc
// family interposed below, so Arrow's pool and clickhouse-cpp's columns are
// both counted, not only operator new). Connection setup is excluded: each arm
// holds one persistent connection, as a long scan would.
//
// CACHE STATE (D13):
//   warm  untimed warm-up runs, then timed runs back to back.
//   cold  before EVERY timed run: ClickHouse's own caches are dropped (mark,
//         uncompressed, query, mmap — each statement's acceptance is printed)
//         and the OS page cache is evicted for every file of the table's data
//         path with posix_fadvise(POSIX_FADV_DONTNEED), which needs no
//         privilege. Residency is READ BACK with mincore() before and after the
//         eviction and after the run, and a cold run whose eviction left a page
//         resident aborts. This is NOT `echo 3 > drop_caches` (root only):
//         dentries, inodes and the device's own cache are not dropped.
//
// It is an executable, not a ctest case, for krn_ingest's reason: it needs a
// live server, and a benchmark that skips when the server is absent reads as
// one that passed. Exit: 0 ran, 1 a finding (transports disagree, a cap not in
// force, a cold run that was not cold), 2 could not run.

#include "krn/data/Schema.hpp"

#include <clickhouse/client.h>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <dlfcn.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// ===========================================================================
// Instruments: heap allocations and socket bytes, process-wide.
// ===========================================================================
// Interposed in the executable, so every call from a statically linked library
// (Arrow, clickhouse-cpp, Boost.Asio) resolves here first. glibc exports the
// __libc_* entry points precisely so an interposer can forward to them.
extern "C" {
void* __libc_malloc(size_t);
void* __libc_calloc(size_t, size_t);
void* __libc_realloc(void*, size_t);
void* __libc_memalign(size_t, size_t);
void  __libc_free(void*);
}

namespace {
std::atomic<std::uint64_t> gAllocs{0};
std::atomic<std::uint64_t> gRecvBytes{0};
}  // namespace

extern "C" {
void* malloc(size_t n) {
  gAllocs.fetch_add(1, std::memory_order_relaxed);
  return __libc_malloc(n);
}
void* calloc(size_t a, size_t b) {
  gAllocs.fetch_add(1, std::memory_order_relaxed);
  return __libc_calloc(a, b);
}
void* realloc(void* p, size_t n) {
  gAllocs.fetch_add(1, std::memory_order_relaxed);
  return __libc_realloc(p, n);
}
void free(void* p) { __libc_free(p); }
int posix_memalign(void** out, size_t align, size_t n) {
  gAllocs.fetch_add(1, std::memory_order_relaxed);
  void* p = __libc_memalign(align, n);
  if (!p) return ENOMEM;
  *out = p;
  return 0;
}
void* aligned_alloc(size_t align, size_t n) {
  gAllocs.fetch_add(1, std::memory_order_relaxed);
  return __libc_memalign(align, n);
}
void* memalign(size_t align, size_t n) {
  gAllocs.fetch_add(1, std::memory_order_relaxed);
  return __libc_memalign(align, n);
}

// clickhouse-cpp reads with ::recv; Boost.Asio with ::recvmsg.
ssize_t recv(int fd, void* buf, size_t len, int flags) {
  using Fn = ssize_t (*)(int, void*, size_t, int);
  static Fn real = reinterpret_cast<Fn>(dlsym(RTLD_NEXT, "recv"));
  const ssize_t r = real(fd, buf, len, flags);
  if (r > 0) gRecvBytes.fetch_add(static_cast<std::uint64_t>(r), std::memory_order_relaxed);
  return r;
}
ssize_t recvmsg(int fd, struct msghdr* msg, int flags) {
  using Fn = ssize_t (*)(int, struct msghdr*, int);
  static Fn real = reinterpret_cast<Fn>(dlsym(RTLD_NEXT, "recvmsg"));
  const ssize_t r = real(fd, msg, flags);
  if (r > 0) gRecvBytes.fetch_add(static_cast<std::uint64_t>(r), std::memory_order_relaxed);
  return r;
}
}  // extern "C"

namespace {

namespace beast = boost::beast;
namespace http  = boost::beast::http;
using tcp = boost::asio::ip::tcp;
using Clock = std::chrono::steady_clock;

constexpr int kRan         = 0;
constexpr int kFinding     = 1;
constexpr int kCouldNotRun = 2;

struct CouldNotRun : std::runtime_error { using std::runtime_error::runtime_error; };
struct Finding     : std::runtime_error { using std::runtime_error::runtime_error; };

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------
struct Args {
  std::string host = "127.0.0.1";
  int nativePort = 0;
  int httpPort = 0;
  std::string database{krn::data::kL1Database};
  std::vector<std::string> tables{"book_events", "trades"};
  std::vector<std::size_t> blocks{16, 32, 64, 128, 256, 512, 1024};
  std::vector<std::string> transports{"native", "http"};
  std::vector<std::string> caches{"cold", "warm"};
  int warmIters = 200;
  int warmupIters = 5;
  int coldIters = 30;
  std::string hostDataDir;   // host path of the server's /var/lib/clickhouse
  std::string serverDataDir = "/var/lib/clickhouse/";
  std::string csvOut;
};

std::vector<std::string> splitList(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string tok;
  while (std::getline(ss, tok, ',')) if (!tok.empty()) out.push_back(tok);
  return out;
}

int parseInt(const std::string& flag, const std::string& v) {
  std::size_t pos = 0;
  long n = 0;
  try { n = std::stol(v, &pos); } catch (...) { pos = 0; }
  if (pos != v.size() || n < 0 || n > 1000000000)
    throw CouldNotRun(flag + " needs a non-negative integer (got '" + v + "')");
  return static_cast<int>(n);
}

void usage() {
  std::cout <<
      "usage: krn_b1_transport [options]\n"
      "  --host <h>             ClickHouse host (default 127.0.0.1)\n"
      "  --native-port <p>      default 9000 + STACK_PORT_OFFSET\n"
      "  --http-port <p>        default 8123 + STACK_PORT_OFFSET\n"
      "  --database <d>         default krn::data::kL1Database (\"krn\")\n"
      "  --tables <a,b>         default book_events,trades\n"
      "  --blocks <n,...>       max_block_size sweep (default 16,...,1024)\n"
      "  --transports <..>      native,http\n"
      "  --caches <..>          cold,warm\n"
      "  --warm-iters <n>       timed warm runs per cell (default 200)\n"
      "  --cold-iters <n>       timed cold runs per cell (default 30)\n"
      "  --host-data-dir <p>    host path bind-mounted at /var/lib/clickhouse;\n"
      "                         REQUIRED for cold (it is what gets evicted)\n"
      "  --csv <file>           write every timed run as a CSV row\n"
      "exit: 0 ran, 1 a finding, 2 could not run\n";
}

Args parseArgs(int argc, char** argv) {
  Args a;
  std::int64_t offset = 0;
  if (const char* o = std::getenv("STACK_PORT_OFFSET"); o && *o)
    offset = parseInt("STACK_PORT_OFFSET", o);
  a.nativePort = static_cast<int>(9000 + offset);
  a.httpPort   = static_cast<int>(8123 + offset);
  for (int i = 1; i < argc; ++i) {
    const std::string f = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) throw CouldNotRun(f + " needs a value");
      return argv[++i];
    };
    if (f == "--host") a.host = next();
    else if (f == "--native-port") a.nativePort = parseInt(f, next());
    else if (f == "--http-port") a.httpPort = parseInt(f, next());
    else if (f == "--database") a.database = next();
    else if (f == "--tables") a.tables = splitList(next());
    else if (f == "--blocks") {
      a.blocks.clear();
      for (const auto& b : splitList(next())) {
        const int n = parseInt(f, b);
        if (n == 0) throw CouldNotRun("--blocks: 0 is not a block size");
        a.blocks.push_back(static_cast<std::size_t>(n));
      }
    }
    else if (f == "--transports") a.transports = splitList(next());
    else if (f == "--caches") a.caches = splitList(next());
    else if (f == "--warm-iters") a.warmIters = parseInt(f, next());
    else if (f == "--cold-iters") a.coldIters = parseInt(f, next());
    else if (f == "--host-data-dir") a.hostDataDir = next();
    else if (f == "--csv") a.csvOut = next();
    else if (f == "-h" || f == "--help") { usage(); std::exit(kRan); }
    else throw CouldNotRun("unknown flag " + f + " (see --help)");
  }
  for (const auto& t : a.transports)
    if (t != "native" && t != "http") throw CouldNotRun("unknown transport " + t);
  for (const auto& c : a.caches)
    if (c != "cold" && c != "warm") throw CouldNotRun("unknown cache state " + c);
  if (a.tables.empty() || a.blocks.empty() || a.transports.empty() || a.caches.empty())
    throw CouldNotRun("an empty sweep measures nothing");
  if (std::find(a.caches.begin(), a.caches.end(), "cold") != a.caches.end() &&
      a.hostDataDir.empty())
    throw CouldNotRun("cold runs need --host-data-dir: without it nothing is evicted, "
                      "and a 'cold' number would be a warm one under another name");
  return a;
}

// ---------------------------------------------------------------------------
// One run's measurements
// ---------------------------------------------------------------------------
struct Sample {
  std::int64_t ns = 0;
  std::uint64_t rows = 0;
  std::uint64_t blocks = 0;
  std::uint64_t recvBytes = 0;
  std::uint64_t allocs = 0;
};

struct Checksum {
  std::uint64_t rows = 0;
  std::int64_t priceSum = 0;   // wrapping is fine: it is an identity, not a value
  std::uint64_t sizeSum = 0;
  bool operator==(const Checksum&) const = default;
};

std::string selectSql(const Args& a, const std::string& table, std::size_t block,
                      bool arrow) {
  std::ostringstream q;
  q << "SELECT * FROM " << a.database << "." << table
    << " SETTINGS max_block_size=" << block << ", max_threads=1";
  if (arrow) q << " FORMAT ArrowStream";
  return q.str();
}

// ---------------------------------------------------------------------------
// Native arm
// ---------------------------------------------------------------------------
clickhouse::ClientOptions nativeOptions(const Args& a) {
  clickhouse::ClientOptions o;
  o.SetHost(a.host)
      .SetPort(static_cast<std::uint16_t>(a.nativePort))
      .SetUser("default")
      .SetCompressionMethod(clickhouse::CompressionMethod::None)
      .SetPingBeforeQuery(false)
      .SetDefaultDatabase(a.database);
  return o;
}

Sample runNative(clickhouse::Client& c, const std::string& sql) {
  Sample s;
  const auto a0 = gAllocs.load(std::memory_order_relaxed);
  const auto b0 = gRecvBytes.load(std::memory_order_relaxed);
  const auto t0 = Clock::now();
  c.Select(sql, [&](const clickhouse::Block& b) {
    const auto n = b.GetRowCount();
    if (n) { s.rows += n; ++s.blocks; }
  });
  const auto t1 = Clock::now();
  s.allocs = gAllocs.load(std::memory_order_relaxed) - a0;
  s.recvBytes = gRecvBytes.load(std::memory_order_relaxed) - b0;
  s.ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  return s;
}

Checksum checksumNative(clickhouse::Client& c, const std::string& sql) {
  Checksum k;
  c.Select(sql, [&](const clickhouse::Block& b) {
    if (!b.GetRowCount()) return;
    std::optional<std::size_t> pi, si;
    for (std::size_t i = 0; i < b.GetColumnCount(); ++i) {
      if (b.GetColumnName(i) == "price") pi = i;
      if (b.GetColumnName(i) == "size")  si = i;
    }
    if (!pi || !si) throw Finding("native block has no price/size column");
    auto price = b[*pi]->As<clickhouse::ColumnInt64>();
    auto size  = b[*si]->As<clickhouse::ColumnUInt64>();
    if (!price || !size) throw Finding("native price/size are not Int64/UInt64");
    for (std::size_t r = 0; r < b.GetRowCount(); ++r) {
      k.priceSum = static_cast<std::int64_t>(static_cast<std::uint64_t>(k.priceSum) +
                                             static_cast<std::uint64_t>(price->At(r)));
      k.sizeSum += size->At(r);
    }
    k.rows += b.GetRowCount();
  });
  return k;
}

// ---------------------------------------------------------------------------
// HTTP ArrowStream arm
// ---------------------------------------------------------------------------
class ArrowHttp {
 public:
  explicit ArrowHttp(const Args& a) : a_(a), stream_(ioc_) {
    target_ = "/?database=" + a.database +
              "&output_format_arrow_compression_method=none";
    connect();
  }

  // Runs `sql`; every decoded RecordBatch is handed to `onBatch`. Returns
  // false if the server closed the connection afterwards (reconnect is done
  // OUTSIDE any timed window by the caller via `ensureConnected`).
  template <class OnBatch>
  void query(const std::string& sql, OnBatch&& onBatch) {
    struct Listener final : arrow::ipc::Listener {
      OnBatch* cb;
      explicit Listener(OnBatch* c) : cb(c) {}
      arrow::Status OnRecordBatchDecoded(
          std::shared_ptr<arrow::RecordBatch> batch) override {
        (*cb)(*batch);
        return arrow::Status::OK();
      }
    };
    auto listener = std::make_shared<Listener>(&onBatch);
    arrow::ipc::StreamDecoder decoder(listener);

    http::request<http::string_body> req{http::verb::post, target_, 11};
    req.set(http::field::host, a_.host);
    req.set(http::field::connection, "keep-alive");
    req.body() = sql;
    req.prepare_payload();
    http::write(stream_, req);

    http::response_parser<http::buffer_body> parser;
    parser.body_limit(boost::none);
    http::read_header(stream_, buf_, parser);
    const unsigned status = parser.get().result_int();
    std::string errBody;
    char chunk[64 * 1024];
    while (!parser.is_done()) {
      parser.get().body().data = chunk;
      parser.get().body().size = sizeof(chunk);
      beast::error_code ec;
      http::read(stream_, buf_, parser, ec);
      if (ec == http::error::need_buffer) ec = {};
      if (ec) throw CouldNotRun("http read: " + ec.message());
      const std::size_t got = sizeof(chunk) - parser.get().body().size;
      if (!got) continue;
      if (status != 200) { errBody.append(chunk, got); continue; }
      auto st = decoder.Consume(reinterpret_cast<const std::uint8_t*>(chunk),
                                static_cast<std::int64_t>(got));
      if (!st.ok()) throw Finding("ArrowStream decode: " + st.ToString());
    }
    keepAlive_ = parser.keep_alive();
    if (status != 200)
      throw CouldNotRun("http " + std::to_string(status) + ": " + errBody.substr(0, 400));
  }

  void ensureConnected() {
    if (!keepAlive_) { stream_.close(); buf_.clear(); connect(); }
  }

 private:
  void connect() {
    tcp::resolver r(ioc_);
    stream_.connect(r.resolve(a_.host, std::to_string(a_.httpPort)));
    stream_.socket().set_option(tcp::no_delay(true));
    keepAlive_ = true;
  }

  const Args& a_;
  boost::asio::io_context ioc_;
  beast::tcp_stream stream_;
  beast::flat_buffer buf_;
  std::string target_;
  bool keepAlive_ = true;
};

Sample runHttp(ArrowHttp& h, const std::string& sql) {
  h.ensureConnected();
  Sample s;
  const auto a0 = gAllocs.load(std::memory_order_relaxed);
  const auto b0 = gRecvBytes.load(std::memory_order_relaxed);
  const auto t0 = Clock::now();
  h.query(sql, [&](const arrow::RecordBatch& b) {
    const auto n = static_cast<std::uint64_t>(b.num_rows());
    if (n) { s.rows += n; ++s.blocks; }
  });
  const auto t1 = Clock::now();
  s.allocs = gAllocs.load(std::memory_order_relaxed) - a0;
  s.recvBytes = gRecvBytes.load(std::memory_order_relaxed) - b0;
  s.ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  return s;
}

Checksum checksumHttp(ArrowHttp& h, const std::string& sql) {
  h.ensureConnected();
  Checksum k;
  h.query(sql, [&](const arrow::RecordBatch& b) {
    auto price = std::dynamic_pointer_cast<arrow::Int64Array>(b.GetColumnByName("price"));
    auto size  = std::dynamic_pointer_cast<arrow::UInt64Array>(b.GetColumnByName("size"));
    if (!price || !size) throw Finding("ArrowStream price/size are not int64/uint64");
    for (std::int64_t r = 0; r < b.num_rows(); ++r) {
      k.priceSum = static_cast<std::int64_t>(static_cast<std::uint64_t>(k.priceSum) +
                                             static_cast<std::uint64_t>(price->Value(r)));
      k.sizeSum += size->Value(r);
    }
    k.rows += static_cast<std::uint64_t>(b.num_rows());
  });
  return k;
}

// ---------------------------------------------------------------------------
// Control plane: caps, cache drops, data paths — on its OWN native connection
// so the measured connections carry nothing but measured queries.
// ---------------------------------------------------------------------------
std::vector<std::string> selectStrings(clickhouse::Client& c, const std::string& sql) {
  std::vector<std::string> out;
  c.Select(sql, [&](const clickhouse::Block& b) {
    if (!b.GetRowCount()) return;
    auto col = b[0]->As<clickhouse::ColumnString>();
    if (!col) throw CouldNotRun("expected a String column from: " + sql);
    for (std::size_t r = 0; r < b.GetRowCount(); ++r) out.emplace_back(col->At(r));
  });
  return out;
}

std::string selectOne(clickhouse::Client& c, const std::string& sql) {
  auto v = selectStrings(c, sql);
  if (v.size() != 1) throw CouldNotRun("expected one row from: " + sql);
  return v[0];
}

// The statements a cold run issues, and whether this server accepted each.
const std::vector<std::string> kDropStatements = {
    "SYSTEM DROP MARK CACHE",
    "SYSTEM DROP UNCOMPRESSED CACHE",
    "SYSTEM DROP QUERY CACHE",
    "SYSTEM DROP MMAP CACHE",
};

std::vector<std::string> probeDrops(clickhouse::Client& c) {
  std::vector<std::string> ok;
  for (const auto& s : kDropStatements) {
    try { c.Execute(s); ok.push_back(s); std::cout << "  [ok]      " << s << "\n"; }
    catch (const std::exception& e) {
      std::cout << "  [refused] " << s << " — " << std::string(e.what()).substr(0, 120) << "\n";
    }
  }
  return ok;
}

// ---------------------------------------------------------------------------
// OS page cache: evict and read back, per table data path.
// ---------------------------------------------------------------------------
struct Residency { std::uint64_t files = 0, pages = 0, resident = 0; };

Residency residency(const std::vector<std::filesystem::path>& files) {
  Residency r;
  const long pg = sysconf(_SC_PAGESIZE);
  for (const auto& f : files) {
    const int fd = ::open(f.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) throw CouldNotRun("cannot open " + f.string() + ": " + std::strerror(errno));
    struct stat st{};
    ::fstat(fd, &st);
    ++r.files;
    if (st.st_size > 0) {
      void* m = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ, MAP_SHARED, fd, 0);
      if (m == MAP_FAILED) { ::close(fd); throw CouldNotRun("mmap " + f.string()); }
      const std::size_t n = static_cast<std::size_t>((st.st_size + pg - 1) / pg);
      std::vector<unsigned char> vec(n);
      if (::mincore(m, static_cast<size_t>(st.st_size), vec.data()) != 0) {
        ::munmap(m, static_cast<size_t>(st.st_size)); ::close(fd);
        throw CouldNotRun("mincore " + f.string());
      }
      r.pages += n;
      for (auto v : vec) r.resident += (v & 1u);
      ::munmap(m, static_cast<size_t>(st.st_size));
    }
    ::close(fd);
  }
  return r;
}

void evict(const std::vector<std::filesystem::path>& files) {
  for (const auto& f : files) {
    const int fd = ::open(f.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) throw CouldNotRun("cannot open " + f.string() + ": " + std::strerror(errno));
    const int rc = ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    ::close(fd);
    if (rc != 0) throw CouldNotRun("posix_fadvise " + f.string() + ": " + std::strerror(rc));
  }
}

std::vector<std::filesystem::path> tableFiles(clickhouse::Client& ctl, const Args& a,
                                              const std::string& table) {
  const auto paths = selectStrings(
      ctl, "SELECT arrayJoin(data_paths) FROM system.tables WHERE database = '" +
               a.database + "' AND name = '" + table + "'");
  if (paths.empty()) throw CouldNotRun("no data_paths for " + a.database + "." + table);
  std::vector<std::filesystem::path> out;
  for (const auto& p : paths) {
    if (p.rfind(a.serverDataDir, 0) != 0)
      throw CouldNotRun("data path " + p + " is not under " + a.serverDataDir);
    const std::filesystem::path host =
        std::filesystem::path(a.hostDataDir) / p.substr(a.serverDataDir.size());
    if (!std::filesystem::is_directory(host))
      throw CouldNotRun("host data path " + host.string() + " does not exist — is "
                        "--host-data-dir the directory bind-mounted at " + a.serverDataDir + "?");
    for (const auto& e : std::filesystem::recursive_directory_iterator(host))
      if (e.is_regular_file()) out.push_back(e.path());
  }
  if (out.empty()) throw CouldNotRun("no files under " + table + "'s data path");
  return out;
}

// ---------------------------------------------------------------------------
// Statistics and the table
// ---------------------------------------------------------------------------
double pct(std::vector<double> v, double p) {
  std::sort(v.begin(), v.end());
  const double idx = p * static_cast<double>(v.size() - 1);
  const auto lo = static_cast<std::size_t>(idx);
  const auto hi = std::min(lo + 1, v.size() - 1);
  return v[lo] + (v[hi] - v[lo]) * (idx - static_cast<double>(lo));
}

struct Cell {
  std::string table, transport, cache;
  std::size_t block = 0;
  std::vector<Sample> samples;
};

std::string fmtRate(double v) {
  std::ostringstream o;
  if (v >= 1e6) o << std::fixed << std::setprecision(2) << v / 1e6 << " M";
  else if (v >= 1e3) o << std::fixed << std::setprecision(1) << v / 1e3 << " k";
  else o << std::fixed << std::setprecision(0) << v << " ";
  return o.str();
}

void printTable(const std::vector<Cell>& cells) {
  std::cout << "\n| table | cache | block | transport | blocks/query | rows/s median"
               " | rows/s p10–p90 | µs/query median | wire bytes/query | wire MB/s median"
               " | allocs/query | allocs/block |\n"
               "|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n";
  for (const auto& c : cells) {
    std::vector<double> rps, us, mbps, allocs;
    for (const auto& s : c.samples) {
      const double sec = static_cast<double>(s.ns) / 1e9;
      rps.push_back(static_cast<double>(s.rows) / sec);
      us.push_back(static_cast<double>(s.ns) / 1e3);
      mbps.push_back(static_cast<double>(s.recvBytes) / sec / 1e6);
      allocs.push_back(static_cast<double>(s.allocs));
    }
    const auto& s0 = c.samples.front();
    const double medAllocs = pct(allocs, 0.5);
    std::cout << "| " << c.table << " | " << c.cache << " | " << c.block << " | "
              << c.transport << " | " << s0.blocks << " | " << fmtRate(pct(rps, 0.5))
              << "| " << fmtRate(pct(rps, 0.1)) << "– " << fmtRate(pct(rps, 0.9))
              << "| " << std::fixed << std::setprecision(0) << pct(us, 0.5) << " | "
              << s0.recvBytes << " | " << std::setprecision(1) << pct(mbps, 0.5) << " | "
              << std::setprecision(0) << medAllocs << " | " << std::setprecision(1)
              << medAllocs / static_cast<double>(s0.blocks) << " |\n";
  }
}

std::string cpuList() {
  cpu_set_t set;
  CPU_ZERO(&set);
  if (sched_getaffinity(0, sizeof(set), &set) != 0) return "?";
  std::string out;
  for (int i = 0; i < CPU_SETSIZE; ++i)
    if (CPU_ISSET(i, &set)) out += (out.empty() ? "" : ",") + std::to_string(i);
  return out;
}

int run(int argc, char** argv) {
  const Args a = parseArgs(argc, argv);

  std::cout << "=== B1: ClickHouse -> krn transport (ENC-1485) ===\n"
            << "native  " << a.host << ":" << a.nativePort << "   http  " << a.host << ":"
            << a.httpPort << "   database " << a.database << "\n"
            << "this process runs on CPUs: " << cpuList() << "\n";

  clickhouse::Client ctl(nativeOptions(a));
  std::cout << "server: " << selectOne(ctl, "SELECT version()") << "\n";

  // ---- D13: the caps, read back from the server, not from the config ----
  const auto serverMem = std::stoull(selectOne(
      ctl, "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'"));
  const auto queryMem = std::stoull(selectOne(
      ctl, "SELECT value FROM system.settings WHERE name = 'max_memory_usage'"));
  const auto markCache = std::stoull(selectOne(
      ctl, "SELECT value FROM system.server_settings WHERE name = 'mark_cache_size'"));
  const auto uncCache = std::stoull(selectOne(
      ctl, "SELECT value FROM system.server_settings WHERE name = 'uncompressed_cache_size'"));
  std::cout << "caps (read back): max_server_memory_usage=" << serverMem
            << " max_memory_usage=" << queryMem << " mark_cache_size=" << markCache
            << " uncompressed_cache_size=" << uncCache << "\n";
  if (serverMem == 0 || queryMem == 0)
    throw Finding("a ClickHouse memory cap is 0 (unlimited): SPEC D13 says this run "
                  "would not be a transport measurement. `tools/stack.sh up clickhouse`.");

  std::cout << "cache-drop statements:\n";
  const auto drops = probeDrops(ctl);

  // ---- rows and the checksum identity, untimed ----
  clickhouse::Client nat(nativeOptions(a));
  ArrowHttp http(a);
  std::map<std::string, std::vector<std::filesystem::path>> files;
  for (const auto& t : a.tables) {
    const auto sql = selectSql(a, t, 65536, false);
    const Checksum kn = checksumNative(nat, sql);
    const Checksum kh = checksumHttp(http, selectSql(a, t, 65536, true));
    std::cout << a.database << "." << t << ": native rows=" << kn.rows << " sum(price)="
              << kn.priceSum << " sum(size)=" << kn.sizeSum << " | http rows=" << kh.rows
              << " sum(price)=" << kh.priceSum << " sum(size)=" << kh.sizeSum << "\n";
    if (!(kn == kh)) throw Finding("the two transports delivered different rows for " + t);
    if (kn.rows == 0) throw CouldNotRun(a.database + "." + t + " is empty: ingest first");
    if (!a.hostDataDir.empty()) {
      files[t] = tableFiles(ctl, a, t);
      const auto r = residency(files[t]);
      std::cout << "  data files: " << r.files << ", " << r.pages << " pages ("
                << r.resident << " resident now)\n";
    }
  }

  std::ofstream csv;
  if (!a.csvOut.empty()) {
    csv.open(a.csvOut);
    if (!csv) throw CouldNotRun("cannot write " + a.csvOut);
    csv << "table,cache,block,transport,iter,ns,rows,blocks,recv_bytes,allocs\n";
  }

  std::vector<Cell> cells;
  std::uint64_t coldChecked = 0;
  // Sequential protocol (D13): one query in flight at a time, and the two
  // transports alternate per block size so slow drift lands on both.
  for (const auto& t : a.tables)
    for (const auto& cache : a.caches)
      for (const auto block : a.blocks)
        for (const auto& tr : a.transports) {
          Cell c{t, tr, cache, block, {}};
          const bool arrow = tr == "http";
          const auto sql = selectSql(a, t, block, arrow);
          auto once = [&]() { return arrow ? runHttp(http, sql) : runNative(nat, sql); };
          const bool cold = cache == "cold";
          if (!cold) for (int i = 0; i < a.warmupIters; ++i) (void)once();
          const int iters = cold ? a.coldIters : a.warmIters;
          for (int i = 0; i < iters; ++i) {
            if (cold) {
              for (const auto& s : drops) ctl.Execute(s);
              evict(files[t]);
              const auto r = residency(files[t]);
              if (r.resident != 0)
                throw Finding("cold run not cold: " + std::to_string(r.resident) +
                              " page(s) of " + t + " still resident after eviction");
              ++coldChecked;
            }
            const Sample s = once();
            if (cold && i == 0) {
              const auto r = residency(files[t]);
              std::cout << "  cold " << t << " " << tr << " block " << block
                        << ": resident after eviction 0, after the run " << r.resident
                        << "/" << r.pages << " pages\n";
            }
            c.samples.push_back(s);
            if (csv) csv << t << "," << cache << "," << block << "," << tr << "," << i << ","
                         << s.ns << "," << s.rows << "," << s.blocks << "," << s.recvBytes
                         << "," << s.allocs << "\n";
          }
          if (c.samples.empty()) throw CouldNotRun("zero iterations for a cell");
          for (const auto& s : c.samples)
            if (s.rows != c.samples.front().rows || s.rows == 0)
              throw Finding("row count changed between runs of one cell");
          cells.push_back(std::move(c));
        }

  printTable(cells);
  std::cout << "\ncold runs whose eviction was read back as 0 resident pages: "
            << coldChecked << "\n";
  return kRan;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return run(argc, argv);
  } catch (const Finding& e) {
    std::cerr << "krn_b1_transport: FINDING: " << e.what() << "\n";
    return kFinding;
  } catch (const CouldNotRun& e) {
    std::cerr << "krn_b1_transport: COULD NOT RUN: " << e.what() << "\n";
    return kCouldNotRun;
  } catch (const std::exception& e) {
    std::cerr << "krn_b1_transport: COULD NOT RUN: " << e.what() << "\n";
    return kCouldNotRun;
  }
}
