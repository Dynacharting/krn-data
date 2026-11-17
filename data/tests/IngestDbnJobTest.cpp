//
// A delivered vendor DBN job as an ingest input (ENC-1640).
//
// TWO HALVES. The first builds job DIRECTORIES — metadata.json, manifest.json,
// condition.json and EMPTY day files — to drive `planDbnJob`'s refusals and
// window arithmetic. No market data is synthesized: the day files are
// zero-byte placeholders whose names and manifest sizes are the subject, and
// nothing decodes them. The second reads Nick's real NVDA trades job at
// `KRN_DATA_NVDA_TRADES_JOB` (data/CMakeLists.txt) through `ingest::run` with
// no insert; a missing job FAILS, never skips (a skip reads like a pass,
// ENC-1095). Licensed vendor data stays outside every working tree.
//

#include "krn/data/IngestJob.hpp"
#include "krn/data/Verdict.hpp"

#include <arrow/util/compression.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

using namespace krn::data;

class JobDir {
 public:
  JobDir() {
    for (int attempt = 0;; ++attempt) {
      path_ = std::filesystem::temp_directory_path() /
              ("ENC-1640-dbnjob-" + std::to_string(::getpid()) + "-" +
               std::to_string(counter_++) + "-" + std::to_string(attempt));
      std::error_code ec;
      if (std::filesystem::create_directories(path_, ec)) return;
      if (attempt > 16) {
        ADD_FAILURE() << "cannot create a temp directory: " << ec.message();
        return;
      }
    }
  }
  ~JobDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  JobDir(const JobDir&) = delete;
  JobDir& operator=(const JobDir&) = delete;

  void write(const std::string& name, const std::string& content) const {
    std::ofstream out(path_ / name, std::ios::binary | std::ios::trunc);
    out << content;
  }
  std::string dir() const { return path_.string(); }

 private:
  std::filesystem::path path_;
  static inline int counter_ = 0;
};

// 2026-10-27T00:00Z and the next two midnights.
constexpr std::uint64_t k1027 = 1793059200000000000ULL;
constexpr std::uint64_t kDay  = 86400000000000ULL;

std::string metadata(const std::string& schema, const std::string& encoding,
                     std::uint64_t start, std::uint64_t end,
                     const std::string& compression = "\"zstd\"",
                     const std::string& split = "\"day\"") {
  return R"({"version":1,"job_id":"XNAS-TEST-JOB","query":{"dataset":"XNAS.ITCH",)"
         R"("schema":")" + schema + R"(","symbols":["NVDA"],"stype_in":"raw_symbol",)"
         R"("stype_out":"instrument_id","start":)" + std::to_string(start) +
         R"(,"end":)" + std::to_string(end) + R"(,"limit":null,"encoding":")" +
         encoding + R"(","compression":)" + compression +
         R"(},"customizations":{"pretty_px":false,"pretty_ts":false,)"
         R"("map_symbols":false,"split_symbols":false,"split_duration":)" + split +
         "}}";
}

// A job with one zero-byte file per day in `days`, of `fileSchema`.
void makeJob(const JobDir& j, const std::string& schema, const std::string& fileSchema,
             const std::vector<std::string>& days, std::uint64_t start,
             std::uint64_t end, const std::string& encoding = "dbn") {
  j.write("metadata.json", metadata(schema, encoding, start, end));
  std::string manifest = R"({"job_id":"XNAS-TEST-JOB","files":[)";
  std::string condition = "[";
  for (std::size_t i = 0; i < days.size(); ++i) {
    const std::string name = "xnas-itch-" + days[i] + "." + fileSchema + ".dbn.zst";
    j.write(name, "");
    manifest += (i ? "," : "") + std::string(R"({"filename":")") + name +
                R"(","size":0})";
    condition += (i ? "," : "") + std::string(R"({"date":")") + days[i].substr(0, 4) +
                 "-" + days[i].substr(4, 2) + "-" + days[i].substr(6, 2) +
                 R"(","condition":"available"})";
  }
  j.write("manifest.json", manifest + "]}");
  j.write("condition.json", condition + "]");
}

TEST(IngestDbnJob, AValidJobPlansOneWindowPerDayFile) {
  JobDir j;
  makeJob(j, "trades", "trades", {"20261028", "20261027"}, k1027, k1027 + 2 * kDay);
  auto plan = ingest::planDbnJob(j.dir(), {});
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  EXPECT_EQ(plan->kind, "trades");
  EXPECT_EQ(plan->md.jobId, "XNAS-TEST-JOB");
  ASSERT_EQ(plan->days.size(), 2u);
  EXPECT_EQ(plan->days[0].day, 20261027u);  // sorted, whatever the manifest order
  EXPECT_EQ(plan->days[0].fromNs, static_cast<std::int64_t>(k1027));
  EXPECT_EQ(plan->days[0].toNs, static_cast<std::int64_t>(k1027 + kDay));
  EXPECT_EQ(plan->days[1].fromNs, static_cast<std::int64_t>(k1027 + kDay));

  auto one = ingest::planDbnJob(j.dir(), {20261028});
  ASSERT_TRUE(one.ok()) << one.status().ToString();
  ASSERT_EQ(one->days.size(), 1u);
  EXPECT_EQ(one->days[0].day, 20261028u);
}

TEST(IngestDbnJob, TheWindowIsTheUtcDayIntersectedWithTheQuery) {
  // A query that starts at 13:30 and ends at 20:00 of the same day.
  const std::uint64_t from = k1027 + 13 * 3600000000000ULL + 1800000000000ULL;
  const std::uint64_t to   = k1027 + 20 * 3600000000000ULL;
  const auto w = ingest::dayWindow(20261027, from, to);
  EXPECT_EQ(w.first, static_cast<std::int64_t>(from));
  EXPECT_EQ(w.second, static_cast<std::int64_t>(to));
  const auto whole = ingest::dayWindow(20261027, k1027 - kDay, k1027 + 5 * kDay);
  EXPECT_EQ(whole.first, static_cast<std::int64_t>(k1027));
  EXPECT_EQ(whole.second, static_cast<std::int64_t>(k1027 + kDay));
  const auto none = ingest::dayWindow(20261029, from, to);
  EXPECT_LE(none.second, none.first);

  JobDir j;
  makeJob(j, "mbo", "mbo", {"20261027"}, from, to);
  auto plan = ingest::planDbnJob(j.dir(), {});
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  EXPECT_EQ(plan->kind, "book_events");
  EXPECT_EQ(plan->days[0].fromNs, static_cast<std::int64_t>(from));
  EXPECT_EQ(plan->days[0].toNs, static_cast<std::int64_t>(to));
}

// The three refusals the ticket names, each COULD-NOT-RUN (exit 2).
TEST(IngestDbnJob, AMissingMetadataJsonIsExit2) {
  JobDir j;
  makeJob(j, "trades", "trades", {"20261027"}, k1027, k1027 + kDay);
  std::filesystem::remove(std::filesystem::path(j.dir()) / "metadata.json");
  const auto plan = ingest::planDbnJob(j.dir(), {});
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(exitCodeFor(plan.status()), 2) << plan.status().ToString();
}

TEST(IngestDbnJob, AMismatchedSchemaIsExit2) {
  JobDir j;
  makeJob(j, "trades", "mbo", {"20261027"}, k1027, k1027 + kDay);
  const auto plan = ingest::planDbnJob(j.dir(), {});
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(exitCodeFor(plan.status()), 2) << plan.status().ToString();
  EXPECT_NE(plan.status().message().find("`mbo` file"), std::string::npos)
      << plan.status().ToString();
}

TEST(IngestDbnJob, EncodingOtherThanDbnIsExit2) {
  JobDir j;
  makeJob(j, "trades", "trades", {"20261027"}, k1027, k1027 + kDay, "csv");
  const auto plan = ingest::planDbnJob(j.dir(), {});
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(exitCodeFor(plan.status()), 2) << plan.status().ToString();
  EXPECT_NE(plan.status().message().find("encoding"), std::string::npos);
}

// ENC-1640 review finding 2: a job-level limit is cut across day files and
// the short day cannot be identified, so it is refused.
TEST(IngestDbnJob, AJobLevelLimitIsExit2) {
  JobDir j;
  makeJob(j, "trades", "trades", {"20261027"}, k1027, k1027 + kDay);
  std::string m = metadata("trades", "dbn", k1027, k1027 + kDay);
  const auto at = m.find("\"limit\":null");
  ASSERT_NE(at, std::string::npos);
  m.replace(at, 12, "\"limit\":1000");
  j.write("metadata.json", m);
  const auto plan = ingest::planDbnJob(j.dir(), {});
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(exitCodeFor(plan.status()), 2) << plan.status().ToString();
  EXPECT_NE(plan.status().message().find("limit"), std::string::npos);
}

TEST(IngestDbnJob, CompressionSplitManifestAndDayAreChecked) {
  {
    JobDir j;
    makeJob(j, "trades", "trades", {"20261027"}, k1027, k1027 + kDay);
    j.write("metadata.json", metadata("trades", "dbn", k1027, k1027 + kDay, "null"));
    const auto plan = ingest::planDbnJob(j.dir(), {});
    ASSERT_FALSE(plan.ok());
    EXPECT_EQ(exitCodeFor(plan.status()), 2);
  }
  {
    JobDir j;
    makeJob(j, "trades", "trades", {"20261027"}, k1027, k1027 + kDay);
    j.write("metadata.json",
            metadata("trades", "dbn", k1027, k1027 + kDay, "\"zstd\"", "null"));
    const auto plan = ingest::planDbnJob(j.dir(), {});
    ASSERT_FALSE(plan.ok());
    EXPECT_EQ(exitCodeFor(plan.status()), 2);
  }
  {
    // A file whose size disagrees with the manifest.
    JobDir j;
    makeJob(j, "trades", "trades", {"20261027"}, k1027, k1027 + kDay);
    j.write("xnas-itch-20261027.trades.dbn.zst", "x");
    const auto plan = ingest::planDbnJob(j.dir(), {});
    ASSERT_FALSE(plan.ok());
    EXPECT_EQ(exitCodeFor(plan.status()), 2);
  }
  {
    // A requested day with no file, and a day file outside the query.
    JobDir j;
    makeJob(j, "trades", "trades", {"20261027"}, k1027, k1027 + kDay);
    EXPECT_EQ(exitCodeFor(ingest::planDbnJob(j.dir(), {20261030}).status()), 2);
    JobDir k;
    makeJob(k, "trades", "trades", {"20261029"}, k1027, k1027 + kDay);
    EXPECT_EQ(exitCodeFor(ingest::planDbnJob(k.dir(), {}).status()), 2);
  }
}

TEST(IngestDbnJob, DecompressesEveryFrameAndRefusesATruncatedOne) {
  ASSERT_TRUE(arrow::util::Codec::IsAvailable(arrow::Compression::ZSTD));
  auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();
  const auto frame = [&](const std::string& s) {
    std::vector<std::uint8_t> out(
        static_cast<std::size_t>(codec->MaxCompressedLen(
            static_cast<std::int64_t>(s.size()),
            reinterpret_cast<const std::uint8_t*>(s.data()))));
    const auto n = codec
                       ->Compress(static_cast<std::int64_t>(s.size()),
                                  reinterpret_cast<const std::uint8_t*>(s.data()),
                                  static_cast<std::int64_t>(out.size()), out.data())
                       .ValueOrDie();
    out.resize(static_cast<std::size_t>(n));
    return std::string(out.begin(), out.end());
  };
  // Two frames, the first larger than the decompressor's 4 MiB output buffer.
  const std::string a(5u << 20, 'a');
  const std::string b = "second frame";
  JobDir j;
  j.write("two.zst", frame(a) + frame(b));
  const std::string out = j.dir() + "/two";
  auto n = ingest::decompressZstdFile(j.dir() + "/two.zst", out);
  ASSERT_TRUE(n.ok()) << n.status().ToString();
  EXPECT_EQ(*n, a.size() + b.size());
  std::ifstream in(out, std::ios::binary);
  const std::string got((std::istreambuf_iterator<char>(in)), {});
  EXPECT_EQ(got, a + b);

  const std::string whole = frame(b);
  j.write("cut.zst", whole.substr(0, whole.size() - 3));
  auto cut = ingest::decompressZstdFile(j.dir() + "/cut.zst", j.dir() + "/cut");
  EXPECT_FALSE(cut.ok());
  EXPECT_FALSE(std::filesystem::exists(j.dir() + "/cut"))
      << "a refused decompression leaves no partial scratch file";
}

// THE REAL JOB. One day of NVDA trades, decoded from the delivered .dbn.zst
// with no insert. ClickHouse is not contacted (`noInsert`); resolveTarget does
// no I/O.
TEST(IngestDbnJob, RealNvdaTradesDayDecodesEveryRecordIntoTheDayWindow) {
  const std::string job = KRN_DATA_NVDA_TRADES_JOB;
  const std::string file = job + "/xnas-itch-20261027.trades.dbn.zst";
  ASSERT_TRUE(std::filesystem::exists(file))
      << file << " is missing. Point -DKRN_DATA_NVDA_TRADES_JOB=<dir> at the "
                 "delivered NVDA trades job (ENC-1640); this case fails rather "
                 "than skips.";
  const auto sizeBefore = std::filesystem::file_size(file);
  const auto mtimeBefore = std::filesystem::last_write_time(file);

  JobDir scratch;
  ingest::Options o;
  o.dbnJobDir  = job;
  o.scratchDir = scratch.dir();
  o.days       = {20261027};
  o.noInsert   = true;
  auto rep = ingest::run(o);
  ASSERT_TRUE(rep.ok()) << rep.status().ToString();
  ASSERT_EQ(rep->kinds.size(), 1u);
  const auto& k = rep->kinds[0];
  EXPECT_EQ(k.kind, "trades");
  EXPECT_EQ(k.symbol, "NVDA") << "resolved from the DBN's own symbology (D7)";
  EXPECT_GT(k.decodedRecords, 0u);
  EXPECT_EQ(k.canonicalRows, k.decodedRecords);
  EXPECT_EQ(k.snapshotExcluded, 0u);
  EXPECT_EQ(k.recordedFromNs, static_cast<std::int64_t>(k1027));
  EXPECT_EQ(k.recordedToNs, static_cast<std::int64_t>(k1027 + kDay));
  EXPECT_FALSE(k.truncatedByLimit);
  EXPECT_TRUE(k.roundTripOk) << k.roundTripNote;
  EXPECT_EQ(k.jobId, "XNAS-20261117-R7EH4L6T8P");

  EXPECT_EQ(std::filesystem::file_size(file), sizeBefore);
  EXPECT_EQ(std::filesystem::last_write_time(file), mtimeBefore)
      << "the delivered file is the D6 retained file and is never written";
  EXPECT_TRUE(std::filesystem::is_empty(scratch.dir()))
      << "the decompressed copy is deleted once decoded";
}

// ENC-1640 review finding 3: a file already in --scratch under the name the
// decompressed copy would naturally take is neither overwritten nor deleted.
TEST(IngestDbnJob, AnExistingScratchFileIsNotClobbered) {
  const std::string job = KRN_DATA_NVDA_TRADES_JOB;
  ASSERT_TRUE(std::filesystem::exists(job + "/xnas-itch-20261027.trades.dbn.zst"));
  JobDir scratch;
  scratch.write("xnas-itch-20261027.trades.dbn", "somebody else's file");
  ingest::Options o;
  o.dbnJobDir  = job;
  o.scratchDir = scratch.dir();
  o.days       = {20261027};
  o.noInsert   = true;
  auto rep = ingest::run(o);
  ASSERT_TRUE(rep.ok()) << rep.status().ToString();
  std::ifstream in(scratch.dir() + "/xnas-itch-20261027.trades.dbn", std::ios::binary);
  const std::string got((std::istreambuf_iterator<char>(in)), {});
  EXPECT_EQ(got, "somebody else's file");
  std::size_t n = 0;
  for ([[maybe_unused]] const auto& e : std::filesystem::directory_iterator(scratch.dir())) ++n;
  EXPECT_EQ(n, 1u) << "and the run's own scratch copy is gone";
}

// ENC-1640 review finding 4: the ticker the DBN's own symbology resolves to
// must be one query.symbols names. A metadata.json naming another symbol,
// beside symlinks to the real files, is refused with exit 2.
TEST(IngestDbnJob, ADecodedSymbolNotInTheQueryIsExit2) {
  const std::string job = KRN_DATA_NVDA_TRADES_JOB;
  ASSERT_TRUE(std::filesystem::exists(job + "/metadata.json"));
  JobDir j;
  for (const auto& e : std::filesystem::directory_iterator(job)) {
    const auto name = e.path().filename().string();
    if (name == "metadata.json" || e.path().extension() == ".zip") continue;
    std::filesystem::create_symlink(e.path(), std::filesystem::path(j.dir()) / name);
  }
  std::ifstream in(job + "/metadata.json", std::ios::binary);
  std::string m((std::istreambuf_iterator<char>(in)), {});
  const auto at = m.find("\"NVDA\"");
  ASSERT_NE(at, std::string::npos);
  m.replace(at, 6, "\"MSFT\"");
  j.write("metadata.json", m);
  JobDir scratch;
  ingest::Options o;
  o.dbnJobDir  = j.dir();
  o.scratchDir = scratch.dir();
  o.days       = {20261027};
  o.noInsert   = true;
  auto rep = ingest::run(o);
  ASSERT_FALSE(rep.ok());
  EXPECT_EQ(exitCodeFor(rep.status()), 2) << rep.status().ToString();
  EXPECT_NE(rep.status().message().find("query.symbols"), std::string::npos)
      << rep.status().ToString();
}

}  // namespace
