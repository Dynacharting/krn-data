//
// krn.gaps: a seq jump is never inferred to be a hole (ENC-1506).
//
// Design: specs/2026-10-14-krn-data-layer/SPEC.md D11;
//         docs/data-layer.md §8.3, §8.4, §8.8.
//
// TWO HALVES, AND THE SECOND READS THE REAL SAMPLE ON PURPOSE.
//
// `GapsScope` and `GapsInfer` use invented sequence points: they test the
// refusal's shape, which no particular file decides.
//
// `GapsRealSample` departs from DatabentoCsvTest.cpp's rule ("no row of the
// real sample appears here") because the ticket's acceptance is a property OF
// the real sample: a gap writer that cannot be shown to stay silent on known-
// good data must not ship, and invented sequence numbers can be made to say
// anything. It still embeds no row — only counts — and reads the retained DBN
// files through `ingest::run`, the job's own reload path, from
// `KRN_DATA_SAMPLE_DIR` (data/CMakeLists.txt). A missing sample FAILS: a skip
// reads exactly like a pass (ENC-1095). Configure with
// `-DKRN_DATA_SAMPLE_DIR=<dir>` where the sample lives elsewhere.
//
// The naive counts pinned below were measured twice, independently: by this
// job over the retained DBN, and by a Python pass over the vendor CSVs
// (adjacent rows with `sequence[i+1] > sequence[i] + 1`): 434 mbo, 610 trades.
//

#include "krn/data/Gaps.hpp"

#include "krn/data/DatabentoCsv.hpp"
#include "krn/data/IngestJob.hpp"
#include "krn/data/Verdict.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace krn::data::gaps {
namespace {

dbn::Metadata metaWithSymbols(std::vector<std::string> symbols) {
  dbn::Metadata md;
  md.dataset = "XNAS.ITCH";
  md.symbols = std::move(symbols);
  return md;
}

std::vector<SeqPoint> points(const std::vector<std::uint64_t>& seqs,
                             std::uint16_t flagsOnLast = 0) {
  std::vector<SeqPoint> out;
  std::int64_t ts = 1654864201000000000LL;
  for (const auto s : seqs) out.push_back(SeqPoint{ts++, s, 0});
  if (!out.empty()) out.back().flags = flagsOnLast;
  return out;
}

// ---------------------------------------------------------------------------
// The scope comes from the metadata's own symbol list.
// ---------------------------------------------------------------------------
TEST(GapsScope, ANamedSymbolIsAFilteredPull) {
  EXPECT_EQ(pullScope(metaWithSymbols({"MSFT"})), PullScope::SymbolFiltered);
  EXPECT_EQ(pullScope(metaWithSymbols({"MSFT", "AAPL"})), PullScope::SymbolFiltered);
}

TEST(GapsScope, AllSymbolsIsRecognisedOnlyAlone) {
  EXPECT_EQ(pullScope(metaWithSymbols({kAllSymbols})), PullScope::AllSymbols);
  // Mixed with a ticker it is not the all-symbols form; treat as filtered.
  EXPECT_EQ(pullScope(metaWithSymbols({kAllSymbols, "MSFT"})),
            PullScope::SymbolFiltered);
}

TEST(GapsScope, NoSymbolListIsUnknownNotUnfiltered) {
  EXPECT_EQ(pullScope(metaWithSymbols({})), PullScope::Unknown);
}

// ---------------------------------------------------------------------------
// The refusal: no seq-derived row on any branch, the naive count reported.
// ---------------------------------------------------------------------------
TEST(GapsInfer, AFilteredPullWithJumpsWritesNoRowAndCountsThem) {
  // The real sample's opening steps: +7 and +19638 are two jumps; the repeat
  // (a trade and its fill share a sequence number) is not.
  const auto pts = points({11404021, 11404021, 11404028, 11423666});
  const Inference inf = infer(metaWithSymbols({"MSFT"}), pts);
  EXPECT_EQ(inf.scope, PullScope::SymbolFiltered);
  EXPECT_TRUE(inf.rows.empty());
  EXPECT_EQ(inf.naiveSeqJumps, 2u);
  EXPECT_NE(inf.refusal.find("symbol-filtered"), std::string::npos) << inf.refusal;
}

TEST(GapsInfer, AnAllSymbolsPullIsRefusedAsUnmeasuredNotAsWrong) {
  const auto pts = points({100, 101, 105});
  const Inference inf = infer(metaWithSymbols({kAllSymbols}), pts);
  EXPECT_EQ(inf.scope, PullScope::AllSymbols);
  EXPECT_TRUE(inf.rows.empty());
  EXPECT_EQ(inf.naiveSeqJumps, 1u);
  EXPECT_NE(inf.refusal.find("UNMEASURED"), std::string::npos) << inf.refusal;
}

TEST(GapsInfer, AnUnknownScopeIsRefusedLikeAFilteredOne) {
  const Inference inf = infer(metaWithSymbols({}), points({1, 9}));
  EXPECT_EQ(inf.scope, PullScope::Unknown);
  EXPECT_TRUE(inf.rows.empty());
  EXPECT_EQ(inf.naiveSeqJumps, 1u);
  EXPECT_NE(inf.refusal.find("no symbol list"), std::string::npos) << inf.refusal;
}

TEST(GapsInfer, RepeatsAndUnitStepsAreNotJumps) {
  const Inference inf =
      infer(metaWithSymbols({"MSFT"}), points({7, 7, 8, 9, 9, 10}));
  EXPECT_EQ(inf.naiveSeqJumps, 0u);
  EXPECT_TRUE(inf.rows.empty());
}

TEST(GapsInfer, MaybeBadBookIsCountedAndNeverBecomesARow) {
  const Inference inf = infer(metaWithSymbols({"MSFT"}),
                              points({1, 2, 3}, dbn::kFlagMaybeBadBook));
  EXPECT_EQ(inf.vendorMaybeBadBook, 1u);
  EXPECT_TRUE(inf.rows.empty());
  // Only the flags' low byte is DBN's; krn's own high-byte bit is not it.
  const Inference krnBit = infer(metaWithSymbols({"MSFT"}),
                                 points({1, 2}, kKrnFlagSideUnspecified));
  EXPECT_EQ(krnBit.vendorMaybeBadBook, 0u);
}

// ---------------------------------------------------------------------------
// The real sample.
// ---------------------------------------------------------------------------
const std::string kSampleDir = KRN_DATA_SAMPLE_DIR;
const std::string kTradesDbn = "xnas-itch-20220610.trades.dbn";
const std::string kMboDbn    = "xnas-itch-20220610.mbo.dbn";

ingest::Options reloadOptions(const std::string& root) {
  ingest::Options o;
  o.dataRoot    = root;
  o.fromDbnOnly = true;   // the retained files only: the D6 reload path
  o.noInsert    = true;   // touch no table
  // An explicit URL so target resolution cannot wander onto another
  // worktree's port. Nothing connects to it on this path.
  o.url = std::string("http://127.0.0.1:1/");
  return o;
}

void requireSample() {
  for (const auto& f : {kTradesDbn, kMboDbn}) {
    ASSERT_TRUE(std::filesystem::exists(kSampleDir + "/" + f))
        << kSampleDir << "/" << f
        << " is missing. This case FAILS rather than skips: configure with "
           "-DKRN_DATA_SAMPLE_DIR=<dir holding the retained T5 sample DBN>.";
  }
}

TEST(GapsRealSample, ZeroRowsAreWrittenWhereANaiveDetectorWouldWrite1044) {
  requireSample();
  if (::testing::Test::HasFatalFailure()) return;
  const auto rep = ingest::run(reloadOptions(kSampleDir));
  ASSERT_TRUE(rep.ok()) << rep.status().ToString();
  ASSERT_EQ(rep->kinds.size(), 2u);

  std::size_t naive = 0;
  for (const auto& k : rep->kinds) {
    SCOPED_TRACE(k.kind);
    EXPECT_EQ(k.gapPullScope, "symbol-filtered");
    EXPECT_EQ(k.symbol, "MSFT");
    EXPECT_EQ(k.gapRowsWritten, 0u);
    EXPECT_EQ(k.gapMaybeBadBook, 0u);
    EXPECT_EQ(k.canonicalRows, 1000u);
    if (k.kind == "trades") EXPECT_EQ(k.gapNaiveSeqJumps, 610u);
    if (k.kind == "book_events") EXPECT_EQ(k.gapNaiveSeqJumps, 434u);
    naive += k.gapNaiveSeqJumps;
  }
  // Non-vacuity: the refusal is only meaningful where there was something to
  // refuse. Every one of these jumps is a phantom hole — the pull is complete.
  EXPECT_EQ(naive, 1044u);
}

// The vendor-declared signal reaches the job's refusal on REAL bytes: a temp
// COPY of the retained files with F_MAYBE_BAD_BOOK set on the last trades
// record. A falsification mutation in a scratch directory, never retained,
// never ingested, and asserted to have applied.
TEST(GapsRealSample, AMaybeBadBookRecordRefusesTheWindowAsAFinding) {
  requireSample();
  if (::testing::Test::HasFatalFailure()) return;
  const auto dir = std::filesystem::temp_directory_path() /
                   ("ENC-1506-gapstest-" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  ASSERT_TRUE(std::filesystem::create_directories(dir));
  for (const auto& f : {kTradesDbn, kMboDbn}) {
    std::filesystem::copy_file(kSampleDir + "/" + f, dir / f);
  }

  auto bytes = csv::readFile((dir / kTradesDbn).string());
  ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();
  // A TradeMsg is 48 bytes with `flags` at offset 30 (DatabentoAdapter.cpp),
  // and the file ends on a record.
  constexpr std::size_t kTradeMsgBytes = 48;
  constexpr std::size_t kTradeFlagsAt  = 30;
  ASSERT_GE(bytes->size(), kTradeMsgBytes);
  auto& flags = (*bytes)[bytes->size() - kTradeMsgBytes + kTradeFlagsAt];
  const auto before = static_cast<std::uint8_t>(flags);
  ASSERT_EQ(before & dbn::kFlagMaybeBadBook, 0) << "already set: no mutation";
  flags = static_cast<std::byte>(before | dbn::kFlagMaybeBadBook);
  {
    std::ofstream out(dir / kTradesDbn, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes->data()),
              static_cast<std::streamsize>(bytes->size()));
  }
  auto reread = csv::readFile((dir / kTradesDbn).string());
  ASSERT_TRUE(reread.ok());
  ASSERT_EQ(*reread, *bytes) << "the mutation did not apply";

  const auto rep = ingest::run(reloadOptions(dir.string()));
  std::filesystem::remove_all(dir);
  ASSERT_FALSE(rep.ok()) << "a vendor-flagged hole was recorded complete";
  EXPECT_EQ(exitCodeFor(rep.status()), 1) << rep.status().ToString();
  EXPECT_NE(rep.status().message().find("F_MAYBE_BAD_BOOK"), std::string::npos)
      << rep.status().ToString();
}

// ---------------------------------------------------------------------------
// Anti-vacuity, as in DatabentoCsvTest.cpp.
// ---------------------------------------------------------------------------
TEST(GapsAntiVacuity, EveryCaseInThisFileIsRegistered) {
  if (std::string(GTEST_FLAG_GET(filter)) != "*") {
    GTEST_SKIP() << "a filter is active; the count would be about the filter";
  }
  const auto* unit = ::testing::UnitTest::GetInstance();
  int found = 0;
  for (int i = 0; i < unit->total_test_suite_count(); ++i) {
    const auto* suite = unit->GetTestSuite(i);
    if (std::string(suite->name()).rfind("Gaps", 0) == 0) {
      found += suite->total_test_count();
    }
  }
  EXPECT_EQ(found, 11) << "the Gaps* suites in this file lost or gained a case; "
                          "update the number deliberately";
}

}  // namespace
}  // namespace krn::data::gaps
