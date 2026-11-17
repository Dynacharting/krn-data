//
// The vendor start-of-day snapshot's gates and pass-2 writes (ENC-1663).
//
// Every refusal `kSnapshotRuling` names has a case here, built from small
// in-memory BookRecords rather than from vendor files: the shapes these gates
// refuse never occur in the delivered NVDA job (all 23 days pass), so only a
// constructed record can show each gate is live. Nothing here is market data;
// the values are the minimum that put a record on one side of a gate.
//
// `loadBookDay` is driven through a recording `ch::Client` that answers the
// ingest_windows lookups and can fail one INSERT, so the statements pass 2
// sends are asserted in order without a server.
//

#include "krn/data/DatabentoAdapter.hpp"
#include "krn/data/IngestJob.hpp"
#include "krn/data/Verdict.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <regex>
#include <string>
#include <vector>

namespace {

using namespace krn::data;

constexpr std::int64_t kMidnight = 1793059200000000000LL;  // 2026-10-27T00:00Z
constexpr std::int64_t kDay      = 86400000000000LL;
constexpr std::uint16_t kSnap    = dbn::kFlagSnapshot;

BookRecord rec(BookAction action, std::uint64_t seq, std::uint64_t orderId,
               std::uint16_t flags, std::int64_t tsRecv = kMidnight) {
  BookRecord r;
  r.action             = action;
  r.header.providerSeq = seq;
  r.header.tsRecvNs    = tsRecv;
  r.header.tsEventNs   = tsRecv - 3600000000000LL;  // the order's own, earlier time
  r.orderId            = orderId;
  r.side               = action == BookAction::Add ? Side::Bid : Side::None;
  r.flags              = flags;
  r.epoch              = 1;
  return r;
}

// A well-formed day: Clear + two Adds flagged F_SNAPSHOT, then one Delete.
std::vector<BookRecord> goodDay() {
  return {rec(BookAction::Clear, 0, 0, kSnap), rec(BookAction::Add, 10, 1, kSnap),
          rec(BookAction::Add, 11, 2, kSnap),
          rec(BookAction::Delete, 20, 1, 0, kMidnight + 1000)};
}

void expectFinding(const arrow::Status& st, const std::string& needle) {
  ASSERT_FALSE(st.ok());
  EXPECT_EQ(exitCodeFor(st), 1) << st.ToString();
  EXPECT_NE(st.message().find(needle), std::string::npos) << st.ToString();
  EXPECT_NE(st.message().find("Nothing was written"), std::string::npos) << st.ToString();
}

// ---- splitSnapshot ----------------------------------------------------------

TEST(IngestSnapshotSplit, AWellFormedBlockMovesOutWithItsAsOf) {
  auto book = goodDay();
  std::vector<BookRecord> snap;
  std::int64_t asOf = -1;
  ASSERT_TRUE(splitSnapshot(book, snap, asOf, "t").ok());
  ASSERT_EQ(snap.size(), 3u);
  EXPECT_EQ(snap.front().action, BookAction::Clear);
  ASSERT_EQ(book.size(), 1u);
  EXPECT_EQ(book.front().action, BookAction::Delete);
  EXPECT_EQ(asOf, kMidnight);
}

TEST(IngestSnapshotSplit, NoBlockLeavesTheBookUntouched) {
  std::vector<BookRecord> book = {rec(BookAction::Clear, 0, 0, 0),
                                  rec(BookAction::Add, 5, 1, 0)};
  std::vector<BookRecord> snap = {rec(BookAction::Add, 1, 1, kSnap)};
  std::int64_t asOf = -1;
  ASSERT_TRUE(splitSnapshot(book, snap, asOf, "t").ok());
  EXPECT_TRUE(snap.empty());
  EXPECT_EQ(book.size(), 2u);
  EXPECT_EQ(asOf, 0);
}

TEST(IngestSnapshotSplit, AStraySnapshotRecordAfterTheBlockIsRefused) {
  auto book = goodDay();
  book.push_back(rec(BookAction::Add, 30, 3, kSnap, kMidnight + 2000));
  std::vector<BookRecord> snap;
  std::int64_t asOf = 0;
  expectFinding(splitSnapshot(book, snap, asOf, "t"), "follow the first record");
  EXPECT_TRUE(snap.empty());
  EXPECT_EQ(book.size(), 5u) << "a refusal moves nothing";
}

TEST(IngestSnapshotSplit, AFirstRecordThatIsNotAClearIsRefused) {
  std::vector<BookRecord> book = {rec(BookAction::Add, 10, 1, kSnap),
                                  rec(BookAction::Delete, 20, 1, 0)};
  std::vector<BookRecord> snap;
  std::int64_t asOf = 0;
  expectFinding(splitSnapshot(book, snap, asOf, "t"), "not a Clear");
}

TEST(IngestSnapshotSplit, ANonAddAfterTheClearIsRefused) {
  auto book = goodDay();
  book[2].action = BookAction::Update;
  std::vector<BookRecord> snap;
  std::int64_t asOf = 0;
  expectFinding(splitSnapshot(book, snap, asOf, "t"), "not an Add");
}

TEST(IngestSnapshotSplit, AnAddNotAboveTheClearsSeqIsRefused) {
  for (const std::uint64_t clearSeq : {11ULL, 12ULL}) {  // == and > the Add's 11
    auto book = goodDay();
    book[0].header.providerSeq = clearSeq;
    std::vector<BookRecord> snap;
    std::int64_t asOf = 0;
    expectFinding(splitSnapshot(book, snap, asOf, "t"), "not above the Clear's");
  }
}

TEST(IngestSnapshotSplit, TwoAsOfInstantsAreRefused) {
  auto book = goodDay();
  book[2].header.tsRecvNs = kMidnight + 1;
  std::vector<BookRecord> snap;
  std::int64_t asOf = 0;
  expectFinding(splitSnapshot(book, snap, asOf, "t"), "two as-of instants");
}

// ---- checkSnapshotAsOf ------------------------------------------------------

TEST(IngestSnapshotAsOf, MustEqualTheWindowStart) {
  EXPECT_TRUE(checkSnapshotAsOf(kMidnight, kMidnight, kMidnight + kDay, "t").ok());
  // Inside the window but not its start: the events in between would replay
  // onto a book that already holds them.
  expectFinding(checkSnapshotAsOf(kMidnight + 1, kMidnight, kMidnight + kDay, "t"),
                "not the start of the recorded window");
  expectFinding(checkSnapshotAsOf(kMidnight - 1, kMidnight, kMidnight + kDay, "t"),
                "not the start of the recorded window");
}

// ---- checkDeliveredBookDay --------------------------------------------------

TEST(IngestDeliveredDay, ASeededClosedDayPasses) {
  EXPECT_TRUE(checkDeliveredBookDay({rec(BookAction::Delete, 20, 1, 0)}, 3, 0, 0, "t").ok());
}

TEST(IngestDeliveredDay, UnknownOrdersAreRefused) {
  expectFinding(checkDeliveredBookDay({rec(BookAction::Delete, 20, 1, 0)}, 3, 1, 0, "t"),
                "1 Cancel/Modify record(s)");
}

TEST(IngestDeliveredDay, EvictedOrdersAreRefused) {
  expectFinding(checkDeliveredBookDay({rec(BookAction::Delete, 20, 1, 0)}, 3, 0, 1, "t"),
                "1 tracked order(s) were evicted");
}

TEST(IngestDeliveredDay, NoSnapshotMustOpenWithAnUnflaggedClear) {
  // The Mondays' shape: record 0 is an unflagged Clear.
  EXPECT_TRUE(checkDeliveredBookDay({rec(BookAction::Clear, 0, 0, 0),
                                     rec(BookAction::Add, 5, 1, 0)},
                                    0, 0, 0, "t")
                  .ok());
  expectFinding(checkDeliveredBookDay({rec(BookAction::Add, 5, 1, 0)}, 0, 0, 0, "t"),
                "not a Clear");
  expectFinding(checkDeliveredBookDay({}, 0, 0, 0, "t"), "absent");
  // A flagged Clear is a snapshot's, which splitSnapshot would have taken.
  expectFinding(checkDeliveredBookDay({rec(BookAction::Clear, 0, 0, kSnap)}, 0, 0, 0, "t"),
                "not a Clear");
}

// ---- loadBookDay: pass 2 against a recording client -------------------------

struct Window {
  std::string symbol, kind;
  std::int64_t from = 0, to = 0;
  int state = -1;
};

class RecordingClient : public ch::Client {
 public:
  RecordingClient() : ch::Client(ch::Target{"http://127.0.0.1:1/", "test", 0, false, true}) {}

  // Windows the ingest_windows lookup reports, per kind.
  std::vector<Window> recorded;
  // An INSERT whose statement starts with this fails.
  std::string failInsertPrefix;
  mutable std::vector<std::string> sent;

  arrow::Result<std::string> exec(std::string_view sqlView) const override {
    const std::string sql(sqlView);
    sent.push_back(sql);
    if (!failInsertPrefix.empty() && sql.rfind(failInsertPrefix, 0) == 0) {
      return arrow::Status::IOError("injected insert failure");
    }
    if (sql.rfind("SELECT DISTINCT symbol", 0) == 0) {
      std::string body;
      for (const auto& w : recorded) {
        if (sql.find("kind = '" + w.kind + "'") == std::string::npos) continue;
        body += w.symbol + "\t" + std::to_string(w.from) + "\t" + std::to_string(w.to) + "\n";
      }
      return body;
    }
    return std::string();
  }

  // The ingest_windows rows written, in order.
  std::vector<Window> windowWrites() const {
    static const std::regex re(
        R"(INSERT INTO krn\.ingest_windows .*SELECT '([^']*)', '([^']*)', )"
        R"(fromUnixTimestamp64Nano\(toInt64\((-?\d+)\), 'UTC'\), )"
        R"(fromUnixTimestamp64Nano\(toInt64\((-?\d+)\), 'UTC'\), (\d+),)");
    std::vector<Window> out;
    for (const auto& s : sent) {
      std::smatch m;
      if (std::regex_search(s, m, re)) {
        out.push_back({m[1], m[2], std::stoll(m[3]), std::stoll(m[4]), std::stoi(m[5])});
      }
    }
    return out;
  }
  std::size_t indexOf(const std::string& prefix) const {
    for (std::size_t i = 0; i < sent.size(); ++i) {
      if (sent[i].rfind(prefix, 0) == 0) return i;
    }
    return sent.size();
  }
};

ingest::BookDayLoad nvdaDay(bool withSnapshot) {
  ingest::BookDayLoad d;
  d.symbol    = "NVDA";
  d.venue     = "XNAS.ITCH";
  d.jobId     = "TEST-JOB";
  d.fromNs    = kMidnight;
  d.toNs      = kMidnight + kDay;
  d.sessionId = 7;
  auto book = goodDay();
  if (withSnapshot) {
    d.snapshot.assign(book.begin(), book.begin() + 3);
    d.snapshotAsOfNs = kMidnight;
  } else {
    book[0].flags = 0;  // an unflagged opening Clear stays an event
    d.events.assign(book.begin(), book.begin() + 1);
  }
  d.events.push_back(book.back());
  return d;
}

int count(const std::vector<Window>& ws, const std::string& symbol,
          const std::string& kind, int state) {
  int n = 0;
  for (const auto& w : ws) n += w.symbol == symbol && w.kind == kind && w.state == state;
  return n;
}

TEST(IngestLoadBookDay, InvalidatesBothKindsAndRecordsTheSnapshotFirst) {
  RecordingClient c;
  // Another symbol's windows of both kinds on the day, and this window's own.
  c.recorded = {{"MSFT", "book_events", kMidnight, kMidnight + kDay},
                {"MSFT", "book_snapshots", kMidnight, kMidnight + kDay},
                {"NVDA", "book_events", kMidnight, kMidnight + kDay},
                {"NVDA", "book_snapshots", kMidnight, kMidnight + kDay}};
  ingest::Report rep;
  const auto st = ingest::loadBookDay(c, nvdaDay(true), rep);
  ASSERT_TRUE(st.ok()) << st.ToString();

  const auto w = c.windowWrites();
  EXPECT_EQ(count(w, "MSFT", "book_events", 3), 1);
  EXPECT_EQ(count(w, "MSFT", "book_snapshots", 3), 1);
  EXPECT_EQ(count(w, "NVDA", "book_events", 3), 0) << "own window: state=0 covers it";
  EXPECT_EQ(count(w, "NVDA", "book_snapshots", 3), 0);
  for (const std::string kind : {"book_events", "book_snapshots"}) {
    EXPECT_EQ(count(w, "NVDA", kind, 0), 1) << kind;
    EXPECT_EQ(count(w, "NVDA", kind, 1), 1) << kind;
    EXPECT_EQ(count(w, "NVDA", kind, 2), 0) << kind;
  }
  EXPECT_EQ(rep.partitionsDropped,
            (std::vector<std::string>{"book_events:20261027", "book_snapshots:20261027"}));

  // Order: the drops before any insert, the snapshot insert before the
  // events', and the snapshot's state=1 before the events' state=1 (D11).
  const auto dropSnap = c.indexOf("ALTER TABLE krn.book_snapshots DROP PARTITION 20261027");
  const auto insSnap  = c.indexOf("INSERT INTO krn.book_snapshots (");
  const auto insEv    = c.indexOf("INSERT INTO krn.book_events (");
  ASSERT_LT(insEv, c.sent.size());
  EXPECT_LT(dropSnap, insSnap);
  EXPECT_LT(insSnap, insEv);
  std::size_t snapDone = 0, evDone = 0;
  for (std::size_t i = 0; i < w.size(); ++i) {
    if (w[i].symbol == "NVDA" && w[i].state == 1) {
      (w[i].kind == "book_snapshots" ? snapDone : evDone) = i;
    }
  }
  EXPECT_LT(snapDone, evDone);
  // The snapshot's rows: 3, as of the window start, Clear first.
  const std::string& body = c.sent[insSnap];
  EXPECT_EQ(std::count(body.begin(), body.end(), '\n'), 4);  // header line + 3 rows
  EXPECT_NE(body.find("\n" + std::to_string(kMidnight) + "\t"), std::string::npos);
}

TEST(IngestLoadBookDay, WithNoSnapshotItsOwnOldSnapshotWindowIsInvalidated) {
  RecordingClient c;
  c.recorded = {{"NVDA", "book_events", kMidnight, kMidnight + kDay},
                {"NVDA", "book_snapshots", kMidnight, kMidnight + kDay}};
  ingest::Report rep;
  ASSERT_TRUE(ingest::loadBookDay(c, nvdaDay(false), rep).ok());
  const auto w = c.windowWrites();
  // The partition is dropped and no snapshot is reinserted, so the old
  // snapshot window must not keep reading complete.
  EXPECT_EQ(count(w, "NVDA", "book_snapshots", 3), 1);
  EXPECT_EQ(count(w, "NVDA", "book_snapshots", 1), 0);
  EXPECT_EQ(count(w, "NVDA", "book_events", 1), 1);
  EXPECT_EQ(c.indexOf("INSERT INTO krn.book_snapshots ("), c.sent.size());
  EXPECT_LT(c.indexOf("ALTER TABLE krn.book_snapshots DROP PARTITION 20261027"),
            c.sent.size());
}

TEST(IngestLoadBookDay, AFailedInsertLeavesBothWindowsFailed) {
  for (const std::string failing :
       {"INSERT INTO krn.book_snapshots (", "INSERT INTO krn.book_events ("}) {
    RecordingClient c;
    c.failInsertPrefix = failing;
    ingest::Report rep;
    const auto st = ingest::loadBookDay(c, nvdaDay(true), rep);
    EXPECT_FALSE(st.ok()) << failing;
    const auto w = c.windowWrites();
    for (const std::string kind : {"book_events", "book_snapshots"}) {
      EXPECT_EQ(count(w, "NVDA", kind, 2), 1) << failing << " " << kind;
      EXPECT_EQ(count(w, "NVDA", kind, 1), 0) << failing << " " << kind;
    }
    if (failing == "INSERT INTO krn.book_snapshots (") {
      EXPECT_EQ(c.indexOf("INSERT INTO krn.book_events ("), c.sent.size())
          << "the events are not sent after the seed failed";
    }
  }
}

}  // namespace
