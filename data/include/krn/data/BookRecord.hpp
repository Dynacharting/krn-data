#pragma once
//
// The book-side L1 records, moved out of the engine's include/krn/data/Record.hpp
// (ENC-1550). No engine code names them, so they live in krn_data. RecordHeader,
// TickRecord and Aggressor followed them in ENC-1557, into krn/data/TickRecord.hpp,
// which this header includes.
//
// Design: specs/2026-10-15-gma-engine-purity/SPEC.md D4, D10.

#include <cstddef>
#include <cstdint>

#include "krn/data/TickRecord.hpp"

namespace krn::data {

// The resting side of the book. Bid and Ask mirror krn::Side
// (connectors/market/include/krn/book/OrderBook.hpp) and keep its values.
//
// `None` is the third value DBN's own `Side` carries ("No side specified by the
// original source", ENC-1494), and the market connector's `krn::Side` does not
// have. It is normal on real data, not an edge case: on Databento XNAS.ITCH
// MSFT 2022-06-10 it is 65 of 1000 mbo rows (every one a `T` print) and 546 of
// 1000 trades rows. Before ENC-1494 it was spelled `Bid` plus a flag bit in
// `flags`, so a reader that did not consult the flag saw a bid. A consumer that
// places an order on the book must refuse `None` rather than pick a side.
enum class Side : std::uint8_t { Bid = 0, Ask = 1, None = 2 };

// Mirrors the krn::feed::FeedEvent variant set
// (connectors/market/include/krn/feed/FeedEvent.hpp) one-for-one. That variant
// is already the canonical event model in this repo; the schema does not invent
// a second one.
enum class BookAction : std::uint8_t {
  Add      = 0,   // ObAddEvent
  Update   = 1,   // ObUpdateEvent
  Delete   = 2,   // ObDeleteEvent
  Trade    = 3,   // ObTradeEvent (print; see bookNeutral)
  TickSize = 4,   // ObTickSizeEvent
  Clear    = 5,   // ObResetEvent — new epoch
};

// One L2/L3 book mutation. Maps onto the OrderBookManager entry points that
// already exist (onAdd / onUpdate / onDelete / onTrade / setTickSize /
// onReset), so the data layer drives that machinery rather than duplicating it
// (docs/data-layer.md §4.6).
//
// ONE MEANING PER COLUMN (ENC-1494):
//   * `side` is the RESTING side on every row: the order's side on a book
//     mutation, and the passive side a print executed against on a `Trade`
//     row. `Side::None` when the venue gave none.
//   * `aggressor` is the taker, and only a `Trade` row has one; it is
//     `Aggressor::Unknown` on every other action. It is the argument
//     `OrderBookManager::onTrade(..., Aggressor)` takes.
// Before ENC-1494 `side` held the resting side on a mutation and the aggressor
// on a print, which is how ENC-1482's one real bug (a Fill's aggressor flip
// leaking onto the mutation row it also emits) was possible at all.
//
// THE DEFAULT IS "NO SIDE", NOT A BID. A default-constructed BookRecord has
// `side == Side::None` and `aggressor == Aggressor::Unknown`, so a field nobody
// set cannot read as a plausible wrong value (the hazard ENC-1480 met with
// `updated_at` defaulting to epoch 0). It is not made unrepresentable: `None`
// is a legal value on real prints, so the default is "explicitly unknown",
// which a book consumer has to refuse to place, rather than an invalid byte the
// codec would refuse to store.
struct BookRecord {
  RecordHeader  header;
  BookAction    action    = BookAction::Add;
  Side          side      = Side::None;
  // In the padding between `side` and `price`, so the struct stays 96 bytes.
  Aggressor     aggressor = Aggressor::Unknown;
  std::int64_t  price    = kNoPrice;
  std::uint64_t size     = 0;
  std::uint64_t orderId  = 0;     // 0 == venue supplied none (OrderKey::idMissing)
  std::uint64_t priority = 0;     // recv seq or venue ts, per Order::priority
  std::uint16_t depth    = 0;     // level index for aggregated (L2) feeds
  std::uint32_t epoch    = 0;     // FeedScope::epoch; bumped by Clear
  std::uint16_t flags    = 0;
};

// One level of a book snapshot. Snapshots are persisted on a cadence so a
// replay of an arbitrary window can seek instead of rebuilding from session
// open — with MBO, the alternative is replaying every order event from the
// open, per research run (docs/data-layer.md §8.5). A level is always on one
// side, so the codec refuses `Side::None` here (Schema.cpp, kMaxLevelSideValue);
// nothing produces snapshots yet, and this default is left as it was.
struct SnapshotLevel {
  Side          side       = Side::Bid;
  std::int64_t  price      = kNoPrice;
  std::uint64_t size       = 0;
  std::uint32_t orderCount = 0;
};

// NOTE: no longer matches the L1 `krn.book_snapshots` table, which is one row per order since ENC-1663.
// One ROW of `book_snapshots`: a level, plus the header, plus the resume seq
// the snapshot was taken at (`onSnapshotPerOrder`'s snapshotSeq). A snapshot is
// many rows sharing one snapshotSeq; the level alone is not addressable, which
// is why the row type exists rather than persisting bare SnapshotLevels.
struct SnapshotRecord {
  RecordHeader  header;
  std::uint64_t snapshotSeq = 0;
  SnapshotLevel level;
};

// Layout and enumerator values are part of the L1 contract (Schema.cpp casts
// them; clickhouse.sql stores them). Pinned as measured before the move.
static_assert(sizeof(Side) == 1 && static_cast<int>(Side::Bid) == 0 &&
              static_cast<int>(Side::Ask) == 1 && static_cast<int>(Side::None) == 2);
static_assert(sizeof(BookAction) == 1 &&
              static_cast<int>(BookAction::Add) == 0 &&
              static_cast<int>(BookAction::Update) == 1 &&
              static_cast<int>(BookAction::Delete) == 2 &&
              static_cast<int>(BookAction::Trade) == 3 &&
              static_cast<int>(BookAction::TickSize) == 4 &&
              static_cast<int>(BookAction::Clear) == 5);

static_assert(sizeof(BookRecord) == 96 && alignof(BookRecord) == 8);
static_assert(offsetof(BookRecord, header) == 0 &&
              offsetof(BookRecord, action) == 40 &&
              offsetof(BookRecord, side) == 41 &&
              offsetof(BookRecord, aggressor) == 42 &&
              offsetof(BookRecord, price) == 48 &&
              offsetof(BookRecord, size) == 56 &&
              offsetof(BookRecord, orderId) == 64 &&
              offsetof(BookRecord, priority) == 72 &&
              offsetof(BookRecord, depth) == 80 &&
              offsetof(BookRecord, epoch) == 84 &&
              offsetof(BookRecord, flags) == 88);

static_assert(sizeof(BookRecord::price) == 8 && sizeof(BookRecord::size) == 8 &&
              sizeof(BookRecord::orderId) == 8 &&
              sizeof(BookRecord::priority) == 8 &&
              sizeof(BookRecord::depth) == 2 && sizeof(BookRecord::epoch) == 4 &&
              sizeof(BookRecord::flags) == 2);

static_assert(sizeof(SnapshotLevel) == 32 && alignof(SnapshotLevel) == 8);
static_assert(offsetof(SnapshotLevel, side) == 0 &&
              offsetof(SnapshotLevel, price) == 8 &&
              offsetof(SnapshotLevel, size) == 16 &&
              offsetof(SnapshotLevel, orderCount) == 24);

static_assert(sizeof(SnapshotLevel::price) == 8 &&
              sizeof(SnapshotLevel::size) == 8 &&
              sizeof(SnapshotLevel::orderCount) == 4);

static_assert(sizeof(SnapshotRecord) == 80 && alignof(SnapshotRecord) == 8);
static_assert(offsetof(SnapshotRecord, header) == 0 &&
              offsetof(SnapshotRecord, snapshotSeq) == 40 &&
              offsetof(SnapshotRecord, level) == 48);

} // namespace krn::data
