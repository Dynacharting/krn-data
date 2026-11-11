#pragma once
//
// BookRecordFieldSource — the Arrow-free, DOM-free adapter from a
// `krn::data::BookRecord` (one mbo row) to the engine's field view (ENC-1628).
//
// Design: specs/2026-10-14-krn-data-layer/SPEC.md D1, D3, D9;
//         docs/data-layer.md §2.1, §4, §8.4, §C1, §C13.
//
// The sibling of `RecordFieldSource` (TickRecords, ENC-1486), and built the
// same way: a fixed field set resolved once into a stack array, names shared
// with that header's `recordfields`, no allocation per record, and no JSON type
// named (checked textually by RecordIngressParityTest, with a positive control).
//
// ------------------------------------------------------------------------
// THE MAPPING. This table *defines* "the equivalent tick" for a BookRecord —
// the JSON Event `Dispatcher::onTick` would have to receive for the two entry
// points to be indistinguishable. The parity tests derive that Event from this
// class rather than restating the table, and pin the table separately against
// literals.
//
//   record field  engine field  emitted when
//   ------------  ------------  ------------------------------------------
//   price         "price"       action == Trade && price != kNoPrice
//   size          "size"        action == Trade && size != 0
//
//   Add, Update, Delete, TickSize, Clear: NO FIELD AT ALL.
//
// WHY THIS FIELD SET. There is no JSON synthesis of a BookRecord anywhere in
// krn-data or the engine, so there is nothing to be exactly equal to; the
// field set is chosen to mirror what the live JSON path carries for the SAME
// venue event, which is the nearest thing to one:
//
//   * A book MUTATION never reaches `onTick` on the live path. docs §2.1:
//     ItchAdapter routes the MBO vocabulary (add / cancel / modify / clear)
//     onto `OrderBookManager`'s per-order API, and the market connector's
//     `WsFeedClient::dispatchEvent` hands only a `TickEvent` to the
//     Dispatcher (krn-common `connectors/market/src/ws/WsFeedClient.cpp`).
//     The book's consumer is `OrderBookManager` (docs §4: `onRecord ─▶
//     OrderBookManager`), which is the market connector's and not krn-data's
//     to link (D9, amended). Top-of-book is derived from the reconstructed
//     book, not carried per row (docs §8.4, "`ob.*` being pipeline-only").
//     Exposing an Add's price as `price` would write resting-order prices
//     into the same `(symbol, "price")` atomic and history a trade writes:
//     a wrong answer with a plausible shape.
//   * A PRINT (`BookAction::Trade`, from DBN `T` and `F`) does reach `onTick`
//     live: ItchAdapter's `routeTrade` and `routeOrderExecuted` each emit a
//     trade tick carrying the print's price and size and nothing else
//     (`makeTradeTickEvent`). So a print exposes exactly price and size, and
//     it exposes them under krn-data's record vocabulary — `recordfields::
//     kPrice` / `kSize`, the names a `trades` row's `RecordFieldSource` uses —
//     so that an mbo print and the trades row of the same execution flatten
//     to the SAME field set (pinned by
//     BookRecordIngressParityTest.APrintIsTheTradeTickItMirrors). ItchAdapter
//     spells them `lastPrice` / `volume`; both spellings are in the market
//     connector's default `MarketFieldMap` alias lists, so a market computer
//     reads either. The gating on `kNoPrice` and `0` is RecordFieldSource's.
//
// NOT EXPOSED, each for the reason RecordFieldSource gives for its own:
//   * `header.*` timestamps: event time is SPEC D9 / docs §C1, and a
//     nanosecond epoch does not survive the engine's `double`.
//   * `orderId`, `priority`, `depth`, `epoch`, `header.providerSeq` etc.:
//     book identity and provenance, consumed by the book, not values.
//   * `side`, `action`, `flags`: categorical. On a print `side` is the
//     aggressor (DatabentoAdapter.cpp), which RecordFieldSource does not
//     expose for a trade either.
//
// WHAT A FIELDLESS ROW DOES. A mutation's source has `fieldCountHint() == 0`.
// Handing it to `onRecord` is IDENTICAL to `onTick` with `{}` — parity holds,
// and the tests drive every row of a mixed corpus through both — but it is not
// free and not what the live connector does: `ingest` still runs the "tick"
// computers and still takes a `maxSymbols` raw-admission slot for the symbol
// (RecordIngressParityTest.IdenticalForAFieldlessTick). A caller that wants
// the live path's behaviour skips a row whose `fieldCountHint()` is 0.
//
// C13: an execution is in mbo as a print AND in `trades` as a row, so a graph
// fed both double-counts volume. That refusal is the planner's (docs §C13),
// not this adapter's.
//

#include <array>
#include <cstdint>
#include <string>

#include "krn/data/BookRecord.hpp"
#include "krn/data/RecordFieldSource.hpp"
#include "krn/engine/FieldSource.hpp"

namespace krn::engine {

class BookRecordFieldSource final : public FieldSource {
public:
  // Largest possible emitted field count — see the mapping table.
  static constexpr std::size_t kMaxFields = 2;

  explicit BookRecordFieldSource(const data::BookRecord& rec) noexcept {
    if (rec.action != data::BookAction::Trade) return;
    if (rec.price != data::kNoPrice) {
      fields_[n_++] = Entry{&recordfields::kPrice, data::toDouble(rec.price)};
    }
    if (rec.size != 0) {
      fields_[n_++] = Entry{&recordfields::kSize, static_cast<double>(rec.size)};
    }
  }

  bool isObject() const noexcept override { return true; }

  std::size_t fieldCountHint() const noexcept override { return n_; }

  bool has(const std::string& name) const override {
    return find(name) != nullptr;
  }

  bool tryGetNumber(const std::string& name, double& out) const override {
    const Entry* e = find(name);
    if (!e) return false;
    out = e->value;
    return true;
  }

  // ALWAYS FALSE, for RecordFieldSource's reason: a rapidjson value stored as
  // a double answers `IsUint64()` false, so answering true for `size` would
  // make a field map with `timestampField = "size"` diverge between paths.
  bool tryGetUint64(const std::string&, std::uint64_t&) const override {
    return false;
  }

  void forEachNumber(NumericFieldSink& sink) const override {
    for (std::size_t i = 0; i < n_; ++i) {
      sink.field(*fields_[i].name, fields_[i].value);
    }
  }

private:
  struct Entry {
    const std::string* name  = nullptr;
    double             value = 0.0;
  };

  const Entry* find(const std::string& name) const noexcept {
    for (std::size_t i = 0; i < n_; ++i) {
      if (*fields_[i].name == name) return &fields_[i];
    }
    return nullptr;
  }

  std::array<Entry, kMaxFields> fields_{};
  std::size_t                   n_ = 0;
};

} // namespace krn::engine
