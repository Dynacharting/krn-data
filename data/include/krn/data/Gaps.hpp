#pragma once
//
// krn.gaps — what may be written, and why on today's data the answer is
// nothing (ENC-1506).
//
// Design: specs/2026-10-14-krn-data-layer/SPEC.md D11;
//         docs/data-layer.md §8.3, §8.4, §8.8.
//
// THE DETECTOR §8.8 DESCRIBED CANNOT RUN ON A SYMBOL-FILTERED PULL
// ----------------------------------------------------------------
//
// §8.8 told "the provider has a hole" from "the market was quiet" by a
// `provider_seq` discontinuity. Databento's `sequence` is the VENUE's message
// sequence, not the symbol's: on a pull filtered to one symbol every other
// instrument's messages occupy the numbers in between, so consecutive records
// step 11404021 -> 11404028 -> 11423666 on data that is complete. Monotonic,
// as §8.3 says; contiguous, never. A gap row per jump is a PHANTOM HOLE, and a
// research run joining `gaps` reads it as "there was no data here" — the §8.4
// wrong answer inverted, manufactured by the instrument meant to prevent it.
//
// So `infer` writes no seq-derived row from a symbol-filtered pull, and counts
// what a naive detector WOULD have written so the refusal is visible as a
// number rather than as an empty table. That number is diagnostics only: it is
// never a row, never a metric a query can join against.
//
// AN UNFILTERED PULL IS REFUSED TOO, AND THAT IS "UNPROVEN", NOT "WRONG". No
// all-symbol pull has been measured, so nothing shows that its `sequence` is
// contiguous — and the ticket's acceptance is that a gap writer which cannot
// be shown to stay silent on complete data must not ship. When one is pulled
// and measured, the `AllSymbols` branch is where the answer lands.
//
// THE VENDOR'S OWN SIGNAL IS COUNTED, AND IS NOT TURNED INTO A ROW.
// `F_MAYBE_BAD_BOOK` ("an unrecoverable gap was detected in the channel", DBN's
// flags byte) is a vendor-declared hole, so it is the one signal that IS
// evidence. It carries no bounds — not the seq range missing, not the event
// time — and `krn.gaps` requires both, so it cannot become a row without
// inventing them. The ingest job instead REFUSES to record a window as complete
// over a record carrying it (IngestJob.cpp): leaving it unrecorded is correct,
// claiming it complete with no gap row is the §8.4 failure.
//

#include "krn/data/BookRecord.hpp"
#include "krn/data/DatabentoAdapter.hpp"
#include "krn/data/TickRecord.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace krn::data::gaps {

// What the pull selected. Read from the DBN metadata's `symbols` — the query's
// own symbol list, which the retained file carries (DbnEncoder writes it).
enum class PullScope : std::uint8_t {
  SymbolFiltered = 0,  // `symbols` names instruments: venue seq is NOT contiguous
  AllSymbols     = 1,  // `symbols == ["ALL_SYMBOLS"]`: contiguity UNMEASURED
  Unknown        = 2,  // no symbol list: cannot tell, so treated as filtered
};
const char* toString(PullScope s);

// Databento's spelling of "every instrument in the dataset".
inline constexpr const char* kAllSymbols = "ALL_SYMBOLS";

PullScope pullScope(const dbn::Metadata& md);

// One record's sequencing facts, in delivered order.
struct SeqPoint {
  std::int64_t  tsEventNs   = 0;
  std::uint64_t providerSeq = 0;
  std::uint16_t flags       = 0;  // low byte is DBN's flags byte
};
std::vector<SeqPoint> seqPoints(const std::vector<TickRecord>& ticks);
std::vector<SeqPoint> seqPoints(const std::vector<BookRecord>& book);

// One `krn.gaps` row's event-time and seq bounds (clickhouse.sql).
struct GapRow {
  std::int64_t  tsFromNs = 0;
  std::int64_t  tsToNs   = 0;
  std::uint64_t seqFrom  = 0;
  std::uint64_t seqTo    = 0;
  std::uint8_t  cause    = 0;  // 0 seq-gap, as in the DDL
};

struct Inference {
  PullScope scope = PullScope::Unknown;
  // The rows that MAY be written. Empty on every path today; see the header.
  std::vector<GapRow> rows;
  // Adjacent pairs with `next > prev + 1`: the rows a seq-jump detector would
  // have written. Diagnostics only.
  std::size_t naiveSeqJumps = 0;
  // Records carrying F_MAYBE_BAD_BOOK: vendor-declared holes.
  std::size_t vendorMaybeBadBook = 0;
  // Why `rows` is empty, in words the report prints.
  std::string refusal;
};

Inference infer(const dbn::Metadata& md, std::span<const SeqPoint> points);

}  // namespace krn::data::gaps
