//
// Gaps — see Gaps.hpp for why no seq-derived row is ever written today.
//

#include "krn/data/Gaps.hpp"

namespace krn::data::gaps {

const char* toString(PullScope s) {
  switch (s) {
    case PullScope::SymbolFiltered: return "symbol-filtered";
    case PullScope::AllSymbols:     return "all-symbols";
    case PullScope::Unknown:        return "unknown";
  }
  return "unknown";
}

PullScope pullScope(const dbn::Metadata& md) {
  if (md.symbols.empty()) return PullScope::Unknown;
  if (md.symbols.size() == 1 && md.symbols.front() == kAllSymbols) {
    return PullScope::AllSymbols;
  }
  return PullScope::SymbolFiltered;
}

std::vector<SeqPoint> seqPoints(const std::vector<TickRecord>& ticks) {
  std::vector<SeqPoint> out;
  out.reserve(ticks.size());
  for (const auto& t : ticks) {
    out.push_back(SeqPoint{t.header.tsEventNs, t.header.providerSeq, t.flags});
  }
  return out;
}

std::vector<SeqPoint> seqPoints(const std::vector<BookRecord>& book) {
  std::vector<SeqPoint> out;
  out.reserve(book.size());
  for (const auto& b : book) {
    out.push_back(SeqPoint{b.header.tsEventNs, b.header.providerSeq, b.flags});
  }
  return out;
}

Inference infer(const dbn::Metadata& md, std::span<const SeqPoint> points) {
  Inference inf;
  inf.scope = pullScope(md);

  for (std::size_t i = 0; i < points.size(); ++i) {
    if ((points[i].flags & dbn::kFlagMaybeBadBook) != 0) ++inf.vendorMaybeBadBook;
    if (i > 0 && points[i].providerSeq > points[i - 1].providerSeq + 1) {
      ++inf.naiveSeqJumps;
    }
  }

  // THE REFUSAL. `rows` stays empty on every branch: a seq jump is evidence of
  // a hole only where the sequence is known to be contiguous, and no pull this
  // job has seen is. Each branch says which of the two reasons applies.
  switch (inf.scope) {
    case PullScope::SymbolFiltered:
    case PullScope::Unknown:
      inf.refusal =
          "symbol-filtered pull: Databento's `sequence` is the venue's, so it "
          "jumps over every other instrument's messages and is not contiguous "
          "per symbol. A jump is not a hole; no seq-gap row is inferred "
          "(docs/data-layer.md 8.8, ENC-1506).";
      if (inf.scope == PullScope::Unknown) {
        inf.refusal.insert(0, "no symbol list in the DBN metadata, treated as a ");
      }
      break;
    case PullScope::AllSymbols:
      inf.refusal =
          "all-symbols pull: contiguity of the venue `sequence` is UNMEASURED "
          "on any pull, so a jump is not yet evidence of a hole and no seq-gap "
          "row is inferred until one is measured (docs/data-layer.md 8.8, "
          "ENC-1506).";
      break;
  }
  return inf;
}

}  // namespace krn::data::gaps
