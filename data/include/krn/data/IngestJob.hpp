#pragma once
//
// The ingest job: retained Databento raw -> canonical records -> L1 (ENC-1483).
//
// Design: specs/2026-10-14-krn-data-layer/SPEC.md D5, D6, D7, D8, D11, D12;
//         docs/data-layer.md §8.4, §8.6, §8.7, §8.8, §8.9.
//
// THE PATH, AND EVERY LEG OF IT IS THE REAL ONE
// ---------------------------------------------
//
//     vendor CSV ──DatabentoCsv──▶ dbn::MboMsg / dbn::TradeMsg
//                 ──DbnEncoder───▶ REAL DBN BYTES, written to disk (D6)
//                 ──read back────▶ the retained file, from disk
//                 ──DatabentoAdapter──▶ TickRecord / BookRecord
//                 ──TabSeparated──▶ krn.trades / krn.book_events
//
// The read-back is not ceremony. D5's re-ingest is `DROP PARTITION` + reload
// and D6 makes the retained files what it reloads FROM, so the job decodes the
// file on disk on every run — the reload path is the ONLY insert path, and it
// is therefore exercised on the first ingest rather than first used in anger
// during a recovery. `Options::fromDbnOnly` drops the CSV legs entirely and is
// the pure reload: the DBN metadata carries `start`, `end` and `limit`, so a
// reload needs nothing but the file.
//
// WHAT IT DOES NOT DO, deliberately (SPEC §6 assigns each elsewhere): no shim
// planner, no SQL lint, no event time, no `ReplaySource`, no sink, no
// benchmark. And no B1/B2 number — at 1000 records fixed costs dominate and a
// rows/sec from this would be a wrong number, not a weak one (ENC-1485/1487).
//
// `book_snapshots` IS NOT WRITTEN, and that is a scope statement rather than
// an omission. A snapshot is reconstructed book STATE (docs §8.5) and
// reconstructing it is `OrderBookManager`'s job — the single implementation —
// which this job does not drive. With a 68-second MBO window there is also no
// session open to snapshot at.
//
// `gaps` IS NOT WRITTEN EITHER, and the reason is a measurement rather than
// scope: see `Report::gapsNote` and Gaps.hpp. The job REFUSES to infer a gap
// from a seq jump (ENC-1506), reports per kind how many phantom rows that
// refusal kept out, and refuses to record a window complete over a record the
// vendor flagged F_MAYBE_BAD_BOOK.
//

#include "krn/data/ClickHouseHttp.hpp"
#include "krn/data/DatabentoCsv.hpp"
#include "krn/data/IngestWindows.hpp"
#include "krn/data/Verdict.hpp"

#include <arrow/result.h>
#include <arrow/status.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace krn::data::ingest {

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------
struct Options {
  // WHERE THE SAMPLE LIVES, AND WHY IT IS A PARAMETER WITH THIS DEFAULT.
  // `~/market-data/databento/...` is OUTSIDE every git working tree on
  // purpose: it is licensed vendor data and docs/data-layer.md §14.2 names
  // committing Databento data a redistribution question not worth answering.
  // So the path is configurable, nothing here embeds a row of it, and the
  // retained DBN (D6) is written beside the CSVs rather than into the repo.
  std::string dataRoot;

  std::optional<std::string> url;  // ClickHouse; see ch::resolveTarget

  // 0 means DERIVE it from the pull — see `Report::sessionIdRuling`.
  std::uint32_t sessionId = 0;

  std::uint8_t dbnVersion = 3;

  // Skip the CSV legs and decode the retained DBN only. The D6 reload.
  bool fromDbnOnly = false;

  // Transcode, retain and round-trip, but touch no table. Also the way to run
  // the whole raw half with no server.
  bool noInsert = false;

  // Re-encode the DBN even if a retained file is already byte-identical.
  // Normally a no-op; the flag exists so a deliberate re-encode after an
  // encoder change is not indistinguishable from the idempotent case.
  bool forceReencode = false;

  // ---- ENC-1640: a DELIVERED VENDOR DBN JOB ------------------------------
  //
  // Non-empty selects the mode: `dbnJobDir` is one vendor batch job as
  // delivered (`metadata.json`, `manifest.json`, `condition.json` and one
  // `xnas-itch-YYYYMMDD.<schema>.dbn.zst` per UTC day). Each day file is one
  // window, ingested and loaded before the next is read, so a month of mbo is
  // never held in memory at once. The delivered `.dbn.zst` IS the D6 retained
  // file: it is read, never written. It is decompressed into `scratchDir`
  // (required) because the adapter refuses a zstd frame by name, and the
  // decompressed copy is deleted once the day is decoded. `days` limits the
  // run to those yyyymmdd day files; empty means every one in the job.
  std::string dbnJobDir;
  std::string scratchDir;
  std::vector<std::uint32_t> days;
};

// ---------------------------------------------------------------------------
// One day file of a delivered DBN job, and its window (ENC-1640).
// ---------------------------------------------------------------------------
struct DbnJobDay {
  std::uint32_t day = 0;     // yyyymmdd from the file name
  std::string   path;        // the delivered .dbn.zst
  std::uint64_t bytes = 0;   // its size on disk, checked against manifest.json
  std::string   condition;   // condition.json's word for the day
  // [fromNs, toNs) = that UTC day ∩ [query.start, query.end). Half-open.
  std::int64_t  fromNs = 0;
  std::int64_t  toNs   = 0;
};

struct DbnJobPlan {
  csv::PullMetadata md;
  std::string kind;       // 'trades' | 'book_events'
  std::string dbnSchema;  // 'trades' | 'mbo'
  std::vector<DbnJobDay> days;
};

// Reads and checks the job directory without decoding anything. Every refusal
// here is COULD-NOT-RUN (exit 2): no metadata.json, `encoding != dbn`,
// `compression != zstd`, `split_duration != day`, a schema this job has no
// table for, a day file whose schema segment is not `query.schema`, a manifest
// for another job_id or disagreeing on a file's size, a day outside the query,
// or a requested day with no file.
arrow::Result<DbnJobPlan> planDbnJob(const std::string& dir,
                                     const std::vector<std::uint32_t>& onlyDays);

// The UTC day ∩ [queryStart, queryEnd), half-open. Empty (to <= from) when
// they do not meet.
std::pair<std::int64_t, std::int64_t> dayWindow(std::uint32_t yyyymmdd,
                                                std::uint64_t queryStart,
                                                std::uint64_t queryEnd);

// Streams one zstd file (one or more frames) into `outPath` through Arrow's
// ZSTD codec. A truncated final frame is refused. Returns the bytes written.
arrow::Result<std::uint64_t> decompressZstdFile(const std::string& inPath,
                                                const std::string& outPath);

// ---------------------------------------------------------------------------
// What one kind's ingest produced.
// ---------------------------------------------------------------------------
struct KindReport {
  std::string kind;        // 'trades' | 'book_events' — the ingest_windows value
  std::string dbnSchema;   // 'trades' | 'mbo'          — the vendor's name
  std::string csvPath;
  std::string dbnPath;
  std::string jobId;       // vendor batch id -> ingest_windows.source_ref
  std::string symbol;
  std::uint32_t instrumentId = 0;
  std::string venue;
  // PER KIND, not per run: it is FNV-1a-32(dataset | schema | job_id), and the
  // two kinds are two vendor jobs. See `Report::sessionIdRuling`.
  std::uint32_t sessionId = 0;

  std::size_t csvRecords  = 0;
  std::size_t dbnBytes    = 0;
  bool        dbnExisted  = false;
  bool        dbnRewritten = false;

  std::size_t decodedRecords = 0;  // DBN records framed by the adapter
  std::size_t canonicalRows  = 0;  // TickRecords or BookRecords produced
  std::size_t rowsInserted   = 0;

  // The REQUESTED window from the pull metadata, and the window actually
  // RECORDED in krn.ingest_windows. They differ when the pull was truncated,
  // and that difference is the whole D11 story on this sample.
  std::int64_t requestedFromNs = 0;
  std::int64_t requestedToNs   = 0;
  std::int64_t recordedFromNs  = 0;
  std::int64_t recordedToNs    = 0;
  std::uint64_t pullLimit      = 0;
  bool truncatedByLimit        = false;
  std::size_t recordsAtLastTsEvent = 0;

  // Round trip (acceptance criterion 2).
  bool roundTripOk = false;
  std::string roundTripNote;

  // Confirmations of the facts the ticket says to confirm rather than
  // rediscover. Reported as counts so "91 of 91" is visible, not implied.
  std::size_t tsRecvMatchesDelta = 0;
  std::vector<std::pair<char, std::size_t>> actionCounts;
  std::size_t sideUnspecified = 0;

  // Free measurement for ENC-1484, since the rows are in hand: how many
  // (symbol, ts_event, provider_seq) triples are shared by more than one row.
  // D5 chose plain MergeTree precisely because the sort key is not unique;
  // this is the number.
  std::size_t duplicateSortKeys = 0;
  std::size_t distinctSortKeys  = 0;

  // krn.gaps (ENC-1506, Gaps.hpp). `gapRowsWritten` is the rows the job wrote
  // and is 0 by construction; `gapNaiveSeqJumps` is how many a seq-jump
  // detector WOULD have written, i.e. the phantom holes refused.
  std::string gapPullScope;
  std::size_t gapNaiveSeqJumps   = 0;
  std::size_t gapRowsWritten     = 0;
  std::size_t gapMaybeBadBook    = 0;
  std::string gapRefusal;

  // ENC-1640. `dbnPath` is the retained file; on a DBN job that is the
  // delivered .dbn.zst and `dbnBytes` its compressed size, while
  // `decompressedBytes` is what the adapter decoded.
  std::uint64_t decompressedBytes = 0;
  std::string   dbnVersionNote;   // "DBN v1" etc., from the decoded metadata
  std::string   condition;        // condition.json, DBN job only
  // mbo records flagged F_SNAPSHOT: the vendor's start-of-day book snapshot.
  // Not written to krn.book_events: see `kSnapshotRuling`.
  std::size_t   snapshotExcluded = 0;
  std::size_t   actionNone       = 0;  // mbo action N, decoded and row-less
};

struct Report {
  ch::Target target;
  std::vector<KindReport> kinds;
  std::size_t symbologyRows = 0;
  std::vector<std::string> partitionsDropped;
  std::vector<std::string> invalidationRows;  // state=3 rows written

  // THE RULING ON `session_id` REUSE, assigned to this ticket from ENC-1480.
  // Held as text so the tool prints it and the ticket comment and SPEC §5 copy
  // the same words. See `kSessionIdRuling`.
  std::string sessionIdRuling;
  std::string gapsNote;
};

// The two decided questions this ticket owed, in the words the SPEC gets.
extern const char* const kSessionIdRuling;
extern const char* const kGapsNote;
extern const char* const kSnapshotRuling;

// ---------------------------------------------------------------------------
// D6's baseline must never be replaced by bytes the adapter refuses.
// ---------------------------------------------------------------------------
//
// ⚠ CODE-REVIEW FINDING #3, THE WORST OF THE REVIEW, AND IT DESTROYED THE ONE
// THING D6 EXISTS TO PROTECT.
//
// The first version ordered the work transcode -> `retain` -> read back ->
// decode, so the retained `.dbn` — the file D5's re-ingest reloads FROM, and
// the only copy of the pull that does not cost Databento plan allowance — was
// overwritten BEFORE anything had established that the new bytes decode.
// Reproduced against the binary: `--dbn-version 7` replaced
// `xnas-itch-20220610.trades.dbn` with a file the adapter refuses (an unknown
// DBN version is refused, never guessed — correctly) and only THEN failed,
// after which `krn_ingest reload`, whose entire premise is "a reload must
// reload FROM something", failed permanently on that file. The surviving files
// were intact only because a later clean run regenerated them: luck about
// timing, not a property of the design.
//
// So the order is **decode, then retain**. This function proves the bytes
// decode through the real adapter first and writes them only if they do, which
// makes the retained file's invariant "every byte of this was accepted by the
// decoder that will read it back".
//
// It is a named function rather than two reordered statements so that the
// ordering itself is unit-testable without a ClickHouse — see
// `IngestRetainOnlyIfDecodable` in data/tests/DatabentoCsvTest.cpp, which
// asserts an existing good baseline SURVIVES an attempt to replace it with
// undecodable bytes. A reordering with no test is a reordering the next edit
// undoes.
arrow::Result<csv::RetentionResult> retainOnlyIfDecodable(
    const std::string& path, const std::vector<std::byte>& bytes);

// ---------------------------------------------------------------------------
// Which recorded windows a DROP PARTITION invalidates (§8.6, ENC-1635).
// ---------------------------------------------------------------------------
//
// `intersecting` is every distinct recorded window of one kind, any symbol,
// that intersects a day partition about to be dropped. The result is the
// windows that get a state=3 row before the drop. A named function rather
// than a SQL predicate, so the selection is unit-testable without a
// ClickHouse (`IngestWindowsInvalidation` in data/tests/IngestWindowsTest.cpp).
struct RecordedWindow {
  std::string  symbol;
  std::int64_t fromTsNs = 0;
  std::int64_t toTsNs   = 0;  // half-open
  bool operator==(const RecordedWindow&) const = default;
};

std::vector<RecordedWindow> invalidationTargets(
    const std::vector<RecordedWindow>& intersecting,
    const std::string& ownSymbol, std::int64_t ownFromNs, std::int64_t ownToNs);

// ---------------------------------------------------------------------------
// Run it.
// ---------------------------------------------------------------------------
//
// EXIT-CODE CLASSIFICATION: a non-OK status from here is tagged by
// `krn::data::asFinding` when it is a FINDING about the data (exit 1) and left
// untagged when the job COULD NOT RUN (exit 2). `krn::data::exitCodeFor` is the
// only thing `main` needs. See Verdict.hpp for why the classification is
// explicit at the refusal site rather than inferred from a status code.
arrow::Result<Report> run(const Options& opts);

// ---------------------------------------------------------------------------
// The completeness read (D11), against a live server.
// ---------------------------------------------------------------------------
//
// Selects every `krn.ingest_windows` row for one `(symbol, kind)` — projection
// and a `WHERE` on the key prefix, no aggregate, no `FINAL`, no `argMax` — and
// hands them to `krn::data::isComplete`. The coverage test is in C++ because
// the DDL says so: it keeps this a plain selection on this side of the D2
// boundary.
arrow::Result<Completeness> queryCompleteness(const ch::Client& client,
                                              const std::string& symbol,
                                              const std::string& kind,
                                              std::int64_t reqFromNs,
                                              std::int64_t reqToNs);

// Row counts, for the report. NEVER used to decide completeness — a quiet
// window and a missing window have the same count (D11), which is why this
// returns alongside the verdict rather than instead of it.
arrow::Result<std::int64_t> rowCount(const ch::Client& client,
                                     const std::string& table,
                                     const std::string& symbol,
                                     std::int64_t fromNs, std::int64_t toNs);

// Formats a window as `[<iso>, <iso>)` for the report.
std::string formatWindow(std::int64_t fromNs, std::int64_t toNs);
std::string formatNs(std::int64_t ns);

}  // namespace krn::data::ingest
