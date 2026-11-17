# B1 on the NVDA month: ClickHouse → krn transport (ENC-1662, 2026-11-17)

Design: krn `specs/2026-10-14-krn-data-layer/SPEC.md` D13; `docs/data-layer.md` §5.2, §14.
Apparatus: `krn_b1_transport` via `data/bench/run_b1.sh` (ENC-1485), with `--where`/`--label`,
`--warmup-iters`, a per-cell loadavg column, and an untimed reconnect of an idle-closed HTTP
keep-alive added in this ticket. Earlier B1 rows (MSFT T5 sample, 1000 rows) are in
`docs/data-layer.md` §14.3.

## Verdict

**Use the native protocol.** On every range, cache state and block size, native is
**1.8–2.5× the rows/s of HTTP `FORMAT ArrowStream`**. Across 100 cells the two arms' p10–p90
bands never overlap. On the 1000-row sample the two arms were within noise. At this volume they
are not.

- **The gap is on the server.** `system.query_log` on the heaviest MBO day (2026-11-05,
  30.2 M rows, block 8192) gives a server-side duration of 2393 ms native vs 5236 ms HTTP, and
  server user CPU of 2525 ms vs 4474 ms. Both match the client wall times (2.37 s vs 5.17 s).
  The time goes into ClickHouse's single-threaded (`max_threads=1`) Arrow serialisation, not
  into the client's `StreamDecoder`.
- **Wire volume.** HTTP sends 1.25–1.38× more bytes for the same rows: 2.90 GB vs 2.33 GB on
  the heaviest MBO day.
- **Allocations.** Native makes more allocations per block: about 143–160 for clickhouse-cpp
  against 53–105 for Beast+Arrow. That does not offset the server cost.
- **Block size stops helping at 8192**, which is `index_granularity`. 65536 delivers the same
  blocks as 8192, give or take a few, at the same rate. Below 8192 the rate falls
  near-linearly with block size: 64-row blocks run at about 1.2–1.9 M rows/s native. The native
  read path should therefore use `max_block_size ≥ 8192`.
- **Compact parts ignore `max_block_size`.** This explains ENC-1485's collapsed sweep. A
  MergeTree part under `min_bytes_for_wide_part` (10 MB uncompressed) is Compact, and a Compact
  part is read one granule (≤ 8192 rows) per block whatever `max_block_size` says. The NVDA
  trades days 10-27, 10-29 and 11-03 are Compact parts and deliver 19–23 blocks at every
  requested size. 11-02, 11-05 and every MBO day are Wide and follow the request. The T5 sample
  was 1000-row Compact parts, hence one block per query at every size.

## The B1 half of `B2 ≥ B1`, by day

This is the number a krn graph has to beat to avoid being the bottleneck: rows/s delivered into
krn's address space, with no compute. **B1 ceiling** means native, warm, at the better of block
8192 and 65536. The cold figures and the HTTP figures are given alongside. ENC-1641 (B2) is
timing the same NVDA days: trades on 10-27, 10-29 and 11-05, and the MBO days once it can. Its
numbers had not been posted when this ran, so this side is stated for that comparison and no
verdict is drawn here.

| range | rows | **B1 native warm** | HTTP warm | native cold | HTTP cold |
|---|---:|---:|---:|---:|---:|
| trades 2026-10-27 | 155,905 | **13.26 M/s** | 6.05 M/s | 12.09 M/s | 5.72 M/s |
| trades 2026-10-29 | 176,160 | **13.75 M/s** | 6.75 M/s | 12.12 M/s | 6.23 M/s |
| trades 2026-11-02 (typical) | 236,165 | **14.54 M/s** | 6.77 M/s | 9.56 M/s | 5.24 M/s |
| trades 2026-11-05 (heaviest MBO day) | 959,298 | **17.25 M/s** | 7.27 M/s | 13.70 M/s | 6.65 M/s |
| trades week 2026-11-02..08 | 2,163,840 | **17.33 M/s** | 7.35 M/s | 12.75 M/s | 6.33 M/s |
| trades month (all NVDA) | 9,920,914 | **19.36 M/s** | 7.70 M/s | 14.42 M/s | 6.83 M/s |
| mbo 2026-10-27 | 7,010,872 | **12.77 M/s** | 6.23 M/s | 11.28 M/s | 5.91 M/s |
| mbo 2026-10-29 | 9,375,081 | **12.84 M/s** | 6.04 M/s | 10.88 M/s | 5.80 M/s |
| mbo 2026-11-02 (typical) | 11,822,359 | **12.96 M/s** | 6.33 M/s | 11.58 M/s | 6.01 M/s |
| mbo 2026-11-05 (heaviest) | 30,176,956 | **12.86 M/s** | 5.84 M/s | 11.91 M/s | 5.91 M/s |

For B2, the threshold is rows/s per graph per day, at least the B1 native warm value. Use the
cold value where B2 is compared against a cold first read. In wall time, the heaviest MBO day
crosses the transport in **2.35 s** (native, warm). That is 2.4 s; HTTP takes 5.2 s.

- **trades:** the rate rises with range length, from 13 M/s for a day to 19 M/s for the month.
  Per-query fixed cost is amortised over more rows, and the Wide parts of the heavier days read
  in full 8192-row blocks.
- **mbo:** flat at about 12.8 M/s from 7 M to 30 M rows. `book_events` rows are wider than
  trades rows: 77 vs 62 wire bytes per row, native.
- **Cold vs warm.** The cold median is 0.65–1.02× the warm one. L1 is small (trades 89 MiB, `book_events`
  3.56 GiB on disk), and "cold" here evicts the page cache but not the device cache (see
  *Method*).

## Against the MSFT T5 rows (ENC-1485)

| | MSFT T5 sample, 1000 rows | NVDA, this run |
|---|---|---|
| trades warm, native / HTTP | 906 k / 888 k rows/s | 13.3–19.4 M / 6.1–7.7 M rows/s |
| book_events warm, native / HTTP | 861 k / 823 k rows/s | 12.8–13.0 M / 5.8–6.3 M rows/s |
| what the figure measures | per-query fixed cost: ~1.1 ms per 1000 rows | a streaming rate: 12 ms (one trades day) to 2.35 s (heaviest MBO day) per query |
| native vs HTTP | inside each other's p10–p90 in every cell | **1.8–2.5×**, bands disjoint in all 100 cells |
| blocks per query | 1 at every requested size (Compact parts) | follows the request on Wide parts and stops at 8192; one per granule on Compact parts |
| HTTP extra wire bytes | +23–27 % | +25–38 % |
| allocs/block, native / HTTP | 288–309 / 123–139 | 143–160 / 53–105 |

The sample's "within noise" was true, but it measured connection and query overhead, not
transport. With multi-granule ranges the transports separate, and the per-row encoder cost
dominates.

## Ingest and verify (before timing)

The ingest ran against `STACK_PORT_OFFSET=130 tools/stack.sh up clickhouse`: ClickHouse
25.3.14.14, image `sha256:a4e8cf46…ae62`. The schema was applied fresh to the empty store with
`STACK_PORT_OFFSET=130 CH_HTTP_URL=http://127.0.0.1:8253 data/schema/apply.sh --fresh` and was
ACCEPTED.

```
STACK_PORT_OFFSET=130 build/data/krn_ingest ingest \
  --dbn-job ~/market-data/databento/XNAS.ITCH/NVDA/trades-dbn-XNAS-20261117-R7EH4L6T8P --scratch <_ENC-1662>/scratch
STACK_PORT_OFFSET=130 build/data/krn_ingest ingest \
  --dbn-job ~/market-data/databento/XNAS.ITCH/NVDA/mbo-dbn-XNAS-20261117-4NJAJ5R9RQ  --scratch <_ENC-1662>/scratch
STACK_PORT_OFFSET=130 build/data/krn_ingest verify --symbol NVDA
```

| job | day files | DBN records decoded | rows in L1 | difference | wall | max RSS |
|---|---:|---:|---:|---|---:|---:|
| trades | 23 | 9,920,914 | 9,920,914 | 0 | 36.4 s | 0.58 GB |
| mbo | 23 | 304,286,848 | 304,003,761 | 283,087 = exactly the F_SNAPSHOT records excluded by the ENC-1640 ruling | 20 min 30 s | 7.69 GB |

`verify --symbol NVDA` returned `VERDICT: clean (0)`, exit 0, with 58 `[ok]` checks: 23
windows per kind, and every state=1 claim matched its rows. The machine then crashed and was
restarted. After the restart, the caps were in force again (`doctor` exit 0) and `verify` was
clean again (58 `[ok]`). `CHECK TABLE` returned 1 on both tables, there were no detached
parts, and the counts were unchanged, so no day was re-ingested. No timing cell had started
before the crash.

The memory caps were never raised. `max_server_memory_usage` stayed at 8 GiB and
`max_memory_usage` at 4 GiB; both halves of the constraint are binding and were re-read by
every run. `mark_cache_size` was 1 GiB and `uncompressed_cache_size` 0. Neither the MBO month
ingest nor any B1 query hit a cap: the B1 query streams at `max_threads=1`. Disk use was
3.65 GiB for L1.

## Method

Build: `cmake -B build -DCMAKE_BUILD_TYPE=Release -DKRN_DATA_BUILD_BENCHMARKS=ON
-DKRN_DATA_BUILD_TESTS=ON`. No "not used by the project" lines; the engine was pinned at `75a5616d`;
no fast-math or FP flags; ctest 3/3.

Every range is one `run_b1.sh` invocation. The five-size block sweep (64, 256, 1024, 8192,
65536) runs × {cold, warm} × {native, http}, and the transports alternate per block size.
Server threads are pinned to CPUs 0-5,8-13 and the client to CPU 7. One query is in flight at
a time.

**Cold** means that before every timed run the mark, uncompressed, query and mmap caches are
dropped and every file of the table is `posix_fadvise(DONTNEED)`'d. The eviction is then read
back with `mincore` as 0 resident pages; all cold runs were verified. This is not
`drop_caches`. **Warm** means untimed warm-up runs, then timed runs back to back.

The ranges are `WHERE symbol = 'NVDA' AND ts_event >= toDateTime64('<d> 00:00:00', 9, 'UTC')
AND ts_event < toDateTime64('<d+1> 00:00:00', 9, 'UTC')`, identical on both arms and selected
through the primary key. The month is `WHERE symbol = 'NVDA'`. An untimed checksum (rows,
Σprice, Σsize) agreed between the arms on every range.

The exact command for one range, from the krn-data worktree:

```
STACK_PORT_OFFSET=130 KRN_B1_WORKSPACE=/home/ndrandal/encultured-workspace KRN_B1_CSV=<out>/<label>.csv \
  data/bench/run_b1.sh build -- --tables <trades|book_events> \
  --where "symbol = 'NVDA' AND ts_event >= toDateTime64('2026-11-05 00:00:00', 9, 'UTC') AND ts_event < toDateTime64('2026-11-06 00:00:00', 9, 'UTC')" \
  --label <label> --warm-iters <W> --warmup-iters <U> --cold-iters <C>
```

| label | table | range [from, to) | W / U / C |
|---|---|---|---|
| trades-day-1027, -1029, -1102 | trades | one UTC day | 100 / 5 / 20 |
| trades-day-1105 | trades | 2026-11-05 | 50 / 5 / 15 |
| trades-week-1102 | trades | 2026-11-02 to 2026-11-09 | 30 / 3 / 10 |
| trades-month | trades | `symbol = 'NVDA'` (2026-10-19 to 2026-11-16) | 10 / 2 / 5 |
| mbo-day-1027, -1029, -1102 | book_events | one UTC day | 10 / 2 / 5 |
| mbo-day-1105 | book_events | 2026-11-05 | 6 / 1 / 4 |

The iteration counts were scaled so that each range finishes in 0.5–25 min. All are real rows,
repeated as iterations. "Typical day" is the median weekday by row count, which is 2026-11-02
for both schemas. 10-27 and 10-29 were added because they are ENC-1641's B2 days.

**Timing hygiene.** Each range ran under the shared `flock …/_bench.lock`, so it never
overlapped B2 or another locked heavy job. Inside the lock it waited for 1-min loadavg < 3.0
and zero running merges, and released the lock to retry if that did not happen. Every cell
records the 1-min loadavg at its end. Across all final cells it ranged from **1.27 to 3.63**,
and that figure includes this benchmark's own server and client threads. trades-day-1102 was
first taken while the 5/15-min averages were still 7–10 from an earlier burst, and its cold
bands were wide. It was re-taken at 1.6–2.0, and the re-take is the row below. Both CSVs are in
the evidence directory. Unlocked work from other sessions (desktop apps, unrelated builds) was
not excluded: the gate only bounds it.

**HTTP keep-alive.** ClickHouse closes a keep-alive connection that has been idle past
`keep_alive_timeout` (10 s). A long native cell left the HTTP connection idle that long, and
the first attempt aborted on `end of stream`. The HTTP arm now peeks the socket before each run
and reconnects outside the timed window. The reconnect count is printed: 0–10 per range.

**Cold right after ingest.** A cold run straight after an ingest aborts with `cold run not
cold`. Dirty pages that have not been written back cannot be dropped by `POSIX_FADV_DONTNEED`.
The gate is correct to refuse; wait for writeback.

Evidence, outside every git tree:
`trees/gma-v3-data-layer-l1-clickhouse-gma-as-t/_ENC-1662/`:

- `ingest-{trades,mbo}.log`
- `verify-nvda*.log`
- `doctor-after-crash.log`
- `runs/final/*.{txt,csv}`
- `runs/rerun-1102/`
- `runs/combined/`, the set tabulated here
- `server-side-split.txt`
- `parts.tsv`
- `b1_nvda*.sh`, the gated driver
- `summarize.py`, which builds this table from the CSVs

## Full table

Medians and p10–p90 of per-run rows/s. "bands overlap?" is whether native's p10–p90 intersects
HTTP's. MB/q is bytes received on the socket per query. n is timed runs per arm.

| range | rows | cache | block | blocks/q | native rows/s med (p10–p90) | http rows/s med (p10–p90) | native/http | bands overlap? | native MB/q | http MB/q | n (per arm) | load1 at end of cell (native/http) |
|---|---:|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---|
| trades-day-1027 | 155,905 | cold | 64 | 19/19 | 11.25 M (10.52 M–11.91 M) | 5.67 M (5.47 M–5.75 M) | 1.98× | no | 9.7 | 12.6 | 20 | 1.29 / 1.29 |
| trades-day-1027 | 155,905 | cold | 256 | 19/19 | 11.75 M (11.06 M–12.15 M) | 5.81 M (5.64 M–5.86 M) | 2.02× | no | 9.7 | 12.6 | 20 | 1.29 / 1.29 |
| trades-day-1027 | 155,905 | cold | 1024 | 19/19 | 12.03 M (11.42 M–12.15 M) | 5.71 M (5.55 M–5.76 M) | 2.11× | no | 9.7 | 12.6 | 20 | 1.29 / 1.27 |
| trades-day-1027 | 155,905 | cold | 8192 | 19/19 | 12.09 M (11.25 M–12.19 M) | 5.72 M (5.68 M–5.75 M) | 2.11× | no | 9.7 | 12.6 | 20 | 1.27 / 1.27 |
| trades-day-1027 | 155,905 | cold | 65536 | 19/19 | 11.07 M (10.22 M–11.94 M) | 5.67 M (5.32 M–5.72 M) | 1.95× | no | 9.7 | 12.6 | 20 | 1.27 / 1.27 |
| trades-day-1027 | 155,905 | warm | 64 | 19/19 | 12.98 M (11.93 M–13.76 M) | 6.15 M (6.07 M–6.19 M) | 2.11× | no | 9.7 | 12.6 | 100 | 1.27 / 1.41 |
| trades-day-1027 | 155,905 | warm | 256 | 19/19 | 13.30 M (12.17 M–13.74 M) | 6.07 M (5.82 M–6.17 M) | 2.19× | no | 9.7 | 12.6 | 100 | 1.41 / 1.45 |
| trades-day-1027 | 155,905 | warm | 1024 | 19/19 | 12.85 M (12.03 M–13.50 M) | 6.10 M (5.91 M–6.19 M) | 2.11× | no | 9.7 | 12.6 | 100 | 1.45 / 1.50 |
| trades-day-1027 | 155,905 | warm | 8192 | 19/19 | 13.25 M (12.11 M–13.67 M) | 6.01 M (5.81 M–6.17 M) | 2.20× | no | 9.7 | 12.6 | 100 | 1.50 / 1.50 |
| trades-day-1027 | 155,905 | warm | 65536 | 19/19 | 13.26 M (12.37 M–13.56 M) | 6.05 M (5.86 M–6.16 M) | 2.19× | no | 9.7 | 12.6 | 100 | 1.94 / 1.94 |
| trades-day-1029 | 176,160 | cold | 64 | 22/22 | 12.08 M (11.39 M–12.45 M) | 6.24 M (5.79 M–6.28 M) | 1.94× | no | 10.9 | 14.3 | 20 | 2.40 / 2.40 |
| trades-day-1029 | 176,160 | cold | 256 | 22/22 | 11.55 M (10.79 M–12.20 M) | 6.18 M (5.86 M–6.26 M) | 1.87× | no | 10.9 | 14.3 | 20 | 2.40 / 2.40 |
| trades-day-1029 | 176,160 | cold | 1024 | 22/22 | 12.21 M (11.55 M–12.42 M) | 6.18 M (5.67 M–6.35 M) | 1.98× | no | 10.9 | 14.3 | 20 | 2.40 / 2.37 |
| trades-day-1029 | 176,160 | cold | 8192 | 22/22 | 12.12 M (11.25 M–12.49 M) | 6.23 M (6.06 M–6.31 M) | 1.94× | no | 10.9 | 14.3 | 20 | 2.37 / 2.37 |
| trades-day-1029 | 176,160 | cold | 65536 | 21/21 | 11.31 M (10.97 M–11.78 M) | 6.04 M (5.95 M–6.09 M) | 1.87× | no | 10.9 | 14.3 | 20 | 2.37 / 2.37 |
| trades-day-1029 | 176,160 | warm | 64 | 22/22 | 13.10 M (12.36 M–13.98 M) | 6.70 M (6.44 M–6.79 M) | 1.96× | no | 10.9 | 14.3 | 100 | 2.37 / 2.34 |
| trades-day-1029 | 176,160 | warm | 256 | 22/22 | 13.74 M (12.66 M–14.00 M) | 6.74 M (6.66 M–6.80 M) | 2.04× | no | 10.9 | 14.3 | 100 | 2.34 / 2.31 |
| trades-day-1029 | 176,160 | warm | 1024 | 22/22 | 13.14 M (12.14 M–14.03 M) | 6.72 M (6.48 M–6.78 M) | 1.96× | no | 10.9 | 14.3 | 100 | 2.31 / 2.53 |
| trades-day-1029 | 176,160 | warm | 8192 | 22/22 | 13.75 M (12.67 M–14.01 M) | 6.75 M (6.46 M–6.83 M) | 2.04× | no | 10.9 | 14.3 | 100 | 2.53 / 2.56 |
| trades-day-1029 | 176,160 | warm | 65536 | 21/21 | 13.60 M (12.61 M–13.91 M) | 6.50 M (6.39 M–6.57 M) | 2.09× | no | 10.9 | 14.3 | 100 | 2.56 / 2.56 |
| trades-day-1102 | 236,165 | cold | 64 | 3691/3691 | 1.45 M (1.41 M–1.46 M) | 0.65 M (0.64 M–0.65 M) | 2.24× | no | 16.0 | 22.0 | 20 | 1.68 / 1.62 |
| trades-day-1102 | 236,165 | cold | 256 | 923/923 | 3.97 M (3.60 M–4.04 M) | 1.95 M (1.90 M–1.97 M) | 2.04× | no | 15.0 | 19.8 | 20 | 1.65 / 1.65 |
| trades-day-1102 | 236,165 | cold | 1024 | 231/231 | 7.05 M (6.41 M–7.16 M) | 3.87 M (3.83 M–3.91 M) | 1.82× | no | 14.7 | 19.3 | 20 | 1.65 / 1.60 |
| trades-day-1102 | 236,165 | cold | 8192 | 29/29 | 9.56 M (9.08 M–9.92 M) | 5.24 M (5.05 M–5.32 M) | 1.82× | no | 14.7 | 19.2 | 20 | 1.60 / 1.60 |
| trades-day-1102 | 236,165 | cold | 65536 | 29/29 | 9.43 M (9.13 M–9.78 M) | 5.20 M (5.01 M–5.31 M) | 1.81× | no | 14.7 | 19.2 | 20 | 1.60 / 1.55 |
| trades-day-1102 | 236,165 | warm | 64 | 3691/3691 | 1.48 M (1.43 M–1.51 M) | 0.66 M (0.65 M–0.67 M) | 2.25× | no | 16.0 | 22.0 | 100 | 1.58 / 1.64 |
| trades-day-1102 | 236,165 | warm | 256 | 923/923 | 4.50 M (4.14 M–4.61 M) | 2.08 M (2.04 M–2.10 M) | 2.16× | no | 15.0 | 19.8 | 100 | 1.70 / 1.75 |
| trades-day-1102 | 236,165 | warm | 1024 | 231/231 | 9.39 M (7.79 M–9.57 M) | 4.45 M (4.41 M–4.48 M) | 2.11× | no | 14.7 | 19.3 | 100 | 1.77 / 1.71 |
| trades-day-1102 | 236,165 | warm | 8192 | 29/29 | 14.49 M (13.36 M–15.08 M) | 6.77 M (6.63 M–6.83 M) | 2.14× | no | 14.7 | 19.2 | 100 | 1.71 / 1.97 |
| trades-day-1102 | 236,165 | warm | 65536 | 29/29 | 14.54 M (13.42 M–15.16 M) | 6.75 M (6.34 M–6.84 M) | 2.15× | no | 14.7 | 19.2 | 100 | 1.97 / 1.98 |
| trades-day-1105 | 959,298 | cold | 64 | 14990/14990 | 1.46 M (1.42 M–1.49 M) | 0.66 M (0.65 M–0.66 M) | 2.22× | no | 65.1 | 89.3 | 15 | 2.17 / 1.97 |
| trades-day-1105 | 959,298 | cold | 256 | 3748/3748 | 4.22 M (4.14 M–4.47 M) | 2.06 M (2.03 M–2.08 M) | 2.05× | no | 60.9 | 80.6 | 15 | 2.06 / 2.05 |
| trades-day-1105 | 959,298 | cold | 1024 | 937/937 | 8.83 M (8.54 M–9.07 M) | 4.31 M (4.24 M–4.34 M) | 2.05× | no | 59.8 | 78.4 | 15 | 1.97 / 1.89 |
| trades-day-1105 | 959,298 | cold | 8192 | 118/118 | 13.54 M (13.20 M–13.71 M) | 6.65 M (6.54 M–6.67 M) | 2.04× | no | 59.5 | 77.8 | 15 | 1.89 / 1.89 |
| trades-day-1105 | 959,298 | cold | 65536 | 117/117 | 13.70 M (13.56 M–13.77 M) | 6.63 M (6.57 M–6.68 M) | 2.07× | no | 59.5 | 77.8 | 15 | 1.89 / 1.90 |
| trades-day-1105 | 959,298 | warm | 64 | 14990/14990 | 1.48 M (1.45 M–1.50 M) | 0.66 M (0.66 M–0.67 M) | 2.23× | no | 65.1 | 89.3 | 50 | 2.25 / 1.83 |
| trades-day-1105 | 959,298 | warm | 256 | 3748/3748 | 4.69 M (4.55 M–4.78 M) | 2.11 M (2.00 M–2.14 M) | 2.23× | no | 60.9 | 80.6 | 50 | 1.93 / 2.14 |
| trades-day-1105 | 959,298 | warm | 1024 | 937/937 | 10.07 M (9.71 M–10.45 M) | 4.58 M (4.43 M–4.63 M) | 2.20× | no | 59.8 | 78.4 | 50 | 2.05 / 2.12 |
| trades-day-1105 | 959,298 | warm | 8192 | 118/118 | 16.98 M (15.69 M–17.17 M) | 7.27 M (6.99 M–7.38 M) | 2.34× | no | 59.5 | 77.8 | 50 | 2.12 / 2.26 |
| trades-day-1105 | 959,298 | warm | 65536 | 117/117 | 17.25 M (16.49 M–17.63 M) | 7.15 M (6.74 M–7.29 M) | 2.41× | no | 59.5 | 77.8 | 50 | 2.26 / 2.38 |
| trades-week-1102 | 2,163,840 | cold | 64 | 30938/30938 | 1.52 M (1.44 M–1.54 M) | 0.69 M (0.69 M–0.70 M) | 2.19× | no | 145.7 | 199.3 | 10 | 2.28 / 2.24 |
| trades-week-1102 | 2,163,840 | cold | 256 | 7754/7754 | 4.52 M (4.39 M–4.56 M) | 2.14 M (2.13 M–2.15 M) | 2.11× | no | 137.1 | 181.3 | 10 | 2.38 / 2.25 |
| trades-week-1102 | 2,163,840 | cold | 1024 | 1958/1958 | 8.79 M (8.64 M–8.91 M) | 4.32 M (4.16 M–4.35 M) | 2.03× | no | 134.9 | 176.8 | 10 | 2.25 / 2.15 |
| trades-week-1102 | 2,163,840 | cold | 8192 | 268/268 | 12.62 M (12.30 M–12.81 M) | 6.31 M (6.16 M–6.39 M) | 2.00× | no | 134.3 | 175.5 | 10 | 2.14 / 2.05 |
| trades-week-1102 | 2,163,840 | cold | 65536 | 264/264 | 12.75 M (12.10 M–12.93 M) | 6.33 M (6.07 M–6.41 M) | 2.02× | no | 134.3 | 175.5 | 10 | 2.05 / 2.12 |
| trades-week-1102 | 2,163,840 | warm | 64 | 30938/30938 | 1.57 M (1.54 M–1.59 M) | 0.70 M (0.69 M–0.71 M) | 2.23× | no | 145.7 | 199.3 | 30 | 2.60 / 2.02 |
| trades-week-1102 | 2,163,840 | warm | 256 | 7754/7754 | 4.92 M (4.72 M–4.95 M) | 2.23 M (2.21 M–2.24 M) | 2.20× | no | 137.1 | 181.3 | 30 | 2.09 / 1.93 |
| trades-week-1102 | 2,163,840 | warm | 1024 | 1958/1958 | 10.57 M (10.36 M–10.65 M) | 4.75 M (4.70 M–4.78 M) | 2.22× | no | 134.9 | 176.8 | 30 | 1.94 / 1.80 |
| trades-week-1102 | 2,163,840 | warm | 8192 | 268/268 | 17.33 M (16.81 M–17.39 M) | 7.35 M (7.17 M–7.38 M) | 2.36× | no | 134.3 | 175.5 | 30 | 1.73 / 1.70 |
| trades-week-1102 | 2,163,840 | warm | 65536 | 264/264 | 17.33 M (16.58 M–17.43 M) | 7.34 M (7.25 M–7.37 M) | 2.36× | no | 134.3 | 175.5 | 30 | 1.70 / 1.67 |
| trades-month | 9,920,914 | cold | 64 | 135673/135673 | 1.91 M (1.91 M–1.93 M) | 0.80 M (0.79 M–0.80 M) | 2.40× | no | 665.5 | 908.9 | 5 | 2.00 / 2.24 |
| trades-month | 9,920,914 | cold | 256 | 34039/34039 | 5.43 M (5.36 M–5.44 M) | 2.41 M (2.39 M–2.42 M) | 2.25× | no | 627.8 | 830.0 | 5 | 2.22 / 2.00 |
| trades-month | 9,920,914 | cold | 1024 | 8632/8632 | 10.21 M (10.17 M–10.24 M) | 4.75 M (4.72 M–4.75 M) | 2.15× | no | 618.3 | 810.3 | 5 | 1.92 / 1.93 |
| trades-month | 9,920,914 | cold | 8192 | 1222/1222 | 14.42 M (14.30 M–14.63 M) | 6.80 M (6.66 M–6.83 M) | 2.12× | no | 615.6 | 804.5 | 5 | 1.93 / 2.02 |
| trades-month | 9,920,914 | cold | 65536 | 1209/1209 | 14.32 M (14.11 M–14.69 M) | 6.83 M (6.76 M–6.86 M) | 2.10× | no | 615.6 | 804.5 | 5 | 2.17 / 2.08 |
| trades-month | 9,920,914 | warm | 64 | 135673/135673 | 1.94 M (1.92 M–1.96 M) | 0.81 M (0.80 M–0.81 M) | 2.41× | no | 665.5 | 908.9 | 10 | 2.44 / 2.01 |
| trades-month | 9,920,914 | warm | 256 | 34039/34039 | 5.94 M (5.88 M–5.99 M) | 2.49 M (2.46 M–2.51 M) | 2.38× | no | 627.8 | 830.0 | 10 | 2.28 / 2.00 |
| trades-month | 9,920,914 | warm | 1024 | 8632/8632 | 12.08 M (11.76 M–12.20 M) | 5.05 M (5.01 M–5.07 M) | 2.39× | no | 618.3 | 810.3 | 10 | 3.63 / 3.54 |
| trades-month | 9,920,914 | warm | 8192 | 1222/1222 | 19.33 M (18.97 M–19.50 M) | 7.69 M (7.54 M–7.72 M) | 2.52× | no | 615.6 | 804.5 | 10 | 3.42 / 3.34 |
| trades-month | 9,920,914 | warm | 65536 | 1209/1209 | 19.36 M (19.20 M–19.55 M) | 7.70 M (7.67 M–7.73 M) | 2.51× | no | 615.6 | 804.5 | 10 | 3.28 / 2.92 |
| mbo-day-1027 | 7,010,872 | cold | 64 | 109376/109376 | 1.20 M (1.19 M–1.21 M) | 0.57 M (0.56 M–0.57 M) | 2.12× | no | 586.1 | 778.9 | 5 | 1.53 / 1.54 |
| mbo-day-1027 | 7,010,872 | cold | 256 | 27346/27346 | 3.51 M (3.46 M–3.57 M) | 1.78 M (1.77 M–1.80 M) | 1.97× | no | 551.4 | 699.5 | 5 | 2.30 / 2.33 |
| mbo-day-1027 | 7,010,872 | cold | 1024 | 6838/6838 | 7.35 M (7.25 M–7.36 M) | 3.81 M (3.77 M–3.82 M) | 1.93× | no | 542.8 | 679.7 | 5 | 2.27 / 2.25 |
| mbo-day-1027 | 7,010,872 | cold | 8192 | 857/857 | 11.08 M (10.97 M–11.15 M) | 5.86 M (5.85 M–5.87 M) | 1.89× | no | 540.2 | 673.9 | 5 | 2.23 / 2.12 |
| mbo-day-1027 | 7,010,872 | cold | 65536 | 855/855 | 11.28 M (10.87 M–11.32 M) | 5.91 M (5.81 M–5.93 M) | 1.91× | no | 540.2 | 673.9 | 5 | 2.12 / 1.95 |
| mbo-day-1027 | 7,010,872 | warm | 64 | 109376/109376 | 1.21 M (1.19 M–1.21 M) | 0.57 M (0.57 M–0.58 M) | 2.11× | no | 586.1 | 778.9 | 10 | 2.19 / 1.47 |
| mbo-day-1027 | 7,010,872 | warm | 256 | 27346/27346 | 3.70 M (3.64 M–3.74 M) | 1.84 M (1.83 M–1.84 M) | 2.01× | no | 551.4 | 699.5 | 10 | 1.82 / 1.92 |
| mbo-day-1027 | 7,010,872 | warm | 1024 | 6838/6838 | 7.87 M (7.76 M–7.96 M) | 3.95 M (3.92 M–3.97 M) | 1.99× | no | 542.7 | 679.7 | 10 | 1.92 / 1.72 |
| mbo-day-1027 | 7,010,872 | warm | 8192 | 857/857 | 12.67 M (12.51 M–12.71 M) | 6.21 M (6.18 M–6.24 M) | 2.04× | no | 540.2 | 673.9 | 10 | 1.74 / 2.09 |
| mbo-day-1027 | 7,010,872 | warm | 65536 | 855/855 | 12.77 M (12.72 M–12.87 M) | 6.23 M (6.20 M–6.25 M) | 2.05× | no | 540.2 | 673.9 | 10 | 2.08 / 2.06 |
| mbo-day-1029 | 9,375,081 | cold | 64 | 146486/146486 | 1.19 M (1.18 M–1.21 M) | 0.57 M (0.57 M–0.57 M) | 2.07× | no | 783.8 | 1041.8 | 5 | 2.39 / 1.94 |
| mbo-day-1029 | 9,375,081 | cold | 256 | 36625/36625 | 3.62 M (3.60 M–3.64 M) | 1.78 M (1.78 M–1.79 M) | 2.03× | no | 737.4 | 935.5 | 5 | 1.80 / 1.67 |
| mbo-day-1029 | 9,375,081 | cold | 1024 | 9158/9158 | 7.32 M (7.10 M–7.35 M) | 3.80 M (3.76 M–3.82 M) | 1.92× | no | 725.8 | 908.9 | 5 | 1.87 / 1.89 |
| mbo-day-1029 | 9,375,081 | cold | 8192 | 1148/1148 | 10.88 M (10.64 M–10.91 M) | 5.69 M (5.65 M–5.74 M) | 1.91× | no | 722.4 | 901.1 | 5 | 1.82 / 1.92 |
| mbo-day-1029 | 9,375,081 | cold | 65536 | 1144/1144 | 10.84 M (10.74 M–11.01 M) | 5.80 M (5.77 M–5.82 M) | 1.87× | no | 722.4 | 901.1 | 5 | 1.85 / 1.80 |
| mbo-day-1029 | 9,375,081 | warm | 64 | 146486/146486 | 1.21 M (1.20 M–1.22 M) | 0.57 M (0.57 M–0.57 M) | 2.12× | no | 783.8 | 1041.8 | 10 | 2.11 / 1.55 |
| mbo-day-1029 | 9,375,081 | warm | 256 | 36625/36625 | 3.74 M (3.69 M–3.76 M) | 1.84 M (1.83 M–1.84 M) | 2.03× | no | 737.4 | 935.5 | 10 | 1.86 / 1.69 |
| mbo-day-1029 | 9,375,081 | warm | 1024 | 9158/9158 | 8.00 M (7.91 M–8.03 M) | 3.98 M (3.94 M–4.01 M) | 2.01× | no | 725.8 | 908.9 | 10 | 1.68 / 1.73 |
| mbo-day-1029 | 9,375,081 | warm | 8192 | 1148/1148 | 12.83 M (12.72 M–12.88 M) | 5.66 M (5.62 M–5.71 M) | 2.27× | no | 722.4 | 901.1 | 10 | 1.62 / 1.73 |
| mbo-day-1029 | 9,375,081 | warm | 65536 | 1144/1144 | 12.84 M (12.74 M–12.92 M) | 6.04 M (5.83 M–6.27 M) | 2.13× | no | 722.4 | 901.1 | 10 | 1.77 / 1.83 |
| mbo-day-1102 | 11,822,359 | cold | 64 | 184725/184725 | 1.21 M (1.20 M–1.21 M) | 0.57 M (0.57 M–0.57 M) | 2.11× | no | 988.4 | 1313.8 | 5 | 1.94 / 2.26 |
| mbo-day-1102 | 11,822,359 | cold | 256 | 46182/46182 | 3.66 M (3.62 M–3.67 M) | 1.80 M (1.78 M–1.81 M) | 2.03× | no | 929.9 | 1179.7 | 5 | 2.44 / 2.32 |
| mbo-day-1102 | 11,822,359 | cold | 1024 | 11546/11546 | 7.44 M (7.33 M–7.51 M) | 3.86 M (3.85 M–3.87 M) | 1.93× | no | 915.2 | 1146.1 | 5 | 2.22 / 2.00 |
| mbo-day-1102 | 11,822,359 | cold | 8192 | 1444/1444 | 11.39 M (11.22 M–11.43 M) | 5.94 M (5.82 M–5.98 M) | 1.92× | no | 911.0 | 1136.3 | 5 | 2.00 / 2.29 |
| mbo-day-1102 | 11,822,359 | cold | 65536 | 1442/1442 | 11.58 M (11.49 M–11.68 M) | 6.01 M (5.98 M–6.03 M) | 1.93× | no | 911.0 | 1136.3 | 5 | 2.19 / 2.31 |
| mbo-day-1102 | 11,822,359 | warm | 64 | 184725/184725 | 1.21 M (1.18 M–1.22 M) | 0.57 M (0.57 M–0.58 M) | 2.11× | no | 988.4 | 1313.8 | 10 | 3.52 / 1.73 |
| mbo-day-1102 | 11,822,359 | warm | 256 | 46182/46182 | 3.70 M (3.62 M–3.72 M) | 1.83 M (1.82 M–1.84 M) | 2.03× | no | 929.9 | 1179.7 | 10 | 2.10 / 2.07 |
| mbo-day-1102 | 11,822,359 | warm | 1024 | 11546/11546 | 8.01 M (7.92 M–8.04 M) | 3.98 M (3.95 M–4.00 M) | 2.01× | no | 915.2 | 1146.1 | 10 | 1.84 / 1.58 |
| mbo-day-1102 | 11,822,359 | warm | 8192 | 1444/1444 | 12.84 M (12.78 M–12.89 M) | 6.27 M (6.19 M–6.29 M) | 2.05× | no | 911.0 | 1136.3 | 10 | 1.64 / 1.68 |
| mbo-day-1102 | 11,822,359 | warm | 65536 | 1442/1442 | 12.96 M (12.88 M–13.04 M) | 6.33 M (6.31 M–6.35 M) | 2.05× | no | 911.0 | 1136.3 | 10 | 1.73 / 1.62 |
| mbo-day-1105 | 30,176,956 | cold | 64 | 471515/471515 | 1.19 M (1.17 M–1.20 M) | 0.57 M (0.57 M–0.57 M) | 2.09× | no | 2523.0 | 3353.4 | 4 | 2.58 / 1.77 |
| mbo-day-1105 | 30,176,956 | cold | 256 | 117881/117881 | 3.66 M (3.65 M–3.69 M) | 1.80 M (1.80 M–1.81 M) | 2.03× | no | 2373.6 | 3011.1 | 4 | 2.30 / 2.27 |
| mbo-day-1105 | 30,176,956 | cold | 1024 | 29471/29471 | 7.65 M (7.64 M–7.67 M) | 3.89 M (3.88 M–3.90 M) | 1.97× | no | 2336.2 | 2925.5 | 4 | 2.71 / 2.17 |
| mbo-day-1105 | 30,176,956 | cold | 8192 | 3687/3687 | 11.90 M (11.83 M–11.92 M) | 5.73 M (5.68 M–5.78 M) | 2.08× | no | 2325.2 | 2900.6 | 4 | 2.07 / 2.04 |
| mbo-day-1105 | 30,176,956 | cold | 65536 | 3682/3682 | 11.91 M (11.82 M–12.02 M) | 5.91 M (5.70 M–6.06 M) | 2.01× | no | 2325.2 | 2900.6 | 4 | 1.88 / 2.00 |
| mbo-day-1105 | 30,176,956 | warm | 64 | 471515/471515 | 1.20 M (1.19 M–1.21 M) | 0.57 M (0.57 M–0.57 M) | 2.11× | no | 2523.0 | 3353.4 | 6 | 2.71 / 1.61 |
| mbo-day-1105 | 30,176,956 | warm | 256 | 117881/117881 | 3.73 M (3.71 M–3.75 M) | 1.81 M (1.81 M–1.82 M) | 2.06× | no | 2373.6 | 3011.1 | 6 | 1.85 / 1.82 |
| mbo-day-1105 | 30,176,956 | warm | 1024 | 29471/29471 | 7.90 M (7.85 M–7.94 M) | 3.94 M (3.94 M–3.95 M) | 2.00× | no | 2336.2 | 2925.5 | 6 | 1.81 / 1.94 |
| mbo-day-1105 | 30,176,956 | warm | 8192 | 3687/3687 | 12.72 M (12.51 M–12.77 M) | 5.84 M (5.75 M–6.19 M) | 2.18× | no | 2325.2 | 2900.6 | 6 | 2.03 / 2.41 |
| mbo-day-1105 | 30,176,956 | warm | 65536 | 3682/3682 | 12.86 M (12.76 M–12.92 M) | 5.78 M (5.77 M–5.80 M) | 2.22× | no | 2325.2 | 2900.6 | 6 | 2.14 / 2.19 |
