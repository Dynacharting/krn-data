# B2 on the NVDA vendor DBN (ENC-1641)

This is B2 (`krn_b2_bench`, `run_b2.sh`) run on three real days of Nick's Databento
`XNAS.ITCH` NVDA pull. The pull is batch jobs `XNAS-20261117-R7EH4L6T8P` (trades) and
`XNAS-20261117-4NJAJ5R9RQ` (mbo). Until now B2 had only seen the 1000-row MSFT 2022-06-10
sample.

- **Scope.** These are B2 numbers only. There is no `B2 ≥ B1` verdict here.
- **Engine.** krn `75a5616` (the krn-data pin; no bump). Release build,
  `-DKRN_DATA_BUILD_BENCHMARKS=ON`, no FP-changing flags.
- **Machine.** AMD Ryzen 7 5800XT, 16 hardware threads, 62 GB.
- **Evidence.** Raw outputs are in
  `encultured-workspace/trees/data-layer-bench/_ENC-1641/`.

## 1. Results: what you need to know

1. **Only trades could be run. mbo is refused.** The vendor files are DBN v1. The adapter
   decodes v1 trades cleanly. It **refuses every vendor mbo day** at the first record,
   before any row reaches the engine (§3, filed as ENC-1642). So every number below is
   the trades data point. No mbo row was taken on vendor data, and none was faked.
2. **Byte-identical holds on real volume.** Every step reproduced step 1 byte for byte
   under its contract, on all three days, both graphs and both pool sizes. That is
   12 of 12 timing rows plus every correctness run.
3. **All falsifications are red.**
   - Vendor data: 10 of 10 per day (5 mutations × 2 graphs, trades), so 30 of 30.
   - The MSFT control run of the unchanged `run_b2.sh` default: 20 of 20 (§5).
4. **Step 5 (ENC-1627, store-write elision) gains +0.0% to +1.2% over step 3 on real
   volume.** That is the same size as on the MSFT sample, and it does not grow with
   volume (§6).
5. **New on real volume: at one pool thread, step 3 (inline DAG) is *slower* than
   step 2.** It runs at 0.87–0.89× on the default graph. On the MSFT sample the same
   step was 1.36× faster (§7).

## 2. The data

| day | role | trades rows | trades `.dbn` bytes | mbo records | mbo `.dbn` bytes |
|---|---|---:|---:|---:|---:|
| 2026-10-27 | thin | 155,905 | 7,483,646 | 7,014,988 | 392,839,534 |
| 2026-10-29 | typical | 176,160 | 8,455,886 | — (refused) | 525,189,374 |
| 2026-11-05 | heaviest mbo day | 959,298 | 46,046,510 | — (refused) | 1,690,275,254 |

- **Provenance.** Each `.dbn.zst` was decompressed with `zstd -d -k` into a scratch
  directory outside every git tree. The vendor files were never written to.
- **Integrity.** The sha256 of every source `.zst` was recorded before decompression. It
  was re-checked after the 02:50 machine crash, together with
  `zstd -dc <src> | sha256sum` against each decompressed file. All 6 match
  (`post-crash-integrity.txt`).
- **Which day is heaviest.** 2026-11-05 is the heaviest day by mbo bytes (475 MB
  compressed). By trades it is second: 2026-11-12 is larger (18.6 MB compressed against
  11.3 MB). I kept 11-05 as the brief asked, and the trades numbers are flat in volume
  anyway (§4).
- **mbo record count.** The 7,014,988 figure for 10-27 comes from a direct read of the
  DBN framing (§3), not from the adapter.

## 3. Vendor-DBN findings (feasibility, before any timing)

**DBN version.**
- The vendor files are **DBN v1**: the metadata carries v1's `record_count`, set to
  `u64::MAX`, and 22-byte symbols.
- The MSFT sample is **v3**. It was written by our own encoder from the vendor CSV.
- The adapter's v1 metadata path had so far been exercised only by fixtures. It decodes
  these files correctly: the ticker resolves from symbology, and the rows are counted
  and decoded.

**Symbology and instruments.**
- Each file has one instrument (11667) and one mapping, `NVDA [day, day+1)`, with
  `stype_in=raw_symbol` and `stype_out=instrument_id`.
- No file had more than one `instrument_id`, so the harness's single-symbol refusal never
  fired.
- The trades files hold only rtype `0x00`. The mbo files hold only rtype `0xA0`.

**mbo is refused. This is a real adapter defect, not a harness one.** Every mbo day file
opens with the start-of-day snapshot:
- one Clear (`R`) followed by the resting-order Adds;
- flags `0x28` (`F_SNAPSHOT | F_BAD_TS_RECV`);
- `ts_recv` = 00:00:00 UTC of the file's day;
- `ts_event` = each order's original time on the *previous* UTC day.

On 10-27 there are 4,116 such records out of 7,014,988. `DatabentoAdapter::resolveSymbol`
chooses the symbology window by `ts_event`'s date, so the first record already falls
outside `[20261027, 20261028)`:

```
krn_b2_bench: decode refused: Invalid: instrument_id 11667 has 1 symbology window(s), none covering 20261026. ...
```

10-29 and 11-05 fail the same way (`probe/mbo-*-refusal.txt`). Databento resolves
symbology by the record's *index timestamp*, which is `ts_recv` for `MboMsg` and
`TradeMsg`. In `databento/dbn`, `TsSymbolMap::get_for_rec` calls `raw_index_ts()`, and
`ts_recv` carries `#[dbn(index_ts)]`.

The adapter is shared with `krn_ingest`, so **an ingest of these mbo days is refused
too**. Filed as **ENC-1642** (adapter, out of this ticket's scope).

I did **not** route around it:
- no `symbolOverride`, which would bypass the delisting check this refusal exists for;
- no skipping the snapshot prefix;
- no re-encoded file.

A record-count slice inside the harness cannot help either: the adapter refuses the
whole decode at record 0, before the harness gets any rows to slice.

**Memory.** The harness loads the whole file and decodes it into memory before timing.
- Measured RSS for 156k trades was 0.50 GB on the default graph and 1.07 GB on the wide
  graph, most of it captured deliveries.
- Every trades day fits easily.
- The largest mbo day (1.7 GB raw, about 30M records) would also fit in 62 GB. No mbo
  slice was needed or taken.

**A behavioural difference the sample could not show.**
- The Dispatcher keeps a per-(symbol, field) history capped at `taHistoryMax = 1000`.
  The builtins are recomputed over that window on every tick.
- With 1000 rows the MSFT window never rolled: it averaged about 500 entries.
- On NVDA the window is full, at 1000, from row 1000 onward. Each tick then copies and
  reduces a full window per field.
- This is a plausible reason the per-row cost is higher on NVDA (§7). It is reasoned from
  the code, not measured in isolation.

## 4. Results: final timing table

Each row is the trades data point: median rows/s over `iters` measured iterations,
after 1 warm-up. The same real rows are replayed into a fresh engine each iteration, so
the cache is warm by construction.

- Every row is byte-identical, harness exit 0.
- *load before/after* is `/proc/loadavg` (1/5/15 min) at the row's start and end.
- `5 vs 3` is step 5's median over step 3's.
- `hw` is 16 pool threads, the Host default.

| day | graph | threads | rows | iters | step 1 | step 2 | step 3 | step 5 | 3 vs 2 | 5 vs 3 | step 3 p10–p90 | step 5 p10–p90 | load before | load after |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 10-27 thin | default | hw | 155905 | 10 | 62112 | 62412 | 82972 | 83011 | 1.33x | +0.0% | 81915–83495 | 81961–83587 | 2.38 3.30 6.46 | 3.44 3.35 6.16 |
| 10-27 thin | default | 1 | 155905 | 10 | 93940 | 96239 | 84018 | 84709 | 0.87x | +0.8% | 83066–84428 | 84458–85197 | 1.67 2.72 4.78 | 1.80 2.52 4.53 |
| 10-27 thin | wide | hw | 155905 | 10 | 13233 | 13257 | 17909 | 18108 | 1.35x | +1.1% | 17719–17968 | 17960–18272 | 1.62 1.95 2.49 | 2.14 2.14 2.36 |
| 10-27 thin | wide | 1 | 155905 | 10 | 17005 | 17169 | 17964 | 18164 | 1.05x | +1.1% | 17847–18072 | 18014–18226 | 2.19 2.03 2.08 | 1.29 1.49 1.81 |
| 10-29 typical | default | hw | 176160 | 9 | 63124 | 63357 | 83479 | 84471 | 1.32x | +1.2% | 82721–83877 | 83793–84776 | 1.94 1.62 1.84 | 2.40 1.90 1.92 |
| 10-29 typical | default | 1 | 176160 | 9 | 93540 | 96126 | 83918 | 84301 | 0.87x | +0.5% | 82953–84343 | 83860–84971 | 1.83 1.85 1.90 | 1.55 1.76 1.87 |
| 10-29 typical | wide | hw | 176160 | 9 | 13141 | 13163 | 17679 | 17878 | 1.34x | +1.1% | 17599–17766 | 17822–18028 | 2.06 1.86 1.86 | 2.22 2.12 1.98 |
| 10-29 typical | wide | 1 | 176160 | 9 | 16825 | 17014 | 17810 | 17966 | 1.05x | +0.9% | 17674–17843 | 17872–18046 | 2.22 2.12 1.98 | 1.56 1.75 1.85 |
| 11-05 heavy | default | hw | 959298 | 3 | 64462 | 65347 | 84223 | 84776 | 1.29x | +0.7% | 83415–84586 | 84669–85086 | 1.98 1.80 1.85 | 2.38 2.18 2.00 |
| 11-05 heavy | default | 1 | 959298 | 3 | 94193 | 94646 | 84480 | 85004 | 0.89x | +0.6% | 83700–84806 | 84701–85060 | 2.38 2.18 2.00 | 1.71 1.94 1.94 |
| 11-05 heavy | wide | hw | 959298 | 3 | 13245 | 13301 | 17994 | 18130 | 1.35x | +0.8% | 17885–17998 | 18006–18149 | 0.78 1.65 1.84 | 1.33 1.84 1.93 |
| 11-05 heavy | wide | 1 | 959298 | 3 | 16847 | 17091 | 17931 | 18081 | 1.05x | +0.8% | 17826–17932 | 17993–18092 | 1.64 2.99 2.49 | 1.08 1.40 1.78 |

**Throughput is flat in volume.**
- From 156k to 959k rows, each step moves by under 4%. For example, step 3 runs at
  82,972 / 83,479 / 84,223 rows/s on default/hw.
- After the 1000-row history window fills, the per-row cost is constant, so the thin and
  heavy days measure the same thing.

**Reference counts.** Step 1's first iteration delivered:
- 10-27: 1,247,240 deliveries on the default graph, 3,429,910 on the wide graph.
- 10-29: 1,409,280 and 3,875,520.
- 11-05: 7,674,384 and 21,104,556.
- The store holds 4 entries on the default graph and 11 on the wide graph. 2 of them
  are declared pull reads.

### Load caveats

- Another agent (ENC-1640/ENC-1662, ClickHouse and B1) and unrelated krn-common builds
  shared the machine. The 1-minute load reached 56.
- **Every row above was taken under a gate.** The shared bench lock
  `trees/gma-v3-data-layer-l1-clickhouse-gma-as-t/_bench.lock` was held for that row
  only. The row started only when, inside the lock, the 1-min load was < 2.5 and the
  5-min load was < 4.
- **10-27 default/hw** started with a 15-minute average of 6.46, left over from the busy
  period. Its 1-min and 5-min values were inside the gate, and its p10–p90 is as tight
  as the other rows'. **11-05 wide/1** started with a 5-minute average of 2.99. Neither
  row was taken under load in the gate's sense.
- The 1-min load *during* a row includes this benchmark's own threads. The hw rows run
  16 pool threads, about +1 to +1.5.
- **`run1` rows are not timing rows.** The earlier correctness passes (`run1-*.txt`,
  through `run_b2.sh`) ran at loads of 3 to 56 and are **not** used for timing. They
  are kept as correctness evidence only. Their 11-05 wide rows (load 56 → 19) show
  +8% / +10% for step 5 over step 3. That is load noise: the gated rows of the same
  day show +0.8%.

## 5. Correctness: byte-identical and falsifications

**Byte-identical.**
- Every timing row above exits 0 with `IDENTICAL`.
- Each `run_b2.sh` correctness pass did too. That covers the three days × 2 graphs ×
  2 pool sizes, 9 to 10 measured iterations, plus 1 warm-up.

**Falsifications.** Each mutation is applied to step 2, or to step 5 for `readkey`, and
must exit 1 with `MISMATCH` and `falsify: applied`:

| data | default graph | wide graph | total |
|---|---|---|---|
| NVDA trades 10-27 (`run1-20261027.txt`) | ulp drop swap store readkey: 5/5 red | 5/5 red | 10/10 |
| NVDA trades 10-29 (`run1-20261029.txt`) | 5/5 red | 5/5 red | 10/10 |
| NVDA trades 11-05 (`falsify-20261105.txt`, re-run under the lock after the crash) | 5/5 red | 5/5 red | 10/10 |
| MSFT sample, trades + mbo (`msft-control.txt`, unchanged defaults) | 10/10 red | 10/10 red | **20/20** |

- **What the mutations hit.** On NVDA each one targets a middle row: record 77,952 on
  10-27 and record 479,649 on 11-05. Example: `ulp` 186.19 → 186.19000000000003, then
  `MISMATCH`.
- **The 11-05 run1 pass.** The 02:50 crash cut it off after 7 of 10 falsifications, all
  7 red. `scripts/falsify_day.sh` re-ran all 10 with the same command and the same
  verdict rule as `run_b2.sh`.
- **Why vendor days show 10, not 20.** The harness's 20 are 5 mutations × 2 graphs × 2
  schemas. Vendor data has the trades schema only (§3), so each day can show 10. The
  MSFT control keeps all 20 in force.

## 6. Step 5 (ENC-1627): verdict on real volume

**On real volume, step 5 gains about 1% over step 3, and the gain does not grow with
volume.**

| | step 5 vs step 3 (median) |
|---|---|
| NVDA, gated rows, all 12 | +0.0% to +1.2% (mean +0.8%) |
| NVDA, default graph | +0.0, +0.5, +0.6, +0.7, +0.8, +1.2% |
| NVDA, wide graph | +0.8, +0.8, +0.9, +1.1, +1.1, +1.1% |
| MSFT control, trades, same machine, load ≈2 | +0.5% to +2.2% |
| MSFT control, mbo | +0.8% to +6.5% |
| ENC-1627 (krn PR #115), sample, load ≈28 | −0.5% to +7% |

- **Above noise, but small.** Step 5's median is above step 3's in 12 of 12 rows. In 6
  of the 12, step 5's p10 is above step 3's p90 (4 of 6 wide, 2 of 6 default). In the
  other 6 the two ranges overlap slightly.
- **Why it cannot grow.** The store is a last-value map: 4 entries (default) or 11 (wide)
  after a whole day. Elision skips 2 (default) or 9 (wide) map writes per field tick,
  against about 12 µs/row (default) or about 55 µs/row (wide) of total cost. The saving
  is a fixed per-row cost, so 6× more rows does not change the ratio.
- **The sample's larger mbo gains are not tested here.** On MSFT mbo, step 5 gained up
  to 6.5%. Rows without fields dilute the per-row cost there. That cannot be checked on
  vendor data until ENC-1642 lands.

**Input to keep/revert.** The gain is measurable and real, but about 1%, and it is not
volume-dependent. Whether ~1% justifies the read-set sealing and the build-time refusal
of new keys after the seal is Nick's call. B2 gives no stronger case for it at real
volume than at the sample.

## 7. Comparison against the MSFT sample-only rows

**Trades data point, median rows/s.**

| source | load | graph | threads | step 1 | step 2 | step 3 | step 5 |
|---|---|---|---|---:|---:|---:|---:|
| ENC-1625 (krn pin 2221e99) | — | default | 16 / 1 | 77,940 / 104,859 | 78,400 / 107,838 | 144,885 / 145,703 | — |
| ENC-1633 (pin eda2dc4) | — | default | 16 / 1 | 76,988 / 102,845 | 77,189 / 105,859 | 141,054 / 143,870 | — |
| ENC-1638 (pin 75a5616) | 19–26 | default | hw / 1 | 17,993 / 99,761 | 19,394 / 104,948 | 47,064 / 140,381 | 41,763 / 139,315 |
| ENC-1638 | 19–26 | wide | hw / 1 | 4,868 / 10,024 | 5,218 / 10,859 | 19,523 / 19,222 | 19,221 / 19,695 |
| **MSFT control, this ticket** (pin 75a5616) | 1.2–2.6 | default | hw / 1 | 76,285 / 104,657 | 76,856 / 107,884 | 147,934 / 146,406 | 150,719 / 147,073 |
| **MSFT control, this ticket** | 1.8–2.6 | wide | hw / 1 | 17,764 / 25,364 | 17,942 / 25,615 | 30,243 / 30,487 | 30,896 / 30,976 |
| **NVDA 10-29, this ticket** | 1.6–2.4 | default | hw / 1 | 63,124 / 93,540 | 63,357 / 96,126 | 83,479 / 83,918 | 84,471 / 84,301 |
| **NVDA 10-29, this ticket** | 1.6–2.2 | wide | hw / 1 | 13,141 / 16,825 | 13,163 / 17,014 | 17,679 / 17,810 | 17,878 / 17,966 |

**The ENC-1638 rows were heavily loaded.** Its hw rows ran at a load of 19–26, which is
why they sit at ¼ of this ticket's MSFT control on the same pin. Today's MSFT control is
the clean same-machine, same-pin baseline.

**Steps 1 and 2 on real volume.** Steps 1 and 2 run at 0.82–0.89× of the sample's speed
on the default graph, and at 0.66–0.74× on the wide graph. JSON onTick against onRecord is
still about 1.00–1.03×, as on the sample.

**Step 3 on real volume.**
- It loses most of its sample-measured lead at hw: on the default graph it is
  1.29–1.33× over step 2, against the sample's 1.92×. On the wide graph it is
  1.34–1.35×, against 1.69×.
- At one pool thread it is **slower than step 2** on the default graph: 0.87–0.89×,
  against the sample's 1.36×. On the wide graph it is 1.05×, against 1.19×.
- Absolute step-3 throughput falls from about 147k to about 84k rows/s.

**Hypothesis for step 3, not measured.** With the history window full (§3), the
ingress-side work per tick, which is the history copy and the builtin reductions over
1000 values, grows to dominate:
- In step 2 the DAG runs on the pool thread, overlapping that ingress work.
- Step 3 (`inlineDagDrive`) puts the DAG on the ingress thread, so the two serialise.

That would make step 3's sample-measured gain an artefact of a history that never
filled. It is worth one profile before step 3's gain is quoted on real data.

## 8. Commands

Build (worktree root):

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DKRN_DATA_BUILD_BENCHMARKS=ON
cmake --build build -j8 --target krn_b2_bench
# configure log: "engine pinned at 75a5616d…", no "not used by the project"
```

Decompression (each day, trades and mbo; the vendor files are only read):

```bash
zstd -q -d -k ~/market-data/databento/XNAS.ITCH/NVDA/trades-dbn-XNAS-20261117-R7EH4L6T8P/xnas-itch-$D.trades.dbn.zst -o $SCRATCH/xnas-itch-$D.trades.dbn
zstd -q -d -k ~/market-data/databento/XNAS.ITCH/NVDA/mbo-dbn-XNAS-20261117-4NJAJ5R9RQ/xnas-itch-$D.mbo.dbn.zst    -o $SCRATCH/xnas-itch-$D.mbo.dbn
```

**Iteration rule.**
- `rows` is the decoded TickRecord count: (file bytes − 206) / 48.
- `iterations = max(3, ceil(1,500,000 / rows))`, so each step replays at least 1.5M real
  rows per row of the table. That gives 10 (10-27), 9 (10-29) and 3 (11-05).
- `warmup = 1`, and falsification runs use 1 measured iteration. One iteration is still a
  real comparison: the mutated step is compared against step 1's first iteration.

Correctness pass, one per day (`scripts/run_days.sh`):

```bash
KRN_B2_TRADES_DBN=$SCRATCH/xnas-itch-$D.trades.dbn KRN_B2_SCHEMAS=trades \
KRN_B2_ITERATIONS=$IT KRN_B2_WARMUP=1 KRN_B2_FALSIFY_ITERATIONS=1 data/bench/run_b2.sh build
```

Gated timing rows (`scripts/timing_quiet.sh`). Each row is one invocation of the same
binary call `run_b2.sh` makes, under the shared lock, re-checking load inside it:

```bash
flock trees/gma-v3-data-layer-l1-clickhouse-gma-as-t/_bench.lock \
  build/data/krn_b2_bench --dbn $SCRATCH/xnas-itch-$D.trades.dbn \
  --iterations $IT --warmup 1 --graph=<default|wide> [--threads 1]
```

The mbo refusal:

```bash
build/data/krn_b2_bench --dbn $SCRATCH/xnas-itch-$D.mbo.dbn --iterations 1 --warmup 0   # exit 2, §3
```

MSFT control (unchanged defaults: trades + mbo, 200 iterations, 20 falsifications):

```bash
data/bench/run_b2.sh build
```

## 9. Harness change in this ticket

`run_b2.sh` gained two environment variables. Its defaults are unchanged, and the MSFT
control above proves it still runs all 20 falsifications.

- `KRN_B2_SCHEMAS` (default `trades mbo`) selects the data points. The falsification
  total follows it, at 5 per schema per graph.
- `KRN_B2_FALSIFY_ITERATIONS` (default 3) scales the falsification runs to a day of rows.

Each measurement also prints the load average after it. `krn_b2_bench.cpp` is
unchanged.
