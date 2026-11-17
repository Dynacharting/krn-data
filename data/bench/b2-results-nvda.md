# B2 on the NVDA vendor DBN (ENC-1641)

This is B2 (`krn_b2_bench`, `run_b2.sh`) run on three real days of Nick's Databento
`XNAS.ITCH` NVDA pull: batch jobs `XNAS-20261117-R7EH4L6T8P` (trades) and
`XNAS-20261117-4NJAJ5R9RQ` (mbo). Until now B2 had only seen the 1000-row MSFT 2022-06-10
sample.

- **Scope.** These are B2 numbers only. There is no `B2 ≥ B1` verdict here.
- **Engine.** krn `75a5616` (the krn-data pin; no bump). Release build,
  `-DKRN_DATA_BUILD_BENCHMARKS=ON`, no FP-changing flags.
- **Machine.** AMD Ryzen 7 5800XT, 16 hardware threads, 62 GB.
- **Evidence.** Raw outputs are in
  `encultured-workspace/trees/data-layer-bench/_ENC-1641/`.

## 1. Results

1. **Both data points ran on all three days.** The vendor files are DBN v1.
   - Trades decoded at the original base.
   - **mbo was refused by the adapter at the original base (`9dced58`).** The cause was
     the start-of-day snapshot rows: the adapter resolved symbology by `ts_event`,
     Databento resolves it by `ts_recv` (§3).
   - ENC-1640 fixed this independently and merged it as `32a08f5`. This branch merges
     that commit. Every mbo number below was taken with it.
   - No row was faked, overridden or sliced.
2. **Byte-identical holds on real volume.** Every step reproduced step 1 byte for byte
   under its contract, on 2 schemas × 3 days × 2 graphs × 2 pool sizes. That is 24 of
   24 timing rows, plus every correctness pass.
3. **All falsifications are red.**
   - Vendor data: 10 of 10 per day per schema (5 mutations × 2 graphs), 60 of 60 in
     total.
   - The MSFT control, `run_b2.sh` at its defaults, before and after the merge: 20 of 20
     each time (§5).
4. **Step 5 (ENC-1627, store-write elision) is worth about 1% on real volume, or
   nothing.**
   - Trades: step 5 over step 3 is +0.0% to +1.2%.
   - mbo, wide graph: +0.9% to +1.6%.
   - mbo, default graph: −1.1% to +0.5%, i.e. no gain.
   - Only the wide-graph gain is consistent, at about 1.0–1.1%. Even that is only about
     1.3× the row-to-row floor, with no A/A control.
   - The default-graph gain is not corroborated across schemas.
   - The gain does not grow with volume (§6).
5. **New on real volume: at one pool thread, step 3 (inline DAG) is *slower* than
   step 2.**
   - On the default graph it runs at 0.87–0.90× on both schemas. On the MSFT sample it
     was 1.36× (trades) and 1.64× (mbo) faster.
   - At 16 threads its lead shrinks from 1.92× to about 1.3× (§7).
   - The cause is unprofiled. Engine history cost and the harness's own capture buffers
     are both candidates.

## 2. The data

| day | role | trades rows | trades `.dbn` bytes | mbo records | of which carry fields | mbo `.dbn` bytes |
|---|---|---:|---:|---:|---:|---:|
| 2026-10-27 | thin | 155,905 | 7,483,646 | 7,014,988 | 274,351 | 392,839,534 |
| 2026-10-29 | typical | 176,160 | 8,455,886 | 9,378,378 | 309,310 | 525,189,374 |
| 2026-11-05 | heaviest (mbo) | 959,298 | 46,046,510 | 30,183,483 | 1,724,300 | 1,690,275,254 |

**What the mbo field count is.** mbo rows that carry fields are the prints, actions `T`
and `F` (BookRecordFieldSource, ENC-1628). On 10-27 those are 155,905 `T` (equal to the
trades count) plus 118,446 `F`. The rest are book mutations, which the graph sees as
fieldless.

**Provenance and integrity.**
- Each `.dbn.zst` was decompressed with `zstd -d -k` into a scratch directory outside
  every git tree. The vendor files were only read.
- The sha256 of each source `.zst` was recorded before decompression.
- After the 02:50 machine crash, those hashes were re-checked, together with
  `zstd -dc <src> | sha256sum` against each decompressed file. All 6 match
  (`post-crash-integrity.txt`).

**Which day is heaviest.** 2026-11-05 is the heaviest day by mbo bytes (475 MB
compressed). By trades, 2026-11-12 is larger (18.6 MB compressed, against 11.3 MB). I
kept 11-05 as briefed. The per-row numbers are flat in volume (§4), so this choice does
not move them.

## 3. Vendor-DBN findings (feasibility, before any timing)

**DBN version.**
- The vendor files are **DBN v1**: the metadata carries v1's `record_count`, set to
  `u64::MAX`, and 22-byte symbols.
- The MSFT sample is **v3**. Our own encoder wrote it from the vendor CSV.
- The adapter's v1 metadata path had been exercised only by fixtures until now. It
  decodes these files correctly.

**Symbology and instruments.**
- Each file has exactly one instrument (11667) and one mapping, `NVDA [day, day+1)`.
  stype is `raw_symbol` → `instrument_id`.
- No file carries more than one `instrument_id`, so the harness's single-symbol refusal
  never fired.
- Record types: trades files hold only rtype `0x00`, mbo files only `0xA0`.
- The 10-27 mbo file contains two Clears (`R`).

**At the original base, mbo was refused. This was an adapter defect, not a harness
one.**
- Every mbo day file opens with the start-of-day snapshot: one Clear, then the resting
  orders as Adds.
- These rows carry flags `0x28` (`F_SNAPSHOT | F_BAD_TS_RECV`). Their `ts_recv` is
  00:00:00 UTC of the file's day; their `ts_event` is each order's original time on the
  *previous* UTC day.
- On 10-27 there are 4,116 such rows.
- `DatabentoAdapter::resolveSymbol` chose the symbology window by `ts_event`'s date, so
  record 0 fell outside `[20261027, 20261028)`:

  ```
  krn_b2_bench: decode refused: Invalid: instrument_id 11667 has 1 symbology window(s), none covering 20261026. ...
  ```

- 10-29 and 11-05 failed the same way (`probe/mbo-*-refusal.txt`).
- Databento resolves symbology by the record's index timestamp, which is `ts_recv` for
  `MboMsg` and `TradeMsg`. In `databento/dbn`, `TsSymbolMap::get_for_rec` calls
  `raw_index_ts()`, and `ts_recv` carries `#[dbn(index_ts)]`.
- The same adapter backs `krn_ingest`, so an ingest of these days was refused too.

**How it was fixed.**
- I filed this as ENC-1642. ENC-1640 had found the same thing and fixed it on `main` in
  `32a08f5` (`dbn::indexTs`: `ts_recv`, falling back to `ts_event` when `ts_recv` is
  undefined). ENC-1642 is closed as superseded.
- I did not route around the refusal: no `symbolOverride`, no skipping the snapshot
  prefix, and no re-encoded file.
- A record-count slice inside the harness could not have helped either: the adapter
  refused the whole decode at record 0.
- After the merge, the 10-27 trades run matches the pre-merge run on its input and its
  output counts (`probe/trades-1027-postmerge-1it.txt`):
  - the same input bytes (size and FNV-1a-32);
  - the same decoded row count;
  - the same per-terminal delivery and store-entry counts.
- The output *bytes* were not hashed across the two binaries, so this is not a
  byte-identity claim.
- `32a08f5`'s decode change touches only symbol and day resolution: which symbology
  window a record is looked up in. Trades rows carry the same `ts_recv` and `ts_event`
  date, so the same ticker resolves either way. On that basis the trades rows taken
  before the merge stand.

**Memory.**
- The harness loads the whole file and decodes it into memory before timing.
- RSS was 0.50 GB (default graph) and 1.07 GB (wide graph) for 156k trades, most of it
  captured deliveries. For the 7.0M-row mbo day on the default graph it was 1.58 GB.
- The heaviest mbo day (30.2M rows) ran whole, so **no mbo slice was needed or taken**.

**A behavioural difference the sample could not show.**
- The Dispatcher keeps a per-(symbol, field) history capped at `taHistoryMax = 1000`,
  and recomputes the builtins over it on every tick.
- The MSFT window never rolled. Over its 1000 trades rows it averaged about 500
  entries; over its 248 mbo prints, about 124. On NVDA it is full, at 1000, from the
  1000th print onward.
- This is one candidate reason per-print cost is higher on NVDA. §7 names a second, the
  harness's own capture buffers. Neither has been measured in isolation.

## 4. Final timing tables

**How to read them.**
- Each row reports the median rows/s over `iters` measured iterations, after 1 warm-up.
- Each iteration replays the same real rows into a fresh engine, so the cache is warm by
  construction.
- Every row is byte-identical (harness exit 0).
- *Load* is `/proc/loadavg` (1/5/15 min) at the row's start and end.
- `5 vs 3` is step 5's median over step 3's. `hw` means 16 pool threads, the Host
  default.

### Trades

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

Trades rows were taken on the pre-merge binary (§3 shows the decode is unchanged).

Reference deliveries per day (step 1's first iteration):

| day | default graph | wide graph |
|---|---:|---:|
| 10-27 | 1,247,240 | 3,429,910 |
| 10-29 | 1,409,280 | 3,875,520 |
| 11-05 | 7,674,384 | 21,104,556 |

The store holds 4 entries (default) or 11 (wide); 2 of them are declared pull reads.

### mbo

These are rows/s over **every** mbo record, fieldless book mutations included. They are
not per print (see §2 for the print counts).

| day | graph | threads | rows | iters | step 1 | step 2 | step 3 | step 5 | 3 vs 2 | 5 vs 3 | step 3 p10–p90 | step 5 p10–p90 | load before | load after |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 10-27 thin | default | hw | 7014988 | 3 | 1406254 | 1492698 | 1878705 | 1857850 | 1.26x | −1.1% | 1870313–1882714 | 1855262–1884508 | 1.44 2.49 2.36 | 2.08 2.47 2.36 |
| 10-27 thin | default | 1 | 7014988 | 3 | 1916743 | 2102088 | 1885251 | 1876647 | 0.90x | −0.5% | 1872573–1912009 | 1874729–1886773 | 2.13 2.45 2.36 | 1.55 2.22 2.29 |
| 10-27 thin | wide | hw | 7014988 | 3 | 352850 | 359382 | 493997 | 500210 | 1.37x | +1.3% | 493457–496013 | 498799–501940 | 1.58 2.19 2.27 | 1.89 2.13 2.24 |
| 10-27 thin | wide | 1 | 7014988 | 3 | 452765 | 470194 | 498470 | 503205 | 1.06x | +0.9% | 498436–499001 | 499115–503585 | 1.91 2.12 2.23 | 1.23 1.66 2.01 |
| 10-29 typical | default | hw | 9378378 | 3 | 1631923 | 1762838 | 2186357 | 2185478 | 1.24x | −0.0% | 2179552–2199603 | 2183613–2189347 | 1.51 1.70 2.02 | 2.19 1.90 2.06 |
| 10-29 typical | default | 1 | 9378378 | 3 | 2199807 | 2433463 | 2189688 | 2184439 | 0.90x | −0.2% | 2181949–2190572 | 2182506–2189369 | 1.90 1.89 2.04 | 1.46 1.77 1.99 |
| 10-29 typical | wide | hw | 9378378 | 3 | 412118 | 419734 | 577441 | 586859 | 1.38x | +1.6% | 576313–581656 | 582124–588663 | 1.53 1.74 1.97 | 1.75 1.91 1.99 |
| 10-29 typical | wide | 1 | 9378378 | 3 | 528388 | 549824 | 583453 | 589334 | 1.06x | +1.0% | 583240–584754 | 589317–593533 | 1.70 1.89 1.98 | 1.16 1.49 1.78 |
| 11-05 heavy | default | hw | 30183483 | 3 | 1015967 | 1066996 | 1333437 | 1340543 | 1.25x | +0.5% | 1328734–1342469 | 1337989–1342802 | 1.97 1.74 1.85 | 1.95 2.31 2.12 |
| 11-05 heavy | default | 1 | 30183483 | 3 | 1415391 | 1510211 | 1354848 | 1351179 | 0.90x | −0.3% | 1353170–1357776 | 1350610–1353319 | 1.56 2.15 2.08 | 1.28 1.67 1.90 |
| 11-05 heavy | wide | hw | 30183483 | 3 | 245473 | 246774 | 347133 | 350594 | 1.41x | +1.0% | 346774–347606 | 349773–350605 | 1.89 1.79 1.93 | 1.30 1.97 2.04 |
| 11-05 heavy | wide | 1 | 30183483 | 3 | 318838 | 326158 | 347860 | 351289 | 1.07x | +1.0% | 347413–348452 | 350816–351483 | 1.76 2.04 2.06 | 1.98 1.93 1.91 |

mbo reference deliveries:

| day | default graph | wide graph |
|---|---:|---:|
| 10-27 | 2,194,808 | 6,035,722 |
| 10-29 | 2,474,480 | 6,804,820 |
| 11-05 | 13,794,400 | 37,934,600 |

**Throughput is flat in volume, per print.**
- Trades: from 156k to 959k rows, every step moves by under 4%. For example, step 3 runs
  at 82,972 / 83,479 / 84,223 rows/s on default/hw.
- mbo per record falls on 11-05 (1.33M against 2.19M rows/s for step 3) because 5.7% of
  its records are prints, against 3.3% on 10-29.
- Per print the cost is the same. Step 3 on default/hw: 11-05 spends 30.2M / 1.333M ≈
  22.6 s per iteration on 1.72M prints, about 13.1 µs per print. 10-29 spends 4.29 s on
  309k prints, about 13.9 µs per print. Both include the cheap fieldless rows.

### Load caveats

- The machine was shared throughout. ENC-1640/ENC-1662 ran ClickHouse and B1 on it, and
  unrelated krn-common builds ran too. The 1-minute load reached 56.
- **Every row in both tables was taken under a gate.**
  - The shared lock `trees/gma-v3-data-layer-l1-clickhouse-gma-as-t/_bench.lock` was
    held for that one row.
  - Inside the lock, the row started only if the 1-min load was < 2.5 and the 5-min
    load was < 4. Otherwise it released the lock and retried.
  - **The gate is checked at the start of a row only.** Nothing stops another job from
    loading the machine mid-row. The *load after* column is the only record of what
    happened during the row.
- Two rows deserve a note, though neither was taken under load in the gate's sense:
  - **Trades 10-27 default/hw** started with a 15-minute average of 6.46, left over from
    the busy period. Its 1- and 5-min values were inside the gate, and its p10–p90 is as
    tight as its neighbours'.
  - **Trades 11-05 wide/1** started with a 5-minute average of 2.99.
- The 1-min load *during* a row includes this benchmark's own threads. The hw rows run
  16 pool threads, which adds about +1 to +1.5.
- **The earlier correctness passes are not used for timing.** These are `run1-*.txt`, the
  trades `run_b2.sh` passes from before the crash. They ran at loads of 3 to 56 and are
  kept as correctness evidence only.
  - Their 11-05 wide rows (load 56 → 19) show step 5 at +8% and +10% over step 3.
  - That is load noise: the gated rows of the same day show +0.8%.

## 5. Correctness: byte-identical and falsifications

**Byte-identical.**
- Every timing row above exits 0 with `IDENTICAL`.
- So did each trades `run_b2.sh` correctness pass: three days × 2 graphs × 2 pool sizes,
  9–10 measured iterations + 1 warm-up.

**Falsifications.** Each mutation applies to step 2 (to step 5 for `readkey`). It must
exit 1, print `MISMATCH`, and print `falsify: applied`.

| data | default graph (ulp drop swap store readkey) | wide graph | total |
|---|---|---|---|
| NVDA trades 10-27 (`run1-20261027.txt`) | 5/5 red | 5/5 red | 10/10 |
| NVDA trades 10-29 (`run1-20261029.txt`) | 5/5 red | 5/5 red | 10/10 |
| NVDA trades 11-05 (`falsify-20261105.txt`) | 5/5 red | 5/5 red | 10/10 |
| NVDA mbo 10-27 (`falsify-mbo-20261027.txt`) | 5/5 red | 5/5 red | 10/10 |
| NVDA mbo 10-29 (`falsify-mbo-20261029.txt`) | 5/5 red | 5/5 red | 10/10 |
| NVDA mbo 11-05 (`falsify-mbo-20261105.txt`) | 5/5 red | 5/5 red | 10/10 |
| MSFT sample, trades + mbo, pre-merge (`msft-control.txt`, defaults) | 10/10 | 10/10 | **20/20** |
| MSFT sample, post-merge (`msft-control-postmerge.txt`, 20 iterations) | 10/10 | 10/10 | **20/20** |

**Where the mutations land.**
- Each targets a middle row that the engine actually sees.
- Trades: record 77,952 on 10-27 and record 479,649 on 11-05.
- mbo: print 3,507,682 on 10-27, print 4,689,196 on 10-29 and print 15,091,901 on 11-05,
  with a fielded adjacent pair for `swap`.
- Example: `ulp` changes 217.5 → 217.50000000000003, and the run reports `MISMATCH`.

**Notes on the runs.**
- The 02:50 crash cut off the trades 11-05 `run_b2.sh` pass after 7 of its 10
  falsifications, all red. `scripts/falsify_day.sh` re-ran all 10, using the same command
  and the same verdict rule as `run_b2.sh`, under the lock.
- The vendor mbo falsifications used the same script with `SCHEMA=mbo`.
- **What evidence survives differs by run.**
  - The 20 trades falsifications for 10-27 and 10-29 survive only as `run_b2.sh`'s
    summary rows in `run1-20261027.txt` and `run1-20261029.txt`: mutation, what was
    applied, exit code, verdict. `run_b2.sh` writes each falsification's own output to a
    `mktemp` directory and deletes it, so no per-mutation output was kept.
  - The trades 11-05 runs and all 30 mbo runs went through `falsify_day.sh`. Each of
    those has its full output kept as `falsify[-mbo]-<day>-<graph>-<kind>.txt`.
- The harness's "20" means 5 mutations × 2 graphs × 2 schemas, per data-file pair. Each
  vendor day gets 10 per schema, so 20 per day.

## 6. Step 5 (ENC-1627): verdict on real volume

**On real volume, step 5 is worth at most about 1.5% over step 3, and on mbo with the
default graph it is worth nothing. The gain does not grow with volume.**

| | step 5 vs step 3 (median) |
|---|---|
| NVDA trades, all 12 gated rows | +0.0% to +1.2% (mean +0.8%) |
| NVDA mbo, wide graph, 6 rows | +0.9% to +1.6% (mean +1.1%) |
| NVDA mbo, default graph, 6 rows | −1.1% to +0.5% (mean −0.3%) |
| MSFT control, trades, same machine, load ≈ 2 | +0.5% to +2.2% |
| MSFT control, mbo, same machine | +0.8% to +6.5% |
| ENC-1627 (krn PR #115), sample, load ≈ 28 | −0.5% to +7% |

**How solid the gain is.** It is weaker than the medians alone suggest.

- **Wide graph.** This is the only gain with any support.
  - Step 5's median is above step 3's in all 12 wide rows.
  - On mbo, step 5's p10 is above step 3's p90 in 6 of 6 rows.
  - The size of that gain is about 1.0–1.1%.
- **Default graph.** The gain is **not corroborated**.
  - The two schemas disagree. Trades default reads about +0.63% (mean of 6 rows).
    mbo default should read about +0.5% after diluting the same per-print saving over
    fieldless rows. It reads −0.26% instead.
  - That puts a floor of roughly 0.8–0.9 points between rows that should agree. The wide
    gain is only about 1.3× that floor.
  - Trades 10-27 default/hw reads +0.05%, which is nothing.
- **Limits of the measurement itself.**
  - The mbo rows and the trades 11-05 rows have 3 measured iterations. Their p10/p90 are
    therefore effectively min/max, not a distribution.
  - Steps run in a fixed 1, 2, 3, 5 order in every iteration.
  - There is no A/A control, such as step 3 run twice, so a systematic position effect
    of the order of 1% cannot be excluded.

**What elision skips and what it costs**, per krn `75a5616`:

- **Default graph.** Elision skips one raw `setBatch` of {price, size} per print: 2 store
  entries under 1 lock acquisition. The `mean` and `stddev` builtin writes are declared
  pull reads, so they are kept (`Dispatcher.cpp:465–470`).
- **Wide graph.** It skips that batch plus 7 unread builtins × 2 fields: about 16 store
  entries and 15 lock acquisitions per print.
- **What it adds.** With elision on, *every* `ingest` call runs
  `_store->sealReads()` and `_store->sealedReadFieldsFor(symbol)`, an
  `unordered_map<string>` lookup (`Dispatcher.cpp:169–173`, `AtomicStore.cpp:240–253`).
  Fieldless mbo records pay this too, and they save nothing.
  - On mbo default that is a plausible reason for a reading of zero or below: 94–97% of
    the records are fieldless, and the per-print saving is only 2 entries.
  - This is reasoned from the code, not profiled.

**Why it cannot grow with volume.**
- The store is a last-value map. After a whole day it holds 4 entries (default) or 11
  (wide).
- Both the saving and the added lookup are fixed per record. Against them, a print costs
  about 12 µs (default) or about 55 µs (wide) in total.
- So 6× or 30× more rows cannot change the ratio.
- The MSFT mbo rows reached +6.5%. That was a 1000-row sample whose history window never
  filled (about 124 entries on average), and it does not reproduce at volume.

**Input to keep/revert.** At real volume, B2 shows elision at about +1% on a graph with
many unread builtins. That is about 1.3× the row-to-row floor, and it has no A/A control.
On the default graph it shows nothing that can be told apart from zero. That is weaker than the sample
suggested. Whether ~1% pays for the read-set seal, and for refusing a new key at build
time after the seal, is Nick's call.

## 7. Comparison against the MSFT sample-only rows

Median rows/s. A pair of values reads hw / 1 thread.

### Trades data point

| source | load | graph | step 1 | step 2 | step 3 | step 5 |
|---|---|---|---:|---:|---:|---:|
| ENC-1625 (krn pin 2221e99, 16 / 1 threads) | — | default | 77,940 / 104,859 | 78,400 / 107,838 | 144,885 / 145,703 | — |
| ENC-1633 (pin eda2dc4, 16 / 1) | — | default | 76,988 / 102,845 | 77,189 / 105,859 | 141,054 / 143,870 | — |
| ENC-1638 (pin 75a5616) | 19–26 | default | 17,993 / 99,761 | 19,394 / 104,948 | 47,064 / 140,381 | 41,763 / 139,315 |
| ENC-1638 | 19–26 | wide | 4,868 / 10,024 | 5,218 / 10,859 | 19,523 / 19,222 | 19,221 / 19,695 |
| **MSFT control, this ticket** (pin 75a5616) | 1.2–2.6 | default | 76,285 / 104,657 | 76,856 / 107,884 | 147,934 / 146,406 | 150,719 / 147,073 |
| **MSFT control, this ticket** | 1.8–2.6 | wide | 17,764 / 25,364 | 17,942 / 25,615 | 30,243 / 30,487 | 30,896 / 30,976 |
| **NVDA 10-29, this ticket** | 1.6–2.4 | default | 63,124 / 93,540 | 63,357 / 96,126 | 83,479 / 83,918 | 84,471 / 84,301 |
| **NVDA 10-29, this ticket** | 1.6–2.2 | wide | 13,141 / 16,825 | 13,163 / 17,014 | 17,679 / 17,810 | 17,878 / 17,966 |

### mbo data point

These are rows/s over every record. The MSFT sample is 248 prints in 1000 rows (24.8%);
NVDA 10-29 is 3.3% prints. So the two columns of rates are **not** comparable as rates.
Compare the ratios between steps instead.

| source | load | graph | step 1 | step 2 | step 3 | step 5 |
|---|---|---|---:|---:|---:|---:|
| ENC-1638 | 19–26 | default | 300,658 / 492,425 | 314,252 / 518,038 | 811,300 / 836,808 | 816,511 / 845,667 |
| **MSFT control, this ticket** | 1.6–1.8 | default | 323,085 / 502,693 | 327,190 / 531,653 | 855,595 / 871,534 | 865,594 / 878,206 |
| **MSFT control, this ticket** | 2.0–2.6 | wide | 111,441 / 202,696 | 116,771 / 210,693 | 336,498 / 349,031 | 358,517 / 364,204 |
| **NVDA 10-29, this ticket** | 1.5–1.9 | default | 1,631,923 / 2,199,807 | 1,762,838 / 2,433,463 | 2,186,357 / 2,189,688 | 2,185,478 / 2,184,439 |
| **NVDA 10-29, this ticket** | 1.5–1.7 | wide | 412,118 / 528,388 | 419,734 / 549,824 | 577,441 / 583,453 | 586,859 / 589,334 |

### What changes on real volume

**The ENC-1638 rows were heavily loaded.** Its hw rows ran at a load of 19–26. That is why
they sit at a quarter of this ticket's MSFT control on the same pin. Today's MSFT control
is the clean same-machine, same-pin baseline.

**Steps 1 and 2.**
- On trades they run at 0.82–0.89× of the sample's speed on the default graph, and at
  0.66–0.74× on the wide graph.
- JSON onTick against onRecord stays at about 1.00–1.03× on trades.
- On mbo it is 1.01–1.11×: the DOM is built for fieldless rows too.

**Step 3, at 16 threads.** It loses much of its sample-measured lead.

| | NVDA, default graph | NVDA, wide graph | MSFT sample, default | MSFT sample, wide |
|---|---|---|---|---|
| trades, step 3 over step 2 | 1.29–1.33× | 1.34–1.35× | 1.92× | 1.69× |
| mbo, step 3 over step 2 | 1.24–1.26× | 1.37–1.41× | 2.61× | 2.88× |

**Step 3, at one pool thread.** It is **slower than step 2** on the default graph:

| | NVDA, default graph | NVDA, wide graph | MSFT sample, default |
|---|---|---|---|
| trades | 0.87–0.89× | 1.05× | 1.36× |
| mbo | 0.90× | 1.06–1.07× | 1.64× |

On trades, absolute step-3 throughput falls from about 147k to about 84k rows/s.

**Two hypotheses for step 3. Neither is measured, and a profile must tell them
apart.**

1. **History cost.**
   - With the history window full (§3), the ingress-side work per print grows to
     dominate: the history copy plus the builtin reductions over 1000 values.
   - In step 2 the DAG runs on the pool thread, overlapping that ingress work.
   - Step 3 (`inlineDagDrive`) puts the DAG on the ingress thread, so the two serialise.
2. **The harness's own capture cost.**
   - `CaptureSink` appends every delivery to growing `std::string` and `std::vector`
     buffers: millions of deliveries per iteration on NVDA, against about 2,000–22,000 on
     the sample.
   - Their reallocation and first-touch page faults land on whichever thread delivers.
     In step 3 that is the ingress thread, inside the serial path. In step 2 it is the
     pool, overlapped with ingress.
   - Supporting evidence: the 1-iteration trades 10-27 probe used 6.19 s sys against
     13.35 s user, with 178,389 minor page faults (`probe/trades-1027-default-1it.err`).
     That is the whole process, decode and all four steps included, so it does not
     attribute the cost to step 3.
   - If this is the cause, the step-3 slowdown at one thread is partly a harness
     artefact, not an engine property.

The profile should separate the two:
- `perf` on the ingress thread, step 3 against step 2;
- or a run with `CaptureSink` pre-reserved;
- or a run with `taHistoryMax` lowered.

Until then, quote neither step 3's sample gain nor its real-volume loss as an engine
property.

## 8. Commands

**Build** (worktree root). The mbo rows need the merged `32a08f5`:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DKRN_DATA_BUILD_BENCHMARKS=ON
cmake --build build -j8 --target krn_b2_bench
# configure log: "engine pinned at 75a5616d…", no "not used by the project"
```

**Decompression.** Run for each day, trades and mbo; the vendor files are only read:

```bash
zstd -q -d -k ~/market-data/databento/XNAS.ITCH/NVDA/trades-dbn-XNAS-20261117-R7EH4L6T8P/xnas-itch-$D.trades.dbn.zst -o $SCRATCH/xnas-itch-$D.trades.dbn
zstd -q -d -k ~/market-data/databento/XNAS.ITCH/NVDA/mbo-dbn-XNAS-20261117-4NJAJ5R9RQ/xnas-itch-$D.mbo.dbn.zst    -o $SCRATCH/xnas-itch-$D.mbo.dbn
```

**Iteration rule.**
- `rows` is the decoded record count: (file bytes − 206) / 48 for trades, / 56 for mbo.
- `iterations = max(3, ceil(1,500,000 / rows))`, so each step replays at least 1.5M real
  rows per table row.
  - Trades: 10 (10-27), 9 (10-29), 3 (11-05).
  - mbo: 3 on every day.
- `warmup = 1`.
- Falsification runs use 1 measured iteration. That is still a real comparison: the
  mutated step is compared against step 1's first iteration.

**Trades correctness pass**, once per day (`scripts/run_days.sh`):

```bash
KRN_B2_TRADES_DBN=$SCRATCH/xnas-itch-$D.trades.dbn KRN_B2_SCHEMAS=trades \
KRN_B2_ITERATIONS=$IT KRN_B2_WARMUP=1 KRN_B2_FALSIFY_ITERATIONS=1 data/bench/run_b2.sh build
```

**Gated timing rows** (`scripts/timing_quiet.sh`, with `SCHEMA=mbo` for mbo).
- Each row is one call of the binary, the same call `run_b2.sh` makes.
- It runs under the shared lock and re-checks the load inside it.

```bash
flock trees/gma-v3-data-layer-l1-clickhouse-gma-as-t/_bench.lock \
  build/data/krn_b2_bench --dbn $SCRATCH/xnas-itch-$D.<trades|mbo>.dbn \
  --iterations $IT --warmup 1 --graph=<default|wide> [--threads 1]
```

**Per-day falsifications** (`scripts/falsify_day.sh $D`, with `SCHEMA=mbo` for mbo). Each
call runs under the lock:

```bash
flock …/_bench.lock build/data/krn_b2_bench --dbn $F --iterations 1 --warmup 0 \
  --graph=<default|wide> --falsify=<ulp|drop|swap|store|readkey>
```

**MSFT control.** Unchanged defaults: trades + mbo, 200 iterations, 20 falsifications:

```bash
data/bench/run_b2.sh build
```

## 9. Harness change in this ticket

`run_b2.sh` gained two environment variables. Its defaults are unchanged; the MSFT control
above shows that, with 20 falsifications on each run.

- `KRN_B2_SCHEMAS` (default `trades mbo`) selects the data points. The falsification
  total follows it, at 5 per schema per graph.
- `KRN_B2_FALSIFY_ITERATIONS` (default 3) scales the falsification runs to a day of rows.

Each measurement now also prints the load average after it. `krn_b2_bench.cpp` is
unchanged.
