# krn-data

The L1 data layer, Arrow and the historical provider adapters, built against the
engine (`Dynacharting/krn`) at a pinned commit. Design:
`encultured-workspace/specs/2026-10-15-gma-engine-purity/SPEC.md` (D2, D5, D9, D17),
and krn's `specs/2026-10-14-krn-data-layer/SPEC.md` (D1: ClickHouse selects, the
engine computes, one implementation of every operator; D14: two deployment profiles).

`data/` and `docs/data-layer.md` are a verbatim import from krn `ca544ec` (§6 row 24,
ENC-1572). `data/CMakeLists.txt` differs from krn's in two glue lines: the test gate reads
`KRN_DATA_BUILD_TESTS`, and `KRN_TEST_SOURCE_DIR` is a build-tree overlay of this repo's
`data/` and the engine's `include/`.

## Build and test

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

An empty `CMAKE_BUILD_TYPE` defaults to `Release` (single-config generators only).
The fetched engine needs RapidJSON and Boost; with no system package, pass
`-DTHIRD_PARTY_ROOT=<dir holding rapidjson/>`.

| option | default | effect |
|---|---|---|
| `KRN_DATA_BUILD_TESTS` | `ON` | fetch googletest; register `krn_data_tests`, `krn_schema_no_compute` and its selftest with ctest |
| `KRN_DATA_BUILD_BENCHMARKS` | `OFF` | build `krn_b2_bench` (ENC-1487, B2 rows/sec per step) and `krn_b1_transport` (ENC-1485, B1; fetches clickhouse-cpp v2.6.2, which uses Arrow's lz4); never ctest cases |

B2: configure `-DCMAKE_BUILD_TYPE=Release -DKRN_DATA_BUILD_BENCHMARKS=ON`, then
`data/bench/run_b2.sh build` prints the step table over the retained trades DBN
(`KRN_B2_TRADES_DBN`) and asserts that each `--falsify` mutation turns the
bit-identical check red. Exit 0 identical, 1 a mismatch, 2 could not run.

B1: the same configure, a ClickHouse from `STACK_PORT_OFFSET=<n> tools/stack.sh up
clickhouse` with `krn` ingested (`krn_ingest ingest`), then `STACK_PORT_OFFSET=<n>
data/bench/run_b1.sh build` pins cores, reads the D13 caps back and prints rows/sec by
table × cache (cold/warm) × block size × transport (native/HTTP ArrowStream). The L1
database name is `krn::data::kL1Database` (`Schema.hpp`), pinned to `clickhouse.sql` by a
test. Exit 0 ran, 1 a finding, 2 could not run.

This repo's names are `KRN_DATA_BUILD_*` (options) and `KRN_DATA_KRN_*` (the pin). Never
add a `KRN_BUILD_*` option here, and never reuse the engine data layer's
`KRN_DATA_ARROW_*`, `KRN_DATA_DBN_*` or `KRN_DATA_TEST_*` names for anything else.

## The engine pin

`CMakeLists.txt` sets `KRN_DATA_KRN_COMMIT` (a full 40-character SHA of krn `master`;
krn has no tags) and `KRN_DATA_KRN_TARGETS`, the targets the engine defines in consumed
mode at that commit. `cmake/KrnEngine.cmake` fetches it and fails the configure unless:

- the fetched tree's `HEAD` equals the pin (printed as `krn-data: engine pinned at …`);
- the engine's directories define exactly `KRN_DATA_KRN_TARGETS`, and in particular no
  `krn_data`, `krn_data_tests`, `krn_tests`, `krn_server` or `krn_connector_market`;
- every engine library `SOURCE` sits under the fetched tree (the capture check).

A bump is one diff: the new SHA, plus `KRN_DATA_KRN_TARGETS` if the engine's consumed-mode
targets changed at it.

krn-data does not depend on `krn-common` (SPEC D9, amended). If it ever does, the edge
runs krn-data → krn-common, never the reverse.

## Override: unmerged engine work

`-DFETCHCONTENT_SOURCE_DIR_KRN=<krn checkout>` builds against that tree; nothing is written
into it. The configure prints a warning naming its `HEAD` and whether it is clean or has
uncommitted changes, because the pin is then not what is compiled.

## FETCHCONTENT_BASE_DIR

Never share it across build trees or worktrees: a shared one is a green build against
another ticket's engine (SPEC D5). The configure refuses one outside the build dir.

## D17: importing from the engine repo

Code from krn comes in verbatim from a named krn commit, recorded in the commit message
and as the pin. The only edits allowed in an import are build glue (this repo's CMake, the
`FetchContent` block, include roots), listed in the ticket. No history graft
(`git subtree`, `filter-repo`, merges of krn history). From the import until krn deletes its
copy, krn's copy is frozen, and the deletion's acceptance is an empty `diff -r`.

## Rules

- `LICENSE` is the root commit, unchanged. Add no other licence file or SPDX header.
- Default branch `main`. One Linear `ENC` ticket per branch.
