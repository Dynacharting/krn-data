# krn-data

The L1 data layer, Arrow and the historical provider adapters, built against the
engine (`Dynacharting/krn`) at a pinned commit. Design:
`encultured-workspace/specs/2026-10-15-gma-engine-purity/SPEC.md` (D2, D5, D9, D17),
and krn's `specs/2026-10-14-krn-data-layer/SPEC.md` (D1: ClickHouse selects, the
engine computes, one implementation of every operator; D14: two deployment profiles).

Today this repo is a scaffold: `krn_data_tests` has no cases. `data/` arrives from the
engine repo in §6 row 24, and Arrow's `FetchContent` with it.

## Build and test

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

Always pass `-DCMAKE_BUILD_TYPE`. The fetched engine needs RapidJSON and Boost; with no
system package, pass `-DTHIRD_PARTY_ROOT=<dir holding rapidjson/>`.

| option | default | effect |
|---|---|---|
| `KRN_DATA_BUILD_TESTS` | `ON` | fetch googletest, build `krn_data_tests`, register it with ctest |

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

Unmerged engine work: `-DFETCHCONTENT_SOURCE_DIR_KRN=<krn checkout>` builds against that
tree. The configure prints a warning naming its `HEAD` and whether it is dirty, because
the pin is then not what is compiled. Never share `FETCHCONTENT_BASE_DIR` across build
trees or worktrees; the configure refuses one outside the build dir.

krn-data does not depend on `krn-common` (SPEC D9, amended). If it ever does, the edge
runs krn-data → krn-common, never the reverse.

## Rules

- `LICENSE` is the root commit, unchanged. Add no other licence file or SPDX header.
- Code from the engine repo comes in verbatim from a named krn commit, recorded in the
  commit message (SPEC D17). The only edits allowed in an import are build glue. No
  history graft (`git subtree`, `filter-repo`, merges of krn history).
- Default branch `main`. One Linear `ENC` ticket per branch.
