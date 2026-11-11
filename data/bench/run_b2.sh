#!/usr/bin/env bash
# run_b2.sh — print the B2 step table over the real sample, then prove the
# bit-identical assertion can fail (ENC-1487).
#
#   data/bench/run_b2.sh [build-dir]
#
# build-dir defaults to ./build and must have been configured with
#   -DCMAKE_BUILD_TYPE=Release -DKRN_DATA_BUILD_BENCHMARKS=ON
#
# Environment:
#   KRN_B2_TRADES_DBN  the retained trades DBN (default: Nick's XNAS.ITCH MSFT
#                      2022-06-10 pull under ~/market-data, the T5 sample)
#   KRN_B2_ITERATIONS  measured iterations per step (default 200)
#   KRN_B2_WARMUP      discarded warm-up iterations per step (default 5)
#
# Exit: 0 every step byte-identical to step 1 AND every falsification turned
# the verdict red; 1 a mismatch, or a falsification that stayed green; 2 could
# not run (no binary, no sample, a refusal from the harness).
set -uo pipefail

build="${1:-build}"
bin="$build/data/krn_b2_bench"
dbn="${KRN_B2_TRADES_DBN:-$HOME/market-data/databento/XNAS.ITCH/MSFT/20220610/xnas-itch-20220610.trades.dbn}"
iters="${KRN_B2_ITERATIONS:-200}"
warm="${KRN_B2_WARMUP:-5}"

if [[ ! -x "$bin" ]]; then
  echo "run_b2.sh: $bin not built; configure with -DKRN_DATA_BUILD_BENCHMARKS=ON" >&2
  exit 2
fi
if [[ ! -r "$dbn" ]]; then
  echo "run_b2.sh: sample not readable: $dbn (set KRN_B2_TRADES_DBN)" >&2
  exit 2
fi
btype=$(sed -n 's/^CMAKE_BUILD_TYPE:[A-Z]*=//p' "$build/CMakeCache.txt" 2>/dev/null)
if [[ "$btype" != "Release" ]]; then
  echo "run_b2.sh: $build is CMAKE_BUILD_TYPE='$btype'; B2 numbers need Release" >&2
  exit 2
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "## B2 steps 1-2 — pool threads = hardware_concurrency (Host's default)"
echo
"$bin" --dbn "$dbn" --iterations "$iters" --warmup "$warm"
rc=$?
[[ $rc -eq 0 ]] || { echo "run_b2.sh: measurement exited $rc" >&2; exit "$rc"; }

echo
echo "## B2 steps 1-2 — pool threads = 1"
echo
"$bin" --dbn "$dbn" --iterations "$iters" --warmup "$warm" --threads 1
rc=$?
[[ $rc -eq 0 ]] || { echo "run_b2.sh: measurement exited $rc" >&2; exit "$rc"; }

echo
echo "## Falsification — each mutation of step 2 must exit 1 with MISMATCH"
echo
echo "| mutation | applied | exit | verdict |"
echo "|---|---|---:|---|"
bad=0
for kind in ulp drop swap store; do
  out="$tmp/falsify-$kind.txt"
  "$bin" --dbn "$dbn" --iterations 3 --warmup 0 --falsify="$kind" >"$out" 2>&1
  frc=$?
  applied=$(sed -n 's/^falsify: applied — //p' "$out")
  if [[ $frc -eq 1 && -n "$applied" ]] && grep -q '^MISMATCH' "$out"; then
    verdict="red, as required"
  else
    verdict="DID NOT FAIL"; bad=1
    sed 's/^/    /' "$out" >&2
  fi
  echo "| $kind | ${applied:-NOT APPLIED} | $frc | $verdict |"
done
[[ $bad -eq 0 ]] || { echo "run_b2.sh: a falsification did not turn the verdict red" >&2; exit 1; }
echo
echo "run_b2.sh: steps 1-2 byte-identical; 4 of 4 falsifications red"
