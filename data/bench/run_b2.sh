#!/usr/bin/env bash
# run_b2.sh — print the B2 step table over the real sample, one data point per
# schema (trades, then mbo), then prove the bit-identical assertion can fail on
# each (ENC-1487, ENC-1628).
#
#   data/bench/run_b2.sh [build-dir]
#
# build-dir defaults to ./build and must have been configured with
#   -DCMAKE_BUILD_TYPE=Release -DKRN_DATA_BUILD_BENCHMARKS=ON
#
# Environment:
#   KRN_B2_TRADES_DBN  the retained trades DBN (default: Nick's XNAS.ITCH MSFT
#                      2022-06-10 pull under ~/market-data, the T5 sample)
#   KRN_B2_MBO_DBN     the retained mbo DBN of the same pull (ENC-1628)
#   KRN_B2_ITERATIONS  measured iterations per step (default 200)
#   KRN_B2_WARMUP      discarded warm-up iterations per step (default 5)
#
# Exit: 0 every step byte-identical to step 1 AND every falsification turned
# the verdict red; 1 a mismatch, or a falsification that stayed green; 2 could
# not run (no binary, no sample, a refusal from the harness).
set -uo pipefail

build="${1:-build}"
bin="$build/data/krn_b2_bench"
sample_dir="$HOME/market-data/databento/XNAS.ITCH/MSFT/20220610"
trades="${KRN_B2_TRADES_DBN:-$sample_dir/xnas-itch-20220610.trades.dbn}"
mbo="${KRN_B2_MBO_DBN:-$sample_dir/xnas-itch-20220610.mbo.dbn}"
iters="${KRN_B2_ITERATIONS:-200}"
warm="${KRN_B2_WARMUP:-5}"

if [[ ! -x "$bin" ]]; then
  echo "run_b2.sh: $bin not built; configure with -DKRN_DATA_BUILD_BENCHMARKS=ON" >&2
  exit 2
fi
for f in "$trades" "$mbo"; do
  if [[ ! -r "$f" ]]; then
    echo "run_b2.sh: sample not readable: $f (set KRN_B2_TRADES_DBN / KRN_B2_MBO_DBN)" >&2
    exit 2
  fi
done
btype=$(sed -n 's/^CMAKE_BUILD_TYPE:[A-Z]*=//p' "$build/CMakeCache.txt" 2>/dev/null)
if [[ "$btype" != "Release" ]]; then
  echo "run_b2.sh: $build is CMAKE_BUILD_TYPE='$btype'; B2 numbers need Release" >&2
  exit 2
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

for schema in trades mbo; do
  dbn=$trades; [[ $schema == mbo ]] && dbn=$mbo
  for threads in hw 1; do
    echo "## B2 steps 1-3, $schema — pool threads = $([[ $threads == hw ]] && echo "hardware_concurrency (Host's default)" || echo 1)"
    echo
    targs=(); [[ $threads == 1 ]] && targs=(--threads 1)
    "$bin" --dbn "$dbn" --iterations "$iters" --warmup "$warm" "${targs[@]}"
    rc=$?
    [[ $rc -eq 0 ]] || { echo "run_b2.sh: $schema measurement exited $rc" >&2; exit "$rc"; }
    echo
  done
done

echo "## Falsification — each mutation of step 2 must exit 1 with MISMATCH, on each schema"
echo
echo "| schema | mutation | applied | exit | verdict |"
echo "|---|---|---|---:|---|"
bad=0
n=0
for schema in trades mbo; do
  dbn=$trades; [[ $schema == mbo ]] && dbn=$mbo
  for kind in ulp drop swap store; do
    out="$tmp/falsify-$schema-$kind.txt"
    "$bin" --dbn "$dbn" --iterations 3 --warmup 0 --falsify="$kind" >"$out" 2>&1
    frc=$?
    applied=$(sed -n 's/^falsify: applied — //p' "$out")
    if [[ $frc -eq 1 && -n "$applied" ]] && grep -q '^MISMATCH' "$out"; then
      verdict="red, as required"; n=$((n + 1))
    else
      verdict="DID NOT FAIL"; bad=1
      sed 's/^/    /' "$out" >&2
    fi
    echo "| $schema | $kind | ${applied:-NOT APPLIED} | $frc | $verdict |"
  done
done
[[ $bad -eq 0 ]] || { echo "run_b2.sh: a falsification did not turn the verdict red" >&2; exit 1; }
echo
echo "run_b2.sh: steps 1-3 byte-identical on trades and mbo; $n of 8 falsifications red"
