#!/usr/bin/env bash
# run_b1.sh — B1: ClickHouse -> krn transport, native vs HTTP ArrowStream,
# block-size sweep, cold and warm page cache (ENC-1485).
#
#   data/bench/run_b1.sh [build-dir] [-- extra krn_b1_transport flags]
#
# build-dir defaults to ./build and must have been configured with
#   -DCMAKE_BUILD_TYPE=Release -DKRN_DATA_BUILD_BENCHMARKS=ON
#
# Needs a ClickHouse from `tools/stack.sh up clickhouse` at STACK_PORT_OFFSET
# (the same offset this script reads), with `krn` ingested (krn_ingest ingest).
#
# Environment:
#   STACK_PORT_OFFSET     selects the server: native 9000+n, http 8123+n,
#                         container encultured-clickhouse[-n]
#   KRN_B1_WORKSPACE      workspace root (default: three levels above the
#                         repo's main checkout); its .run[-n]/clickhouse/data is
#                         the host side of the server's /var/lib/clickhouse,
#                         which the cold runs evict
#   KRN_B1_CH_CPUS        CPUs ClickHouse is pinned to (default 0-5,8-13:
#                         physical cores 0-5 with their SMT siblings)
#   KRN_B1_BENCH_CPU      the CPU this process is pinned to (default 7; its
#                         sibling 15 is left to neither)
#   KRN_B1_CSV            per-run CSV path (default: none)
#
# CONFOUND CONTROL (SPEC D13): ClickHouse and the client share this machine.
#   - cores: the server's threads are pinned to KRN_B1_CH_CPUS and the client
#     to KRN_B1_BENCH_CPU, disjoint physical cores; both are read back and
#     printed. The server's affinity is restored to every CPU on exit.
#   - sequential: one query in flight at a time; transports alternate.
#   - memory: krn_b1_transport reads max_server_memory_usage / max_memory_usage
#     back from the server and refuses (exit 1) if either is 0 (unlimited).
#
# Exit: krn_b1_transport's — 0 ran, 1 a finding, 2 could not run.
set -uo pipefail

build="${1:-build}"; shift || true
[[ "${1:-}" == "--" ]] && shift
bin="$build/data/krn_b1_transport"
off="${STACK_PORT_OFFSET:-0}"
ch_cpus="${KRN_B1_CH_CPUS:-0-5,8-13}"
bench_cpu="${KRN_B1_BENCH_CPU:-7}"

if [[ ! -x "$bin" ]]; then
  echo "run_b1.sh: $bin not built; configure with -DKRN_DATA_BUILD_BENCHMARKS=ON" >&2
  exit 2
fi
btype=$(sed -n 's/^CMAKE_BUILD_TYPE:[A-Z]*=//p' "$build/CMakeCache.txt" 2>/dev/null)
if [[ "$btype" != "Release" ]]; then
  echo "run_b1.sh: $build is CMAKE_BUILD_TYPE='$btype'; B1 numbers need Release" >&2
  exit 2
fi

if [[ -n "${KRN_B1_WORKSPACE:-}" ]]; then ws="$KRN_B1_WORKSPACE"
else
  common=$(git -C "$(dirname "$0")" rev-parse --path-format=absolute --git-common-dir 2>/dev/null) \
    || { echo "run_b1.sh: not in a git checkout; set KRN_B1_WORKSPACE" >&2; exit 2; }
  ws=$(dirname "$(dirname "$common")")     # <ws>/krn-data/.git -> <ws>
fi
run="$ws/.run"; [[ "$off" == 0 ]] || run="$ws/.run-$off"
data="$run/clickhouse/data"
[[ -d "$data/store" ]] || { echo "run_b1.sh: no ClickHouse data at $data (STACK_PORT_OFFSET=$off?)" >&2; exit 2; }

ctr="encultured-clickhouse"; [[ "$off" == 0 ]] || ctr="$ctr-$off"
pid=$(podman inspect -f '{{.State.Pid}}' "$ctr" 2>/dev/null)
[[ -n "$pid" && "$pid" != 0 ]] || { echo "run_b1.sh: container $ctr is not running" >&2; exit 2; }

# Pin every thread of the server (and any child) to the ClickHouse cores.
pids=$(ps -o pid= --ppid "$pid"; echo "$pid")
restore() { for p in $pids; do taskset -a -p -c 0-"$(( $(nproc --all) - 1 ))" "$p" >/dev/null 2>&1; done; }
trap restore EXIT
for p in $pids; do
  taskset -a -p -c "$ch_cpus" "$p" >/dev/null \
    || { echo "run_b1.sh: could not pin ClickHouse pid $p to $ch_cpus" >&2; exit 2; }
done

echo "=== run_b1.sh ==="
echo "build:     $build ($btype)"
echo "server:    $ctr pid $pid, data $data"
for p in $pids; do
  echo "pinned:    pid $p ($(cat /proc/$p/comm)) Cpus_allowed_list=$(awk '/Cpus_allowed_list/{print $2}' /proc/$p/status), threads $(ls /proc/$p/task | wc -l)"
done
echo "client:    CPU $bench_cpu"
echo "loadavg:   $(cat /proc/loadavg)  (start)"
echo "kernel:    $(uname -r)   cpu: $(lscpu | sed -n 's/^Model name:[[:space:]]*//p')"

csvflag=(); [[ -n "${KRN_B1_CSV:-}" ]] && csvflag=(--csv "$KRN_B1_CSV")
STACK_PORT_OFFSET="$off" taskset -c "$bench_cpu" "$bin" --host-data-dir "$data" "${csvflag[@]}" "$@"
rc=$?
echo "loadavg:   $(cat /proc/loadavg)  (end)"
exit $rc
