#!/usr/bin/env bash
set -euo pipefail

command -v sysbench >/dev/null || { echo "sysbench 1.0.20 is required" >&2; exit 1; }
sysbench --version | grep -q 'sysbench 1.0.20' || { echo "sysbench 1.0.20 is required" >&2; exit 1; }
curl --fail --silent http://127.0.0.1:8080/healthz >/dev/null || {
  echo "start Hamstergres Proxy with 'make run-proxy' first" >&2
  exit 1
}

mode=${HAMSTERGRES_BENCHMARK_MODE:-sharded}
if [[ "$mode" != "sharded" && "$mode" != "unsharded" ]]; then
  echo "HAMSTERGRES_BENCHMARK_MODE must be 'sharded' or 'unsharded'" >&2
  exit 2
fi
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workload="$script_dir/sysbench-oltp-read-write.lua"
sharding=off
if [[ "$mode" == "sharded" ]]; then
  sharding=on
fi

tables=${HAMSTERGRES_BENCHMARK_TABLES:-2}
table_size=${HAMSTERGRES_BENCHMARK_TABLE_SIZE:-1000}
threads=${HAMSTERGRES_BENCHMARK_THREADS:-4}
seconds=${HAMSTERGRES_BENCHMARK_SECONDS:-15}

common=(
  --db-driver=pgsql --auto_inc=off --pgsql-host=127.0.0.1
  --pgsql-user=hamster --pgsql-password=hamster --pgsql-db=hamstergres
  --tables="$tables" --table-size="$table_size" --threads="$threads" --time="$seconds" --events=0
  --report-interval=5 --rand-seed=1
)

cleanup() {
  sysbench "${common[@]}" --pgsql-port=6432 --hamstergres-sharding="$sharding" "$workload" cleanup >/dev/null 2>&1 || true
  sysbench "${common[@]}" --pgsql-port=5541 --hamstergres-sharding="$sharding" "$workload" cleanup >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_pair() {
  local label=$1 port=$2
  sysbench "${common[@]}" --pgsql-port="$port" --hamstergres-sharding="$sharding" "$workload" cleanup >/dev/null 2>&1 || true
  sysbench "${common[@]}" --pgsql-port="$port" --hamstergres-sharding="$sharding" "$workload" prepare >/dev/null
  sysbench "${common[@]}" --pgsql-port="$port" oltp_read_only run | tee "/tmp/hamstergres-${label}-read.log"
  sysbench "${common[@]}" --pgsql-port="$port" oltp_read_write run | tee "/tmp/hamstergres-${label}-mixed.log"
  sysbench "${common[@]}" --pgsql-port="$port" --hamstergres-sharding="$sharding" "$workload" cleanup >/dev/null
}

metric() {
  awk -v field="$2" '$1 == field":" {gsub(/[()]/, "", $3); print $3; exit}' "$1"
}

run_pair proxy 6432
run_pair direct 5541

proxy_read=$(metric /tmp/hamstergres-proxy-read.log transactions)
direct_read=$(metric /tmp/hamstergres-direct-read.log transactions)
proxy_mixed=$(metric /tmp/hamstergres-proxy-mixed.log transactions)
direct_mixed=$(metric /tmp/hamstergres-direct-mixed.log transactions)
read_ratio=$(awk -v proxy="$proxy_read" -v direct="$direct_read" 'BEGIN { printf "%.4f", proxy / direct }')
mixed_ratio=$(awk -v proxy="$proxy_mixed" -v direct="$direct_mixed" 'BEGIN { printf "%.4f", proxy / direct }')

printf '{"sysbench":"1.0.20","mode":"%s","threads":%s,"seconds":%s,"table_size":%s,"tables":%s,"read_only":{"proxy_tps":%s,"direct_tps":%s,"ratio":%s},"read_write":{"proxy_tps":%s,"direct_tps":%s,"ratio":%s}}\n' \
  "$mode" "$threads" "$seconds" "$table_size" "$tables" "$proxy_read" "$direct_read" "$read_ratio" "$proxy_mixed" "$direct_mixed" "$mixed_ratio"
