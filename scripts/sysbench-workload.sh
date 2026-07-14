#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <sharded|unsharded> <prepare|run|cleanup> [sysbench options]" >&2
}

mode=${1:-}
action=${2:-}
if [[ "$mode" != "sharded" && "$mode" != "unsharded" ]]; then
  usage
  exit 2
fi
if [[ "$action" != "prepare" && "$action" != "run" && "$action" != "cleanup" ]]; then
  usage
  exit 2
fi
shift 2

command -v sysbench >/dev/null || { echo "sysbench 1.0.20 is required" >&2; exit 1; }
sysbench --version | grep -q 'sysbench 1.0.20' || { echo "sysbench 1.0.20 is required" >&2; exit 1; }

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
host=${HAMSTERGRES_BENCHMARK_HOST:-127.0.0.1}
port=${HAMSTERGRES_BENCHMARK_PORT:-6432}
user=${HAMSTERGRES_BENCHMARK_USER:-hamster}
password=${HAMSTERGRES_BENCHMARK_PASSWORD:-hamster}
database=${HAMSTERGRES_BENCHMARK_DATABASE:-hamstergres}
sharding=off
if [[ "$mode" == "sharded" ]]; then
  sharding=on
fi

sysbench \
  --db-driver=pgsql \
  --pgsql-host="$host" \
  --pgsql-port="$port" \
  --pgsql-user="$user" \
  --pgsql-password="$password" \
  --pgsql-db="$database" \
  --auto-inc=off \
  --hamstergres-sharding="$sharding" \
  "$@" \
  "$script_dir/sysbench-oltp-read-write.lua" \
  "$action"
