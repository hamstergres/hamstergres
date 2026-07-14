#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <sharded|unsharded> <prepare|run|cleanup> [pgbench options]" >&2
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

command -v pgbench >/dev/null || { echo "pgbench is required" >&2; exit 1; }

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
host=${HAMSTERGRES_BENCHMARK_HOST:-127.0.0.1}
port=${HAMSTERGRES_BENCHMARK_PORT:-6432}
user=${HAMSTERGRES_BENCHMARK_USER:-hamster}
password=${HAMSTERGRES_BENCHMARK_PASSWORD:-hamster}
database=${HAMSTERGRES_BENCHMARK_DATABASE:-hamstergres}
common=(-h "$host" -p "$port" -U "$user" "$database")
export PGPASSWORD=$password

scale=${HAMSTERGRES_PGBENCH_SCALE:-1}
options=("$@")
for ((index = 0; index < ${#options[@]}; index++)); do
  case ${options[$index]} in
    --scale=*) scale=${options[$index]#--scale=} ;;
    -s) ((index += 1)); scale=${options[$index]:-} ;;
    -s*) scale=${options[$index]#-s} ;;
    --partitions=*|--partition-method=*|--foreign-keys)
      if [[ "$mode" == "sharded" && "$action" == "prepare" ]]; then
        echo "sharded pgbench preparation does not support partitions or foreign keys" >&2
        exit 2
      fi
      ;;
  esac
done
if [[ ! "$scale" =~ ^[1-9][0-9]*$ ]]; then
  echo "pgbench scale must be a positive integer, got '$scale'" >&2
  exit 2
fi

copy_rows() {
  local table=$1 columns=$2
  psql -X -v ON_ERROR_STOP=1 "${common[@]}" \
    --command="\\copy $table ($columns) FROM STDIN"
}

generate_pgbench_data() {
  awk -v scale="$scale" 'BEGIN {
    for (bid = 1; bid <= scale; bid++)
      print bid "\t0\t\\N"
  }' | copy_rows pgbench_branches "bid, bbalance, filler"

  awk -v scale="$scale" 'BEGIN {
    for (tid = 1; tid <= scale * 10; tid++) {
      bid = int((tid - 1) / 10) + 1
      print tid "\t" bid "\t0\t\\N"
    }
  }' | copy_rows pgbench_tellers "tid, bid, tbalance, filler"

  awk -v scale="$scale" 'BEGIN {
    for (aid = 1; aid <= scale * 100000; aid++) {
      bid = int((aid - 1) / 100000) + 1
      print aid "\t" bid "\t0\t\\N"
    }
  }' | copy_rows pgbench_accounts "aid, bid, abalance, filler"
}

case "$action" in
  prepare)
    if [[ "$mode" == "unsharded" ]]; then
      pgbench "${common[@]}" --initialize "$@"
      exit
    fi
	command -v psql >/dev/null || { echo "psql is required for sharded pgbench preparation" >&2; exit 1; }

    # pgbench exposes initialization as independent steps. Declare all shard
    # the large accounts table's key after table creation (dt), then use
    # explicit-column COPY streams so every account is routed before pgbench
    # creates indexes and vacuums (vp). The small branches/tellers dimensions
    # and append-only history remain unsharded; among other things, pgbench
    # reads branches to discover the scale factor before every run.
    # Stock pgbench COPY omits the column list, which is intentionally unsafe
    # for the Proxy's fail-closed shard-key decoder.
    pgbench "${common[@]}" --initialize --init-steps=dt "$@"
    pgbench "${common[@]}" --no-vacuum --file="$script_dir/pgbench-shard-keys.sql" --transactions=1
    generate_pgbench_data
    pgbench "${common[@]}" --initialize --init-steps=vp "$@"
    ;;
  run)
    pgbench "${common[@]}" "$@"
    ;;
  cleanup)
    pgbench "${common[@]}" --initialize --init-steps=d "$@"
    ;;
esac
