#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v sysbench >/dev/null || { echo "sysbench 1.0.20 is required" >&2; exit 1; }
sysbench --version | grep -q 'sysbench 1.0.20' || { echo "sysbench 1.0.20 is required" >&2; exit 1; }

if [[ -n "${HAMSTERGRES_EXPERIMENT_THREADS:-}" ]]; then
  concurrency_values=($HAMSTERGRES_EXPERIMENT_THREADS)
else
  concurrency_values=(${HAMSTERGRES_EXPERIMENT_CONCURRENCY:-1 4 8 16 32 64})
fi
seconds=${HAMSTERGRES_EXPERIMENT_SECONDS:-15}
warmup_seconds=${HAMSTERGRES_EXPERIMENT_WARMUP_SECONDS:-3}
rounds=${HAMSTERGRES_EXPERIMENT_ROUNDS:-3}
table_size=${HAMSTERGRES_EXPERIMENT_TABLE_SIZE:-10000}
workload="$repo_root/scripts/sysbench-sharded-point-select.lua"
proxy_port=16432
status_url=http://127.0.0.1:18080
proxy_pid=
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/hamstergres-cpu-scaling.XXXXXX")

stop_proxy() {
  if [[ -n "$proxy_pid" ]]; then
    kill "$proxy_pid" >/dev/null 2>&1 || true
    wait "$proxy_pid" >/dev/null 2>&1 || true
    proxy_pid=
  fi
}

cleanup() {
  stop_proxy
  for burrow in burrow-01 burrow-02; do
    docker compose exec -T "$burrow" psql -U hamster -d hamstergres -qc \
      "DROP TABLE IF EXISTS hamstergres_cpu_scaling" >/dev/null 2>&1 || true
  done
  rm -rf "$temporary_directory"
}
trap cleanup EXIT

docker compose up -d --wait hamstergres-nest burrow-01 burrow-02
docker compose exec -T hamstergres-nest etcdctl del --prefix \
  /hamstergres/experiments/cpu-scaling >/dev/null

for burrow in burrow-01 burrow-02; do
  container_id=$(docker compose ps -q "$burrow")
  nano_cpus=$(docker inspect --format '{{.HostConfig.NanoCpus}}' "$container_id")
  if [[ "$nano_cpus" != "1000000000" ]]; then
    echo "$burrow has NanoCpus=$nano_cpus; expected 1000000000 (one CPU)" >&2
    exit 1
  fi
done

docker_cpus=$(docker info --format '{{.NCPU}}')
go build -o "$temporary_directory/hamstergres-proxy" ./cmd/hamstergres-proxy

start_proxy() {
  local config=$1 log_file=$2
  "$temporary_directory/hamstergres-proxy" --config "$config" >"$log_file" 2>&1 &
  proxy_pid=$!
  for _ in {1..100}; do
    if curl --fail --silent "$status_url/healthz" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "$proxy_pid" >/dev/null 2>&1; then
      echo "Hamstergres Proxy exited while starting; log follows:" >&2
      sed -n '1,200p' "$log_file" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo "Hamstergres Proxy did not become healthy; log follows:" >&2
  sed -n '1,200p' "$log_file" >&2
  exit 1
}

row_count() {
  local burrow=$1
  docker compose exec -T "$burrow" psql -U hamster -d hamstergres -Atqc \
    "SELECT count(*) FROM hamstergres_cpu_scaling" 2>/dev/null || printf '0\n'
}

transaction_rate() {
  awk '$1 == "transactions:" {gsub(/[()]/, "", $3); print $3; exit}' "$1"
}

average() {
  awk '{ total += $1 } END { printf "%.2f", total / NR }' "$1"
}

sysbench_run() {
  local port=$1 threads=$2 duration=$3
  sysbench --db-driver=pgsql --pgsql-host=127.0.0.1 \
    --pgsql-user=hamster --pgsql-password=hamster --pgsql-db=hamstergres \
    --pgsql-port="$port" --table-size="$table_size" --threads="$threads" \
    --time="$duration" --events=0 --report-interval=5 --rand-seed=1 \
    "$workload" run
}

prepare_dataset() {
  local port=$1
  sysbench --db-driver=pgsql --pgsql-host=127.0.0.1 \
    --pgsql-user=hamster --pgsql-password=hamster --pgsql-db=hamstergres \
    --pgsql-port="$port" --table-size="$table_size" --threads=1 \
    "$workload" cleanup >/dev/null 2>&1 || true
  sysbench --db-driver=pgsql --pgsql-host=127.0.0.1 \
    --pgsql-user=hamster --pgsql-password=hamster --pgsql-db=hamstergres \
    --pgsql-port="$port" --table-size="$table_size" --threads=1 \
    "$workload" prepare >/dev/null
}

cleanup_dataset() {
  local port=$1
  sysbench --db-driver=pgsql --pgsql-host=127.0.0.1 \
    --pgsql-user=hamster --pgsql-password=hamster --pgsql-db=hamstergres \
    --pgsql-port="$port" --table-size="$table_size" --threads=1 \
    "$workload" cleanup >/dev/null
}

metric_value() {
  local file=$1 metric=$2 label=${3:-}
  awk -v metric="$metric" -v label="$label" '
    ($1 == metric || index($1, metric "{") == 1) && (label == "" || index($1, label) > 0) { print $2; found=1; exit }
    END { if (!found) print 0 }
  ' "$file"
}

capture_metrics() {
  curl --fail --silent "$status_url/metrics" >"$1"
}

verify_single_owner_delta() {
  local before=$1 after=$2 topology=$3
  local before_single after_single before_scatter after_scatter before_01 after_01 before_02 after_02
  before_single=$(metric_value "$before" hamstergres_proxy_query_routes_total 'route="single_burrow"')
  after_single=$(metric_value "$after" hamstergres_proxy_query_routes_total 'route="single_burrow"')
  before_scatter=$(metric_value "$before" hamstergres_proxy_query_routes_total 'route="scatter"')
  after_scatter=$(metric_value "$after" hamstergres_proxy_query_routes_total 'route="scatter"')
  before_01=$(metric_value "$before" hamstergres_proxy_burrow_executions_total 'burrow="burrow-01"')
  after_01=$(metric_value "$after" hamstergres_proxy_burrow_executions_total 'burrow="burrow-01"')
  before_02=$(metric_value "$before" hamstergres_proxy_burrow_executions_total 'burrow="burrow-02"')
  after_02=$(metric_value "$after" hamstergres_proxy_burrow_executions_total 'burrow="burrow-02"')

  local single_delta=$((after_single - before_single))
  local scatter_delta=$((after_scatter - before_scatter))
  local burrow_01_delta=$((after_01 - before_01))
  local burrow_02_delta=$((after_02 - before_02))
  local execution_delta=$((burrow_01_delta + burrow_02_delta))
  if (( single_delta <= 0 || scatter_delta != 0 || execution_delta != single_delta )); then
    echo "$topology routing check failed: single=$single_delta scatter=$scatter_delta executions=$execution_delta" >&2
    exit 1
  fi
  if [[ "$topology" == two ]] && (( burrow_01_delta <= 0 || burrow_02_delta <= 0 )); then
    echo "two-Burrow routing check failed: burrow-01=$burrow_01_delta burrow-02=$burrow_02_delta" >&2
    exit 1
  fi
}

run_timed() {
  local label=$1 port=$2 threads=$3 round=$4 topology=${5:-}
  local output_file="$temporary_directory/$label-$threads-tps"
  local log_file="$temporary_directory/$label-$threads-round-$round.log"
  local before="$temporary_directory/$label-$threads-round-$round-before.metrics"
  local after="$temporary_directory/$label-$threads-round-$round-after.metrics"

  if (( warmup_seconds > 0 )); then
    echo "Warming $label at $threads clients for ${warmup_seconds}s..." >&2
    sysbench_run "$port" "$threads" "$warmup_seconds" >/dev/null
  fi
  if [[ -n "$topology" ]]; then
    capture_metrics "$before"
  fi
  echo "Running $label at $threads clients, round $round/$rounds..." >&2
  sysbench_run "$port" "$threads" "$seconds" | tee "$log_file" >&2
  transaction_rate "$log_file" >>"$output_file"
  if [[ -n "$topology" ]]; then
    capture_metrics "$after"
    verify_single_owner_delta "$before" "$after" "$topology"
  fi
}

ordered_concurrency() {
  local round=$1
  if (( round % 2 == 1 )); then
    printf '%s\n' "${concurrency_values[@]}"
  else
    local index
    for ((index=${#concurrency_values[@]}-1; index>=0; index--)); do
      printf '%s\n' "${concurrency_values[index]}"
    done
  fi
}

run_direct_rounds() {
  prepare_dataset 5541
  local direct_rows
  direct_rows=$(row_count burrow-01)
  [[ "$direct_rows" == "$table_size" ]] || { echo "direct dataset has $direct_rows rows; expected $table_size" >&2; exit 1; }
  local round threads
  for round in $(seq 1 "$rounds"); do
    while read -r threads; do
      run_timed direct-one-burrow 5541 "$threads" "$round"
    done < <(ordered_concurrency "$round")
  done
  cleanup_dataset 5541
}

run_proxy_topology() {
  local topology=$1 round=$2 config label
  if [[ "$topology" == single ]]; then
    config=config/hamstergres.experiment-single-burrow.yaml
    label=proxy-one-burrow
  else
    config=config/hamstergres.experiment-two-burrows.yaml
    label=proxy-two-burrows
  fi

  start_proxy "$config" "$temporary_directory/$label-round-$round.proxy.log"
  prepare_dataset "$proxy_port"
  local burrow_01_rows burrow_02_rows total_rows
  burrow_01_rows=$(row_count burrow-01)
  burrow_02_rows=$(row_count burrow-02)
  total_rows=$((burrow_01_rows + burrow_02_rows))
  if [[ "$topology" == single ]]; then
    if (( burrow_01_rows != table_size )); then
      echo "one-Burrow dataset has $burrow_01_rows rows; expected $table_size" >&2
      exit 1
    fi
  elif (( burrow_01_rows == 0 || burrow_02_rows == 0 || total_rows != table_size )); then
    echo "two-Burrow dataset is not split correctly: burrow-01=$burrow_01_rows burrow-02=$burrow_02_rows total=$total_rows expected=$table_size" >&2
    exit 1
  fi

  local threads
  while read -r threads; do
    run_timed "$label" "$proxy_port" "$threads" "$round" "$topology"
  done < <(ordered_concurrency "$round")
  cleanup_dataset "$proxy_port"
  stop_proxy
}

for threads in "${concurrency_values[@]}"; do
  : >"$temporary_directory/direct-one-burrow-$threads-tps"
  : >"$temporary_directory/proxy-one-burrow-$threads-tps"
  : >"$temporary_directory/proxy-two-burrows-$threads-tps"
done

run_direct_rounds

for round in $(seq 1 "$rounds"); do
  if (( round % 2 == 1 )); then
    run_proxy_topology single "$round"
    run_proxy_topology two "$round"
  else
    run_proxy_topology two "$round"
    run_proxy_topology single "$round"
  fi
done

for threads in "${concurrency_values[@]}"; do
  direct_tps=$(average "$temporary_directory/direct-one-burrow-$threads-tps")
  proxy_single_tps=$(average "$temporary_directory/proxy-one-burrow-$threads-tps")
  proxy_two_tps=$(average "$temporary_directory/proxy-two-burrows-$threads-tps")
  sharding_speedup=$(awk -v two="$proxy_two_tps" -v one="$proxy_single_tps" 'BEGIN { printf "%.4f", two / one }')
  proxy_vs_direct=$(awk -v two="$proxy_two_tps" -v direct="$direct_tps" 'BEGIN { printf "%.4f", two / direct }')
  printf '{"sysbench":"1.0.20","workload":"keyed-point-select","docker_cpus":%s,"burrow_cpu_limit":1,"threads":%s,"warmup_seconds":%s,"seconds_per_round":%s,"rounds":%s,"table_size":%s,"tps":{"direct_one_burrow":%s,"proxy_one_burrow":%s,"proxy_two_burrows":%s},"ratios":{"two_vs_one_burrow_proxy":%s,"two_burrow_proxy_vs_direct":%s}}\n' \
    "$docker_cpus" "$threads" "$warmup_seconds" "$seconds" "$rounds" "$table_size" \
    "$direct_tps" "$proxy_single_tps" "$proxy_two_tps" "$sharding_speedup" "$proxy_vs_direct"
done
