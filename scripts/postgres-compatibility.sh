#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
postgres_version="${POSTGRES_COMPATIBILITY_VERSION:-17.10}"
postgres_ref="REL_${postgres_version//./_}"
image="${POSTGRES_COMPATIBILITY_IMAGE:-hamstergres-postgres-compatibility:${postgres_version}}"
results_dir="${POSTGRES_COMPATIBILITY_RESULTS_DIR:-${repo_root}/build/postgres-compatibility}"
baseline="${POSTGRES_COMPATIBILITY_BASELINE:-}"
suite_timeout="${POSTGRES_COMPATIBILITY_TIMEOUT:-10m}"
compose_project="hamstergres-pgcompat"
proxy_pid=""

export COMPOSE_PROJECT_NAME="${compose_project}"
export POSTGRES_IMAGE="${image}"
export POSTGRES_DB="regression"
export NEST_PORT="3379"
export BURROW_01_PORT="6541"
export BURROW_02_PORT="6542"
export NEST_DATA_VOLUME="${compose_project}-nest-data"
export BURROW_01_DATA_VOLUME="${compose_project}-burrow-01-data"
export BURROW_02_DATA_VOLUME="${compose_project}-burrow-02-data"

cleanup() {
  if [[ -n "${proxy_pid}" ]]; then
    kill -INT "${proxy_pid}" 2>/dev/null || true
    for _ in $(seq 1 50); do
      if ! kill -0 "${proxy_pid}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "${proxy_pid}" 2>/dev/null; then
      kill -KILL "${proxy_pid}" 2>/dev/null || true
    fi
    wait "${proxy_pid}" 2>/dev/null || true
  fi
  docker compose -f "${repo_root}/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "${repo_root}/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "${results_dir}"
mkdir -p "${results_dir}"

echo "==> Building PostgreSQL ${postgres_version} compatibility image"
docker build \
  --build-arg "POSTGRES_VERSION=${postgres_version}" \
  --build-arg "POSTGRES_GIT_REF=${postgres_ref}" \
  --file "${repo_root}/docker/Dockerfile.postgres-compatibility" \
  --tag "${image}" \
  "${repo_root}"

echo "==> Starting isolated Hamstergres Nest and Burrows"
docker compose -f "${repo_root}/docker-compose.yml" up -d --wait hamstergres-nest burrow-01 burrow-02

for burrow in burrow-01 burrow-02; do
  docker compose -f "${repo_root}/docker-compose.yml" exec -T "${burrow}" \
    psql --username hamster --dbname regression --set ON_ERROR_STOP=1 \
    --command 'DROP SCHEMA public CASCADE; CREATE SCHEMA public AUTHORIZATION hamster;' \
    --command "ALTER DATABASE \"${POSTGRES_DB}\" SET lc_messages TO 'C';" \
    --command "ALTER DATABASE \"${POSTGRES_DB}\" SET lc_monetary TO 'C';" \
    --command "ALTER DATABASE \"${POSTGRES_DB}\" SET lc_numeric TO 'C';" \
    --command "ALTER DATABASE \"${POSTGRES_DB}\" SET lc_time TO 'C';" \
    --command "ALTER DATABASE \"${POSTGRES_DB}\" SET bytea_output TO 'hex';" \
    --command "ALTER DATABASE \"${POSTGRES_DB}\" SET timezone_abbreviations TO 'Default';"
done

echo "==> Starting Hamstergres Proxy"
go -C "${repo_root}" build -o "${results_dir}/hamstergres-proxy" ./cmd/hamstergres-proxy
"${results_dir}/hamstergres-proxy" --config "${repo_root}/config/hamstergres.compatibility.yaml" >"${results_dir}/proxy.log" 2>&1 &
proxy_pid=$!

for _ in $(seq 1 150); do
  if curl --fail --silent "http://127.0.0.1:9080/healthz" >/dev/null; then
    break
  fi
  if ! kill -0 "${proxy_pid}" 2>/dev/null; then
    echo "Hamstergres Proxy exited during startup" >&2
    sed -n '1,240p' "${results_dir}/proxy.log" >&2
    exit 2
  fi
  sleep 0.2
done
curl --fail --silent "http://127.0.0.1:9080/healthz" >/dev/null

echo "==> Running the complete PostgreSQL regression schedule through Hamstergres Proxy"
set +e
docker run --rm \
  --add-host host.docker.internal:host-gateway \
  --entrypoint bash \
  --user "$(id -u):$(id -g)" \
  --volume "${results_dir}:/results" \
  --env PGHOST=host.docker.internal \
  --env PGPORT=7432 \
  --env PGUSER=hamster \
  --env PGPASSWORD=hamster \
  --env PGDATABASE=regression \
  --env "POSTGRES_COMPATIBILITY_TIMEOUT=${suite_timeout}" \
  "${image}" \
  -euo pipefail -c '
    cp /usr/src/postgresql/src/test/regress/parallel_schedule /results/parallel_schedule
    cd /usr/src/postgresql/src/test/regress
    timeout --foreground --signal=INT --kill-after=30s "${POSTGRES_COMPATIBILITY_TIMEOUT}" \
      make installcheck EXTRA_REGRESS_OPTS="--use-existing --dbname=regression --outputdir=/results --dlpath=/usr/lib/postgresql/17/lib"
  ' 2>&1 | tee "${results_dir}/pg_regress.log"
pg_regress_status=${PIPESTATUS[0]}
set -e

# GNU make maps pg_regress's ordinary status 1 (one or more failed tests) to 2.
# Completeness validation below distinguishes that expected compatibility state
# from a broken or truncated harness.
if [[ "${pg_regress_status}" -ne 0 ]]; then
  echo "pg_regress reported compatibility differences (status ${pg_regress_status})"
fi
if [[ "${pg_regress_status}" -eq 124 ]]; then
  echo "pg_regress exceeded the ${suite_timeout} compatibility timeout" >&2
fi

report_args=(
  -log "${results_dir}/pg_regress.log"
  -schedule "${results_dir}/parallel_schedule"
  -output "${results_dir}"
  -postgresql-version "${postgres_version}"
  -proxy-log "${results_dir}/proxy.log"
)
if [[ -n "${baseline}" && -f "${baseline}" ]]; then
  report_args+=(-baseline "${baseline}")
fi
go -C "${repo_root}" run ./tools/pgcompat "${report_args[@]}"

echo "==> Compatibility report: ${results_dir}/compatibility-report.md"
