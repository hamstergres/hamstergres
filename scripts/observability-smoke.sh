#!/bin/sh
set -eu

export COMPOSE_PROJECT_NAME=hamstergres-observability-smoke
export NEST_PORT=${NEST_PORT:-12379}
export BURROW_01_PORT=${BURROW_01_PORT:-15541}
export BURROW_02_PORT=${BURROW_02_PORT:-15542}
export HAMSTERGRES_PROXY_PORT=${HAMSTERGRES_PROXY_PORT:-16432}
export HAMSTERGRES_STATUS_PORT=${HAMSTERGRES_STATUS_PORT:-18080}
export PROMETHEUS_PORT=${PROMETHEUS_PORT:-19090}
export GRAFANA_PORT=${GRAFANA_PORT:-13000}
export NEST_DATA_VOLUME=${NEST_DATA_VOLUME:-hamstergres-observability-smoke-nest}
export BURROW_01_DATA_VOLUME=${BURROW_01_DATA_VOLUME:-hamstergres-observability-smoke-burrow-01}
export BURROW_02_DATA_VOLUME=${BURROW_02_DATA_VOLUME:-hamstergres-observability-smoke-burrow-02}

compose="docker compose --profile observability"
prometheus_url="http://127.0.0.1:${PROMETHEUS_PORT:-9090}"
grafana_url="http://127.0.0.1:${GRAFANA_PORT:-3000}"
proxy_port="${HAMSTERGRES_PROXY_PORT:-6432}"

$compose down --volumes --remove-orphans >/dev/null 2>&1 || true
$compose config --quiet
$compose up -d --build --wait

PGPASSWORD="${POSTGRES_PASSWORD:-hamster}" psql \
  "host=127.0.0.1 port=$proxy_port user=${POSTGRES_USER:-hamster} dbname=${POSTGRES_DB:-hamstergres} sslmode=disable" \
  -v ON_ERROR_STOP=1 -c 'SELECT count(*) FROM accounts;' >/dev/null

targets=$(curl --fail --silent "$prometheus_url/api/v1/targets")
up_count=$(printf '%s' "$targets" | grep -o '"health":"up"' | wc -l | tr -d ' ')
test "$up_count" -eq 4
printf '%s' "$targets" | grep -q 'hamstergres-proxy:8080'
printf '%s' "$targets" | grep -q 'hamstergres-nest:2381'
printf '%s' "$targets" | grep -q 'burrow-01-exporter:9187'
printf '%s' "$targets" | grep -q 'burrow-02-exporter:9187'

curl --fail --silent "$prometheus_url/api/v1/query?query=hamstergres_proxy_queries_total" | grep -q '"status":"success"'
curl --fail --silent "$grafana_url/api/health" | grep -q '"database"'
curl --fail --silent -u "${GRAFANA_ADMIN_USER:-admin}:${GRAFANA_ADMIN_PASSWORD:-admin}" \
  "$grafana_url/api/dashboards/uid/hamstergres-demo" | grep -q 'Hamstergres Demo Overview'

echo "Observability smoke test passed. Grafana: $grafana_url (dashboard: Hamstergres Demo Overview)"
