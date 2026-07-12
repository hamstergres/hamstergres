#!/bin/sh
set -eu

psql --set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=monitoring_user="${POSTGRES_MONITORING_USER:-hamstergres_monitor}" \
  --set=monitoring_password="${POSTGRES_MONITORING_PASSWORD:-hamstergres_monitor}" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'monitoring_user', :'monitoring_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'monitoring_user') \gexec
SELECT format('GRANT pg_monitor TO %I', :'monitoring_user') \gexec
SQL
