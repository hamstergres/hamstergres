#!/bin/sh
set -eu

etcdctl() {
  docker compose exec -T hamstergres-nest /usr/local/bin/etcdctl "$@"
}

count_prefix() {
  etcdctl get "$1" --prefix --keys-only | sed '/^$/d' | wc -l | tr -d ' '
}

echo "Hamstergres Nest endpoint status:"
status=$(etcdctl endpoint status --write-out=json)
etcdctl endpoint status --write-out=table
db_size=$(printf '%s\n' "$status" | sed -n 's/.*"dbSize":\([0-9][0-9]*\).*/\1/p')
db_size_in_use=$(printf '%s\n' "$status" | sed -n 's/.*"dbSizeInUse":\([0-9][0-9]*\).*/\1/p')
echo "DB size bytes: $db_size"
echo "DB size in-use bytes: $db_size_in_use"
echo "Hamstergres keys: $(count_prefix /hamstergres/)"
echo "Integration-test keys: $(count_prefix /hamstergres/tests/)"

alarms=$(etcdctl alarm list)
if [ -n "$alarms" ]; then
  echo "Active alarms:"
  printf '%s\n' "$alarms"
else
  echo "Active alarms: none"
fi
