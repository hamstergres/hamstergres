#!/bin/sh
set -eu

etcdctl() {
  docker compose exec -T hamstergres-nest /usr/local/bin/etcdctl "$@"
}

prefix=/hamstergres/tests/
before=$(etcdctl get "$prefix" --prefix --keys-only | sed '/^$/d' | wc -l | tr -d ' ')
echo "Deleting $before integration-test keys below $prefix"
deleted=$(etcdctl del "$prefix" --prefix)

# Move past the bulk-delete revision before compaction. Otherwise etcd must
# retain that revision's tombstones and defragmentation cannot reclaim most of
# the pages that held the test snapshots.
marker=/hamstergres/maintenance/test-cleanup-$$
etcdctl put "$marker" 1 >/dev/null
status=$(etcdctl endpoint status --write-out=json)
revision=$(printf '%s\n' "$status" | sed -n 's/.*"revision":\([0-9][0-9]*\).*/\1/p')
if [ -z "$revision" ]; then
  etcdctl del "$marker" >/dev/null
  echo "Could not determine the current Nest revision" >&2
  exit 1
fi

echo "Compacting Nest through revision $revision"
etcdctl compact "$revision"
etcdctl del "$marker" >/dev/null
echo "Defragmenting the local Nest member"
etcdctl defrag
echo "Deleted $deleted keys. Current endpoint status:"
etcdctl endpoint status --write-out=table
