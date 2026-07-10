package statistics

import (
	"testing"
	"time"
)

func TestSnapshotAggregatesWindowsRoutesAndSummaries(t *testing.T) {
	now := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	collector := newCollector(func() time.Time { return now })
	collector.Record(QueryEvent{SQL: "SELECT * FROM accounts WHERE tenant_id = 42", Success: true, Duration: 12 * time.Millisecond, Shards: []string{"burrow-01"}})
	now = now.Add(11 * time.Second)
	collector.Record(QueryEvent{SQL: "SELECT * FROM accounts WHERE tenant_id = 99", Success: false, Duration: 28 * time.Millisecond, Shards: []string{"burrow-01", "burrow-02"}})

	snapshot := collector.Snapshot()
	if snapshot.Total.Queries != 2 || snapshot.Total.ScatteredQueries != 1 || snapshot.Total.SingleShardQueries != 1 || snapshot.Total.FailedQueries != 1 {
		t.Fatalf("total = %#v, want two queries with one failure, single-shard, and scatter", snapshot.Total)
	}
	if len(snapshot.Windows) != 4 || snapshot.Windows[0].Statistics.Queries != 1 || snapshot.Windows[1].Statistics.Queries != 2 {
		t.Fatalf("windows = %#v, want 1 query in 10 seconds and 2 in 1 minute", snapshot.Windows)
	}
	if len(snapshot.ShardExecutions) != 2 || snapshot.ShardExecutions[0].Queries != 2 || snapshot.ShardExecutions[1].Queries != 1 {
		t.Fatalf("Burrow executions = %#v, want burrow-01=2 and burrow-02=1", snapshot.ShardExecutions)
	}
	if len(snapshot.QuerySummaries) != 1 || snapshot.QuerySummaries[0].QueryShape != "SELECT * FROM accounts WHERE tenant_id = ?" || snapshot.QuerySummaries[0].Fingerprint != Fingerprint("SELECT * FROM accounts WHERE tenant_id = ?") || snapshot.QuerySummaries[0].Statistics.Queries != 2 {
		t.Fatalf("summaries = %#v, want parameterized and fingerprinted SELECT summary", snapshot.QuerySummaries)
	}
}

func TestNormalizePreservesStructureAndReplacesLiterals(t *testing.T) {
	query := "SELECT * FROM accounts WHERE tenant_id = 42 AND label = 'a secret value'"
	want := "SELECT * FROM accounts WHERE tenant_id = ? AND label = ?"
	if got := Normalize(query); got != want {
		t.Fatalf("Normalize(%q) = %q, want %q", query, got, want)
	}
	if Fingerprint(want) != Fingerprint(Normalize("SELECT * FROM accounts WHERE tenant_id = 99 AND label = 'another value'")) {
		t.Fatal("equivalent parameterized query shapes have different fingerprints")
	}
}
