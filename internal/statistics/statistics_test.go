package statistics

import (
	"sync"
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

func TestLatencyHistogramAndConcurrentScraping(t *testing.T) {
	collector := NewCollector()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				collector.Record(QueryEvent{SQL: "SELECT 1", Success: true, Duration: 7 * time.Millisecond, Shards: []string{"burrow-01"}})
				_ = collector.Snapshot()
			}
		}()
	}
	wg.Wait()
	snapshot := collector.Snapshot()
	if snapshot.Latency.Count != 2000 || snapshot.Latency.Sum < 13.9 || snapshot.Latency.Sum > 14.1 {
		t.Fatalf("latency = %#v, want 2000 observations totaling 14 seconds", snapshot.Latency)
	}
	if snapshot.Latency.Buckets[1].Count != 0 || snapshot.Latency.Buckets[2].Count != 2000 {
		t.Fatalf("buckets = %#v, want all 7ms observations in the 10ms bucket", snapshot.Latency.Buckets)
	}
}

func TestOperationalCountersUseStableDimensions(t *testing.T) {
	collector := NewCollector()
	collector.RecordOperation("two_phase_commit", "uncertain")
	collector.RecordOperation("two_phase_commit", "uncertain")
	collector.RecordOperation("nest_request", "failure")
	got := collector.Snapshot().Operations
	if len(got) != 2 || got[0] != (OperationCount{Operation: "nest_request", Outcome: "failure", Count: 1}) || got[1] != (OperationCount{Operation: "two_phase_commit", Outcome: "uncertain", Count: 2}) {
		t.Fatalf("operations = %#v", got)
	}
}

func TestFailedQueriesAreCountedByStableCategory(t *testing.T) {
	collector := NewCollector()
	collector.Record(QueryEvent{SQL: "SELECT broken", Success: false, ErrorCategory: "sql_error", Shards: []string{"burrow-01"}})
	collector.Record(QueryEvent{SQL: "SELECT disconnected", Success: false, ErrorCategory: "burrow_transport", Shards: []string{"burrow-01"}})
	got := collector.Snapshot().Failures
	if len(got) != 2 || got[0] != (FailureCount{Category: "burrow_transport", Count: 1}) || got[1] != (FailureCount{Category: "sql_error", Count: 1}) {
		t.Fatalf("failures = %#v", got)
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

func BenchmarkCollectorRecordSysbenchQuery(b *testing.B) {
	collector := NewCollector()
	event := QueryEvent{SQL: "SELECT c FROM sbtest1 WHERE id=$1", Success: true, Shards: []string{"burrow-01"}}
	for b.Loop() {
		collector.Record(event)
	}
}
