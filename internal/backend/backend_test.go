package backend

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jruszo/hamstergres/internal/statistics"
)

func TestAppendShardKeyPreservesCompoundColumnOrder(t *testing.T) {
	keys := make(map[string][]string)
	appendShardKey(keys, "public", "events", "tenant")
	appendShardKey(keys, "public", "events", "region")
	want := []string{"tenant", "region"}
	for _, name := range []string{"events", "public.events"} {
		if !reflect.DeepEqual(keys[name], want) {
			t.Fatalf("%s shard key = %#v", name, keys[name])
		}
	}
}

func TestSchemaMismatchHasDedicatedOperationalSignal(t *testing.T) {
	m := &Manager{metrics: statistics.NewCollector()}
	m.recordSchemaRefreshFailure(fmt.Errorf("schema registry mismatch at Burrow burrow-02"))
	operations := m.QueryMetrics().Operations
	if len(operations) != 2 || operations[0].Operation != "schema_registry_mismatch" || operations[0].Outcome != "detected" {
		t.Fatalf("operations = %#v", operations)
	}
}

func TestMergeAppendsRowsAndCountsSelect(t *testing.T) {
	field := pgproto3.FieldDescription{Name: []byte("tenant_id"), DataTypeOID: 20}
	merged, err := merge([]Result{
		{Fields: []pgproto3.FieldDescription{field}, Rows: [][][]byte{{[]byte("1")}}, CommandTag: "SELECT 1"},
		{Fields: []pgproto3.FieldDescription{field}, Rows: [][][]byte{{[]byte("2")}}, CommandTag: "SELECT 1"},
	})
	if err != nil {
		t.Fatalf("merge returned error: %v", err)
	}
	if merged.CommandTag != "SELECT 2" {
		t.Fatalf("command tag = %q, want SELECT 2", merged.CommandTag)
	}
	if len(merged.Rows) != 2 || string(merged.Rows[0][0]) != "1" || string(merged.Rows[1][0]) != "2" {
		t.Fatalf("merged rows = %#v, want rows from both shards", merged.Rows)
	}
}

func TestMergeSumsWriteCommandTags(t *testing.T) {
	merged, err := merge([]Result{
		{CommandTag: "UPDATE 3"},
		{CommandTag: "UPDATE 4"},
	})
	if err != nil {
		t.Fatalf("merge returned error: %v", err)
	}
	if merged.CommandTag != "UPDATE 7" {
		t.Fatalf("command tag = %q, want UPDATE 7", merged.CommandTag)
	}
}

func TestMergeSumsInsertCommandTags(t *testing.T) {
	merged, err := merge([]Result{
		{CommandTag: "INSERT 0 2"},
		{CommandTag: "INSERT 0 5"},
	})
	if err != nil {
		t.Fatalf("merge returned error: %v", err)
	}
	if merged.CommandTag != "INSERT 0 7" {
		t.Fatalf("command tag = %q, want INSERT 0 7", merged.CommandTag)
	}
}

func TestMergeRejectsDifferentResultShapes(t *testing.T) {
	_, err := merge([]Result{
		{Fields: []pgproto3.FieldDescription{{Name: []byte("tenant_id"), DataTypeOID: 20}}},
		{Fields: []pgproto3.FieldDescription{{Name: []byte("account_id"), DataTypeOID: 20}}},
	})
	if err == nil {
		t.Fatal("merge accepted different result shapes")
	}
}

func TestSessionWriteLockIsHeldUntilUnlock(t *testing.T) {
	writeGate := newWriteGate()
	session := &Session{writeGate: writeGate}
	session.LockWrites()
	session.LockWrites()

	acquired := make(chan struct{})
	go func() {
		<-writeGate
		defer func() { writeGate <- struct{}{} }()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("write lock was released before UnlockWrites")
	case <-time.After(10 * time.Millisecond):
	}

	session.UnlockWrites()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("write lock was not released")
	}
}

func TestSessionWriteLockWaitIsCanceled(t *testing.T) {
	writeGate := newWriteGate()
	first := &Session{writeGate: writeGate}
	first.LockWrites()

	ctx, cancel := context.WithCancel(context.Background())
	second := &Session{writeGate: writeGate}
	result := make(chan bool, 1)
	go func() { result <- second.LockWritesContext(ctx) }()
	cancel()

	select {
	case acquired := <-result:
		if acquired {
			t.Fatal("canceled session acquired the write gate")
		}
	case <-time.After(time.Second):
		t.Fatal("write gate wait did not observe cancellation")
	}
	first.UnlockWrites()
}
