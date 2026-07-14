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
	types := make(map[string][]string)
	appendShardKey(keys, types, "public", "events", "tenant", "bigint")
	appendShardKey(keys, types, "public", "events", "region", "text")
	want := []string{"tenant", "region"}
	for _, name := range []string{"events", "public.events"} {
		if !reflect.DeepEqual(keys[name], want) {
			t.Fatalf("%s shard key = %#v", name, keys[name])
		}
		if !reflect.DeepEqual(types[name], []string{"bigint", "text"}) {
			t.Fatalf("%s shard-key types = %#v", name, types[name])
		}
	}
}

func TestNewSessionDoesNotAcquireHundredsOfBurrows(t *testing.T) {
	manager := &Manager{fleetWriteGate: newFleetWriteGate(), prepared: make(map[string]map[string]struct{})}
	for index := 0; index < 500; index++ {
		manager.shards = append(manager.shards, &shard{name: fmt.Sprintf("burrow-%03d", index+1)})
	}
	session, err := manager.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if names := session.ConnectedNames(); len(names) != 0 {
		t.Fatalf("new lazy session connected to %d Burrows: %#v", len(names), names)
	}
}

func TestInvalidatePreparedStatementsOnlyForSelectedBurrows(t *testing.T) {
	manager := &Manager{prepared: map[string]map[string]struct{}{
		preparedConnectionKey("burrow-01", 1): {"statement": {}},
		preparedConnectionKey("burrow-02", 2): {"statement": {}},
	}}
	manager.InvalidatePreparedStatements([]string{"burrow-01"})
	if _, ok := manager.prepared[preparedConnectionKey("burrow-01", 1)]; ok {
		t.Fatal("selected Burrow prepared cache was not invalidated")
	}
	if _, ok := manager.prepared[preparedConnectionKey("burrow-02", 2)]; !ok {
		t.Fatal("unselected Burrow prepared cache was invalidated")
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

func TestSessionFleetWriteLockIsHeldUntilUnlock(t *testing.T) {
	writeGate := newFleetWriteGate()
	session := &Session{fleetWriteGate: writeGate}
	session.LockFleetWrites()
	session.LockFleetWrites()

	acquired := make(chan struct{})
	go func() {
		<-writeGate
		defer func() { writeGate <- struct{}{} }()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("fleet-write lock was released before UnlockFleetWrites")
	case <-time.After(10 * time.Millisecond):
	}

	session.UnlockFleetWrites()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("write lock was not released")
	}
}

func TestSessionFleetWriteLockWaitIsCanceled(t *testing.T) {
	writeGate := newFleetWriteGate()
	first := &Session{fleetWriteGate: writeGate}
	first.LockFleetWrites()

	ctx, cancel := context.WithCancel(context.Background())
	second := &Session{fleetWriteGate: writeGate}
	result := make(chan bool, 1)
	go func() { result <- second.LockFleetWritesContext(ctx) }()
	cancel()

	select {
	case acquired := <-result:
		if acquired {
			t.Fatal("canceled session acquired the write gate")
		}
	case <-time.After(time.Second):
		t.Fatal("write gate wait did not observe cancellation")
	}
	first.UnlockFleetWrites()
}
