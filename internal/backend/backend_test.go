package backend

import (
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

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
	var writeMu sync.Mutex
	session := &Session{writeMu: &writeMu}
	session.LockWrites()
	session.LockWrites()

	acquired := make(chan struct{})
	go func() {
		writeMu.Lock()
		defer writeMu.Unlock()
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
