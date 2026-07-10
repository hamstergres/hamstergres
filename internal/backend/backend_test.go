package backend

import (
	"testing"

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

func TestMergeRejectsDifferentResultShapes(t *testing.T) {
	_, err := merge([]Result{
		{Fields: []pgproto3.FieldDescription{{Name: []byte("tenant_id"), DataTypeOID: 20}}},
		{Fields: []pgproto3.FieldDescription{{Name: []byte("account_id"), DataTypeOID: 20}}},
	})
	if err == nil {
		t.Fatal("merge accepted different result shapes")
	}
}
