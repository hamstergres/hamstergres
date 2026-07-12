package proxy

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jruszo/hamstergres/internal/schema"
)

func TestRoutingParametersDecodeBinaryIntegers(t *testing.T) {
	bigint := make([]byte, 8)
	binary.BigEndian.PutUint64(bigint, 42)
	got := routingParameters(&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{bigint}}, []uint32{20})
	if len(got) != 1 || string(got[0]) != "42" {
		t.Fatalf("routing parameters = %q, want 42", got)
	}
}

func TestCanonicalStatementNameDeduplicatesEquivalentParses(t *testing.T) {
	first := canonicalStatementName("SELECT $1::int8", []uint32{20})
	second := canonicalStatementName("SELECT $1::int8", []uint32{20})
	if first != second || first == canonicalStatementName("SELECT $1::int8", []uint32{23}) {
		t.Fatalf("canonical names were not stable and type-sensitive: %q %q", first, second)
	}
}

func TestMergedCommandTagCountsSelectRows(t *testing.T) {
	got := mergedCommandTag([]string{"SELECT 1", "SELECT 1"}, 2)
	if got != "SELECT 2" {
		t.Fatalf("mergedCommandTag = %q, want SELECT 2", got)
	}
}

func TestMergedCommandTagSumsWriteRows(t *testing.T) {
	got := mergedCommandTag([]string{"UPDATE 3", "UPDATE 4"}, 0)
	if got != "UPDATE 7" {
		t.Fatalf("mergedCommandTag = %q, want UPDATE 7", got)
	}
}

func TestMergedCommandTagSumsInsertRows(t *testing.T) {
	got := mergedCommandTag([]string{"INSERT 0 2", "INSERT 0 5"}, 0)
	if got != "INSERT 0 7" {
		t.Fatalf("mergedCommandTag = %q, want INSERT 0 7", got)
	}
}

func TestMergedCommandTagKeepsUniformCommands(t *testing.T) {
	got := mergedCommandTag([]string{"CREATE TABLE", "CREATE TABLE"}, 0)
	if got != "CREATE TABLE" {
		t.Fatalf("mergedCommandTag = %q, want CREATE TABLE", got)
	}
}

func TestRequiresGlobalWriteOrderForMutatingStatements(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO sbtest1 (id, k) VALUES ($1, $2)",
		" /* sysbench */ update sbtest1 set k = k + 1 where id = $1",
		"-- generated\ndelete from sbtest1 where id = $1",
		"MERGE INTO accounts USING incoming ON accounts.id = incoming.id WHEN MATCHED THEN UPDATE SET balance = incoming.balance",
		"CREATE TABLE sbtest1 (id int primary key)",
		"ALTER TABLE sbtest1 ADD COLUMN extra int",
		"DROP TABLE sbtest1",
		"TRUNCATE sbtest1",
	} {
		if !requiresGlobalWriteOrder(sql) {
			t.Fatalf("requiresGlobalWriteOrder(%q) = false, want true", sql)
		}
	}
}

func TestRequiresGlobalWriteOrderLeavesReadsAndTransactionsUnlocked(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM sbtest1 WHERE id = $1",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SHOW server_version",
	} {
		if requiresGlobalWriteOrder(sql) {
			t.Fatalf("requiresGlobalWriteOrder(%q) = true, want false", sql)
		}
	}
}

func TestTransactionStatePinsAndClearsTarget(t *testing.T) {
	state := extendedState{}
	updateTransactionState(&state, "BEGIN")
	if !state.transaction || state.txStatus() != 'T' {
		t.Fatalf("BEGIN state = %#v, want active transaction", state)
	}
	state.target = "burrow-01"
	updateTransactionState(&state, "COMMIT")
	if state.transaction || state.target != "" || state.txStatus() != 'I' {
		t.Fatalf("COMMIT state = %#v, want idle and unpinned", state)
	}
}

func TestPrepareStatementAddsHiddenGeneratedParameter(t *testing.T) {
	registry := schema.NewWithGenerated(map[string][]string{"widgets": {"id"}}, map[string]schema.GeneratedPrimary{"widgets": {Column: "id", Kind: "identity"}})
	prepared, err := prepareStatement(&pgproto3.Parse{Name: "insert_widget", Query: "INSERT INTO widgets (name) VALUES ($1) RETURNING id", ParameterOIDs: []uint32{25}}, registry)
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.generated || prepared.sql != `INSERT INTO widgets (name, "id") VALUES ($1, $2) RETURNING id` {
		t.Fatalf("prepared statement = %#v", prepared)
	}
	if len(prepared.message.ParameterOIDs) != 2 || prepared.message.ParameterOIDs[1] != 0 {
		t.Fatalf("parameter OIDs = %#v, want client parameter plus inferred generated parameter", prepared.message.ParameterOIDs)
	}
}

func TestMaxParameterHandlesUnspecifiedParseOIDs(t *testing.T) {
	if got := maxParameter("INSERT INTO widgets (name, category) VALUES ($2, $7)"); got != 7 {
		t.Fatalf("maxParameter = %d, want 7", got)
	}
}
