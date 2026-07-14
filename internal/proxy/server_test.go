package proxy

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jruszo/hamstergres/internal/schema"
)

func TestCloneFrontendMessageOwnsCopyAndBindBuffers(t *testing.T) {
	copyData := &pgproto3.CopyData{Data: []byte("first frame")}
	clonedCopy := cloneFrontendMessage(copyData).(*pgproto3.CopyData)
	copy(copyData.Data, "overwritten!")
	if string(clonedCopy.Data) != "first frame" {
		t.Fatalf("cloned CopyData = %q, want owned first frame", clonedCopy.Data)
	}

	bind := &pgproto3.Bind{
		ParameterFormatCodes: []int16{1},
		Parameters:           [][]byte{[]byte("42")},
		ResultFormatCodes:    []int16{1},
	}
	clonedBind := cloneFrontendMessage(bind).(*pgproto3.Bind)
	bind.ParameterFormatCodes[0] = 0
	bind.Parameters[0][0] = '9'
	bind.ResultFormatCodes[0] = 0
	if !bytes.Equal(clonedBind.Parameters[0], []byte("42")) || clonedBind.ParameterFormatCodes[0] != 1 || clonedBind.ResultFormatCodes[0] != 1 {
		t.Fatalf("cloned Bind reused receive buffers: %#v", clonedBind)
	}
}

func TestMergedCopyFromTagUsesLogicalRowCounts(t *testing.T) {
	if tag, err := mergedCopyFromTag([]string{"COPY 2", "COPY 3"}, true, false, 5); err != nil || tag != "COPY 5" {
		t.Fatalf("sharded tag = %q, %v", tag, err)
	}
	if tag, err := mergedCopyFromTag([]string{"COPY 5", "COPY 5"}, false, true, -1); err != nil || tag != "COPY 5" {
		t.Fatalf("replicated tag = %q, %v", tag, err)
	}
	if _, err := mergedCopyFromTag([]string{"COPY 5", "COPY 4"}, false, true, -1); err == nil {
		t.Fatal("mismatched replicated COPY counts were accepted")
	}
	if _, err := mergedCopyFromTag([]string{"COPY 2", "COPY 2"}, true, false, 5); err == nil {
		t.Fatal("mismatched sharded COPY count was accepted")
	}
}

func TestRoutingParametersDecodeBinaryIntegers(t *testing.T) {
	bigint := make([]byte, 8)
	binary.BigEndian.PutUint64(bigint, 42)
	got := routingParameters(&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{bigint}}, []uint32{20})
	if len(got) != 1 || string(got[0]) != "42" {
		t.Fatalf("routing parameters = %q, want 42", got)
	}
}

func BenchmarkRoutingParametersTextBigint(b *testing.B) {
	message := &pgproto3.Bind{Parameters: [][]byte{[]byte("42")}}
	for b.Loop() {
		routingParameters(message, []uint32{20})
	}
}

func BenchmarkRoutingParametersBinaryBigint(b *testing.B) {
	bigint := make([]byte, 8)
	binary.BigEndian.PutUint64(bigint, 42)
	message := &pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{bigint}}
	for b.Loop() {
		routingParameters(message, []uint32{20})
	}
}

func TestCanonicalStatementNameDeduplicatesEquivalentParses(t *testing.T) {
	first := canonicalStatementName("SELECT $1::int8", []uint32{20})
	second := canonicalStatementName("SELECT $1::int8", []uint32{20})
	if first != second || first == canonicalStatementName("SELECT $1::int8", []uint32{23}) {
		t.Fatalf("canonical names were not stable and type-sensitive: %q %q", first, second)
	}
}

func TestNormalizeDDLMarksDropAsSchemaChange(t *testing.T) {
	got, err := normalizeDDL("DROP TABLE accounts")
	if err != nil {
		t.Fatal(err)
	}
	if got.sql != "DROP TABLE accounts" || !got.schema {
		t.Fatalf("normalized DROP = %#v, want unchanged schema DDL", got)
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

func TestRequiresFleetWriteOrderForSchemaStatements(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE sbtest1 (id int primary key)",
		"ALTER TABLE sbtest1 ADD COLUMN extra int",
		"COMMENT ON COLUMN sbtest1.id IS 'hamstergres.shard_key'",
		"DROP TABLE sbtest1",
		"TRUNCATE sbtest1",
	} {
		if !requiresFleetWriteOrder(sql, 2) {
			t.Fatalf("requiresFleetWriteOrder(%q) = false, want true", sql)
		}
		if requiresFleetWriteOrder(sql, 1) {
			t.Fatalf("requiresFleetWriteOrder(%q) = true for one target, want false", sql)
		}
	}
}

func TestRequiresFleetWriteOrderLeavesDMLReadsAndTransactionsUnlocked(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO sbtest1 (id, k) VALUES ($1, $2)",
		" /* sysbench */ update sbtest1 set k = k + 1 where id = $1",
		"-- generated\ndelete from sbtest1 where id = $1",
		"MERGE INTO accounts USING incoming ON accounts.id = incoming.id WHEN MATCHED THEN UPDATE SET balance = incoming.balance",
		"SELECT * FROM sbtest1 WHERE id = $1",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SHOW server_version",
	} {
		if requiresFleetWriteOrder(sql, 2) {
			t.Fatalf("requiresFleetWriteOrder(%q) = true, want false", sql)
		}
	}
}

func TestRecordWriteParticipantsExcludesReadOnlyBurrows(t *testing.T) {
	state := extendedState{transaction: true, writeParticipants: make(map[string]struct{})}
	recordWriteParticipants(&state, "SELECT * FROM accounts", []string{"burrow-01", "burrow-02"})
	if len(state.writeParticipants) != 0 || state.mutated {
		t.Fatalf("read-only participants = %#v, mutated = %t", state.writeParticipants, state.mutated)
	}

	recordWriteParticipants(&state, "UPDATE accounts SET balance = 1 WHERE id = 42", []string{"burrow-02"})
	if len(state.writeParticipants) != 1 {
		t.Fatalf("write participants = %#v, want only burrow-02", state.writeParticipants)
	}
	if _, ok := state.writeParticipants["burrow-02"]; !ok || !state.mutated {
		t.Fatalf("write participants = %#v, mutated = %t", state.writeParticipants, state.mutated)
	}
}

func TestPreparedCacheInvalidationCommands(t *testing.T) {
	for _, sql := range []string{"DEALLOCATE ALL", "discard all"} {
		if !invalidatesPreparedStatements(sql) {
			t.Fatalf("invalidatesPreparedStatements(%q) = false", sql)
		}
	}
	if invalidatesPreparedStatements("SELECT 1") {
		t.Fatal("ordinary query invalidated prepared statements")
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
	if !prepared.generated || prepared.sql != `INSERT INTO widgets (name, id) VALUES ($1, $2) RETURNING id` {
		t.Fatalf("prepared statement = %#v", prepared)
	}
	if len(prepared.message.ParameterOIDs) != 2 || prepared.message.ParameterOIDs[1] != 0 {
		t.Fatalf("parameter OIDs = %#v, want client parameter plus inferred generated parameter", prepared.message.ParameterOIDs)
	}
	if prepared.routing == nil || prepared.routing.MaxParameter() != 2 {
		t.Fatal("prepared statement did not retain its rewritten routing AST")
	}
}

func TestMaxParameterHandlesUnspecifiedParseOIDs(t *testing.T) {
	if got := maxParameter("INSERT INTO widgets (name, category) VALUES ($2, $7)"); got != 7 {
		t.Fatalf("maxParameter = %d, want 7", got)
	}
}
