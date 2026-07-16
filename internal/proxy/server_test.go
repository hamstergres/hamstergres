// SPDX-License-Identifier: AGPL-3.0-only

package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jruszo/hamstergres/internal/backend"
	"github.com/jruszo/hamstergres/internal/config"
	"github.com/jruszo/hamstergres/internal/schema"
)

func TestStartupRuntimeParametersPreservePostgreSQLClientSettings(t *testing.T) {
	startup := &pgproto3.StartupMessage{Parameters: map[string]string{
		"user":             "hamster",
		"database":         "regression",
		"datestyle":        "Postgres, MDY",
		"timezone":         "America/Los_Angeles",
		"application_name": "direct_name",
		"options":          "-c intervalstyle=postgres_verbose --search_path=public -ctimezone=UTC -capplication_name=options_name",
		"unsupported":      "ignored",
	}}
	got := startupRuntimeParameters(startup)
	want := map[string]string{
		"DateStyle":        "Postgres, MDY",
		"TimeZone":         "America/Los_Angeles",
		"IntervalStyle":    "postgres_verbose",
		"search_path":      "public",
		"application_name": "direct_name",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("startup runtime parameters = %#v, want %#v", got, want)
	}
}

func TestPostgresErrorResponsePreservesStructuredFields(t *testing.T) {
	postgresError := &pgconn.PgError{
		Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: "42883",
		Message: "function length(integer) does not exist", Detail: "detail", Hint: "hint",
		Position: 15, InternalPosition: 4, InternalQuery: "SELECT 1", Where: "context",
		SchemaName: "public", TableName: "items", ColumnName: "value", DataTypeName: "integer",
		ConstraintName: "items_value_check", File: "parse_func.c", Line: 629, Routine: "ParseFuncOrColumn",
	}
	response := postgresErrorResponse(postgresError)
	if response == nil || response.Code != postgresError.Code || response.Message != postgresError.Message ||
		response.Detail != postgresError.Detail || response.Hint != postgresError.Hint || response.Position != postgresError.Position ||
		response.InternalPosition != postgresError.InternalPosition || response.InternalQuery != postgresError.InternalQuery ||
		response.Where != postgresError.Where || response.SchemaName != postgresError.SchemaName || response.TableName != postgresError.TableName ||
		response.ColumnName != postgresError.ColumnName || response.DataTypeName != postgresError.DataTypeName ||
		response.ConstraintName != postgresError.ConstraintName || response.File != postgresError.File || response.Line != postgresError.Line ||
		response.Routine != postgresError.Routine || response.Severity != postgresError.Severity ||
		response.SeverityUnlocalized != postgresError.SeverityUnlocalized {
		t.Fatalf("structured PostgreSQL error changed: %#v", response)
	}
}

func TestSendStartupReportsEffectiveRuntimeParameters(t *testing.T) {
	var wire bytes.Buffer
	frontend := pgproto3.NewBackend(bytes.NewReader(nil), &wire)
	if err := (&Server{}).sendStartup(frontend, map[string]string{"standard_conforming_strings": "off"}); err != nil {
		t.Fatal(err)
	}

	client := pgproto3.NewFrontend(bytes.NewReader(wire.Bytes()), io.Discard)
	statuses := make(map[string]string)
	statusCounts := make(map[string]int)
	for {
		message, err := client.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch message := message.(type) {
		case *pgproto3.ParameterStatus:
			statusCounts[message.Name]++
			if statusCounts[message.Name] > 1 {
				t.Fatalf("ParameterStatus %s emitted %d times, want once", message.Name, statusCounts[message.Name])
			}
			statuses[message.Name] = message.Value
		case *pgproto3.ReadyForQuery:
			want := map[string]string{
				"DateStyle":                   "ISO, MDY",
				"IntervalStyle":               "postgres",
				"TimeZone":                    "UTC",
				"standard_conforming_strings": "off",
			}
			for name, value := range want {
				if statuses[name] != value {
					t.Fatalf("ParameterStatus %s = %q, want %q", name, statuses[name], value)
				}
			}
			return
		}
	}
}

func TestStartupSessionAffinityIgnoresApplicationName(t *testing.T) {
	if requiresStartupSessionAffinity(map[string]string{"application_name": "pg_regress"}) {
		t.Fatal("application_name unexpectedly requires session affinity")
	}
	if !requiresStartupSessionAffinity(map[string]string{"DateStyle": "SQL, DMY"}) {
		t.Fatal("formatting-sensitive DateStyle did not require session affinity")
	}
}

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

func TestPendingExtendedFailureAtSyncEmitsReadyForQuery(t *testing.T) {
	manager := &backend.Manager{}
	session, err := manager.NewSession(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	state := extendedState{pending: &pendingExtended{
		targets:   []string{"burrow-missing"},
		bind:      &pgproto3.Bind{},
		statement: statementState{},
	}}
	var wire bytes.Buffer
	frontend := pgproto3.NewBackend(bytes.NewReader(nil), &wire)
	if (&Server{}).flushPendingExtended(frontend, session, &state, true) {
		t.Fatal("failed pending request reported success")
	}
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	client := pgproto3.NewFrontend(bytes.NewReader(wire.Bytes()), io.Discard)
	message, err := client.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := message.(*pgproto3.ErrorResponse); !ok || response.Code != "08006" {
		t.Fatalf("first response = %#v, want connection error", message)
	}
	message, err = client.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := message.(*pgproto3.ReadyForQuery); !ok {
		t.Fatalf("second response = %#v, want ReadyForQuery", message)
	}
	if state.failed {
		t.Fatal("Sync recovery left the extended protocol in failed state")
	}
}

func TestRequiresSessionAffinityForSessionSettings(t *testing.T) {
	for _, sql := range []string{
		"SET standard_conforming_strings = off",
		"RESET ALL",
		"DISCARD ALL",
		"LISTEN events",
		"UNLISTEN events",
		"PREPARE lookup AS SELECT 1",
		"SELECT 1; SET search_path = private, public",
		"SELECT 1; SET ROLE application_user",
	} {
		if !requiresSessionAffinity(sql) {
			t.Fatalf("requiresSessionAffinity(%q) = false", sql)
		}
	}
	if requiresSessionAffinity("SELECT 1") {
		t.Fatal("ordinary query unexpectedly requires session affinity")
	}
	if requiresSessionAffinity("SELECT 'SET ROLE application_user'") {
		t.Fatal("affinity command text inside a literal required session affinity")
	}
}

func TestPendingFleetWriteFailureReleasesGateOutsideTransaction(t *testing.T) {
	manager, err := backend.New(t.Context(), emptyBackendConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	session, err := manager.NewSession(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	state := extendedState{pending: fleetWritePending(), statements: make(map[string]statementState), portals: make(map[string]portalState)}
	frontend := pgproto3.NewBackend(bytes.NewReader(nil), io.Discard)
	if (&Server{}).flushPendingExtended(frontend, session, &state, true) {
		t.Fatal("failed pending fleet write reported success")
	}

	next, err := manager.NewSession(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if !next.LockFleetWritesContext(ctx) {
		t.Fatal("fleet-write gate remained locked after non-transaction failure")
	}
	next.UnlockFleetWrites()
}

func TestPendingFleetWriteFailurePreservesGateDuringTransaction(t *testing.T) {
	manager, err := backend.New(t.Context(), emptyBackendConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	session, err := manager.NewSession(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	state := extendedState{transaction: true, pending: fleetWritePending(), statements: make(map[string]statementState), portals: make(map[string]portalState)}
	frontend := pgproto3.NewBackend(bytes.NewReader(nil), io.Discard)
	if (&Server{}).flushPendingExtended(frontend, session, &state, true) {
		t.Fatal("failed pending fleet write reported success")
	}

	next, err := manager.NewSession(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if next.LockFleetWritesContext(ctx) {
		next.UnlockFleetWrites()
		t.Fatal("transactional fleet-write gate was released after pending failure")
	}
	session.UnlockFleetWrites()
}

func emptyBackendConfig() config.Config {
	var cfg config.Config
	cfg.Sharding.Unsharded.Mode = config.UnshardedReplicated
	return cfg
}

func fleetWritePending() *pendingExtended {
	return &pendingExtended{
		targets: []string{"burrow-missing-01", "burrow-missing-02"},
		bind:    &pgproto3.Bind{},
		execute: &pgproto3.Execute{},
		portal:  portalState{sql: "CREATE TABLE widgets (id bigint)"},
	}
}

func TestCopyStartDrainsReadyForQueryAfterError(t *testing.T) {
	if isCopyStarted(&pgproto3.ErrorResponse{}) {
		t.Fatal("COPY startup stopped before draining ReadyForQuery")
	}
	if !isCopyStarted(&pgproto3.ReadyForQuery{}) {
		t.Fatal("COPY startup did not stop at ReadyForQuery")
	}
}

func TestContainsCopyStatementInMultiStatementQuery(t *testing.T) {
	if !containsCopyStatement("SELECT 0; COPY test3 FROM STDIN; SELECT 1") {
		t.Fatal("multi-statement COPY was not detected")
	}
	if containsCopyStatement("SELECT 'COPY test3 FROM STDIN'") {
		t.Fatal("COPY text inside a literal was treated as a statement")
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

func TestBalancedBurrowUsesStableRoundRobin(t *testing.T) {
	server := &Server{}
	burrows := []string{"burrow-03", "burrow-01", "burrow-02"}
	want := []string{"burrow-01", "burrow-02", "burrow-03", "burrow-01"}
	for index, expected := range want {
		if got := server.balancedBurrow(burrows); got != expected {
			t.Fatalf("balanced selection %d = %q, want %q", index, got, expected)
		}
	}
	if got := server.balancedBurrow(nil); got != "" {
		t.Fatalf("empty balanced selection = %q, want empty", got)
	}
}

func TestParserFallbackUsesOneDeterministicBurrow(t *testing.T) {
	server := &Server{backends: &backend.Manager{}}
	if got := server.parserFallbackBurrow([]string{"burrow-02", "burrow-01"}); got != "burrow-01" {
		t.Fatalf("parser fallback = %q, want burrow-01", got)
	}
}

func TestRandomBurrowReturnsAvailableBurrow(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	for range 20 {
		got := randomBurrow(burrows)
		if got != burrows[0] && got != burrows[1] {
			t.Fatalf("randomBurrow = %q, want an available Burrow", got)
		}
	}
	if got := randomBurrow(nil); got != "" {
		t.Fatalf("empty random selection = %q, want empty", got)
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
