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
	if (&Server{backends: manager, twoPhaseCommit: true}).flushPendingExtended(frontend, session, &state, true) {
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

func TestSessionStatePolicyReplaysSettingsAndPinsBackendState(t *testing.T) {
	for _, sql := range []string{
		"SET standard_conforming_strings = off",
		"RESET ALL",
		"DISCARD ALL",
		"SET ROLE application_user",
	} {
		if !requiresSessionBackend(sql) {
			t.Fatalf("requiresSessionBackend(%q) = false", sql)
		}
		if requiresSessionAffinity(sql) {
			t.Fatalf("requiresSessionAffinity(%q) = true for replayable state", sql)
		}
	}
	for _, sql := range []string{
		"LISTEN events",
		"UNLISTEN events",
		"PREPARE lookup AS SELECT 1",
		"SELECT 1; SET search_path = private, public",
		"CREATE TEMP TABLE session_items (id bigint)",
		"SELECT pg_advisory_lock(42)",
		"UPDATE pg_settings SET setting = '64MB' WHERE name = 'work_mem'",
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
	if requiresSessionBackend("UPDATE application_settings SET value = '64MB'") {
		t.Fatal("ordinary UPDATE unexpectedly required a session backend")
	}
	doPolicy := classifySessionState("DO $$ BEGIN PERFORM set_config('work_mem', '64MB', false); END $$")
	if !doPolicy.requiresBackend || !doPolicy.pin || !doPolicy.destroy {
		t.Fatalf("DO policy = %#v, want pinned session backend destruction", doPolicy)
	}
}

func TestSessionSettingReplayUsesLatestCommandOrder(t *testing.T) {
	state := extendedState{}
	for _, sql := range []string{
		"SET ROLE application_user",
		"SET SESSION AUTHORIZATION application_owner",
		"SET ROLE reporting_user",
		"SET search_path = private, public",
	} {
		policy := classifySessionState(sql)
		state.beginSessionState(policy)
		state.commitSessionState(policy)
	}
	want := []string{
		"SET SESSION AUTHORIZATION application_owner",
		"SET ROLE reporting_user",
		"SET search_path = private, public",
	}
	if got := state.sessionReplaySQL(); !reflect.DeepEqual(got, want) {
		t.Fatalf("session replay = %#v, want %#v", got, want)
	}
	if state.sessionAffinity {
		t.Fatal("replayable settings pinned the frontend to a Tunnel")
	}

	policy := classifySessionState("DISCARD ALL")
	state.beginSessionState(policy)
	state.commitSessionState(policy)
	if got := state.sessionReplaySQL(); len(got) != 0 {
		t.Fatalf("session replay after DISCARD ALL = %#v, want empty", got)
	}
}

func TestUnsafeSessionStateDestroysTunnelEvenAfterDiscard(t *testing.T) {
	state := extendedState{}
	load := classifySessionState("LOAD 'unsafe_extension'")
	state.beginSessionState(load)
	state.commitSessionState(load)
	if !state.sessionAffinity || !state.sessionDestroy {
		t.Fatalf("LOAD policy = affinity %t destroy %t, want both", state.sessionAffinity, state.sessionDestroy)
	}
	discard := classifySessionState("DISCARD ALL")
	state.beginSessionState(discard)
	state.commitSessionState(discard)
	if !state.sessionAffinity || !state.sessionDestroy {
		t.Fatal("DISCARD ALL incorrectly made process-local LOAD state reusable")
	}
}

func TestTransactionSessionSettingsStayPinnedInsteadOfBeingReplayed(t *testing.T) {
	state := extendedState{transaction: true}
	policy := state.classifySessionState("SET search_path = private, public")
	if !policy.pin || policy.replayName != "" {
		t.Fatalf("transactional SET policy = %#v, want pinned without replay", policy)
	}
	reset := state.classifySessionState("RESET ALL")
	if !reset.pin || reset.resetAll {
		t.Fatalf("transactional RESET policy = %#v, want pinned without replay reset", reset)
	}
}

func TestRollbackToSavepointKeepsTransactionPinned(t *testing.T) {
	state := extendedState{transaction: true, transactionFailed: true}
	updateTransactionState(&state, "ROLLBACK TO SAVEPOINT before_enum_check")
	if !state.transaction {
		t.Fatal("ROLLBACK TO SAVEPOINT ended the frontend transaction")
	}
	if state.transactionFailed {
		t.Fatal("ROLLBACK TO SAVEPOINT did not restore usable transaction state")
	}
	updateTransactionState(&state, "ROLLBACK")
	if state.transaction {
		t.Fatal("full ROLLBACK left the frontend transaction active")
	}
}

func TestTransactionStateHandlesBatchesAndChainedCompletion(t *testing.T) {
	state := extendedState{writeParticipants: make(map[string]struct{})}
	updateTransactionState(&state, "BEGIN; INSERT INTO accounts (id) VALUES (1)")
	if !state.transaction {
		t.Fatal("BEGIN batch did not leave the frontend transaction active")
	}

	for _, sql := range []string{"COMMIT AND CHAIN", "ROLLBACK AND CHAIN"} {
		state.transaction = true
		state.transactionFailed = true
		state.target = "burrow-01"
		state.mutated = true
		state.writeParticipants["burrow-01"] = struct{}{}
		updateTransactionState(&state, sql)
		if !state.transaction || state.transactionFailed || state.target != "" || state.mutated || len(state.writeParticipants) != 0 {
			t.Fatalf("%s state = %#v, want a clean chained transaction", sql, state)
		}
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
	if (&Server{backends: manager, twoPhaseCommit: true}).flushPendingExtended(frontend, session, &state, true) {
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
	if (&Server{backends: manager, twoPhaseCommit: true}).flushPendingExtended(frontend, session, &state, true) {
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

func TestNormalizeDDLMarksSelectIntoAsSchemaChange(t *testing.T) {
	got, err := normalizeDDL("SELECT 1 AS id INTO created_from_select")
	if err != nil {
		t.Fatal(err)
	}
	if !got.schema {
		t.Fatalf("normalized SELECT INTO = %#v, want schema DDL", got)
	}
}

func TestValidateTransactionalFleetDDLRejectsUnsafeCommands(t *testing.T) {
	for _, sql := range []string{
		"CREATE DATABASE application",
		"DROP DATABASE application",
		"CREATE TABLESPACE fast LOCATION '/srv/postgres'",
		"ALTER SYSTEM SET work_mem = '64MB'",
		"CREATE INDEX CONCURRENTLY accounts_balance_idx ON accounts (balance)",
		"DROP INDEX CONCURRENTLY accounts_balance_idx",
		"CREATE FUNCTION widget_count() RETURNS bigint LANGUAGE SQL AS $$ SELECT count(*) FROM widgets $$",
		"CREATE PROCEDURE refresh_widgets() LANGUAGE SQL AS $$ SELECT 1 $$",
		"BEGIN; CREATE TABLE widgets (id bigint); COMMIT",
	} {
		if err := validateTransactionalFleetDDL(sql); err == nil {
			t.Fatalf("validateTransactionalFleetDDL(%q) accepted unsafe fleet DDL", sql)
		}
	}
}

func TestValidateTransactionalFleetDDLAllowsTransactionalSchemaChanges(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE widgets (id bigint)",
		"ALTER TABLE widgets ADD COLUMN name text",
		"CREATE INDEX widgets_name_idx ON widgets (name)",
		"COMMENT ON TABLE widgets IS 'application data'",
		"TRUNCATE widgets",
		"DROP TABLE widgets",
	} {
		if err := validateTransactionalFleetDDL(sql); err != nil {
			t.Fatalf("validateTransactionalFleetDDL(%q): %v", sql, err)
		}
	}
}

func TestAtomicFleetDDLRequiresTwoPhaseCommit(t *testing.T) {
	server := &Server{}
	if err := server.validateAtomicFleetDDL("CREATE FUNCTION hidden_write() RETURNS void LANGUAGE SQL AS $$ SELECT 1 $$", 1); err == nil {
		t.Fatal("function creation was accepted for a single current Burrow")
	}
	if err := server.validateAtomicFleetDDL("CREATE TABLE widgets (id bigint)", 2); err == nil {
		t.Fatal("multi-Burrow DDL was accepted with two-phase commit disabled")
	}
	if err := server.validateAtomicFleetDDL("CREATE TABLE widgets (id bigint)", 1); err != nil {
		t.Fatalf("single-Burrow DDL unexpectedly required two-phase commit: %v", err)
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
		"SELECT 1 AS id INTO created_from_select",
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
		"CREATE TEMP TABLE session_items (id bigint)",
		"SELECT 1 AS id INTO TEMP TABLE session_items_from_select",
		"CREATE TABLESPACE regress_tblspace LOCATION ''",
		"DROP TABLESPACE regress_tblspace",
		"CREATE DATABASE isolated_database",
		"ALTER SYSTEM SET work_mem = '64MB'",
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

func TestExtendedStateTracksTemporaryRelationsAcrossDropStatements(t *testing.T) {
	state := extendedState{}
	create := "CREATE TEMP TABLE session_items (id bigint)"
	if !state.referencesTemporaryDDL(create) {
		t.Fatal("CREATE TEMP TABLE was not recognized as temporary DDL")
	}
	state.rememberTemporaryDDL(create)
	if !state.referencesTemporaryDDL("ALTER TABLE session_items ADD COLUMN note text") {
		t.Fatal("ALTER TABLE of a tracked temporary relation was not recognized as temporary DDL")
	}
	drop := "DROP TABLE session_items"
	if !state.referencesTemporaryDDL(drop) {
		t.Fatal("DROP TABLE of a tracked temporary relation was not recognized as temporary DDL")
	}
	state.rememberTemporaryDDL(drop)
	if state.referencesTemporaryDDL(drop) {
		t.Fatal("dropped temporary relation remained tracked")
	}
	if state.referencesTemporaryDDL("DROP TABLE permanent_items") {
		t.Fatal("permanent relation was recognized as temporary DDL")
	}
	createAs := "CREATE TEMP TABLE selected_items AS SELECT 1 AS id"
	state.rememberTemporaryDDL(createAs)
	if !state.referencesTemporaryDDL("CREATE INDEX selected_items_id_idx ON selected_items (id)") {
		t.Fatal("index on a temporary CREATE TABLE AS relation was not recognized as temporary DDL")
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

	state = extendedState{transaction: true, writeParticipants: make(map[string]struct{})}
	recordWriteParticipants(&state, "ALTER TABLE accounts ADD COLUMN note text", []string{"burrow-01", "burrow-02"})
	if len(state.writeParticipants) != 2 || !state.mutated {
		t.Fatalf("DDL participants = %#v, mutated = %t; want both Burrows", state.writeParticipants, state.mutated)
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
