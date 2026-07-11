package proxy

import "testing"

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
