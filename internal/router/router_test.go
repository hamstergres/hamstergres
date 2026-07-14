package router

import (
	"testing"

	"github.com/jruszo/hamstergres/internal/schema"
)

func TestTargetForSchemaUsesCompoundShardKeyWithText(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	registry := schema.New(map[string][]string{"accounts": {"region", "tenant_id"}})
	target, ok := TargetForSchema("SELECT * FROM accounts WHERE tenant_id = 42 AND region = 'eu-west'", nil, registry, burrows)
	if !ok || target != BurrowForKey("eu-west\x0042", burrows) {
		t.Fatalf("TargetForSchema = %q, %t; want compound shard-key target", target, ok)
	}
	if _, ok := TargetForSchema("SELECT * FROM accounts WHERE tenant_id = 42", nil, registry, burrows); ok {
		t.Fatal("partial compound shard key was routed")
	}
	target, ok = TargetForSchema("INSERT INTO accounts (tenant_id, region) VALUES ($1, $2)", [][]byte{[]byte("42"), []byte("eu-west")}, registry, burrows)
	if !ok || target != BurrowForKey("eu-west\x0042", burrows) {
		t.Fatalf("bound compound TargetForSchema = %q, %t", target, ok)
	}
}

func TestTableForSQL(t *testing.T) {
	if table, ok := TableForSQL("UPDATE public.accounts SET balance = 1"); !ok || table != "public.accounts" {
		t.Fatalf("TableForSQL = %q, %v", table, ok)
	}
}

func TestBurrowForKeyUsesOneIndexedModuloPlacement(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	for key := 0; key < 1000; key++ {
		text := string(rune('0' + key%10))
		vshard := int(HashKey(text) % VirtualShards)
		got := BurrowForKey(text, burrows)
		if vshard%2 == 0 && got != "burrow-02" {
			t.Fatalf("even vshard %d routed to %q, want burrow-02", vshard, got)
		}
		if vshard%2 == 1 && got != "burrow-01" {
			t.Fatalf("odd vshard %d routed to %q, want burrow-01", vshard, got)
		}
	}
}

func TestRewriteGeneratedInsertAddsOmittedPrimaryKey(t *testing.T) {
	registry := schema.NewWithGenerated(map[string][]string{"widgets": {"id"}}, map[string]schema.GeneratedPrimary{"widgets": {Column: "id", Kind: "identity"}})
	result, ok := RewriteGeneratedInsert("INSERT INTO widgets (name) VALUES ($1) RETURNING id", registry, "$2")
	if !ok || result.SQL != `INSERT INTO widgets (name, id) VALUES ($1, $2) RETURNING id` {
		t.Fatalf("RewriteGeneratedInsert = %#v, %t", result, ok)
	}
	target, routed := TargetForSchema(result.SQL, [][]byte{[]byte("wheel"), []byte("42")}, registry, []string{"burrow-01", "burrow-02"})
	if !routed || target != BurrowForKey("42", []string{"burrow-01", "burrow-02"}) {
		t.Fatal("rewritten INSERT was not routed by generated id")
	}
}

func TestRewriteGeneratedInsertReplacesDefaultButPreservesExplicitKey(t *testing.T) {
	registry := schema.NewWithGenerated(map[string][]string{"widgets": {"id"}}, map[string]schema.GeneratedPrimary{"widgets": {Column: "id", Kind: "sequence"}})
	result, ok := RewriteGeneratedInsert("INSERT INTO widgets (id, name) VALUES (DEFAULT, 'x')", registry, "7")
	if !ok || result.SQL != "INSERT INTO widgets (id, name) VALUES (7, 'x')" {
		t.Fatalf("got %q, %t", result.SQL, ok)
	}
	if _, ok := RewriteGeneratedInsert("INSERT INTO widgets (id, name) VALUES (9, 'x')", registry, "7"); ok {
		t.Fatal("explicit key was rewritten")
	}
}

func TestRewriteGeneratedInsertRejectsMultipleRows(t *testing.T) {
	registry := schema.NewWithGenerated(map[string][]string{"widgets": {"id"}}, map[string]schema.GeneratedPrimary{"widgets": {Column: "id", Kind: "identity"}})
	result, ok := RewriteGeneratedInsert("INSERT INTO widgets (name) VALUES ('first'), ('second')", registry, "42")
	if ok {
		t.Fatalf("RewriteGeneratedInsert = %#v, true; want unchanged multi-row insert", result)
	}
}

func TestAnalyzeHandlesAliasesCastsCommentsAndReorderedCompoundKeys(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	registry := schema.New(map[string][]string{"public.accounts": {"region", "tenant_id"}})
	query := `/* route structurally */ SELECT * FROM public.accounts AS a
		WHERE (a.tenant_id = ($2)::bigint) AND ('eu-west'::text = a.region)`
	plan, err := Analyze(query, [][]byte{[]byte("ignored"), []byte("42")}, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Routed || plan.Table != "public.accounts" || plan.Target != BurrowForKey("eu-west\x0042", burrows) {
		t.Fatalf("Analyze = %#v, want one compound-key Burrow", plan)
	}
}

func TestAnalyzeRoutesTupleEquality(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	registry := schema.New(map[string][]string{"accounts": {"tenant_id", "region"}})
	plan, err := Analyze("SELECT * FROM accounts WHERE (tenant_id, region) = (42, 'eu-west')", nil, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Routed || plan.Target != BurrowForKey("42\x00eu-west", burrows) {
		t.Fatalf("Analyze = %#v, want tuple-equality target", plan)
	}
}

func TestAnalyzeResolvesQuotedQualifiedIdentifiers(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	registry := schema.New(map[string][]string{
		"public.Accounts": {"Tenant_ID"},
		"public.accounts": {"tenant_id"},
	})
	plan, err := Analyze(`SELECT * FROM "public"."Accounts" AS "Account" WHERE "Account"."Tenant_ID" = 42`, nil, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Routed || plan.Table != "public.Accounts" || plan.Target != BurrowForKey("42", burrows) {
		t.Fatalf("Analyze = %#v, want quoted qualified target", plan)
	}
	wrongCase, err := Analyze(`SELECT * FROM "public"."Accounts" WHERE tenant_id = 42`, nil, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	if wrongCase.Routed {
		t.Fatalf("wrong-case quoted column routed as %#v", wrongCase)
	}
}

func TestAnalyzeCanonicalizesTypedShardKeyValues(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	registry := schema.NewWithTypes(
		map[string][]string{"accounts": {"tenant_id"}},
		map[string][]string{"accounts": {"bigint"}},
	)
	queries := []struct {
		sql        string
		parameters [][]byte
	}{
		{sql: "SELECT * FROM accounts WHERE tenant_id = 1"},
		{sql: "SELECT * FROM accounts WHERE tenant_id = '01'::bigint"},
		{sql: "SELECT * FROM accounts WHERE tenant_id = $1", parameters: [][]byte{[]byte("0001")}},
	}
	for _, test := range queries {
		plan, err := Analyze(test.sql, test.parameters, registry, burrows)
		if err != nil {
			t.Fatal(err)
		}
		if !plan.Routed || plan.Target != BurrowForKey("1", burrows) {
			t.Fatalf("Analyze(%q) = %#v, want canonical integer key", test.sql, plan)
		}
	}
}

func TestAnalyzeFailsClosedForUnsupportedShardKeyType(t *testing.T) {
	registry := schema.NewWithTypes(
		map[string][]string{"events": {"occurred_at"}},
		map[string][]string{"events": {"timestamp with time zone"}},
	)
	plan, err := Analyze("UPDATE events SET payload = 'x' WHERE occurred_at = '2026-07-13 12:00:00+02'", nil, registry, []string{"burrow-01", "burrow-02"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Routed {
		t.Fatalf("unsupported typed key routed as %#v", plan)
	}
}

func TestAnalyzeFailsClosedForAmbiguousShapes(t *testing.T) {
	registry := schema.New(map[string][]string{"accounts": {"tenant_id"}})
	burrows := []string{"burrow-01", "burrow-02"}
	tests := []string{
		"SELECT * FROM accounts WHERE tenant_id = 1 OR tenant_id = 2",
		"SELECT * FROM accounts WHERE tenant_id BETWEEN 1 AND 2",
		"SELECT * FROM accounts WHERE tenant_id = (SELECT 1)",
		"SELECT (SELECT count(*) FROM other) FROM accounts WHERE tenant_id = 1",
		"SELECT * FROM accounts a JOIN accounts b ON a.tenant_id = b.tenant_id WHERE a.tenant_id = 1",
		"WITH selected AS (SELECT * FROM accounts WHERE tenant_id = 1) SELECT * FROM selected",
		"WITH removed AS (DELETE FROM accounts WHERE tenant_id = 1 RETURNING *) SELECT * FROM removed",
		"WITH source AS (SELECT 1) INSERT INTO accounts (tenant_id) VALUES (1)",
		"UPDATE accounts SET tenant_id = 2 FROM other WHERE accounts.tenant_id = 1",
		"UPDATE accounts SET tenant_id = 2 WHERE tenant_id = 1",
		"DELETE FROM accounts USING other WHERE accounts.tenant_id = 1",
		"INSERT INTO accounts (tenant_id) VALUES (1), (2)",
		"INSERT INTO accounts (tenant_id) SELECT tenant_id FROM other",
		"INSERT INTO accounts (tenant_id) VALUES (1) ON CONFLICT DO NOTHING",
	}
	for _, query := range tests {
		plan, err := Analyze(query, nil, registry, burrows)
		if err != nil {
			t.Fatalf("Analyze(%q): %v", query, err)
		}
		if plan.Routed {
			t.Errorf("Analyze(%q) = %#v, want fail-closed unrouted plan", query, plan)
		}
	}
}

func TestAnalyzeSimpleAndExtendedProduceEquivalentPlans(t *testing.T) {
	registry := schema.New(map[string][]string{"accounts": {"tenant_id"}})
	burrows := []string{"burrow-01", "burrow-02"}
	simple, err := Analyze("UPDATE accounts a SET balance = balance + 1 WHERE a.tenant_id = 42", nil, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	extended, err := Analyze("UPDATE accounts a SET balance = balance + 1 WHERE a.tenant_id = $1", [][]byte{[]byte("42")}, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	if !simple.Routed || !extended.Routed || simple.Target != extended.Target || simple.Table != extended.Table || simple.Write != extended.Write {
		t.Fatalf("simple = %#v, extended = %#v", simple, extended)
	}
}

func TestAnalyzeCopyRecordsRelationAndDirection(t *testing.T) {
	from, err := Analyze("COPY public.accounts (id, value) FROM STDIN", nil, schema.Registry{}, []string{"burrow-01", "burrow-02"})
	if err != nil {
		t.Fatal(err)
	}
	if from.Table != "public.accounts" || !from.Write || from.Routed {
		t.Fatalf("COPY FROM plan = %#v", from)
	}

	to, err := Analyze("COPY public.accounts TO STDOUT", nil, schema.Registry{}, []string{"burrow-01", "burrow-02"})
	if err != nil {
		t.Fatal(err)
	}
	if to.Table != "public.accounts" || to.Write || to.Routed {
		t.Fatalf("COPY TO plan = %#v", to)
	}
}

func TestPreparedAnalyzeReusesSyntaxWithCurrentBindingsAndRegistry(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	prepared, err := Prepare("SELECT payload FROM accounts WHERE tenant_id = $1")
	if err != nil {
		t.Fatal(err)
	}
	if prepared.MaxParameter() != 1 {
		t.Fatalf("MaxParameter = %d, want 1", prepared.MaxParameter())
	}

	unsharded := prepared.Analyze([][]byte{[]byte("41")}, schema.Registry{}, burrows)
	if unsharded.Routed || unsharded.Sharded || unsharded.Table != "accounts" {
		t.Fatalf("unsharded prepared plan = %#v", unsharded)
	}

	registry := schema.New(map[string][]string{"accounts": {"tenant_id"}})
	for _, value := range []string{"41", "42"} {
		plan := prepared.Analyze([][]byte{[]byte(value)}, registry, burrows)
		if !plan.Routed || plan.Target != BurrowForKey(value, burrows) {
			t.Fatalf("prepared Analyze(%q) = %#v", value, plan)
		}
	}
}

func TestTableSpecificTopologyCanPlaceSameKeyOnDifferentBurrows(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	ownersOne := make([]string, VirtualShards)
	ownersTwo := make([]string, VirtualShards)
	for vshard := 0; vshard < VirtualShards; vshard++ {
		ownersOne[vshard] = "burrow-01"
		ownersTwo[vshard] = "burrow-02"
	}
	registry := schema.NewWithTypes(
		map[string][]string{"public.accounts": {"id"}, "public.orders": {"id"}},
		map[string][]string{"public.accounts": {"bigint"}, "public.orders": {"bigint"}},
	).WithAllTables([]string{"public.accounts", "public.orders"}).WithTableVShards(map[string][]string{
		"public.accounts": ownersOne,
		"public.orders":   ownersTwo,
	})
	accounts, err := Analyze("SELECT * FROM public.accounts WHERE id = 42", nil, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	orders, err := Analyze("SELECT * FROM public.orders WHERE id = 42", nil, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	if accounts.Target != "burrow-01" || orders.Target != "burrow-02" || !accounts.Routed || !orders.Routed {
		t.Fatalf("table placements = accounts %#v, orders %#v", accounts, orders)
	}
}

func TestAnalyzeRejectsParserErrorsAndMultiStatementWrites(t *testing.T) {
	registry := schema.New(map[string][]string{"accounts": {"tenant_id"}})
	if _, err := Analyze("SELECT * FROM accounts WHERE (", nil, registry, []string{"burrow-01"}); err == nil {
		t.Fatal("parser error was accepted")
	}
	plan, err := Analyze("SELECT 1; DELETE FROM accounts WHERE tenant_id = 1", nil, registry, []string{"burrow-01"})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Write || plan.Routed {
		t.Fatalf("multi-statement plan = %#v, want unrouted write", plan)
	}
}

func TestRewriteGeneratedInsertUsesASTForCommaExpressions(t *testing.T) {
	registry := schema.NewWithGenerated(map[string][]string{"widgets": {"id"}}, map[string]schema.GeneratedPrimary{"widgets": {Column: "id", Kind: "identity"}})
	result, ok := RewriteGeneratedInsert("INSERT INTO widgets (name) VALUES (concat('left', ',', 'right')) RETURNING id", registry, "42")
	if !ok {
		t.Fatal("AST generated-key rewrite rejected a value expression containing commas")
	}
	plan, err := Analyze(result.SQL, nil, registry, []string{"burrow-01", "burrow-02"})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Routed || plan.Target != BurrowForKey("42", []string{"burrow-01", "burrow-02"}) {
		t.Fatalf("rewritten plan = %#v", plan)
	}
}

func TestMaxParameterIgnoresCommentsAndLiterals(t *testing.T) {
	maximum, err := MaxParameter("SELECT '$99', $2 /* $88 */ FROM accounts WHERE tenant_id = $7 -- $100")
	if err != nil {
		t.Fatal(err)
	}
	if maximum != 7 {
		t.Fatalf("MaxParameter = %d, want 7", maximum)
	}
}

func BenchmarkTargetForSchemaBoundPrimaryKey(b *testing.B) {
	registry := schema.New(map[string][]string{"sbtest1": {"id"}})
	burrows := []string{"burrow-01", "burrow-02"}
	parameters := [][]byte{[]byte("42")}
	for b.Loop() {
		TargetForSchema("SELECT c FROM sbtest1 WHERE id=$1", parameters, registry, burrows)
	}
}

func BenchmarkPreparedAnalyzeBoundPrimaryKey(b *testing.B) {
	owners := make([]string, VirtualShards)
	for vshard := range owners {
		owners[vshard] = []string{"burrow-01", "burrow-02"}[vshard%2]
	}
	registry := schema.New(map[string][]string{"sbtest1": {"id"}}).WithVShards(owners)
	burrows := []string{"burrow-01", "burrow-02"}
	parameters := [][]byte{[]byte("42")}
	prepared, err := Prepare("SELECT c FROM sbtest1 WHERE id=$1")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		prepared.Analyze(parameters, registry, burrows)
	}
}
