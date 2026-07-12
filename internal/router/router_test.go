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
	if !ok || result.SQL != `INSERT INTO widgets (name, "id") VALUES ($1, $2) RETURNING id` {
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

func BenchmarkTargetForSchemaBoundPrimaryKey(b *testing.B) {
	registry := schema.New(map[string][]string{"sbtest1": {"id"}})
	burrows := []string{"burrow-01", "burrow-02"}
	parameters := [][]byte{[]byte("42")}
	for b.Loop() {
		TargetForSchema("SELECT c FROM sbtest1 WHERE id=$1", parameters, registry, burrows)
	}
}
