// SPDX-License-Identifier: AGPL-3.0-only

package schema

import "testing"

func TestSnapshotPreservesSchemaAndExcludesTopologyPlacement(t *testing.T) {
	want := NewWithTypes(
		map[string][]string{"public.accounts": {"region", "tenant_id"}},
		map[string][]string{"public.accounts": {"text", "bigint"}},
	).WithVShards([]string{"burrow-02", "burrow-01"}).WithAllTables([]string{"public.accounts", "public.settings"})
	data, err := want.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := FromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := want.EqualSchema(got); err != nil {
		t.Fatal(err)
	}
	if got.VShardCount() != 0 {
		t.Fatalf("schema v4 snapshot retained %d topology owners", got.VShardCount())
	}
	if !got.IsSharded("public.accounts") {
		t.Fatal("annotated table is not sharded")
	}
	columns, _ := got.ShardKey("public.accounts")
	if len(columns) != 2 || columns[0] != "region" || columns[1] != "tenant_id" {
		t.Fatalf("compound shard key = %#v", columns)
	}
	types, _ := got.ShardKeyTypes("public.accounts")
	if len(types) != 2 || types[0] != "text" || types[1] != "bigint" {
		t.Fatalf("compound shard-key types = %#v", types)
	}
	inventory := got.Inventory()
	if len(inventory) != 2 || !inventory[0].Sharded || inventory[1].Sharded {
		t.Fatalf("inventory = %#v", inventory)
	}
}

func TestFromJSONImportsLegacyV3VShardOwners(t *testing.T) {
	got, err := FromJSON([]byte(`{"tables":{"public.accounts":["id"]},"vshards":["burrow-02","burrow-01"],"all_tables":["public.accounts"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision() != 1 {
		t.Fatalf("legacy revision = %d, want 1", got.Revision())
	}
	if owner, ok := got.VShardOwner(1); !ok || owner != "burrow-01" {
		t.Fatalf("legacy owner = %q, %v", owner, ok)
	}
}

func TestTableSpecificVShardOwnersCanonicalizePublicAlias(t *testing.T) {
	registry := New(map[string][]string{"accounts": {"id"}, "public.accounts": {"id"}}).
		WithAllTables([]string{"public.accounts"}).
		WithTableVShards(map[string][]string{"public.accounts": {"burrow-01", "burrow-02"}})
	if owner, ok := registry.VShardOwnerFor("accounts", 1); !ok || owner != "burrow-02" {
		t.Fatalf("table owner = %q, %v", owner, ok)
	}
	if !registry.HasTopologyPlacement() {
		t.Fatal("topology placement marker is missing")
	}
}

func TestHasTableUsesCanonicalInventoryAndExcludesCatalogs(t *testing.T) {
	registry := New(nil).WithAllTables([]string{"public.accounts", "sales.orders"})
	for _, table := range []string{"accounts", "public.accounts", "sales.orders"} {
		if !registry.HasTable(table) {
			t.Fatalf("HasTable(%q) = false, want inventory match", table)
		}
	}
	for _, table := range []string{"pg_catalog.pg_class", "information_schema.tables", "missing"} {
		if registry.HasTable(table) {
			t.Fatalf("HasTable(%q) = true, want topology-independent relation", table)
		}
	}
}

func TestRegistryPreservesQuotedIdentifierCase(t *testing.T) {
	registry := New(map[string][]string{
		"public.Accounts": {"Tenant_ID"},
		"public.accounts": {"tenant_id"},
	})
	quoted, ok := registry.ShardKey("public.Accounts")
	if !ok || len(quoted) != 1 || quoted[0] != "Tenant_ID" {
		t.Fatalf("quoted shard key = %#v, %v", quoted, ok)
	}
	unquoted, ok := registry.ShardKey("public.accounts")
	if !ok || len(unquoted) != 1 || unquoted[0] != "tenant_id" {
		t.Fatalf("unquoted shard key = %#v, %v", unquoted, ok)
	}
}

func TestVShardOwnerReadsOneValidatedEntry(t *testing.T) {
	registry := New(nil).WithVShards([]string{"burrow-01", "burrow-02"})
	if registry.VShardCount() != 2 {
		t.Fatalf("VShardCount = %d, want 2", registry.VShardCount())
	}
	if owner, ok := registry.VShardOwner(1); !ok || owner != "burrow-02" {
		t.Fatalf("VShardOwner(1) = %q, %v", owner, ok)
	}
	for _, invalid := range []int{-1, 2} {
		if owner, ok := registry.VShardOwner(invalid); ok || owner != "" {
			t.Fatalf("VShardOwner(%d) = %q, %v", invalid, owner, ok)
		}
	}
}
