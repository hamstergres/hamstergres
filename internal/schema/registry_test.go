package schema

import "testing"

func TestSnapshotPreservesShardKeysAndVShardOwners(t *testing.T) {
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
	if err := want.Equal(got); err != nil {
		t.Fatal(err)
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
