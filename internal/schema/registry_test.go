package schema

import "testing"

func TestSnapshotPreservesShardKeysAndVShardOwners(t *testing.T) {
	want := New(map[string][]string{"public.accounts": {"region", "tenant_id"}}).WithVShards([]string{"burrow-02", "burrow-01"}).WithAllTables([]string{"public.accounts", "public.settings"})
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
	inventory := got.Inventory()
	if len(inventory) != 2 || !inventory[0].Sharded || inventory[1].Sharded {
		t.Fatalf("inventory = %#v", inventory)
	}
}
