package router

import (
	"testing"

	"github.com/jruszo/hamstergres/internal/schema"
)

func TestTargetForSchemaUsesDiscoveredPrimaryKey(t *testing.T) {
	burrows := []string{"burrow-01", "burrow-02"}
	registry := schema.New(map[string][]string{"accounts": {"tenant_id", "account_id"}})
	target, ok := TargetForSchema("SELECT * FROM accounts WHERE tenant_id = 42 AND account_id = 9", nil, registry, burrows)
	if !ok || target != BurrowForKey("42\x009", burrows) {
		t.Fatalf("TargetForSchema = %q, %t; want composite primary-key target", target, ok)
	}
	if _, ok := TargetForSchema("SELECT * FROM accounts WHERE tenant_id = 42", nil, registry, burrows); ok {
		t.Fatal("partial composite primary key was routed")
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
