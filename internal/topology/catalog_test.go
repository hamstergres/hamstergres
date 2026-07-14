package topology

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/jruszo/hamstergres/internal/schema"
)

func testRegistry() schema.Registry {
	return schema.NewWithTypes(
		map[string][]string{"accounts": {"id"}, "public.accounts": {"id"}},
		map[string][]string{"accounts": {"bigint"}, "public.accounts": {"bigint"}},
	).WithAllTables([]string{"public.accounts"}).WithRevision(7)
}

func TestBootstrapImportsLegacyPlacementExactly(t *testing.T) {
	registry := testRegistry()
	catalog, err := Bootstrap(registry, []BootstrapBurrow{
		{Name: "burrow-02", DSN: "postgres://two"},
		{Name: "burrow-01", DSN: "postgres://one"},
	}, []string{"burrow-02", "burrow-01", "burrow-02"}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	placements, err := catalog.TablePlacements()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(placements["public.accounts"], []string{"burrow-02", "burrow-01", "burrow-02"}) {
		t.Fatalf("placement = %#v", placements["public.accounts"])
	}
	if catalog.Schema.Revision != 7 || catalog.Schema.Fingerprint != registry.Fingerprint() {
		t.Fatalf("schema compatibility = %#v", catalog.Schema)
	}
	if catalog.Burrows[0].ID == catalog.Burrows[0].Name {
		t.Fatal("immutable Burrow ID is not distinct from its operator-facing name")
	}

	data, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip Catalog
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if err := roundTrip.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapReproducesFullModuloMap(t *testing.T) {
	catalog, err := Bootstrap(testRegistry(), []BootstrapBurrow{
		{Name: "burrow-01", DSN: "postgres://one"},
		{Name: "burrow-02", DSN: "postgres://two"},
	}, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	placements, _ := catalog.TablePlacements()
	owners := placements["public.accounts"]
	if len(owners) != DefaultVShardCount {
		t.Fatalf("owners = %d, want %d", len(owners), DefaultVShardCount)
	}
	if owners[0] != "burrow-02" || owners[1] != "burrow-01" || owners[2] != "burrow-02" {
		t.Fatalf("first modulo owners = %#v", owners[:3])
	}
}

func TestValidateRejectsIncompleteAndUnknownPlacement(t *testing.T) {
	catalog, err := Bootstrap(testRegistry(), []BootstrapBurrow{{Name: "burrow-01", DSN: "postgres://one"}}, []string{"burrow-01", "burrow-01"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	incomplete := catalog
	incomplete.Distributions = append([]Distribution(nil), catalog.Distributions...)
	incomplete.Distributions[0].Owners = []string{catalog.Burrows[0].ID}
	if err := incomplete.Validate(); err == nil {
		t.Fatal("incomplete placement was accepted")
	}
	unknown := catalog
	unknown.Distributions = append([]Distribution(nil), catalog.Distributions...)
	unknown.Distributions[0].Owners = append([]string(nil), catalog.Distributions[0].Owners...)
	unknown.Distributions[0].Owners[0] = "missing"
	if err := unknown.Validate(); err == nil {
		t.Fatal("unknown owner was accepted")
	}
}

func TestAddingBurrowDoesNotChangePlacement(t *testing.T) {
	current, err := Bootstrap(testRegistry(), []BootstrapBurrow{{Name: "burrow-01", DSN: "postgres://one"}}, []string{"burrow-01", "burrow-01"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	next := cloneForTest(t, current)
	next.Revision++
	next.Change.Timestamp = next.Change.Timestamp.Add(time.Second)
	next.Change.Reason = "register capacity without routing"
	next.Burrows = append(next.Burrows, Burrow{
		ID: StableBurrowID("burrow-02"), Name: "burrow-02", State: BurrowAdding,
		Tunnels: []TunnelEndpoint{{Name: "primary", DSN: "postgres://two"}}, Weight: 1,
	})
	if err := ValidateTransition(current, next); err != nil {
		t.Fatal(err)
	}
	before, _ := current.TablePlacements()
	after, _ := next.TablePlacements()
	if !slices.Equal(before["public.accounts"], after["public.accounts"]) {
		t.Fatal("adding a Burrow changed placement")
	}
}

func TestTransitionRejectsVShardCountChangeAndOwnedRemoval(t *testing.T) {
	current, err := Bootstrap(testRegistry(), []BootstrapBurrow{{Name: "burrow-01", DSN: "postgres://one"}}, []string{"burrow-01", "burrow-01"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	resized := cloneForTest(t, current)
	resized.Revision++
	resized.Change.Timestamp = resized.Change.Timestamp.Add(time.Second)
	resized.Distributions[0].VShardCount++
	resized.Distributions[0].Owners = append(resized.Distributions[0].Owners, resized.Burrows[0].ID)
	if err := ValidateTransition(current, resized); err == nil {
		t.Fatal("vshard count change was accepted")
	}
	removed := cloneForTest(t, current)
	removed.Revision++
	removed.Change.Timestamp = removed.Change.Timestamp.Add(time.Second)
	removed.Burrows[0].State = BurrowRemoved
	if err := ValidateTransition(current, removed); err == nil {
		t.Fatal("removal of an owner was accepted")
	}
}

func TestSchemaCompatibilityAllowsIndependentNonRoutingRevision(t *testing.T) {
	catalog, err := Bootstrap(testRegistry(), []BootstrapBurrow{{Name: "burrow-01", DSN: "postgres://one"}}, []string{"burrow-01"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	newer := testRegistry().WithRevision(8)
	if err := catalog.ValidateSchema(newer); err != nil {
		t.Fatal(err)
	}
	changed := schema.NewWithTypes(
		map[string][]string{"accounts": {"tenant_id"}, "public.accounts": {"tenant_id"}},
		map[string][]string{"accounts": {"bigint"}, "public.accounts": {"bigint"}},
	).WithAllTables([]string{"public.accounts"}).WithRevision(8)
	if err := catalog.ValidateSchema(changed); err == nil {
		t.Fatal("incompatible sharding schema was accepted")
	}
}

func cloneForTest(t *testing.T, catalog Catalog) Catalog {
	t.Helper()
	data, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	var clone Catalog
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}
