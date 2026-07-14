package nest

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jruszo/hamstergres/internal/schema"
	"github.com/jruszo/hamstergres/internal/topology"
)

type fakeEtcd struct {
	mu          sync.Mutex
	value       string
	modRevision int64
}

func (f *fakeEtcd) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch request.URL.Path {
		case "/v3/kv/range":
			f.writeRange(writer)
		case "/v3/kv/txn":
			var payload struct {
				Compare []struct {
					Target         string `json:"target"`
					ModRevision    string `json:"mod_revision"`
					CreateRevision string `json:"create_revision"`
				} `json:"compare"`
				Success []struct {
					Put struct {
						Value string `json:"value"`
					} `json:"request_put"`
				} `json:"success"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			succeeded := false
			if len(payload.Compare) == 1 {
				compare := payload.Compare[0]
				switch compare.Target {
				case "CREATE":
					succeeded = f.modRevision == 0 && compare.CreateRevision == "0"
				case "MOD":
					expected, _ := strconv.ParseInt(compare.ModRevision, 10, 64)
					succeeded = expected == f.modRevision && expected > 0
				}
			}
			if succeeded {
				f.modRevision++
				f.value = payload.Success[0].Put.Value
			}
			_, _ = writer.Write([]byte(`{"succeeded":` + strconv.FormatBool(succeeded) + `}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	})
}

func (f *fakeEtcd) writeRange(writer http.ResponseWriter) {
	if f.value == "" {
		_, _ = writer.Write([]byte(`{}`))
		return
	}
	_, _ = writer.Write([]byte(`{"kvs":[{"value":"` + f.value + `","mod_revision":"` + strconv.FormatInt(f.modRevision, 10) + `"}]}`))
}

func nestTestRegistry(tables ...string) schema.Registry {
	keys := make(map[string][]string)
	types := make(map[string][]string)
	allTables := make([]string, 0, len(tables))
	for _, table := range tables {
		keys[table] = []string{"id"}
		types[table] = []string{"bigint"}
		allTables = append(allTables, table)
	}
	return schema.NewWithTypes(keys, types).WithAllTables(allTables).WithRevision(1)
}

func nestTestCatalog(t *testing.T, registry schema.Registry) topology.Catalog {
	t.Helper()
	catalog, err := topology.Bootstrap(registry, []topology.BootstrapBurrow{
		{Name: "burrow-01", DSN: "postgres://one"},
		{Name: "burrow-02", DSN: "postgres://two"},
	}, []string{"burrow-02", "burrow-01"}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestTopologyStoreBootstrapsAndRoundTrips(t *testing.T) {
	fake := &fakeEtcd{}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()
	store := NewTopologyStore(server.URL, "/hamstergres/topology/v1")
	want := nestTestCatalog(t, nestTestRegistry("public.accounts"))
	stored, err := store.VerifyOrBootstrap(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ModRevision != 1 || stored.Catalog.Revision != 1 {
		t.Fatalf("stored topology = %#v", stored)
	}
	data, _ := json.Marshal(stored.Catalog)
	if fake.value == "" || fake.value == base64.StdEncoding.EncodeToString(nil) || len(data) == 0 {
		t.Fatal("topology was not stored")
	}
	winner, err := store.VerifyOrBootstrap(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	if winner.ModRevision != stored.ModRevision {
		t.Fatal("existing topology was rewritten during bootstrap")
	}
}

func TestTopologyStoreCASConflictPreservesWinner(t *testing.T) {
	fake := &fakeEtcd{}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()
	store := NewTopologyStore(server.URL, "/topology")
	current, err := store.VerifyOrBootstrap(t.Context(), nestTestCatalog(t, nestTestRegistry("public.accounts")))
	if err != nil {
		t.Fatal(err)
	}
	winner := cloneTopology(t, current.Catalog)
	winner.Revision++
	winner.Change.Reason = "winner"
	winner.Change.Timestamp = winner.Change.Timestamp.Add(time.Second)
	storedWinner, err := store.Update(t.Context(), current, winner)
	if err != nil {
		t.Fatal(err)
	}
	loser := cloneTopology(t, current.Catalog)
	loser.Revision++
	loser.Change.Reason = "loser"
	loser.Change.Timestamp = loser.Change.Timestamp.Add(2 * time.Second)
	if _, err := store.Update(t.Context(), current, loser); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	after, found, err := store.Get(t.Context())
	if err != nil || !found {
		t.Fatal(err)
	}
	if after.ModRevision != storedWinner.ModRevision || after.Catalog.Change.Reason != "winner" {
		t.Fatalf("CAS rollback lost winner: %#v", after)
	}
}

func TestTopologyStoreReconcilesNewTableWithoutMovingOwners(t *testing.T) {
	fake := &fakeEtcd{}
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()
	store := NewTopologyStore(server.URL, "/topology")
	current, err := store.VerifyOrBootstrap(t.Context(), nestTestCatalog(t, nestTestRegistry("public.accounts")))
	if err != nil {
		t.Fatal(err)
	}
	before, _ := current.Catalog.TablePlacements()
	registry := nestTestRegistry("public.accounts", "public.orders").WithRevision(2)
	updated, err := store.ReconcileSchema(t.Context(), registry)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := updated.Catalog.TablePlacements()
	if _, exists := after["public.orders"]; !exists {
		t.Fatal("new sharded table has no distribution")
	}
	if len(before["public.accounts"]) != len(after["public.accounts"]) || before["public.accounts"][0] != after["public.accounts"][0] {
		t.Fatal("schema reconciliation moved an existing vshard")
	}
	if err := updated.Catalog.ValidateSchema(registry); err != nil {
		t.Fatal(err)
	}
}

func cloneTopology(t *testing.T, catalog topology.Catalog) topology.Catalog {
	t.Helper()
	data, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	var clone topology.Catalog
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}
