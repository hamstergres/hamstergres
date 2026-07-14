package nest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jruszo/hamstergres/internal/schema"
	"github.com/jruszo/hamstergres/internal/topology"
)

var ErrTopologyConflict = errors.New("Nest topology compare-and-swap conflict")

type StoredTopology struct {
	Catalog     topology.Catalog
	ModRevision int64
}

type TopologyStore struct {
	endpoint string
	key      string
	client   *http.Client
}

func NewTopologyStore(endpoint, key string) *TopologyStore {
	return &TopologyStore{endpoint: strings.TrimRight(endpoint, "/"), key: key, client: http.DefaultClient}
}

func (s *TopologyStore) Get(ctx context.Context) (StoredTopology, bool, error) {
	body, err := json.Marshal(map[string]string{"key": s.encodedKey()})
	if err != nil {
		return StoredTopology{}, false, err
	}
	response, err := s.request(ctx, "/v3/kv/range", body)
	if err != nil {
		return StoredTopology{}, false, err
	}
	var result struct {
		KVs []struct {
			Value       string `json:"value"`
			ModRevision string `json:"mod_revision"`
		} `json:"kvs"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return StoredTopology{}, false, fmt.Errorf("decode Nest topology response: %w", err)
	}
	if len(result.KVs) == 0 {
		return StoredTopology{}, false, nil
	}
	value, err := base64.StdEncoding.DecodeString(result.KVs[0].Value)
	if err != nil {
		return StoredTopology{}, false, fmt.Errorf("decode Nest topology value: %w", err)
	}
	var catalog topology.Catalog
	if err := json.Unmarshal(value, &catalog); err != nil {
		return StoredTopology{}, false, fmt.Errorf("decode Nest topology catalog: %w", err)
	}
	if err := catalog.Validate(); err != nil {
		return StoredTopology{}, false, fmt.Errorf("invalid Nest topology: %w", err)
	}
	modRevision, err := strconv.ParseInt(result.KVs[0].ModRevision, 10, 64)
	if err != nil || modRevision <= 0 {
		return StoredTopology{}, false, fmt.Errorf("invalid Nest topology mod_revision %q", result.KVs[0].ModRevision)
	}
	return StoredTopology{Catalog: catalog, ModRevision: modRevision}, true, nil
}

// VerifyOrBootstrap publishes the initial v3-compatible placement exactly
// once. Concurrent Proxies either win the create transaction or consume the
// same complete winner; no partial map is observable.
func (s *TopologyStore) VerifyOrBootstrap(ctx context.Context, bootstrap topology.Catalog) (StoredTopology, error) {
	stored, found, err := s.Get(ctx)
	if err != nil {
		return StoredTopology{}, err
	}
	if found {
		return stored, nil
	}
	if err := bootstrap.Validate(); err != nil {
		return StoredTopology{}, err
	}
	if err := s.compareAndSwap(ctx, 0, bootstrap); err != nil {
		if !errors.Is(err, ErrTopologyConflict) {
			return StoredTopology{}, err
		}
		winner, found, getErr := s.Get(ctx)
		if getErr != nil {
			return StoredTopology{}, getErr
		}
		if !found {
			return StoredTopology{}, fmt.Errorf("topology create lost CAS but no winner exists: %w", err)
		}
		return winner, nil
	}
	created, found, err := s.Get(ctx)
	if err != nil {
		return StoredTopology{}, err
	}
	if !found {
		return StoredTopology{}, fmt.Errorf("Nest topology create succeeded but catalog is missing")
	}
	return created, nil
}

func (s *TopologyStore) Update(ctx context.Context, current StoredTopology, next topology.Catalog) (StoredTopology, error) {
	if err := topology.ValidateTransition(current.Catalog, next); err != nil {
		return StoredTopology{}, err
	}
	if err := s.compareAndSwap(ctx, current.ModRevision, next); err != nil {
		return StoredTopology{}, err
	}
	updated, found, err := s.Get(ctx)
	if err != nil {
		return StoredTopology{}, err
	}
	if !found {
		return StoredTopology{}, fmt.Errorf("updated Nest topology is missing")
	}
	return updated, nil
}

// ReconcileSchema keeps table-to-distribution membership compatible with a
// successful fleet-wide DDL refresh. Existing distributions and owners are
// never changed; newly sharded empty tables inherit the bootstrap placement.
func (s *TopologyStore) ReconcileSchema(ctx context.Context, registry schema.Registry) (StoredTopology, error) {
	for attempt := 0; attempt < 3; attempt++ {
		current, found, err := s.Get(ctx)
		if err != nil {
			return StoredTopology{}, err
		}
		if !found {
			return StoredTopology{}, fmt.Errorf("cannot reconcile schema without a Nest topology")
		}
		next, err := cloneCatalog(current.Catalog)
		if err != nil {
			return StoredTopology{}, err
		}
		sharded := make(map[string]struct{})
		for _, table := range registry.CanonicalShardedTables() {
			sharded[table] = struct{}{}
			if _, exists := next.TableDistributions[table]; !exists {
				next.TableDistributions[table] = topology.BootstrapDistribution
			}
		}
		for table := range next.TableDistributions {
			if _, exists := sharded[table]; !exists {
				delete(next.TableDistributions, table)
			}
		}
		next.Schema = topology.SchemaCompatibility{
			RegistryFormat: schema.CurrentFormatVersion,
			Revision:       registry.Revision(),
			Fingerprint:    registry.Fingerprint(),
		}
		next.Revision++
		next.Change = topology.ChangeMetadata{
			Actor: "hamstergres-proxy", Reason: "reconcile successful fleet-wide schema change", Timestamp: time.Now().UTC(),
		}
		updated, err := s.Update(ctx, current, next)
		if errors.Is(err, ErrTopologyConflict) {
			continue
		}
		return updated, err
	}
	return StoredTopology{}, fmt.Errorf("reconcile topology after repeated conflicts: %w", ErrTopologyConflict)
}

func (s *TopologyStore) compareAndSwap(ctx context.Context, expectedModRevision int64, catalog topology.Catalog) error {
	value, err := json.Marshal(catalog)
	if err != nil {
		return err
	}
	target := "MOD"
	compareRevision := map[string]any{"mod_revision": strconv.FormatInt(expectedModRevision, 10)}
	if expectedModRevision == 0 {
		target = "CREATE"
		compareRevision = map[string]any{"create_revision": "0"}
	}
	compare := map[string]any{"target": target, "result": "EQUAL", "key": s.encodedKey()}
	for key, item := range compareRevision {
		compare[key] = item
	}
	payload := map[string]any{
		"compare": []any{compare},
		"success": []any{map[string]any{"request_put": map[string]string{
			"key": s.encodedKey(), "value": base64.StdEncoding.EncodeToString(value),
		}}},
		"failure": []any{map[string]any{"request_range": map[string]string{"key": s.encodedKey()}}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	response, err := s.request(ctx, "/v3/kv/txn", body)
	if err != nil {
		return err
	}
	var result struct {
		Succeeded bool `json:"succeeded"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return fmt.Errorf("decode Nest topology transaction: %w", err)
	}
	if !result.Succeeded {
		return ErrTopologyConflict
	}
	return nil
}

func (s *TopologyStore) encodedKey() string {
	return base64.StdEncoding.EncodeToString([]byte(s.key))
}

func (s *TopologyStore) request(ctx context.Context, path string, body []byte) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("contact Hamstergres Nest at %s: %w", s.endpoint, err)
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("Nest returned %s: %s", response.Status, strings.TrimSpace(string(contents)))
	}
	return contents, nil
}

func cloneCatalog(catalog topology.Catalog) (topology.Catalog, error) {
	data, err := json.Marshal(catalog)
	if err != nil {
		return topology.Catalog{}, err
	}
	var clone topology.Catalog
	if err := json.Unmarshal(data, &clone); err != nil {
		return topology.Catalog{}, err
	}
	return clone, nil
}
