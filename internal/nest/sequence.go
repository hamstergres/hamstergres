package nest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
)

// SequenceStore allocates strictly increasing BIGINT values through etcd's
// compare-and-swap transaction API. Values can have gaps, just like a native
// PostgreSQL sequence, but an allocated value is never returned twice.
type SequenceStore struct {
	registry *RegistryStore
	key      string
}

func NewSequenceStore(endpoint, key string) *SequenceStore {
	return &SequenceStore{registry: NewRegistryStore(endpoint, key), key: key}
}

func (s *SequenceStore) Next(ctx context.Context) (int64, error) {
	for attempts := 0; attempts < 64; attempts++ {
		current, revision, found, err := s.read(ctx)
		if err != nil {
			return 0, err
		}
		if current == int64(^uint64(0)>>1) {
			return 0, fmt.Errorf("Hamstergres global ID sequence exhausted")
		}
		next := current + 1
		succeeded, err := s.swap(ctx, revision, found, next)
		if err != nil {
			return 0, err
		}
		if succeeded {
			return next, nil
		}
	}
	return 0, fmt.Errorf("allocate Hamstergres global ID: too much concurrent contention")
}

func (s *SequenceStore) read(ctx context.Context) (int64, int64, bool, error) {
	body, _ := json.Marshal(map[string]string{"key": base64.StdEncoding.EncodeToString([]byte(s.key))})
	response, err := s.registry.request(ctx, "/v3/kv/range", body)
	if err != nil {
		return 0, 0, false, err
	}
	var result struct {
		KVs []struct {
			Value       string `json:"value"`
			ModRevision string `json:"mod_revision"`
		} `json:"kvs"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return 0, 0, false, err
	}
	if len(result.KVs) == 0 {
		return 0, 0, false, nil
	}
	value, err := base64.StdEncoding.DecodeString(result.KVs[0].Value)
	if err != nil {
		return 0, 0, false, err
	}
	current, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil || current < 0 {
		return 0, 0, false, fmt.Errorf("invalid Hamstergres global ID value %q", value)
	}
	revision, err := strconv.ParseInt(result.KVs[0].ModRevision, 10, 64)
	return current, revision, true, err
}

func (s *SequenceStore) swap(ctx context.Context, revision int64, found bool, next int64) (bool, error) {
	target := "VERSION"
	result := "0"
	if found {
		target = "MOD"
		result = strconv.FormatInt(revision, 10)
	}
	encodedKey := base64.StdEncoding.EncodeToString([]byte(s.key))
	body, _ := json.Marshal(map[string]any{
		"compare": []map[string]string{{"key": encodedKey, "target": target, "result": "EQUAL", targetValueField(target): result}},
		"success": []map[string]any{{"request_put": map[string]string{"key": encodedKey, "value": base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(next, 10)))}}},
	})
	response, err := s.registry.request(ctx, "/v3/kv/txn", body)
	if err != nil {
		return false, err
	}
	var outcome struct {
		Succeeded bool `json:"succeeded"`
	}
	if err := json.Unmarshal(response, &outcome); err != nil {
		return false, err
	}
	return outcome.Succeeded, nil
}

func targetValueField(target string) string {
	if target == "MOD" {
		return "mod_revision"
	}
	return "version"
}
