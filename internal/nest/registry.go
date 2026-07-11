// Package nest provides the Proxy's narrow control-plane interface.
package nest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/jruszo/hamstergres/internal/schema"
)

// RegistryStore persists the schema contract in an etcd-backed Hamstergres Nest.
type RegistryStore struct {
	endpoint string
	key      string
	client   *http.Client
}

func NewRegistryStore(endpoint, key string) *RegistryStore {
	return &RegistryStore{endpoint: strings.TrimRight(endpoint, "/"), key: key, client: http.DefaultClient}
}

// VerifyOrSeed fails closed when the live Burrow registry differs from the
// Nest snapshot. The first healthy Proxy seeds an empty Nest.
func (s *RegistryStore) VerifyOrSeed(ctx context.Context, live schema.Registry) error {
	stored, found, err := s.get(ctx)
	if err != nil {
		return err
	}
	if !found {
		return s.put(ctx, live)
	}
	if err := stored.Equal(live); err != nil {
		return fmt.Errorf("live Burrow schema differs from Nest registry: %w", err)
	}
	return nil
}

type rangeResponse struct {
	KVs []struct {
		Value string `json:"value"`
	} `json:"kvs"`
}

func (s *RegistryStore) get(ctx context.Context) (schema.Registry, bool, error) {
	body, err := json.Marshal(map[string]string{"key": base64.StdEncoding.EncodeToString([]byte(s.key))})
	if err != nil {
		return schema.Registry{}, false, err
	}
	response, err := s.request(ctx, "/v3/kv/range", body)
	if err != nil {
		return schema.Registry{}, false, err
	}
	var result rangeResponse
	if err := json.Unmarshal(response, &result); err != nil {
		return schema.Registry{}, false, fmt.Errorf("decode Nest schema registry: %w", err)
	}
	if len(result.KVs) == 0 {
		return schema.Registry{}, false, nil
	}
	value, err := base64.StdEncoding.DecodeString(result.KVs[0].Value)
	if err != nil {
		return schema.Registry{}, false, fmt.Errorf("decode Nest schema registry value: %w", err)
	}
	registry, err := schema.FromJSON(value)
	if err != nil {
		return schema.Registry{}, false, fmt.Errorf("decode Nest schema registry: %w", err)
	}
	return registry, true, nil
}

func (s *RegistryStore) put(ctx context.Context, registry schema.Registry) error {
	value, err := json.Marshal(registry)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{
		"key":   base64.StdEncoding.EncodeToString([]byte(s.key)),
		"value": base64.StdEncoding.EncodeToString(value),
	})
	if err != nil {
		return err
	}
	_, err = s.request(ctx, "/v3/kv/put", body)
	return err
}

func (s *RegistryStore) request(ctx context.Context, path string, body []byte) ([]byte, error) {
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
