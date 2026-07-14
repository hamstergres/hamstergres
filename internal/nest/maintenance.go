// SPDX-License-Identifier: AGPL-3.0-only

package nest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const testNamespaceRoot = "/hamstergres/tests/"

// MaintenanceClient performs narrowly scoped cleanup of disposable Nest data.
// It deliberately exposes no arbitrary-prefix deletion operation.
type MaintenanceClient struct {
	registry *RegistryStore
}

func NewMaintenanceClient(endpoint string) *MaintenanceClient {
	return &MaintenanceClient{registry: NewRegistryStore(endpoint, "")}
}

// DeleteTestNamespace removes one integration test's registry and sequence
// state. Test keys are restricted to one safe path segment so this method can
// never reach the default, benchmark, or experiment namespaces.
func (c *MaintenanceClient) DeleteTestNamespace(ctx context.Context, testKey string) (int64, error) {
	if err := validateTestKey(testKey); err != nil {
		return 0, err
	}
	prefix := testNamespaceRoot + testKey + "/"
	body, err := json.Marshal(map[string]string{
		"key":       base64.StdEncoding.EncodeToString([]byte(prefix)),
		"range_end": base64.StdEncoding.EncodeToString(prefixRangeEnd([]byte(prefix))),
	})
	if err != nil {
		return 0, err
	}
	response, err := c.registry.request(ctx, "/v3/kv/deleterange", body)
	if err != nil {
		return 0, fmt.Errorf("delete Nest test namespace %q: %w", prefix, err)
	}
	var result struct {
		Header struct {
			Revision string `json:"revision"`
		} `json:"header"`
		Deleted string `json:"deleted"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return 0, fmt.Errorf("decode Nest test cleanup response: %w", err)
	}
	if result.Deleted == "" {
		result.Deleted = "0"
	}
	deleted, err := strconv.ParseInt(result.Deleted, 10, 64)
	if err != nil || deleted < 0 {
		return 0, fmt.Errorf("decode Nest test cleanup count %q", result.Deleted)
	}
	if result.Header.Revision != "" {
		if err := c.compact(ctx, result.Header.Revision); err != nil {
			return 0, err
		}
	}
	return deleted, nil
}

func (c *MaintenanceClient) compact(ctx context.Context, revision string) error {
	body, err := json.Marshal(map[string]any{"revision": revision, "physical": false})
	if err != nil {
		return err
	}
	_, err = c.registry.request(ctx, "/v3/kv/compaction", body)
	if err != nil && !strings.Contains(err.Error(), "required revision has been compacted") {
		return fmt.Errorf("compact Nest after test cleanup at revision %s: %w", revision, err)
	}
	return nil
}

func validateTestKey(testKey string) error {
	if testKey == "" || len(testKey) > 128 {
		return fmt.Errorf("Nest test key must contain 1 to 128 safe characters")
	}
	for _, character := range testKey {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return fmt.Errorf("Nest test key %q contains an unsafe character", testKey)
	}
	return nil
}

func prefixRangeEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] < 0xff {
			end[index]++
			return end[:index+1]
		}
	}
	return []byte{0}
}
