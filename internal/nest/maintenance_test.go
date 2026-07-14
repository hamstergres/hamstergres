// SPDX-License-Identifier: AGPL-3.0-only

package nest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDeleteTestNamespaceUsesOnlyExactTestPrefix(t *testing.T) {
	const prefix = "/hamstergres/tests/gateway-127-0-0-1-1234/"
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		switch request.URL.Path {
		case "/v3/kv/deleterange":
			var payload map[string]string
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Error(err)
				return
			}
			key, err := base64.StdEncoding.DecodeString(payload["key"])
			if err != nil {
				t.Error(err)
			}
			rangeEnd, err := base64.StdEncoding.DecodeString(payload["range_end"])
			if err != nil {
				t.Error(err)
			}
			if string(key) != prefix || !bytes.Equal(rangeEnd, prefixRangeEnd([]byte(prefix))) {
				t.Errorf("delete range = [%q, %q), want exact prefix %q", key, rangeEnd, prefix)
			}
			_, _ = writer.Write([]byte(`{"header":{"revision":"42"},"deleted":"2"}`))
		case "/v3/kv/compaction":
			var payload struct {
				Revision string `json:"revision"`
				Physical bool   `json:"physical"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Error(err)
			}
			if payload.Revision != "42" || payload.Physical {
				t.Errorf("compaction = %#v, want logical revision 42", payload)
			}
			_, _ = writer.Write([]byte(`{}`))
		default:
			t.Errorf("request path = %q", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	deleted, err := NewMaintenanceClient(server.URL).DeleteTestNamespace(t.Context(), "gateway-127-0-0-1-1234")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want registry and sequence keys", deleted)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want delete and compaction", requests.Load())
	}

	end := prefixRangeEnd([]byte(prefix))
	for key, wantDeleted := range map[string]bool{
		prefix + "schema-registry":                              true,
		prefix + "global-id":                                    true,
		"/hamstergres/tests/gateway-127-0-0-1-12345/global-id":  false,
		"/hamstergres/schema-registry/v3":                       false,
		"/hamstergres/benchmarks/read-scaling/schema-registry":  false,
		"/hamstergres/experiments/sharding-cpu/schema-registry": false,
		"/hamstergres/sequences/global-id/v1":                   false,
	} {
		gotDeleted := strings.Compare(key, prefix) >= 0 && bytes.Compare([]byte(key), end) < 0
		if gotDeleted != wantDeleted {
			t.Errorf("key %q selected = %t, want %t", key, gotDeleted, wantDeleted)
		}
	}
}

func TestDeleteTestNamespaceRejectsUnsafeKeysWithoutContactingNest(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	client := NewMaintenanceClient(server.URL)

	for _, key := range []string{"", "../schema-registry", "other/test", "test key", strings.Repeat("x", 129)} {
		if _, err := client.DeleteTestNamespace(t.Context(), key); err == nil {
			t.Errorf("unsafe key %q was accepted", key)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("unsafe keys made %d Nest requests", requests.Load())
	}
}

func TestDeleteTestNamespaceToleratesConcurrentCompaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v3/kv/deleterange":
			_, _ = writer.Write([]byte(`{"header":{"revision":"42"},"deleted":"2"}`))
		case "/v3/kv/compaction":
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte(`{"message":"etcdserver: mvcc: required revision has been compacted"}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	deleted, err := NewMaintenanceClient(server.URL).DeleteTestNamespace(t.Context(), "concurrent-test")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
}
