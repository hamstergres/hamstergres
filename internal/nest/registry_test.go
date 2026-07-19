// SPDX-License-Identifier: AGPL-3.0-only

package nest

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/jruszo/hamstergres/internal/schema"
)

func TestVerifyOrSeedSeedsThenRejectsDrift(t *testing.T) {
	var mu sync.Mutex
	var value string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		switch request.URL.Path {
		case "/v3/kv/range":
			if value == "" {
				_, _ = writer.Write([]byte(`{}`))
				return
			}
			_, _ = writer.Write([]byte(`{"kvs":[{"value":"` + value + `"}]}`))
		case "/v3/kv/put":
			var payload map[string]string
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatal(err)
			}
			value = payload["value"]
			_, _ = writer.Write([]byte(`{}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	store := NewRegistryStore(server.URL, "/registry")
	live := schema.New(map[string][]string{"accounts": {"id"}})
	if err := store.VerifyOrSeed(t.Context(), live); err != nil {
		t.Fatal(err)
	}
	if value == "" {
		t.Fatal("store was not seeded")
	}
	if _, err := base64.StdEncoding.DecodeString(value); err != nil {
		t.Fatalf("stored value is not base64: %v", err)
	}
	if err := store.VerifyOrSeed(t.Context(), live); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifyOrSeed(t.Context(), schema.New(map[string][]string{"accounts": {"other_id"}})); err == nil {
		t.Fatal("schema drift was accepted")
	}
}

func TestReplaceVersionedSkipsUnchangedRegistryWrite(t *testing.T) {
	stored, err := json.Marshal(schema.New(map[string][]string{"accounts": {"tenant_id"}}).WithRevision(7))
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(stored)
	putCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v3/kv/range":
			_, _ = writer.Write([]byte(`{"kvs":[{"value":"` + encoded + `"}]}`))
		case "/v3/kv/put":
			putCount++
			_, _ = writer.Write([]byte(`{}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	registry, err := NewRegistryStore(server.URL, "/registry").ReplaceVersioned(t.Context(), schema.New(map[string][]string{"accounts": {"tenant_id"}}))
	if err != nil {
		t.Fatal(err)
	}
	if registry.Revision() != 7 || putCount != 0 {
		t.Fatalf("unchanged replacement revision = %d, writes = %d; want revision 7 and no write", registry.Revision(), putCount)
	}
}
