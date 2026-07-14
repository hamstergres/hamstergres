// SPDX-License-Identifier: AGPL-3.0-only

package nest

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
)

func TestSequenceStoreIsUniqueAcrossConcurrentProxies(t *testing.T) {
	var mu sync.Mutex
	var value, revision int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/v3/kv/range" {
			if revision == 0 {
				_, _ = writer.Write([]byte(`{}`))
				return
			}
			encoded := base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(value, 10)))
			_ = json.NewEncoder(writer).Encode(map[string]any{"kvs": []map[string]string{{"value": encoded, "mod_revision": strconv.FormatInt(revision, 10)}}})
			return
		}
		var transaction struct {
			Compare []map[string]any `json:"compare"`
			Success []struct {
				Put map[string]string `json:"request_put"`
			} `json:"success"`
		}
		_ = json.NewDecoder(request.Body).Decode(&transaction)
		expected := int64(0)
		if raw, ok := transaction.Compare[0]["mod_revision"].(string); ok {
			expected, _ = strconv.ParseInt(raw, 10, 64)
		}
		succeeded := revision == expected
		if succeeded {
			decoded, _ := base64.StdEncoding.DecodeString(transaction.Success[0].Put["value"])
			value, _ = strconv.ParseInt(string(decoded), 10, 64)
			revision++
		}
		_ = json.NewEncoder(writer).Encode(map[string]bool{"succeeded": succeeded})
	}))
	defer server.Close()
	stores := []*SequenceStore{NewSequenceStore(server.URL, "/sequence"), NewSequenceStore(server.URL, "/sequence")}
	values := make(chan int64, 100)
	var wait sync.WaitGroup
	for index := 0; index < 100; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			value, err := stores[index%len(stores)].Next(t.Context())
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			values <- value
		}(index)
	}
	wait.Wait()
	close(values)
	got := make([]int, 0, 100)
	for value := range values {
		got = append(got, int(value))
	}
	sort.Ints(got)
	for index, value := range got {
		if value != index+1 {
			t.Fatalf("allocated IDs = %v", got)
		}
	}
}

func TestSequenceStoreFailsClosedWhenNestIsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	store := NewSequenceStore(server.URL, "/sequence")
	server.Close()
	if _, err := store.Next(t.Context()); err == nil {
		t.Fatal("Next succeeded while Hamstergres Nest was unavailable")
	}
}
