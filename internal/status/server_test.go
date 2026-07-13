package status

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProfilingEndpointIsExplicitlyOptIn(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	disabled := httptest.NewRecorder()
	(&Server{}).Handler().ServeHTTP(disabled, request)
	if disabled.Code != http.StatusNotFound {
		t.Fatalf("disabled profile status = %d, want 404", disabled.Code)
	}

	enabled := httptest.NewRecorder()
	(&Server{}).Handler(true).ServeHTTP(enabled, request)
	if enabled.Code != http.StatusOK || !strings.Contains(enabled.Body.String(), "Types of profiles available") {
		t.Fatalf("enabled profile response = %d %q", enabled.Code, enabled.Body.String())
	}
}
