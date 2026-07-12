package observability

import "testing"

func TestTracingEnabledOnlyByExplicitStandardEnvironment(t *testing.T) {
	for _, name := range []string{"OTEL_SDK_DISABLED", "OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"} {
		t.Setenv(name, "")
	}
	if tracingEnabled() {
		t.Fatal("tracing enabled without opt-in")
	}
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	if !tracingEnabled() {
		t.Fatal("tracing not enabled by OTEL_TRACES_EXPORTER=otlp")
	}
	t.Setenv("OTEL_SDK_DISABLED", "true")
	if tracingEnabled() {
		t.Fatal("OTEL_SDK_DISABLED did not disable tracing")
	}
}

func TestConfiguredServiceNameHonorsStandardEnvironment(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "proxy-west")
	if got := configuredServiceName(); got != "proxy-west" {
		t.Fatalf("service name = %q", got)
	}
}
