// SPDX-License-Identifier: AGPL-3.0-only

package proxy

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTunnelSpansShareQueryTraceWithoutSensitiveAttributes(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	ctx, parent := otel.Tracer("test").Start(context.Background(), "proxy.query")
	spans := startTunnelSpans(ctx, []string{"burrow-01", "burrow-02"})
	endTunnelSpans(spans, nil)
	parent.End()
	ended := recorder.Ended()
	if len(ended) != 3 {
		t.Fatalf("ended spans = %d, want query plus two Tunnels", len(ended))
	}
	parentID := ended[2].SpanContext().SpanID()
	for _, span := range ended[:2] {
		if span.Parent().SpanID() != parentID {
			t.Fatal("Tunnel span is not correlated with query span")
		}
		for _, item := range span.Attributes() {
			if item.Key == "db.statement" || item.Key == "db.query.text" {
				t.Fatalf("sensitive query attribute exported: %s", item.Key)
			}
		}
	}
}
