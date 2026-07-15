// SPDX-License-Identifier: AGPL-3.0-only

package copyrouter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/jruszo/hamstergres/internal/router"
	"github.com/jruszo/hamstergres/internal/schema"
)

func TestCSVCompoundQuotedShardKeyRoutesCompleteRows(t *testing.T) {
	registry := schema.NewWithTypes(
		map[string][]string{"public.CopyData": {"Tenant_ID", "Region"}},
		map[string][]string{"public.CopyData": {"bigint", "text"}},
	)
	burrows := []string{"burrow-01", "burrow-02"}
	plan, err := Parse(`COPY "public"."CopyData" ("Region", payload, "Tenant_ID") FROM STDIN
		WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NULL', QUOTE '"', ESCAPE '"', ENCODING 'UTF8')`, registry)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Table != "public.CopyData" || !plan.From || !plan.Sharded || plan.Format != FormatCSV || !plan.Header {
		t.Fatalf("plan = %#v", plan)
	}
	stream, err := NewStream(plan, registry, burrows)
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Write([]byte("Region;payload;Tenant_ID\n\"eu;west\";\"hello"))
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Target != "" {
		t.Fatalf("header chunks = %#v", first)
	}
	second, err := stream.Write([]byte("\nworld\";0001\n"))
	if err != nil {
		t.Fatal(err)
	}
	want, ok := router.TargetForShardKey([]string{"1", "eu;west"}, []string{"bigint", "text"}, registry, burrows)
	if !ok || len(second) != 1 || second[0].Target != want {
		t.Fatalf("row chunks = %#v, want target %q", second, want)
	}
	if string(second[0].Data) != "\"eu;west\";\"hello\nworld\";0001\n" || stream.Rows() != 1 {
		t.Fatalf("routed row = %q, rows = %d", second[0].Data, stream.Rows())
	}
	if chunks, err := stream.Finish(); err != nil || len(chunks) != 0 {
		t.Fatalf("Finish = %#v, %v", chunks, err)
	}
}

func TestTextCOPYRoutesEscapedFieldsAndFailsClosed(t *testing.T) {
	registry := schema.NewWithTypes(
		map[string][]string{"events": {"tenant_id", "region"}},
		map[string][]string{"events": {"bigint", "text"}},
	)
	burrows := []string{"burrow-01", "burrow-02"}
	plan, err := Parse(`COPY events (payload, region, tenant_id) FROM STDIN`, registry)
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := NewStream(plan, registry, burrows)
	chunks, err := stream.Write([]byte("hello\\tworld\teu\\twest\t0007\n"))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := router.TargetForShardKey([]string{"7", "eu\twest"}, []string{"bigint", "text"}, registry, burrows)
	if len(chunks) != 1 || chunks[0].Target != want {
		t.Fatalf("chunks = %#v, want target %q", chunks, want)
	}

	missing, err := Parse(`COPY events (payload, tenant_id) FROM STDIN`, registry)
	if err == nil || !strings.Contains(err.Error(), `missing shard-key column "region"`) || missing.Table != "" {
		t.Fatalf("missing key plan = %#v, err = %v", missing, err)
	}
	stream, _ = NewStream(plan, registry, burrows)
	if _, err := stream.Write([]byte("payload\t\\N\t7\n")); err == nil || !strings.Contains(err.Error(), "cannot be NULL") {
		t.Fatalf("NULL key error = %v", err)
	}
}

func TestGeneratedShardKeyMustBeExplicit(t *testing.T) {
	registry := schema.NewWithGeneratedAndTypes(
		map[string][]string{"widgets": {"id"}},
		map[string]schema.GeneratedPrimary{"widgets": {Column: "id", Kind: "identity"}},
		map[string][]string{"widgets": {"bigint"}},
	)
	_, err := Parse(`COPY widgets (value) FROM STDIN`, registry)
	if err == nil || !strings.Contains(err.Error(), "cannot omit generated shard key") {
		t.Fatalf("generated-key error = %v", err)
	}
}

func TestBinaryCOPYRoutesTuplesAndEnvelopes(t *testing.T) {
	registry := schema.NewWithTypes(
		map[string][]string{"binary_events": {"id"}},
		map[string][]string{"binary_events": {"bigint"}},
	)
	burrows := []string{"burrow-01", "burrow-02"}
	plan, err := Parse(`COPY binary_events (id, payload) FROM STDIN WITH (FORMAT binary)`, registry)
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := NewStream(plan, registry, burrows)
	data := binaryInput(
		binaryTuple(int64Field(1), []byte("first")),
		binaryTuple(int64Field(2), []byte("second")),
	)
	var chunks []Chunk
	for len(data) > 0 {
		length := 7
		if len(data) < length {
			length = len(data)
		}
		next, err := stream.Write(data[:length])
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, next...)
		data = data[length:]
	}
	last, err := stream.Finish()
	if err != nil {
		t.Fatal(err)
	}
	chunks = append(chunks, last...)
	if len(chunks) != 4 || chunks[0].Target != "" || chunks[3].Target != "" || stream.Rows() != 2 {
		t.Fatalf("binary chunks = %#v, rows = %d", chunks, stream.Rows())
	}
	for index, key := range []string{"1", "2"} {
		want, _ := router.TargetForShardKey([]string{key}, []string{"bigint"}, registry, burrows)
		if chunks[index+1].Target != want {
			t.Fatalf("tuple %s target = %q, want %q", key, chunks[index+1].Target, want)
		}
	}
}

func TestLargeTextStreamKeepsOnlyAnIncompleteRow(t *testing.T) {
	registry := schema.NewWithTypes(map[string][]string{"events": {"id"}}, map[string][]string{"events": {"bigint"}})
	plan, err := Parse(`COPY events (id, payload) FROM STDIN`, registry)
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := NewStream(plan, registry, []string{"burrow-01", "burrow-02"})
	for batch := 0; batch < 100; batch++ {
		var input strings.Builder
		for row := 0; row < 100; row++ {
			fmt.Fprintf(&input, "%d\t%s\n", batch*100+row, strings.Repeat("x", 64))
		}
		chunks, err := stream.Write([]byte(input.String()))
		if err != nil {
			t.Fatal(err)
		}
		if len(chunks) != 100 || stream.BufferedBytes() != 0 {
			t.Fatalf("batch %d emitted %d rows with %d buffered bytes", batch, len(chunks), stream.BufferedBytes())
		}
	}
	if stream.Rows() != 10_000 {
		t.Fatalf("rows = %d", stream.Rows())
	}
}

func TestCSVOutputKeepsOneHeaderInBurrowOrder(t *testing.T) {
	registry := schema.NewWithTypes(map[string][]string{"events": {"id"}}, map[string][]string{"events": {"bigint"}})
	plan, err := Parse(`COPY events (id, payload) TO STDOUT WITH (FORMAT csv, HEADER true)`, registry)
	if err != nil {
		t.Fatal(err)
	}
	first := NewOutputStream(plan, 0, 2)
	firstParts, err := first.Write([]byte("id,payload\n1,first\n"))
	if err != nil {
		t.Fatal(err)
	}
	second := NewOutputStream(plan, 1, 2)
	if parts, err := second.Write([]byte("id,pay")); err != nil || len(parts) != 0 {
		t.Fatalf("partial second header = %#v, %v", parts, err)
	}
	secondParts, err := second.Write([]byte("load\n2,second\n"))
	if err != nil {
		t.Fatal(err)
	}
	merged := append(append([]byte(nil), firstParts[0]...), secondParts[0]...)
	if string(merged) != "id,payload\n1,first\n2,second\n" {
		t.Fatalf("merged CSV = %q", merged)
	}
}

func TestBinaryOutputHasOneEnvelope(t *testing.T) {
	registry := schema.NewWithTypes(map[string][]string{"events": {"id"}}, map[string][]string{"events": {"bigint"}})
	plan, err := Parse(`COPY events (id, payload) TO STDOUT WITH (FORMAT binary)`, registry)
	if err != nil {
		t.Fatal(err)
	}
	streams := []*OutputStream{NewOutputStream(plan, 0, 2), NewOutputStream(plan, 1, 2)}
	inputs := [][]byte{
		binaryInput(binaryTuple(int64Field(1), []byte("first"))),
		binaryInput(binaryTuple(int64Field(2), []byte("second"))),
	}
	var merged []byte
	for index, stream := range streams {
		for len(inputs[index]) > 0 {
			length := 5
			if len(inputs[index]) < length {
				length = len(inputs[index])
			}
			parts, err := stream.Write(inputs[index][:length])
			if err != nil {
				t.Fatal(err)
			}
			for _, part := range parts {
				merged = append(merged, part...)
			}
			inputs[index] = inputs[index][length:]
		}
		if _, err := stream.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	want := binaryInput(
		binaryTuple(int64Field(1), []byte("first")),
		binaryTuple(int64Field(2), []byte("second")),
	)
	if !bytes.Equal(merged, want) {
		t.Fatalf("merged binary length = %d, want %d", len(merged), len(want))
	}
}

func TestShardedCOPYRejectsUnsupportedSemantics(t *testing.T) {
	registry := schema.NewWithTypes(map[string][]string{"events": {"id"}}, map[string][]string{"events": {"bigint"}})
	for _, query := range []string{
		`COPY events (id) FROM STDIN WITH (ENCODING 'LATIN1')`,
		`COPY events (id) FROM STDIN WITH (FORMAT csv, FORCE_NULL (id))`,
		`COPY events FROM STDIN`,
	} {
		if _, err := Parse(query, registry); err == nil {
			t.Fatalf("Parse(%q) succeeded", query)
		}
	}
	if plan, err := Parse(`COPY unsharded (id) FROM STDIN WITH (ENCODING 'LATIN1')`, registry); err != nil || plan.Sharded {
		t.Fatalf("unsharded COPY should retain backend encoding semantics: %#v, %v", plan, err)
	}
}

func TestServerSideCOPYPlansOnlySchemaPlacement(t *testing.T) {
	registry := schema.NewWithTypes(map[string][]string{"events": {"id"}}, map[string][]string{"events": {"bigint"}})
	for _, query := range []string{
		`COPY events FROM '/tmp/events.data' WITH (FREEZE true)`,
		`COPY events TO '/tmp/events.data' WITH (FORMAT csv, FORCE_QUOTE *)`,
	} {
		plan, err := Parse(query, registry)
		if err != nil {
			t.Fatalf("Parse(%q): %v", query, err)
		}
		if !plan.ServerSide || plan.Program || !plan.Sharded || plan.Table != "events" {
			t.Fatalf("Parse(%q) = %#v", query, plan)
		}
	}
	plan, err := Parse(`COPY events FROM PROGRAM 'generate-events'`, registry)
	if err != nil || !plan.Program || !plan.ServerSide {
		t.Fatalf("COPY PROGRAM plan = %#v, %v", plan, err)
	}
	if _, err := Parse(`COPY (SELECT * FROM events) TO '/tmp/events.data'`, registry); err == nil || !strings.Contains(err.Error(), "must name a relation") {
		t.Fatalf("query COPY error = %v", err)
	}
}

func int64Field(value int64) []byte {
	field := make([]byte, 8)
	binary.BigEndian.PutUint64(field, uint64(value))
	return field
}

func binaryTuple(fields ...[]byte) []byte {
	var output bytes.Buffer
	_ = binary.Write(&output, binary.BigEndian, int16(len(fields)))
	for _, field := range fields {
		_ = binary.Write(&output, binary.BigEndian, int32(len(field)))
		output.Write(field)
	}
	return output.Bytes()
}

func binaryInput(tuples ...[]byte) []byte {
	var output bytes.Buffer
	output.Write(binarySignature)
	_ = binary.Write(&output, binary.BigEndian, uint32(0))
	_ = binary.Write(&output, binary.BigEndian, uint32(0))
	for _, tuple := range tuples {
		output.Write(tuple)
	}
	_ = binary.Write(&output, binary.BigEndian, int16(-1))
	return output.Bytes()
}
