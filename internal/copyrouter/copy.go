// Package copyrouter parses PostgreSQL COPY statements and routes streaming
// COPY FROM rows without buffering the complete input.
package copyrouter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jruszo/hamstergres/internal/router"
	"github.com/jruszo/hamstergres/internal/schema"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

const MaxRowBytes = 16 << 20

type Format string

const (
	FormatText   Format = "text"
	FormatCSV    Format = "csv"
	FormatBinary Format = "binary"
)

// Plan is the syntax and schema information needed for one COPY operation.
type Plan struct {
	Table      string
	Columns    []string
	From       bool
	Sharded    bool
	Format     Format
	Delimiter  byte
	Null       string
	Header     bool
	Quote      byte
	Escape     byte
	Encoding   string
	keyIndexes []int
	keyTypes   []string
}

// Parse builds a fail-closed plan for a streaming COPY statement. Sharded
// input requires an explicit column list containing every shard-key component.
func Parse(sql string, registry schema.Registry) (Plan, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return Plan{}, err
	}
	if len(tree.Stmts) != 1 || tree.Stmts[0].Stmt == nil || tree.Stmts[0].Stmt.GetCopyStmt() == nil {
		return Plan{}, fmt.Errorf("expected exactly one COPY statement")
	}
	statement := tree.Stmts[0].Stmt.GetCopyStmt()
	plan := Plan{
		From:      statement.IsFrom,
		Format:    FormatText,
		Delimiter: '\t',
		Null:      `\N`,
		Encoding:  "UTF8",
	}
	if statement.IsProgram || statement.Filename != "" {
		return Plan{}, fmt.Errorf("Hamstergres Proxy supports only COPY FROM STDIN and COPY TO STDOUT")
	}
	if statement.Relation == nil {
		if statement.IsFrom || statement.Query == nil {
			return Plan{}, fmt.Errorf("COPY must name a relation")
		}
		return plan, nil
	}
	plan.Table = copyRelationName(statement.Relation)
	for _, node := range statement.Attlist {
		column := node.GetString_()
		if column == nil || column.Sval == "" {
			return Plan{}, fmt.Errorf("COPY has an invalid column list")
		}
		plan.Columns = append(plan.Columns, column.Sval)
	}

	unsupported := make([]string, 0)
	seen := make(map[string]struct{})
	for _, node := range statement.Options {
		option := node.GetDefElem()
		if option == nil {
			return Plan{}, fmt.Errorf("COPY has an invalid option")
		}
		name := strings.ToLower(option.Defname)
		if _, duplicate := seen[name]; duplicate {
			return Plan{}, fmt.Errorf("COPY option %s is specified more than once", name)
		}
		seen[name] = struct{}{}
		switch name {
		case "format":
			value, ok := copyOptionString(option.Arg)
			if !ok {
				return Plan{}, fmt.Errorf("COPY FORMAT requires a value")
			}
			plan.Format = Format(strings.ToLower(value))
		case "delimiter":
			value, ok := copyOptionString(option.Arg)
			if !ok || len([]byte(value)) != 1 {
				return Plan{}, fmt.Errorf("COPY DELIMITER must be one single-byte character")
			}
			plan.Delimiter = value[0]
		case "null":
			value, ok := copyOptionString(option.Arg)
			if !ok {
				return Plan{}, fmt.Errorf("COPY NULL requires a value")
			}
			plan.Null = value
		case "header":
			value, ok := copyOptionBool(option.Arg)
			if !ok {
				return Plan{}, fmt.Errorf("COPY HEADER requires a boolean value")
			}
			plan.Header = value
		case "quote":
			value, ok := copyOptionString(option.Arg)
			if !ok || len([]byte(value)) != 1 {
				return Plan{}, fmt.Errorf("COPY QUOTE must be one single-byte character")
			}
			plan.Quote = value[0]
		case "escape":
			value, ok := copyOptionString(option.Arg)
			if !ok || len([]byte(value)) != 1 {
				return Plan{}, fmt.Errorf("COPY ESCAPE must be one single-byte character")
			}
			plan.Escape = value[0]
		case "encoding":
			value, ok := copyOptionString(option.Arg)
			if !ok {
				return Plan{}, fmt.Errorf("COPY ENCODING requires a value")
			}
			plan.Encoding = strings.ToUpper(strings.ReplaceAll(value, "-", ""))
		default:
			unsupported = append(unsupported, name)
		}
	}

	switch plan.Format {
	case FormatText:
		if _, supplied := seen["quote"]; supplied {
			return Plan{}, fmt.Errorf("COPY QUOTE is supported only in CSV mode")
		}
		if _, supplied := seen["escape"]; supplied {
			return Plan{}, fmt.Errorf("COPY ESCAPE is supported only in CSV mode")
		}
	case FormatCSV:
		if _, supplied := seen["delimiter"]; !supplied {
			plan.Delimiter = ','
		}
		if _, supplied := seen["null"]; !supplied {
			plan.Null = ""
		}
		if plan.Quote == 0 {
			plan.Quote = '"'
		}
		if plan.Escape == 0 {
			plan.Escape = plan.Quote
		}
	case FormatBinary:
		for _, option := range []string{"delimiter", "null", "header", "quote", "escape"} {
			if _, supplied := seen[option]; supplied {
				return Plan{}, fmt.Errorf("COPY %s is not supported in binary mode", strings.ToUpper(option))
			}
		}
	default:
		return Plan{}, fmt.Errorf("unsupported COPY format %q", plan.Format)
	}
	if plan.Delimiter == '\n' || plan.Delimiter == '\r' || plan.Delimiter == 0 {
		return Plan{}, fmt.Errorf("COPY DELIMITER cannot be a newline or NUL")
	}
	if plan.Format == FormatCSV && (plan.Quote == plan.Delimiter || plan.Escape == plan.Delimiter) {
		return Plan{}, fmt.Errorf("COPY QUOTE and ESCAPE must differ from DELIMITER")
	}

	keys, sharded := registry.ShardKey(plan.Table)
	plan.Sharded = sharded
	if !plan.From || !sharded {
		return plan, nil
	}
	if plan.Encoding != "UTF8" {
		return Plan{}, fmt.Errorf("shard-aware COPY supports only UTF8 encoding")
	}
	if len(unsupported) > 0 {
		return Plan{}, fmt.Errorf("shard-aware COPY FROM does not support option %s", strings.ToUpper(strings.Join(unsupported, ", ")))
	}
	if len(plan.Columns) == 0 {
		return Plan{}, fmt.Errorf("shard-aware COPY FROM requires an explicit column list containing the complete shard key")
	}
	columnIndexes := make(map[string]int, len(plan.Columns))
	for index, column := range plan.Columns {
		if _, duplicate := columnIndexes[column]; duplicate {
			return Plan{}, fmt.Errorf("COPY column %q is specified more than once", column)
		}
		columnIndexes[column] = index
	}
	for _, key := range keys {
		index, ok := columnIndexes[key]
		if !ok {
			if generated, generatedKey := registry.GeneratedPrimaryKey(plan.Table); generatedKey && generated.Column == key {
				return Plan{}, fmt.Errorf("shard-aware COPY FROM cannot omit generated shard key %q; supply an explicit fleet-wide value", key)
			}
			return Plan{}, fmt.Errorf("shard-aware COPY FROM is missing shard-key column %q", key)
		}
		plan.keyIndexes = append(plan.keyIndexes, index)
	}
	types, ok := registry.ShardKeyTypes(plan.Table)
	if !ok || len(types) != len(keys) {
		return Plan{}, fmt.Errorf("shard-aware COPY FROM requires shard-key type metadata")
	}
	plan.keyTypes = types
	if plan.Format == FormatBinary {
		for _, dataType := range types {
			if !supportedBinaryType(dataType) {
				return Plan{}, fmt.Errorf("binary COPY does not support shard-key type %q", dataType)
			}
		}
	}
	return plan, nil
}

func copyRelationName(relation *pg_query.RangeVar) string {
	if relation.Schemaname != "" {
		return relation.Schemaname + "." + relation.Relname
	}
	return relation.Relname
}

func copyOptionString(node *pg_query.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	if value := node.GetString_(); value != nil {
		return value.Sval, true
	}
	if constant := node.GetAConst(); constant != nil && !constant.Isnull {
		if value := constant.GetSval(); value != nil {
			return value.Sval, true
		}
	}
	return "", false
}

func copyOptionBool(node *pg_query.Node) (bool, bool) {
	if node == nil {
		return true, true
	}
	if value := node.GetString_(); value != nil {
		if strings.EqualFold(value.Sval, "match") {
			return true, true
		}
		parsed, err := strconv.ParseBool(value.Sval)
		return parsed, err == nil
	}
	if value := node.GetBoolean(); value != nil {
		return value.Boolval, true
	}
	if constant := node.GetAConst(); constant != nil && !constant.Isnull {
		if value := constant.GetBoolval(); value != nil {
			return value.Boolval, true
		}
		if value := constant.GetSval(); value != nil {
			parsed, err := strconv.ParseBool(value.Sval)
			return parsed, err == nil
		}
	}
	if value := node.GetInteger(); value != nil {
		return value.Ival != 0, true
	}
	return false, false
}

// Chunk is one complete COPY protocol fragment. An empty Target means the
// fragment (a CSV header or binary envelope) must reach every participating
// Burrow; data rows always name exactly one owning Burrow.
type Chunk struct {
	Target string
	Data   []byte
}

type Stream struct {
	plan          Plan
	registry      schema.Registry
	burrows       []string
	buffer        []byte
	headerPending bool
	binaryHeader  bool
	binaryTrailer bool
	rows          int64
}

// OutputStream removes per-Burrow CSV/binary envelopes while preserving the
// configured Burrow order. The merged frontend stream has one CSV header or
// one binary header/trailer even though each PostgreSQL backend produces its
// own complete COPY stream.
type OutputStream struct {
	plan          Plan
	first         bool
	last          bool
	headerPending bool
	binaryHeader  bool
	binaryTrailer bool
	buffer        []byte
}

func NewOutputStream(plan Plan, index, count int) *OutputStream {
	return &OutputStream{
		plan:          plan,
		first:         index == 0,
		last:          index == count-1,
		headerPending: plan.Format == FormatCSV && plan.Header && index > 0,
	}
}

func (s *OutputStream) Write(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if s.plan.Format == FormatBinary {
		s.buffer = append(s.buffer, data...)
		parts, err := s.consumeBinaryOutput()
		if err != nil {
			return nil, err
		}
		if len(s.buffer) > MaxRowBytes {
			return nil, fmt.Errorf("binary COPY output row exceeds the %d-byte routing limit", MaxRowBytes)
		}
		return parts, nil
	}
	if !s.headerPending {
		return [][]byte{append([]byte(nil), data...)}, nil
	}
	s.buffer = append(s.buffer, data...)
	end, quoted := delimitedRecordEnd(s.buffer, s.plan)
	if end == 0 {
		if quoted && len(s.buffer) > MaxRowBytes {
			return nil, fmt.Errorf("COPY output header exceeds the routing limit")
		}
		return nil, nil
	}
	s.headerPending = false
	remainder := append([]byte(nil), s.buffer[end:]...)
	s.buffer = nil
	if len(remainder) == 0 {
		return nil, nil
	}
	return [][]byte{remainder}, nil
}

func (s *OutputStream) Finish() ([][]byte, error) {
	if s.plan.Format == FormatBinary {
		parts, err := s.consumeBinaryOutput()
		if err != nil {
			return nil, err
		}
		if len(s.buffer) != 0 || !s.binaryHeader || !s.binaryTrailer {
			return nil, fmt.Errorf("incomplete binary COPY output stream")
		}
		return parts, nil
	}
	if s.headerPending {
		return nil, fmt.Errorf("COPY output ended before the CSV header")
	}
	return nil, nil
}

func (s *OutputStream) consumeBinaryOutput() ([][]byte, error) {
	var output [][]byte
	if !s.binaryHeader {
		if len(s.buffer) < 19 {
			return nil, nil
		}
		if !bytes.Equal(s.buffer[:len(binarySignature)], binarySignature) {
			return nil, fmt.Errorf("invalid binary COPY output signature")
		}
		extension := int(binary.BigEndian.Uint32(s.buffer[15:19]))
		if extension < 0 || extension > MaxRowBytes {
			return nil, fmt.Errorf("binary COPY output header extension exceeds the routing limit")
		}
		headerLength := 19 + extension
		if len(s.buffer) < headerLength {
			return nil, nil
		}
		if s.first {
			output = append(output, append([]byte(nil), s.buffer[:headerLength]...))
		}
		s.buffer = s.buffer[headerLength:]
		s.binaryHeader = true
	}
	for len(s.buffer) >= 2 && !s.binaryTrailer {
		columns := int(int16(binary.BigEndian.Uint16(s.buffer[:2])))
		if columns == -1 {
			if s.last {
				output = append(output, append([]byte(nil), s.buffer[:2]...))
			}
			s.buffer = s.buffer[2:]
			s.binaryTrailer = true
			break
		}
		if columns < 0 {
			return nil, fmt.Errorf("invalid binary COPY output field count %d", columns)
		}
		position := 2
		complete := true
		for index := 0; index < columns; index++ {
			if len(s.buffer) < position+4 {
				complete = false
				break
			}
			length := int(int32(binary.BigEndian.Uint32(s.buffer[position : position+4])))
			position += 4
			if length == -1 {
				continue
			}
			if length < 0 || length > MaxRowBytes || position+length > MaxRowBytes {
				return nil, fmt.Errorf("binary COPY output field exceeds the routing limit")
			}
			if len(s.buffer) < position+length {
				complete = false
				break
			}
			position += length
		}
		if !complete {
			break
		}
		output = append(output, append([]byte(nil), s.buffer[:position]...))
		s.buffer = s.buffer[position:]
	}
	if s.binaryTrailer && len(s.buffer) != 0 {
		return nil, fmt.Errorf("binary COPY output data follows the trailer")
	}
	return output, nil
}

func NewStream(plan Plan, registry schema.Registry, burrows []string) (*Stream, error) {
	if !plan.From || !plan.Sharded || len(plan.keyIndexes) == 0 {
		return nil, fmt.Errorf("COPY stream routing requires a sharded COPY FROM plan")
	}
	return &Stream{
		plan:          plan,
		registry:      registry,
		burrows:       append([]string(nil), burrows...),
		headerPending: plan.Header,
	}, nil
}

func (s *Stream) Rows() int64        { return s.rows }
func (s *Stream) BufferedBytes() int { return len(s.buffer) }

func (s *Stream) Write(data []byte) ([]Chunk, error) {
	if len(data) == 0 {
		return nil, nil
	}
	s.buffer = append(s.buffer, data...)
	var chunks []Chunk
	var err error
	switch s.plan.Format {
	case FormatText, FormatCSV:
		chunks, err = s.consumeDelimited(false)
	case FormatBinary:
		chunks, err = s.consumeBinary()
	}
	if err != nil {
		return nil, err
	}
	if len(s.buffer) > MaxRowBytes {
		return nil, fmt.Errorf("COPY row exceeds the %d-byte routing limit", MaxRowBytes)
	}
	return chunks, nil
}

func (s *Stream) Finish() ([]Chunk, error) {
	switch s.plan.Format {
	case FormatText, FormatCSV:
		chunks, err := s.consumeDelimited(true)
		if err != nil {
			return nil, err
		}
		if s.headerPending {
			return nil, fmt.Errorf("COPY input ended before the CSV header")
		}
		return chunks, nil
	case FormatBinary:
		chunks, err := s.consumeBinary()
		if err != nil {
			return nil, err
		}
		if len(s.buffer) != 0 || !s.binaryHeader || !s.binaryTrailer {
			return nil, fmt.Errorf("incomplete binary COPY stream")
		}
		return chunks, nil
	default:
		return nil, fmt.Errorf("unsupported COPY format %q", s.plan.Format)
	}
}

func (s *Stream) consumeDelimited(final bool) ([]Chunk, error) {
	var chunks []Chunk
	for len(s.buffer) > 0 {
		end, quoted := delimitedRecordEnd(s.buffer, s.plan)
		if end == 0 {
			if final {
				if quoted {
					return nil, fmt.Errorf("unterminated quoted CSV field")
				}
				end = len(s.buffer)
			} else {
				break
			}
		}
		record := append([]byte(nil), s.buffer[:end]...)
		s.buffer = s.buffer[end:]
		if s.headerPending {
			s.headerPending = false
			chunks = append(chunks, Chunk{Data: record})
			continue
		}
		trimmed := trimRecordTerminator(record)
		if s.plan.Format == FormatText && bytes.Equal(trimmed, []byte(`\.`)) {
			chunks = append(chunks, Chunk{Data: record})
			continue
		}
		target, err := s.targetForRecord(trimmed)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, Chunk{Target: target, Data: record})
		s.rows++
	}
	return chunks, nil
}

func delimitedRecordEnd(data []byte, plan Plan) (int, bool) {
	if plan.Format == FormatText {
		if index := bytes.IndexByte(data, '\n'); index >= 0 {
			return index + 1, false
		}
		return 0, false
	}
	inQuotes := false
	fieldStart := true
	for index := 0; index < len(data); index++ {
		character := data[index]
		if inQuotes {
			if character == plan.Escape && index+1 < len(data) && (data[index+1] == plan.Quote || data[index+1] == plan.Escape) {
				index++
				continue
			}
			if character == plan.Quote {
				inQuotes = false
			}
			continue
		}
		if fieldStart && character == plan.Quote {
			inQuotes = true
			fieldStart = false
			continue
		}
		switch character {
		case plan.Delimiter:
			fieldStart = true
		case '\n':
			return index + 1, false
		default:
			if character != '\r' {
				fieldStart = false
			}
		}
	}
	return 0, inQuotes
}

func trimRecordTerminator(record []byte) []byte {
	if len(record) > 0 && record[len(record)-1] == '\n' {
		record = record[:len(record)-1]
		if len(record) > 0 && record[len(record)-1] == '\r' {
			record = record[:len(record)-1]
		}
	}
	return record
}

func (s *Stream) targetForRecord(record []byte) (string, error) {
	var fields []copyField
	var err error
	if s.plan.Format == FormatCSV {
		fields, err = parseCSVRecord(record, s.plan)
	} else {
		fields, err = parseTextRecord(record, s.plan)
	}
	if err != nil {
		return "", err
	}
	if len(fields) != len(s.plan.Columns) {
		return "", fmt.Errorf("COPY row has %d columns, expected %d", len(fields), len(s.plan.Columns))
	}
	values := make([]string, len(s.plan.keyIndexes))
	for index, columnIndex := range s.plan.keyIndexes {
		field := fields[columnIndex]
		if field.null {
			return "", fmt.Errorf("COPY shard-key column %q cannot be NULL", s.plan.Columns[columnIndex])
		}
		if !utf8.Valid(field.value) {
			return "", fmt.Errorf("COPY shard-key column %q is not valid UTF8", s.plan.Columns[columnIndex])
		}
		values[index] = string(field.value)
	}
	target, ok := router.TargetForShardKey(values, s.plan.keyTypes, s.registry, s.burrows)
	if !ok || target == "" {
		return "", fmt.Errorf("COPY row has an invalid or unsupported shard-key value")
	}
	return target, nil
}

type copyField struct {
	value  []byte
	null   bool
	quoted bool
}

func parseTextRecord(record []byte, plan Plan) ([]copyField, error) {
	var rawFields [][]byte
	start := 0
	for index, character := range record {
		if character != plan.Delimiter {
			continue
		}
		backslashes := 0
		for previous := index - 1; previous >= start && record[previous] == '\\'; previous-- {
			backslashes++
		}
		if backslashes%2 != 0 {
			continue
		}
		rawFields = append(rawFields, record[start:index])
		start = index + 1
	}
	rawFields = append(rawFields, record[start:])
	fields := make([]copyField, len(rawFields))
	for index, raw := range rawFields {
		if string(raw) == plan.Null {
			fields[index].null = true
			continue
		}
		value, err := decodeTextField(raw)
		if err != nil {
			return nil, err
		}
		fields[index].value = value
	}
	return fields, nil
}

func decodeTextField(raw []byte) ([]byte, error) {
	value := make([]byte, 0, len(raw))
	for index := 0; index < len(raw); index++ {
		if raw[index] != '\\' {
			value = append(value, raw[index])
			continue
		}
		index++
		if index >= len(raw) {
			return nil, fmt.Errorf("COPY text field ends with an escape character")
		}
		switch raw[index] {
		case 'b':
			value = append(value, '\b')
		case 'f':
			value = append(value, '\f')
		case 'n':
			value = append(value, '\n')
		case 'r':
			value = append(value, '\r')
		case 't':
			value = append(value, '\t')
		case 'v':
			value = append(value, '\v')
		case 'x':
			start := index + 1
			end := start
			for end < len(raw) && end < start+2 && isHex(raw[end]) {
				end++
			}
			if end == start {
				value = append(value, 'x')
				continue
			}
			parsed, _ := strconv.ParseUint(string(raw[start:end]), 16, 8)
			value = append(value, byte(parsed))
			index = end - 1
		default:
			if raw[index] >= '0' && raw[index] <= '7' {
				start := index
				end := start + 1
				for end < len(raw) && end < start+3 && raw[end] >= '0' && raw[end] <= '7' {
					end++
				}
				parsed, _ := strconv.ParseUint(string(raw[start:end]), 8, 8)
				value = append(value, byte(parsed))
				index = end - 1
			} else {
				value = append(value, raw[index])
			}
		}
	}
	return value, nil
}

func isHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

func parseCSVRecord(record []byte, plan Plan) ([]copyField, error) {
	fields := make([]copyField, 0, len(plan.Columns))
	position := 0
	for {
		field := copyField{}
		if position < len(record) && record[position] == plan.Quote {
			field.quoted = true
			position++
			var value []byte
			closed := false
			for position < len(record) {
				character := record[position]
				if character == plan.Escape && position+1 < len(record) && (record[position+1] == plan.Quote || record[position+1] == plan.Escape) {
					value = append(value, record[position+1])
					position += 2
					continue
				}
				if character == plan.Quote {
					position++
					closed = true
					break
				}
				value = append(value, character)
				position++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted CSV field")
			}
			field.value = value
			if position < len(record) && record[position] != plan.Delimiter {
				return nil, fmt.Errorf("unexpected data after a quoted CSV field")
			}
		} else {
			start := position
			for position < len(record) && record[position] != plan.Delimiter {
				if record[position] == plan.Quote {
					return nil, fmt.Errorf("unexpected quote in an unquoted CSV field")
				}
				position++
			}
			field.value = append([]byte(nil), record[start:position]...)
			field.null = string(field.value) == plan.Null
		}
		fields = append(fields, field)
		if position == len(record) {
			break
		}
		position++
		if position == len(record) {
			fields = append(fields, copyField{null: plan.Null == ""})
			break
		}
	}
	return fields, nil
}

var binarySignature = []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0}

func (s *Stream) consumeBinary() ([]Chunk, error) {
	var chunks []Chunk
	if !s.binaryHeader {
		if len(s.buffer) < 19 {
			return nil, nil
		}
		if !bytes.Equal(s.buffer[:len(binarySignature)], binarySignature) {
			return nil, fmt.Errorf("invalid binary COPY signature")
		}
		extension := int(binary.BigEndian.Uint32(s.buffer[15:19]))
		if extension < 0 || extension > MaxRowBytes {
			return nil, fmt.Errorf("binary COPY header extension exceeds the routing limit")
		}
		headerLength := 19 + extension
		if len(s.buffer) < headerLength {
			return nil, nil
		}
		chunks = append(chunks, Chunk{Data: append([]byte(nil), s.buffer[:headerLength]...)})
		s.buffer = s.buffer[headerLength:]
		s.binaryHeader = true
	}
	for len(s.buffer) >= 2 && !s.binaryTrailer {
		columns := int(int16(binary.BigEndian.Uint16(s.buffer[:2])))
		if columns == -1 {
			chunks = append(chunks, Chunk{Data: append([]byte(nil), s.buffer[:2]...)})
			s.buffer = s.buffer[2:]
			s.binaryTrailer = true
			break
		}
		if columns < 0 || columns != len(s.plan.Columns) {
			return nil, fmt.Errorf("binary COPY row has %d columns, expected %d", columns, len(s.plan.Columns))
		}
		position := 2
		fields := make([][]byte, columns)
		nulls := make([]bool, columns)
		complete := true
		for index := 0; index < columns; index++ {
			if len(s.buffer) < position+4 {
				complete = false
				break
			}
			length := int(int32(binary.BigEndian.Uint32(s.buffer[position : position+4])))
			position += 4
			if length == -1 {
				nulls[index] = true
				continue
			}
			if length < 0 || length > MaxRowBytes || position+length > MaxRowBytes {
				return nil, fmt.Errorf("binary COPY field exceeds the routing limit")
			}
			if len(s.buffer) < position+length {
				complete = false
				break
			}
			fields[index] = s.buffer[position : position+length]
			position += length
		}
		if !complete {
			break
		}
		values := make([]string, len(s.plan.keyIndexes))
		for index, columnIndex := range s.plan.keyIndexes {
			if nulls[columnIndex] {
				return nil, fmt.Errorf("COPY shard-key column %q cannot be NULL", s.plan.Columns[columnIndex])
			}
			value, err := decodeBinaryKey(fields[columnIndex], s.plan.keyTypes[index])
			if err != nil {
				return nil, fmt.Errorf("COPY shard-key column %q: %w", s.plan.Columns[columnIndex], err)
			}
			values[index] = value
		}
		target, ok := router.TargetForShardKey(values, s.plan.keyTypes, s.registry, s.burrows)
		if !ok || target == "" {
			return nil, fmt.Errorf("binary COPY row has an invalid shard-key value")
		}
		chunks = append(chunks, Chunk{Target: target, Data: append([]byte(nil), s.buffer[:position]...)})
		s.buffer = s.buffer[position:]
		s.rows++
	}
	if s.binaryTrailer && len(s.buffer) != 0 {
		return nil, fmt.Errorf("binary COPY data follows the trailer")
	}
	return chunks, nil
}

func supportedBinaryType(dataType string) bool {
	switch normalizedType(dataType) {
	case "int2", "smallint", "smallserial", "serial2", "int4", "int", "integer", "serial", "serial4", "int8", "bigint", "bigserial", "serial8", "oid",
		"float4", "real", "float8", "double precision", "bool", "boolean", "uuid", "text", "varchar", "character varying", "name", "bpchar", "char", "character":
		return true
	default:
		return false
	}
}

func normalizedType(dataType string) string {
	value := strings.ToLower(strings.TrimSpace(dataType))
	value = strings.TrimPrefix(value, "pg_catalog.")
	if index := strings.IndexByte(value, '('); index >= 0 {
		value = strings.TrimSpace(value[:index])
	}
	return value
}

func decodeBinaryKey(value []byte, dataType string) (string, error) {
	switch normalizedType(dataType) {
	case "int2", "smallint", "smallserial", "serial2":
		if len(value) != 2 {
			return "", fmt.Errorf("invalid int2 binary length %d", len(value))
		}
		return strconv.FormatInt(int64(int16(binary.BigEndian.Uint16(value))), 10), nil
	case "int4", "int", "integer", "serial", "serial4":
		if len(value) != 4 {
			return "", fmt.Errorf("invalid int4 binary length %d", len(value))
		}
		return strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(value))), 10), nil
	case "int8", "bigint", "bigserial", "serial8":
		if len(value) != 8 {
			return "", fmt.Errorf("invalid int8 binary length %d", len(value))
		}
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(value)), 10), nil
	case "oid":
		if len(value) != 4 {
			return "", fmt.Errorf("invalid oid binary length %d", len(value))
		}
		return strconv.FormatUint(uint64(binary.BigEndian.Uint32(value)), 10), nil
	case "float4", "real":
		if len(value) != 4 {
			return "", fmt.Errorf("invalid float4 binary length %d", len(value))
		}
		return strconv.FormatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(value))), 'g', -1, 32), nil
	case "float8", "double precision":
		if len(value) != 8 {
			return "", fmt.Errorf("invalid float8 binary length %d", len(value))
		}
		return strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(value)), 'g', -1, 64), nil
	case "bool", "boolean":
		if len(value) != 1 || value[0] > 1 {
			return "", fmt.Errorf("invalid boolean binary value")
		}
		return strconv.FormatBool(value[0] == 1), nil
	case "uuid":
		if len(value) != 16 {
			return "", fmt.Errorf("invalid uuid binary length %d", len(value))
		}
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			binary.BigEndian.Uint32(value[0:4]), binary.BigEndian.Uint16(value[4:6]), binary.BigEndian.Uint16(value[6:8]), binary.BigEndian.Uint16(value[8:10]), value[10:16]), nil
	case "text", "varchar", "character varying", "name", "bpchar", "char", "character":
		if !utf8.Valid(value) {
			return "", fmt.Errorf("value is not valid UTF8")
		}
		return string(value), nil
	default:
		return "", fmt.Errorf("unsupported binary shard-key type %q", dataType)
	}
}
