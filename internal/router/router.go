package router

import (
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/jruszo/hamstergres/internal/schema"
)

const VirtualShards = 65536

var (
	insertValues   = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+[^\s(]+\s*\(([^)]*)\)\s*VALUES\s*\(([^)]*)\)`)
	tableReference = regexp.MustCompile(`(?is)^\s*(?:SELECT\b.*?\bFROM|UPDATE|DELETE\s+FROM|INSERT\s+INTO)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)`)
)

// GeneratedInsert is a rewritten single-row INSERT whose generated primary
// key is now explicit, allowing the Proxy to route before contacting a Burrow.
type GeneratedInsert struct {
	SQL    string
	Table  string
	Column string
}

// RewriteGeneratedInsert injects valueExpression when an eligible generated
// primary key is omitted or specified as DEFAULT. Explicit keys are untouched.
func RewriteGeneratedInsert(sql string, registry schema.Registry, valueExpression string) (GeneratedInsert, bool) {
	if firstSQLKeyword(sql) != "INSERT" {
		return GeneratedInsert{}, false
	}
	tableMatch := tableReference.FindStringSubmatch(sql)
	match := insertValues.FindStringSubmatchIndex(sql)
	if len(tableMatch) != 2 || len(match) != 6 {
		return GeneratedInsert{}, false
	}
	generated, ok := registry.GeneratedPrimaryKey(tableMatch[1])
	if !ok {
		return GeneratedInsert{}, false
	}
	columnsText, valuesText := sql[match[2]:match[3]], sql[match[4]:match[5]]
	// Generated-key synthesis is intentionally limited to one VALUES row. The
	// expression above identifies the first row, so reject any following row
	// instead of appending a column only to that row and producing invalid SQL.
	if strings.HasPrefix(strings.TrimSpace(sql[match[1]:]), ",") {
		return GeneratedInsert{}, false
	}
	columns, values := strings.Split(columnsText, ","), strings.Split(valuesText, ",")
	if len(columns) != len(values) {
		return GeneratedInsert{}, false
	}
	for index, column := range columns {
		if strings.EqualFold(strings.Trim(strings.TrimSpace(column), `"`), generated.Column) {
			if !strings.EqualFold(strings.TrimSpace(values[index]), "DEFAULT") {
				return GeneratedInsert{}, false
			}
			values[index] = valueExpression
			start, end := match[4], match[5]
			return GeneratedInsert{SQL: sql[:start] + strings.Join(values, ",") + sql[end:], Table: tableMatch[1], Column: generated.Column}, true
		}
	}
	columnsText += ", " + quoteIdentifier(generated.Column)
	valuesText += ", " + valueExpression
	rewritten := sql[:match[2]] + columnsText + sql[match[3]:match[4]] + valuesText + sql[match[5]:]
	return GeneratedInsert{SQL: rewritten, Table: tableMatch[1], Column: generated.Column}, true
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// TargetForSchema routes a read or write using the table's discovered primary
// key. It returns false for tables without a primary key and for ambiguous
// predicates, leaving the Proxy to apply its safe scatter/rejection policy.
func TargetForSchema(sql string, parameters [][]byte, registry schema.Registry, burrows []string) (string, bool) {
	if len(burrows) == 0 {
		return "", false
	}
	tableMatch := tableReference.FindStringSubmatch(sql)
	if len(tableMatch) != 2 {
		return "", false
	}
	columns, ok := registry.PrimaryKey(tableMatch[1])
	if !ok || len(columns) == 0 {
		return "", false
	}
	values, ok := primaryKeyValues(sql, parameters, columns)
	if !ok {
		return "", false
	}
	return BurrowForKey(strings.Join(values, "\x00"), burrows), true
}

// BurrowForKey hashes a primary key into the fixed 64k vshard space and maps
// the vshard to the configured Burrow order using one-indexed modulo placement.
func BurrowForKey(key string, burrows []string) string {
	vshard := int(HashKey(key) % VirtualShards)
	remainder := vshard % len(burrows)
	if remainder == 0 {
		return burrows[len(burrows)-1]
	}
	return burrows[remainder-1]
}

func HashKey(key string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return hash.Sum64()
}

func primaryKeyValues(sql string, parameters [][]byte, columns []string) ([]string, bool) {
	if firstSQLKeyword(sql) == "INSERT" {
		match := insertValues.FindStringSubmatch(sql)
		if len(match) != 3 {
			return nil, false
		}
		insertColumns, values := strings.Split(match[1], ","), strings.Split(match[2], ",")
		if len(insertColumns) != len(values) {
			return nil, false
		}
		byColumn := make(map[string]string, len(insertColumns))
		for i, column := range insertColumns {
			byColumn[strings.ToLower(strings.Trim(strings.TrimSpace(column), `"`))] = strings.TrimSpace(values[i])
		}
		result := make([]string, 0, len(columns))
		for _, column := range columns {
			value, ok := boundValue(byColumn[strings.ToLower(column)], parameters)
			if !ok {
				return nil, false
			}
			result = append(result, value)
		}
		return result, true
	}
	if !strings.Contains(strings.ToUpper(sql), "WHERE") {
		return nil, false
	}
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		pattern := `(?is)\b` + regexp.QuoteMeta(column) + `\s*=\s*('(?:''|[^'])*'|-?\d+|\$\d+)`
		match := regexp.MustCompile(pattern).FindStringSubmatch(sql)
		if len(match) != 2 {
			return nil, false
		}
		value, ok := boundValue(match[1], parameters)
		if !ok {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}

func boundValue(value string, parameters [][]byte) (string, bool) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "$") {
		if parameters == nil {
			return "", false
		}
		index, ok := parseParameterIndex(strings.TrimPrefix(value, "$"), len(parameters))
		if !ok || len(parameters[index]) == 0 {
			return "", false
		}
		return string(parameters[index]), true
	}
	return normalizeLiteral(value), true
}

func parseParameterIndex(value string, count int) (int, bool) {
	index := 0
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, false
		}
		index = index*10 + int(r-'0')
	}
	if index < 1 || index > count {
		return 0, false
	}
	return index - 1, true
}

func normalizeLiteral(literal string) string {
	literal = strings.TrimSpace(literal)
	if len(literal) >= 2 && literal[0] == '\'' && literal[len(literal)-1] == '\'' {
		return strings.ReplaceAll(literal[1:len(literal)-1], "''", "'")
	}
	return literal
}

func firstSQLKeyword(sql string) string {
	trimmed := strings.TrimSpace(sql)
	for {
		if strings.HasPrefix(trimmed, "--") {
			if index := strings.IndexByte(trimmed, '\n'); index >= 0 {
				trimmed = strings.TrimSpace(trimmed[index+1:])
				continue
			}
			return ""
		}
		if strings.HasPrefix(trimmed, "/*") {
			index := strings.Index(trimmed, "*/")
			if index < 0 {
				return ""
			}
			trimmed = strings.TrimSpace(trimmed[index+2:])
			continue
		}
		break
	}
	for index, r := range trimmed {
		if r < 'A' || r > 'Z' && r < 'a' || r > 'z' {
			return strings.ToUpper(trimmed[:index])
		}
	}
	return strings.ToUpper(trimmed)
}
