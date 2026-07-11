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
