// Package schema records the database schema contract shared by all Burrows.
package schema

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Snapshot is the durable, portable representation kept in Hamstergres Nest.
type Snapshot struct {
	Tables map[string][]string `json:"tables"`
}

// Registry contains the primary-key columns that may be used for routing.
// Keys are normalized as schema.table; unqualified public tables are also
// available by their table name.
type Registry struct {
	tables map[string][]string
}

func New(tables map[string][]string) Registry {
	copy := make(map[string][]string, len(tables))
	for table, columns := range tables {
		key := strings.ToLower(table)
		copy[key] = append([]string(nil), columns...)
	}
	return Registry{tables: copy}
}

func (r Registry) PrimaryKey(table string) ([]string, bool) {
	columns, ok := r.tables[strings.ToLower(strings.Trim(table, `"`))]
	if !ok {
		return nil, false
	}
	return append([]string(nil), columns...), true
}

func (r Registry) Equal(other Registry) error {
	if len(r.tables) != len(other.tables) {
		return fmt.Errorf("primary-key table count differs: %d and %d", len(r.tables), len(other.tables))
	}
	for table, columns := range r.tables {
		otherColumns, ok := other.tables[table]
		if !ok || strings.Join(columns, "\x00") != strings.Join(otherColumns, "\x00") {
			return fmt.Errorf("primary key for %s differs across Burrows", table)
		}
	}
	return nil
}

func (r Registry) Tables() []string {
	tables := make([]string, 0, len(r.tables))
	for table := range r.tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func (r Registry) Snapshot() Snapshot {
	tables := make(map[string][]string, len(r.tables))
	for table, columns := range r.tables {
		tables[table] = append([]string(nil), columns...)
	}
	return Snapshot{Tables: tables}
}

func (r Registry) MarshalJSON() ([]byte, error) { return json.Marshal(r.Snapshot()) }

func FromJSON(data []byte) (Registry, error) {
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Registry{}, err
	}
	return New(snapshot.Tables), nil
}
