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
	Tables    map[string][]string         `json:"tables"`
	Generated map[string]GeneratedPrimary `json:"generated,omitempty"`
	VShards   []string                    `json:"vshards,omitempty"`
	AllTables []string                    `json:"all_tables,omitempty"`
}

// GeneratedPrimary describes the only generated-key shape the initial Proxy
// strategy can safely rewrite: a single BIGINT-compatible primary key.
type GeneratedPrimary struct {
	Column string `json:"column"`
	Kind   string `json:"kind"`
}

// Registry contains the ordered shard-key columns that may be used for routing.
// Keys are normalized as schema.table; unqualified public tables are also
// available by their table name.
type Registry struct {
	tables    map[string][]string
	generated map[string]GeneratedPrimary
	vshards   []string
	allTables []string
}

func New(tables map[string][]string) Registry {
	return NewWithGenerated(tables, nil)
}

func NewWithGenerated(tables map[string][]string, generated map[string]GeneratedPrimary) Registry {
	copy := make(map[string][]string, len(tables))
	for table, columns := range tables {
		key := strings.ToLower(table)
		copy[key] = append([]string(nil), columns...)
	}
	generatedCopy := make(map[string]GeneratedPrimary, len(generated))
	for table, value := range generated {
		generatedCopy[strings.ToLower(table)] = value
	}
	return Registry{tables: copy, generated: generatedCopy}
}

// WithVShards records the Burrow owner of each vshard in Nest metadata.
func (r Registry) WithVShards(owners []string) Registry {
	r.vshards = append([]string(nil), owners...)
	return r
}

func (r Registry) WithAllTables(tables []string) Registry {
	r.allTables = append([]string(nil), tables...)
	sort.Strings(r.allTables)
	return r
}

func (r Registry) PrimaryKey(table string) ([]string, bool) {
	columns, ok := r.tables[strings.ToLower(strings.Trim(table, `"`))]
	if !ok {
		return nil, false
	}
	return append([]string(nil), columns...), true
}

// ShardKey returns the annotated columns in PostgreSQL attribute order.
func (r Registry) ShardKey(table string) ([]string, bool) { return r.PrimaryKey(table) }

func (r Registry) GeneratedPrimaryKey(table string) (GeneratedPrimary, bool) {
	value, ok := r.generated[strings.ToLower(strings.Trim(table, `"`))]
	return value, ok
}

func (r Registry) IsSharded(table string) bool {
	_, ok := r.ShardKey(table)
	return ok
}

func (r Registry) VShardOwners() []string { return append([]string(nil), r.vshards...) }

type TableInventory struct {
	Table     string   `json:"table"`
	Sharded   bool     `json:"sharded"`
	ShardKeys []string `json:"shard_keys,omitempty"`
}

func (r Registry) Inventory() []TableInventory {
	items := make([]TableInventory, 0, len(r.allTables))
	for _, table := range r.allTables {
		keys, sharded := r.ShardKey(table)
		items = append(items, TableInventory{Table: table, Sharded: sharded, ShardKeys: keys})
	}
	return items
}

func (r Registry) Equal(other Registry) error {
	if len(r.tables) != len(other.tables) {
		return fmt.Errorf("shard-key table count differs: %d and %d", len(r.tables), len(other.tables))
	}
	for table, columns := range r.tables {
		otherColumns, ok := other.tables[table]
		if !ok || strings.Join(columns, "\x00") != strings.Join(otherColumns, "\x00") {
			return fmt.Errorf("shard key for %s differs across Burrows", table)
		}
	}
	if len(r.generated) != len(other.generated) {
		return fmt.Errorf("generated primary-key table count differs: %d and %d", len(r.generated), len(other.generated))
	}
	for table, value := range r.generated {
		if other.generated[table] != value {
			return fmt.Errorf("generated primary key for %s differs across Burrows", table)
		}
	}
	if strings.Join(r.vshards, "\x00") != strings.Join(other.vshards, "\x00") {
		return fmt.Errorf("vshard placement differs")
	}
	if strings.Join(r.allTables, "\x00") != strings.Join(other.allTables, "\x00") {
		return fmt.Errorf("table inventory differs")
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
	generated := make(map[string]GeneratedPrimary, len(r.generated))
	for table, value := range r.generated {
		generated[table] = value
	}
	return Snapshot{Tables: tables, Generated: generated, VShards: append([]string(nil), r.vshards...), AllTables: append([]string(nil), r.allTables...)}
}

func (r Registry) MarshalJSON() ([]byte, error) { return json.Marshal(r.Snapshot()) }

func FromJSON(data []byte) (Registry, error) {
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Registry{}, err
	}
	return NewWithGenerated(snapshot.Tables, snapshot.Generated).WithVShards(snapshot.VShards).WithAllTables(snapshot.AllTables), nil
}
