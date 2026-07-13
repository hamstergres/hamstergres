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
	Tables        map[string][]string         `json:"tables"`
	ShardKeyTypes map[string][]string         `json:"shard_key_types,omitempty"`
	Generated     map[string]GeneratedPrimary `json:"generated,omitempty"`
	VShards       []string                    `json:"vshards,omitempty"`
	AllTables     []string                    `json:"all_tables,omitempty"`
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
	tables        map[string][]string
	shardKeyTypes map[string][]string
	generated     map[string]GeneratedPrimary
	vshards       []string
	allTables     []string
}

func New(tables map[string][]string) Registry {
	return NewWithGeneratedAndTypes(tables, nil, nil)
}

func NewWithGenerated(tables map[string][]string, generated map[string]GeneratedPrimary) Registry {
	return NewWithGeneratedAndTypes(tables, generated, nil)
}

func NewWithTypes(tables, shardKeyTypes map[string][]string) Registry {
	return NewWithGeneratedAndTypes(tables, nil, shardKeyTypes)
}

// NewWithGeneratedAndTypes constructs a registry from PostgreSQL-canonical
// catalog names. Those names are case-sensitive: the parser has already folded
// unquoted identifiers, while quoted identifiers retain their exact spelling.
func NewWithGeneratedAndTypes(tables map[string][]string, generated map[string]GeneratedPrimary, shardKeyTypes map[string][]string) Registry {
	copy := make(map[string][]string, len(tables))
	for table, columns := range tables {
		copy[table] = append([]string(nil), columns...)
	}
	typesCopy := make(map[string][]string, len(shardKeyTypes))
	for table, types := range shardKeyTypes {
		typesCopy[table] = append([]string(nil), types...)
	}
	generatedCopy := make(map[string]GeneratedPrimary, len(generated))
	for table, value := range generated {
		generatedCopy[table] = value
	}
	return Registry{tables: copy, shardKeyTypes: typesCopy, generated: generatedCopy}
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
	columns, ok := r.tables[table]
	if !ok {
		return nil, false
	}
	return append([]string(nil), columns...), true
}

// ShardKey returns the annotated columns in PostgreSQL attribute order.
func (r Registry) ShardKey(table string) ([]string, bool) { return r.PrimaryKey(table) }

// ShardKeyTypes returns PostgreSQL format_type names aligned with ShardKey.
// A registry assembled without catalog types returns no types and routing
// remains conservative for values whose PostgreSQL representation is unclear.
func (r Registry) ShardKeyTypes(table string) ([]string, bool) {
	types, ok := r.shardKeyTypes[table]
	if !ok {
		return nil, false
	}
	return append([]string(nil), types...), true
}

func (r Registry) GeneratedPrimaryKey(table string) (GeneratedPrimary, bool) {
	value, ok := r.generated[table]
	return value, ok
}

func (r Registry) IsSharded(table string) bool {
	_, ok := r.ShardKey(table)
	return ok
}

func (r Registry) VShardOwners() []string { return append([]string(nil), r.vshards...) }

// VShardCount returns the size of the validated ownership map without copying
// it. Routing uses this together with VShardOwner on every query; full copies
// are reserved for snapshots and serialization boundaries.
func (r Registry) VShardCount() int { return len(r.vshards) }

// VShardOwner returns one vshard owner without exposing the mutable backing
// slice. The registry is immutable after construction and swapped atomically by
// its owner when schema metadata changes.
func (r Registry) VShardOwner(vshard int) (string, bool) {
	if vshard < 0 || vshard >= len(r.vshards) {
		return "", false
	}
	return r.vshards[vshard], true
}

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
	if len(r.shardKeyTypes) != len(other.shardKeyTypes) {
		return fmt.Errorf("shard-key type table count differs: %d and %d", len(r.shardKeyTypes), len(other.shardKeyTypes))
	}
	for table, types := range r.shardKeyTypes {
		otherTypes, ok := other.shardKeyTypes[table]
		if !ok || strings.Join(types, "\x00") != strings.Join(otherTypes, "\x00") {
			return fmt.Errorf("shard-key types for %s differ across Burrows", table)
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
	types := make(map[string][]string, len(r.shardKeyTypes))
	for table, values := range r.shardKeyTypes {
		types[table] = append([]string(nil), values...)
	}
	generated := make(map[string]GeneratedPrimary, len(r.generated))
	for table, value := range r.generated {
		generated[table] = value
	}
	return Snapshot{Tables: tables, ShardKeyTypes: types, Generated: generated, VShards: append([]string(nil), r.vshards...), AllTables: append([]string(nil), r.allTables...)}
}

func (r Registry) MarshalJSON() ([]byte, error) { return json.Marshal(r.Snapshot()) }

func FromJSON(data []byte) (Registry, error) {
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Registry{}, err
	}
	return NewWithGeneratedAndTypes(snapshot.Tables, snapshot.Generated, snapshot.ShardKeyTypes).WithVShards(snapshot.VShards).WithAllTables(snapshot.AllTables), nil
}
