// Package topology defines the versioned routing catalog persisted in
// Hamstergres Nest.
package topology

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jruszo/hamstergres/internal/schema"
)

const (
	CurrentFormatVersion  = 1
	DefaultVShardCount    = 65536
	BootstrapDistribution = "distribution-bootstrap-0001"
)

type BurrowState string

const (
	BurrowAdding   BurrowState = "adding"
	BurrowReady    BurrowState = "ready"
	BurrowDraining BurrowState = "draining"
	BurrowRemoved  BurrowState = "removed"
)

type SchemaCompatibility struct {
	RegistryFormat int    `json:"registry_format"`
	Revision       uint64 `json:"revision"`
	Fingerprint    string `json:"fingerprint"`
}

type TunnelEndpoint struct {
	Name                     string `json:"name"`
	Address                  string `json:"address"`
	ConfigurationFingerprint string `json:"configuration_fingerprint"`
}

type Burrow struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	State         BurrowState       `json:"state"`
	Tunnels       []TunnelEndpoint  `json:"tunnels"`
	Weight        uint32            `json:"weight"`
	CapacityBytes int64             `json:"capacity_bytes,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

type Distribution struct {
	ID          string `json:"id"`
	VShardCount int    `json:"vshard_count"`
	// BurrowIDs is the immutable owner dictionary. OwnerIndexes is the complete,
	// zero-indexed vshard map into that dictionary. The compact representation
	// keeps a 65,536-vshard etcd transaction well below its request limit.
	BurrowIDs    []string `json:"burrow_ids"`
	OwnerIndexes []uint16 `json:"owner_indexes"`
}

type ChangeMetadata struct {
	Actor     string    `json:"actor"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}

type Catalog struct {
	FormatVersion      int                 `json:"format_version"`
	Revision           uint64              `json:"revision"`
	Schema             SchemaCompatibility `json:"schema"`
	Burrows            []Burrow            `json:"burrows"`
	Distributions      []Distribution      `json:"distributions"`
	TableDistributions map[string]string   `json:"table_distributions"`
	Change             ChangeMetadata      `json:"change"`
}

type BootstrapBurrow struct {
	Name string
	DSN  string
}

func StableBurrowID(name string) string {
	sum := sha256.Sum256([]byte("hamstergres-burrow\x00" + name))
	return fmt.Sprintf("burrow-%x", sum[:10])
}

// LegacyModuloOwners reproduces schema-registry v3 placement exactly.
func LegacyModuloOwners(names []string, count int) []string {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	owners := make([]string, count)
	if len(sorted) == 0 {
		return owners
	}
	for vshard := range owners {
		remainder := vshard % len(sorted)
		if remainder == 0 {
			owners[vshard] = sorted[len(sorted)-1]
		} else {
			owners[vshard] = sorted[remainder-1]
		}
	}
	return owners
}

func Bootstrap(registry schema.Registry, configured []BootstrapBurrow, legacyOwners []string, now time.Time) (Catalog, error) {
	if len(configured) == 0 {
		return Catalog{}, fmt.Errorf("bootstrap topology requires at least one configured Burrow")
	}
	sort.Slice(configured, func(i, j int) bool { return configured[i].Name < configured[j].Name })
	byName := make(map[string]string, len(configured))
	burrows := make([]Burrow, 0, len(configured))
	for _, configuredBurrow := range configured {
		if configuredBurrow.Name == "" || configuredBurrow.DSN == "" {
			return Catalog{}, fmt.Errorf("bootstrap Burrow name and Tunnel DSN are required")
		}
		if _, exists := byName[configuredBurrow.Name]; exists {
			return Catalog{}, fmt.Errorf("duplicate configured Burrow name %q", configuredBurrow.Name)
		}
		id := StableBurrowID(configuredBurrow.Name)
		byName[configuredBurrow.Name] = id
		address, fingerprint := tunnelMetadata(configuredBurrow.DSN)
		burrows = append(burrows, Burrow{
			ID: id, Name: configuredBurrow.Name, State: BurrowReady,
			Tunnels: []TunnelEndpoint{{Name: "primary", Address: address, ConfigurationFingerprint: fingerprint}}, Weight: 1,
		})
	}
	if len(legacyOwners) == 0 {
		names := make([]string, 0, len(configured))
		for _, item := range configured {
			names = append(names, item.Name)
		}
		legacyOwners = LegacyModuloOwners(names, DefaultVShardCount)
	}
	ownerDictionary := make([]string, 0, len(configured))
	ownerIndexesByID := make(map[string]uint16, len(configured))
	ownerIndexes := make([]uint16, len(legacyOwners))
	for vshard, owner := range legacyOwners {
		id, ok := byName[owner]
		if !ok {
			return Catalog{}, fmt.Errorf("legacy vshard %d references unconfigured Burrow %q", vshard, owner)
		}
		ownerIndex, exists := ownerIndexesByID[id]
		if !exists {
			if len(ownerDictionary) >= 1<<16 {
				return Catalog{}, fmt.Errorf("distribution has more than 65536 Burrow owners")
			}
			ownerIndex = uint16(len(ownerDictionary))
			ownerIndexesByID[id] = ownerIndex
			ownerDictionary = append(ownerDictionary, id)
		}
		ownerIndexes[vshard] = ownerIndex
	}
	tables := registry.CanonicalShardedTables()
	tableDistributions := make(map[string]string, len(tables))
	for _, table := range tables {
		tableDistributions[table] = BootstrapDistribution
	}
	revision := registry.Revision()
	if revision == 0 {
		revision = 1
	}
	catalog := Catalog{
		FormatVersion: CurrentFormatVersion,
		Revision:      1,
		Schema: SchemaCompatibility{
			RegistryFormat: schema.CurrentFormatVersion,
			Revision:       revision,
			Fingerprint:    registry.Fingerprint(),
		},
		Burrows: burrows,
		Distributions: []Distribution{{
			ID: BootstrapDistribution, VShardCount: len(ownerIndexes), BurrowIDs: ownerDictionary, OwnerIndexes: ownerIndexes,
		}},
		TableDistributions: tableDistributions,
		Change:             ChangeMetadata{Actor: "hamstergres-proxy", Reason: "bootstrap schema-registry v3 placement", Timestamp: now.UTC()},
	}
	if err := catalog.Validate(); err != nil {
		return Catalog{}, err
	}
	return catalog, nil
}

func (c Catalog) Validate() error {
	if c.FormatVersion != CurrentFormatVersion {
		return fmt.Errorf("unsupported topology format version %d", c.FormatVersion)
	}
	if c.Revision == 0 {
		return fmt.Errorf("topology revision must be positive")
	}
	if c.Schema.RegistryFormat <= 0 || c.Schema.Revision == 0 || c.Schema.Fingerprint == "" {
		return fmt.Errorf("topology schema compatibility is incomplete")
	}
	if c.Change.Actor == "" || c.Change.Reason == "" || c.Change.Timestamp.IsZero() {
		return fmt.Errorf("topology change audit metadata is incomplete")
	}
	burrows := make(map[string]Burrow, len(c.Burrows))
	names := make(map[string]struct{}, len(c.Burrows))
	for _, burrow := range c.Burrows {
		if burrow.ID == "" || burrow.Name == "" {
			return fmt.Errorf("Burrow ID and name are required")
		}
		if _, exists := burrows[burrow.ID]; exists {
			return fmt.Errorf("duplicate Burrow ID %q", burrow.ID)
		}
		if _, exists := names[burrow.Name]; exists {
			return fmt.Errorf("duplicate Burrow name %q", burrow.Name)
		}
		if !validState(burrow.State) {
			return fmt.Errorf("Burrow %s has invalid state %q", burrow.Name, burrow.State)
		}
		if burrow.Weight == 0 {
			return fmt.Errorf("Burrow %s weight must be positive", burrow.Name)
		}
		if burrow.CapacityBytes < 0 {
			return fmt.Errorf("Burrow %s capacity must not be negative", burrow.Name)
		}
		if burrow.State != BurrowRemoved && len(burrow.Tunnels) == 0 {
			return fmt.Errorf("Burrow %s has no Tunnel endpoint", burrow.Name)
		}
		for _, tunnel := range burrow.Tunnels {
			if tunnel.Name == "" || tunnel.Address == "" || tunnel.ConfigurationFingerprint == "" {
				return fmt.Errorf("Burrow %s has an incomplete Tunnel endpoint", burrow.Name)
			}
		}
		burrows[burrow.ID] = burrow
		names[burrow.Name] = struct{}{}
	}
	distributions := make(map[string]Distribution, len(c.Distributions))
	for _, distribution := range c.Distributions {
		if distribution.ID == "" {
			return fmt.Errorf("distribution ID is required")
		}
		if _, exists := distributions[distribution.ID]; exists {
			return fmt.Errorf("duplicate distribution ID %q", distribution.ID)
		}
		if distribution.VShardCount <= 0 || len(distribution.OwnerIndexes) != distribution.VShardCount {
			return fmt.Errorf("distribution %s has incomplete vshard coverage: %d owners for %d vshards", distribution.ID, len(distribution.OwnerIndexes), distribution.VShardCount)
		}
		if len(distribution.BurrowIDs) == 0 || len(distribution.BurrowIDs) > 1<<16 {
			return fmt.Errorf("distribution %s has an invalid Burrow owner dictionary", distribution.ID)
		}
		seenOwner := make(map[string]struct{}, len(distribution.BurrowIDs))
		for _, owner := range distribution.BurrowIDs {
			if _, duplicate := seenOwner[owner]; duplicate {
				return fmt.Errorf("distribution %s repeats Burrow owner %q", distribution.ID, owner)
			}
			burrow, ok := burrows[owner]
			if !ok {
				return fmt.Errorf("distribution %s has unknown owner %q", distribution.ID, owner)
			}
			if burrow.State != BurrowReady && burrow.State != BurrowDraining {
				return fmt.Errorf("distribution %s is owned by Burrow %s in state %s", distribution.ID, burrow.Name, burrow.State)
			}
			seenOwner[owner] = struct{}{}
		}
		for vshard, ownerIndex := range distribution.OwnerIndexes {
			if int(ownerIndex) >= len(distribution.BurrowIDs) {
				return fmt.Errorf("distribution %s vshard %d has unknown owner index %d", distribution.ID, vshard, ownerIndex)
			}
		}
		distributions[distribution.ID] = distribution
	}
	for table, distributionID := range c.TableDistributions {
		if strings.TrimSpace(table) == "" {
			return fmt.Errorf("topology contains an empty table name")
		}
		if _, ok := distributions[distributionID]; !ok {
			return fmt.Errorf("table %s references unknown distribution %q", table, distributionID)
		}
	}
	return nil
}

func (c Catalog) ValidateSchema(registry schema.Registry) error {
	if c.Schema.RegistryFormat > schema.CurrentFormatVersion {
		return fmt.Errorf("topology requires schema format %d, Proxy supports %d", c.Schema.RegistryFormat, schema.CurrentFormatVersion)
	}
	if registry.Revision() < c.Schema.Revision {
		return fmt.Errorf("topology revision %d requires schema revision %d, have %d", c.Revision, c.Schema.Revision, registry.Revision())
	}
	if fingerprint := registry.Fingerprint(); fingerprint != c.Schema.Fingerprint {
		return fmt.Errorf("topology revision %d is incompatible with schema fingerprint %s", c.Revision, fingerprint)
	}
	for _, table := range registry.CanonicalShardedTables() {
		if _, ok := c.TableDistributions[table]; !ok {
			return fmt.Errorf("sharded table %s has no topology distribution", table)
		}
	}
	for table := range c.TableDistributions {
		if !registry.IsSharded(table) {
			return fmt.Errorf("topology table %s is not sharded in schema revision %d", table, registry.Revision())
		}
	}
	return nil
}

func (c Catalog) ValidateConfigured(configured map[string]string) error {
	for _, burrow := range c.Burrows {
		if burrow.State != BurrowReady && burrow.State != BurrowDraining {
			continue
		}
		dsn, ok := configured[burrow.Name]
		if !ok {
			return fmt.Errorf("topology Burrow %s is not configured for this Proxy", burrow.Name)
		}
		address, fingerprint := tunnelMetadata(dsn)
		if len(burrow.Tunnels) == 0 || burrow.Tunnels[0].Address != address || burrow.Tunnels[0].ConfigurationFingerprint != fingerprint {
			return fmt.Errorf("configured Tunnel for Burrow %s differs from topology revision %d", burrow.Name, c.Revision)
		}
	}
	return nil
}

func tunnelMetadata(dsn string) (string, string) {
	sum := sha256.Sum256([]byte(dsn))
	fingerprint := fmt.Sprintf("sha256:%x", sum[:])
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Host == "" {
		return "configured-endpoint", fingerprint
	}
	address := parsed.Host + strings.TrimSuffix(parsed.EscapedPath(), "/")
	return address, fingerprint
}

func (c Catalog) TablePlacements() (map[string][]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	burrowNames := make(map[string]string, len(c.Burrows))
	for _, burrow := range c.Burrows {
		burrowNames[burrow.ID] = burrow.Name
	}
	distributions := make(map[string]Distribution, len(c.Distributions))
	for _, distribution := range c.Distributions {
		distributions[distribution.ID] = distribution
	}
	placements := make(map[string][]string, len(c.TableDistributions))
	for table, distributionID := range c.TableDistributions {
		distribution := distributions[distributionID]
		owners := make([]string, len(distribution.OwnerIndexes))
		for index, ownerIndex := range distribution.OwnerIndexes {
			owners[index] = burrowNames[distribution.BurrowIDs[ownerIndex]]
		}
		placements[table] = owners
	}
	return placements, nil
}

func (c Catalog) RoutableBurrowNames() []string {
	names := make([]string, 0, len(c.Burrows))
	for _, burrow := range c.Burrows {
		if burrow.State == BurrowReady || burrow.State == BurrowDraining {
			names = append(names, burrow.Name)
		}
	}
	sort.Strings(names)
	return names
}

func ValidateTransition(previous, next Catalog) error {
	if err := previous.Validate(); err != nil {
		return fmt.Errorf("current topology is invalid: %w", err)
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if next.Revision != previous.Revision+1 {
		return fmt.Errorf("topology revision must advance from %d to %d", previous.Revision, previous.Revision+1)
	}
	previousBurrows := make(map[string]Burrow, len(previous.Burrows))
	for _, burrow := range previous.Burrows {
		previousBurrows[burrow.ID] = burrow
	}
	for _, burrow := range next.Burrows {
		old, exists := previousBurrows[burrow.ID]
		if !exists {
			if burrow.State != BurrowAdding {
				return fmt.Errorf("new Burrow %s must start in state adding", burrow.Name)
			}
			continue
		}
		delete(previousBurrows, burrow.ID)
		if !validTransition(old.State, burrow.State) {
			return fmt.Errorf("invalid Burrow %s state transition %s -> %s", burrow.Name, old.State, burrow.State)
		}
	}
	if len(previousBurrows) != 0 {
		return fmt.Errorf("Burrows must transition to removed before leaving the catalog")
	}
	previousDistributions := make(map[string]Distribution, len(previous.Distributions))
	for _, distribution := range previous.Distributions {
		previousDistributions[distribution.ID] = distribution
	}
	for _, distribution := range next.Distributions {
		if old, exists := previousDistributions[distribution.ID]; exists && old.VShardCount != distribution.VShardCount {
			return fmt.Errorf("distribution %s cannot change vshard count from %d to %d", distribution.ID, old.VShardCount, distribution.VShardCount)
		}
	}
	return nil
}

func validState(state BurrowState) bool {
	return state == BurrowAdding || state == BurrowReady || state == BurrowDraining || state == BurrowRemoved
}

func validTransition(from, to BurrowState) bool {
	if from == to {
		return true
	}
	switch from {
	case BurrowAdding:
		return to == BurrowReady || to == BurrowRemoved
	case BurrowReady:
		return to == BurrowDraining
	case BurrowDraining:
		return to == BurrowReady || to == BurrowRemoved
	case BurrowRemoved:
		return false
	default:
		return false
	}
}
