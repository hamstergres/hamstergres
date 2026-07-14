// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"fmt"
	"os"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultStatusAddress             = "127.0.0.1:8080"
	DefaultBackendPoolMaxConnections = 8
	DefaultRuntimeMaxProcs           = 4
	DefaultTransactionLockTimeout    = "1s"
	DefaultTopologyKey               = "/hamstergres/topology/v1"
)

const (
	UnshardedPrimary    = "primary"
	UnshardedReplicated = "replicated"
)

// Config is the static development configuration for a gateway instance.
type Config struct {
	Runtime struct {
		MaxProcs int `yaml:"max_procs"`
	} `yaml:"runtime"`
	Listen struct {
		Address string `yaml:"address"`
	} `yaml:"listen"`
	Status struct {
		Address   string `yaml:"address"`
		Profiling bool   `yaml:"profiling"`
	} `yaml:"status"`
	Nest struct {
		Endpoint    string `yaml:"endpoint"`
		RegistryKey string `yaml:"registry_key"`
		TopologyKey string `yaml:"topology_key"`
		SequenceKey string `yaml:"sequence_key"`
	} `yaml:"nest"`
	Observability struct {
		LogFile string `yaml:"log_file"`
	} `yaml:"observability"`
	Transactions struct {
		TwoPhaseCommit *bool  `yaml:"two_phase_commit"`
		LockTimeout    string `yaml:"lock_timeout"`
	} `yaml:"transactions"`
	Sharding struct {
		PhysicalShards map[string]Shard `yaml:"physical_shards"`
		BackendPool    struct {
			MaxConnections int32 `yaml:"max_connections"`
		} `yaml:"backend_pool"`
		Unsharded struct {
			Mode          string `yaml:"mode"`
			PrimaryBurrow string `yaml:"primary_burrow"`
		} `yaml:"unsharded_tables"`
	} `yaml:"sharding"`
}

// TwoPhaseCommitEnabled defaults to the safe distributed-commit behavior.
// Operators may explicitly disable it when they accept partial cross-Burrow
// commits in exchange for avoiding PostgreSQL prepared transactions.
func (c Config) TwoPhaseCommitEnabled() bool {
	return c.Transactions.TwoPhaseCommit == nil || *c.Transactions.TwoPhaseCommit
}

type Shard struct {
	DSN string `yaml:"dsn"`
}

func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(contents, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	if cfg.Listen.Address == "" {
		return Config{}, fmt.Errorf("config %q: listen.address is required", path)
	}
	if cfg.Status.Address == "" {
		cfg.Status.Address = DefaultStatusAddress
	}
	if cfg.Runtime.MaxProcs < 0 {
		return Config{}, fmt.Errorf("config %q: runtime.max_procs must not be negative", path)
	}
	if cfg.Transactions.LockTimeout == "" {
		cfg.Transactions.LockTimeout = DefaultTransactionLockTimeout
	}
	lockTimeout, err := time.ParseDuration(cfg.Transactions.LockTimeout)
	if err != nil || lockTimeout <= 0 {
		return Config{}, fmt.Errorf("config %q: transactions.lock_timeout must be a positive duration", path)
	}
	if cfg.Nest.RegistryKey == "" {
		cfg.Nest.RegistryKey = "/hamstergres/schema-registry/v3"
	}
	if cfg.Nest.TopologyKey == "" {
		cfg.Nest.TopologyKey = DefaultTopologyKey
	}
	if cfg.Nest.SequenceKey == "" {
		cfg.Nest.SequenceKey = "/hamstergres/sequences/global-id/v1"
	}
	if len(cfg.Sharding.PhysicalShards) == 0 {
		return Config{}, fmt.Errorf("config %q: at least one physical Burrow is required", path)
	}
	if cfg.Sharding.BackendPool.MaxConnections < 0 {
		return Config{}, fmt.Errorf("config %q: sharding.backend_pool.max_connections must be greater than zero", path)
	}
	if cfg.Sharding.BackendPool.MaxConnections == 0 {
		cfg.Sharding.BackendPool.MaxConnections = DefaultBackendPoolMaxConnections
	}
	for name, shard := range cfg.Sharding.PhysicalShards {
		if shard.DSN == "" {
			return Config{}, fmt.Errorf("config %q: physical Burrow %q has no dsn", path, name)
		}
	}
	if cfg.Sharding.Unsharded.Mode == "" {
		cfg.Sharding.Unsharded.Mode = UnshardedPrimary
	}
	if cfg.Sharding.Unsharded.Mode != UnshardedPrimary && cfg.Sharding.Unsharded.Mode != UnshardedReplicated {
		return Config{}, fmt.Errorf("config %q: sharding.unsharded_tables.mode must be %q or %q", path, UnshardedPrimary, UnshardedReplicated)
	}
	if cfg.Sharding.Unsharded.Mode == UnshardedPrimary {
		if cfg.Sharding.Unsharded.PrimaryBurrow == "" {
			cfg.Sharding.Unsharded.PrimaryBurrow = cfg.ShardNames()[0]
		}
		if _, ok := cfg.Sharding.PhysicalShards[cfg.Sharding.Unsharded.PrimaryBurrow]; !ok {
			return Config{}, fmt.Errorf("config %q: unsharded primary Burrow %q is not configured", path, cfg.Sharding.Unsharded.PrimaryBurrow)
		}
	}
	return cfg, nil
}

// TransactionLockTimeout bounds each PostgreSQL lock wait. Independent
// Burrows cannot detect a distributed lock cycle, so a finite timeout turns it
// into a retryable transaction error instead of an indefinitely blocked Proxy.
func (c Config) TransactionLockTimeout() string {
	if c.Transactions.LockTimeout == "" {
		return DefaultTransactionLockTimeout
	}
	return c.Transactions.LockTimeout
}

// RuntimeMaxProcs returns the configured Go scheduler width. The Proxy defaults
// to four execution threads so routing can keep up with concurrent Burrows. An
// explicit configuration value overrides GOMAXPROCS; otherwise GOMAXPROCS
// retains precedence over the default.
func (c Config) RuntimeMaxProcs() int {
	if c.Runtime.MaxProcs > 0 {
		return c.Runtime.MaxProcs
	}
	if _, configured := os.LookupEnv("GOMAXPROCS"); configured {
		return 0
	}
	return DefaultRuntimeMaxProcs
}

// BackendPoolMaxConnections returns the per-Burrow connection limit. Config
// values built directly by tests and embedders receive the same default as a
// file loaded through Load.
func (c Config) BackendPoolMaxConnections() int32 {
	if c.Sharding.BackendPool.MaxConnections > 0 {
		return c.Sharding.BackendPool.MaxConnections
	}
	return DefaultBackendPoolMaxConnections
}

// ShardNames returns a stable ordering for fan-out operations and reporting.
func (c Config) ShardNames() []string {
	names := make([]string, 0, len(c.Sharding.PhysicalShards))
	for name := range c.Sharding.PhysicalShards {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
