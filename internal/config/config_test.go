package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesDefaultStatusAddress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hamstergres.yaml")
	contents := []byte("listen:\n  address: 127.0.0.1:6432\nsharding:\n  physical_shards:\n    burrow-01:\n      dsn: postgres://example\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if config.Status.Address != DefaultStatusAddress {
		t.Fatalf("status address = %q, want %q", config.Status.Address, DefaultStatusAddress)
	}
	if config.BackendPoolMaxConnections() != DefaultBackendPoolMaxConnections {
		t.Fatalf("backend pool maximum = %d, want %d", config.BackendPoolMaxConnections(), DefaultBackendPoolMaxConnections)
	}
	if config.Nest.RegistryKey != "/hamstergres/schema-registry/v3" {
		t.Fatalf("registry key = %q", config.Nest.RegistryKey)
	}
	if got := config.ShardNames(); len(got) != 1 || got[0] != "burrow-01" {
		t.Fatalf("ShardNames() = %#v, want burrow-01", got)
	}
	if config.Sharding.Unsharded.Mode != UnshardedPrimary || config.Sharding.Unsharded.PrimaryBurrow != "burrow-01" {
		t.Fatalf("unsharded defaults = %#v", config.Sharding.Unsharded)
	}
}

func TestLoadConfiguresBackendPoolCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hamstergres.yaml")
	contents := []byte("listen:\n  address: 127.0.0.1:6432\nsharding:\n  backend_pool:\n    max_connections: 32\n  physical_shards:\n    burrow-01:\n      dsn: postgres://example\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.BackendPoolMaxConnections(); got != 32 {
		t.Fatalf("backend pool maximum = %d, want 32", got)
	}
}

func TestLoadValidatesUnshardedPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hamstergres.yaml")
	contents := []byte("listen:\n  address: 127.0.0.1:6432\nsharding:\n  unsharded_tables:\n    mode: replicated\n  physical_shards:\n    burrow-01:\n      dsn: postgres://example\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sharding.Unsharded.Mode != UnshardedReplicated {
		t.Fatalf("mode = %q", cfg.Sharding.Unsharded.Mode)
	}
}

func TestLoadEnablesStatusProfilingExplicitly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hamstergres.yaml")
	contents := []byte("listen:\n  address: 127.0.0.1:6432\nstatus:\n  profiling: true\nsharding:\n  physical_shards:\n    burrow-01:\n      dsn: postgres://example\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Status.Profiling {
		t.Fatal("status profiling was not enabled")
	}
}

func TestRuntimeMaxProcsUsesConservativeDefault(t *testing.T) {
	previous, configured := os.LookupEnv("GOMAXPROCS")
	if err := os.Unsetenv("GOMAXPROCS"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if configured {
			_ = os.Setenv("GOMAXPROCS", previous)
		} else {
			_ = os.Unsetenv("GOMAXPROCS")
		}
	})
	var cfg Config
	if got := cfg.RuntimeMaxProcs(); got != DefaultRuntimeMaxProcs {
		t.Fatalf("RuntimeMaxProcs = %d, want default %d", got, DefaultRuntimeMaxProcs)
	}
}

func TestRuntimeMaxProcsRespectsExplicitConfigurationAndEnvironment(t *testing.T) {
	var cfg Config
	cfg.Runtime.MaxProcs = 3
	if got := cfg.RuntimeMaxProcs(); got != 3 {
		t.Fatalf("configured RuntimeMaxProcs = %d, want 3", got)
	}
	cfg.Runtime.MaxProcs = 0
	t.Setenv("GOMAXPROCS", "8")
	if got := cfg.RuntimeMaxProcs(); got != 0 {
		t.Fatalf("environment-controlled RuntimeMaxProcs = %d, want 0", got)
	}
}

func TestLoadRejectsIncompleteConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hamstergres.yaml")
	if err := os.WriteFile(path, []byte("listen:\n  address: 127.0.0.1:6432\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load accepted a configuration with no physical Burrows")
	}
}

func TestTwoPhaseCommitDefaultsOnAndCanBeDisabled(t *testing.T) {
	var config Config
	if !config.TwoPhaseCommitEnabled() {
		t.Fatal("two-phase commit should default to enabled")
	}
	disabled := false
	config.Transactions.TwoPhaseCommit = &disabled
	if config.TwoPhaseCommitEnabled() {
		t.Fatal("two-phase commit remained enabled after explicit disable")
	}
}
