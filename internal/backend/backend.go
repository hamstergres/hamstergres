// SPDX-License-Identifier: AGPL-3.0-only

package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jruszo/hamstergres/internal/config"
	"github.com/jruszo/hamstergres/internal/nest"
	"github.com/jruszo/hamstergres/internal/schema"
	"github.com/jruszo/hamstergres/internal/statistics"
	"github.com/jruszo/hamstergres/internal/topology"
)

// Result is a PostgreSQL result set in the wire format used by the frontend.
type Result struct {
	Fields     []pgproto3.FieldDescription
	Rows       [][][]byte
	CommandTag string
}

type shard struct {
	name string
	dsn  string
	pool *pgxpool.Pool
	last shardHealth
}

type shardHealth struct {
	checkedAt time.Time
	error     string
}

// Manager owns one connection pool per physical shard.
type Manager struct {
	shards           []*shard
	names            []string
	mu               sync.RWMutex
	fleetWriteGate   chan struct{}
	metrics          *statistics.Collector
	schemaMu         sync.RWMutex
	schema           schema.Registry
	globalIDs        *nest.SequenceStore
	registryStore    *nest.RegistryStore
	topologyStore    *nest.TopologyStore
	topologyRevision uint64
	preparedMu       sync.Mutex
	prepared         map[string]map[string]struct{}
	runtimeParamsMu  sync.Mutex
	runtimeParams    map[string]*runtimeParamState
	unshardedMode    string
	primaryBurrow    string
}

type runtimeParamState struct {
	baseline map[string]string
	current  map[string]string
}

func New(ctx context.Context, cfg config.Config) (*Manager, error) {
	mode := cfg.Sharding.Unsharded.Mode
	if mode == "" {
		mode = config.UnshardedPrimary
	}
	primary := cfg.Sharding.Unsharded.PrimaryBurrow
	if primary == "" && len(cfg.ShardNames()) > 0 {
		primary = cfg.ShardNames()[0]
	}
	m := &Manager{metrics: statistics.NewCollector(), fleetWriteGate: newFleetWriteGate(), prepared: make(map[string]map[string]struct{}), runtimeParams: make(map[string]*runtimeParamState), unshardedMode: mode, primaryBurrow: primary}
	for _, name := range cfg.ShardNames() {
		poolConfig, err := pgxpool.ParseConfig(cfg.Sharding.PhysicalShards[name].DSN)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("parse dsn for shard %q: %w", name, err)
		}
		poolConfig.MaxConns = cfg.BackendPoolMaxConnections()
		if poolConfig.ConnConfig.RuntimeParams == nil {
			poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
		}
		poolConfig.ConnConfig.RuntimeParams["DateStyle"] = "ISO, MDY"
		poolConfig.ConnConfig.RuntimeParams["IntervalStyle"] = "postgres"
		poolConfig.ConnConfig.RuntimeParams["TimeZone"] = "UTC"
		poolConfig.ConnConfig.RuntimeParams["standard_conforming_strings"] = "on"
		poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = cfg.TransactionLockTimeout()
		afterRelease := poolConfig.AfterRelease
		poolConfig.AfterRelease = func(connection *pgx.Conn) bool {
			if afterRelease != nil && !afterRelease(connection) {
				return false
			}
			// Every frontend is told that standard-conforming strings are on.
			// Never return a physical connection with conflicting parser state to
			// another frontend session, even if a state-changing query escaped the
			// affinity path.
			return connection.PgConn().ParameterStatus("standard_conforming_strings") == "on"
		}
		beforeClose := poolConfig.BeforeClose
		poolConfig.BeforeClose = func(connection *pgx.Conn) {
			m.forgetPreparedConnection(name, connection.PgConn().PID())
			m.forgetRuntimeParams(name, connection.PgConn().PID())
			if beforeClose != nil {
				beforeClose(connection)
			}
		}
		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("create pool for shard %q: %w", name, err)
		}
		m.shards = append(m.shards, &shard{name: name, dsn: cfg.Sharding.PhysicalShards[name].DSN, pool: pool})
		m.names = append(m.names, name)
	}
	var storedTopology nest.StoredTopology
	topologyFound := false
	var err error
	if cfg.Nest.Endpoint != "" {
		topologyKey := cfg.Nest.TopologyKey
		if topologyKey == "" {
			topologyKey = config.DefaultTopologyKey
		}
		m.topologyStore = nest.NewTopologyStore(cfg.Nest.Endpoint, topologyKey)
		storedTopology, topologyFound, err = m.topologyStore.Get(ctx)
		if err != nil {
			m.Close()
			return nil, err
		}
		if topologyFound {
			if err := storedTopology.Catalog.ValidateConfigured(configuredDSNs(cfg)); err != nil {
				m.Close()
				return nil, err
			}
			if err := m.retainActiveBurrows(storedTopology.Catalog.RoutableBurrowNames()); err != nil {
				m.Close()
				return nil, err
			}
		}
	}
	registry, err := m.loadSchema(ctx)
	if err != nil {
		m.Close()
		return nil, err
	}
	if cfg.Nest.Endpoint != "" {
		m.registryStore = nest.NewRegistryStore(cfg.Nest.Endpoint, cfg.Nest.RegistryKey)
		verified, err := m.registryStore.VerifyOrSeedVersioned(ctx, registry)
		if err != nil {
			m.Close()
			return nil, err
		}
		registry = verified.Registry
		if !topologyFound {
			bootstrapBurrows := make([]topology.BootstrapBurrow, 0, len(cfg.Sharding.PhysicalShards))
			for _, name := range cfg.ShardNames() {
				bootstrapBurrows = append(bootstrapBurrows, topology.BootstrapBurrow{Name: name, DSN: cfg.Sharding.PhysicalShards[name].DSN})
			}
			bootstrap, err := topology.Bootstrap(registry, bootstrapBurrows, verified.LegacyVShardOwners, time.Now())
			if err != nil {
				m.Close()
				return nil, err
			}
			storedTopology, err = m.topologyStore.VerifyOrBootstrap(ctx, bootstrap)
			if err != nil {
				m.Close()
				return nil, err
			}
		}
		if err := storedTopology.Catalog.ValidateSchema(registry); err != nil {
			m.Close()
			return nil, err
		}
		if err := storedTopology.Catalog.ValidateConfigured(configuredDSNs(cfg)); err != nil {
			m.Close()
			return nil, err
		}
		if err := m.retainActiveBurrows(storedTopology.Catalog.RoutableBurrowNames()); err != nil {
			m.Close()
			return nil, err
		}
		placements, err := storedTopology.Catalog.TablePlacements()
		if err != nil {
			m.Close()
			return nil, err
		}
		registry = registry.WithTableVShards(placements)
		m.topologyRevision = storedTopology.Catalog.Revision
		if len(verified.LegacyVShardOwners) != 0 {
			if err := m.registryStore.PersistVerified(ctx, registry); err != nil {
				m.Close()
				return nil, err
			}
		}
		m.globalIDs = nest.NewSequenceStore(cfg.Nest.Endpoint, cfg.Nest.SequenceKey)
	} else {
		registry = registry.WithVShards(topology.LegacyModuloOwners(m.names, topology.DefaultVShardCount))
	}
	m.schema = registry
	if m.unshardedMode == config.UnshardedPrimary && !containsName(m.names, m.primaryBurrow) {
		m.Close()
		return nil, fmt.Errorf("unsharded primary Burrow %q is not routable in topology revision %d", m.primaryBurrow, m.topologyRevision)
	}
	return m, nil
}

func configuredDSNs(cfg config.Config) map[string]string {
	configured := make(map[string]string, len(cfg.Sharding.PhysicalShards))
	for name, burrow := range cfg.Sharding.PhysicalShards {
		configured[name] = burrow.DSN
	}
	return configured
}

func containsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func (m *Manager) retainActiveBurrows(names []string) error {
	active := make(map[string]struct{}, len(names))
	for _, name := range names {
		active[name] = struct{}{}
	}
	kept := make([]*shard, 0, len(names))
	for _, burrow := range m.shards {
		if _, ok := active[burrow.name]; ok {
			kept = append(kept, burrow)
			delete(active, burrow.name)
			continue
		}
		burrow.pool.Close()
	}
	if len(active) != 0 {
		missing := make([]string, 0, len(active))
		for name := range active {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return fmt.Errorf("topology references unconfigured routable Burrows: %s", strings.Join(missing, ", "))
	}
	m.shards = kept
	m.names = append([]string(nil), names...)
	return nil
}

const primaryKeyQuery = `
SELECT n.nspname, c.relname, a.attname, key_column.ordinality,
	   a.attidentity::text, COALESCE(pg_get_expr(ad.adbin, ad.adrelid), ''),
       format_type(a.atttypid, a.atttypmod)
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN unnest(i.indkey) WITH ORDINALITY AS key_column(attnum, ordinality) ON true
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = key_column.attnum
LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
WHERE i.indisprimary
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_'
ORDER BY n.nspname, c.relname, key_column.ordinality`

const shardKeyQuery = `
SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE a.attnum > 0 AND NOT a.attisdropped
  AND col_description(c.oid, a.attnum) = 'hamstergres.shard_key'
  AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname, c.relname, a.attnum`

const tableInventoryQuery = `
SELECT n.nspname || '.' || c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname, c.relname`

func (m *Manager) loadSchema(ctx context.Context) (schema.Registry, error) {
	var expected schema.Registry
	for index, shard := range m.shards {
		rows, err := shard.pool.Query(ctx, primaryKeyQuery)
		if err != nil {
			return schema.Registry{}, fmt.Errorf("inspect schema on Burrow %s: %w", shard.name, err)
		}
		primaryKeys := make(map[string][]string)
		generated := make(map[string]schema.GeneratedPrimary)
		for rows.Next() {
			var namespace, table, column, identity, defaultExpression, dataType string
			var ordinality int
			if err := rows.Scan(&namespace, &table, &column, &ordinality, &identity, &defaultExpression, &dataType); err != nil {
				rows.Close()
				return schema.Registry{}, fmt.Errorf("read schema on Burrow %s: %w", shard.name, err)
			}
			qualified := namespace + "." + table
			primaryKeys[qualified] = append(primaryKeys[qualified], column)
			if namespace == "public" {
				primaryKeys[table] = append(primaryKeys[table], column)
			}
			kind := ""
			if identity != "" {
				kind = "identity"
			} else if strings.HasPrefix(defaultExpression, "nextval(") {
				kind = "sequence"
			}
			if kind != "" && (dataType == "bigint" || dataType == "integer" || dataType == "smallint") {
				value := schema.GeneratedPrimary{Column: column, Kind: kind}
				generated[qualified] = value
				if namespace == "public" {
					generated[table] = value
				}
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return schema.Registry{}, fmt.Errorf("read schema on Burrow %s: %w", shard.name, err)
		}
		rows.Close()
		// A generated component of a composite key cannot be safely synthesized
		// without values for every other component, so expose only single-column
		// generated primary keys to the rewrite path.
		for table := range generated {
			if len(primaryKeys[table]) != 1 {
				delete(generated, table)
			}
		}
		keyRows, err := shard.pool.Query(ctx, shardKeyQuery)
		if err != nil {
			return schema.Registry{}, fmt.Errorf("inspect shard keys on Burrow %s: %w", shard.name, err)
		}
		shardKeys := make(map[string][]string)
		shardKeyTypes := make(map[string][]string)
		for keyRows.Next() {
			var namespace, table, column, dataType string
			if err := keyRows.Scan(&namespace, &table, &column, &dataType); err != nil {
				keyRows.Close()
				return schema.Registry{}, err
			}
			appendShardKey(shardKeys, shardKeyTypes, namespace, table, column, dataType)
		}
		if err := keyRows.Err(); err != nil {
			keyRows.Close()
			return schema.Registry{}, err
		}
		keyRows.Close()
		inventoryRows, err := shard.pool.Query(ctx, tableInventoryQuery)
		if err != nil {
			return schema.Registry{}, fmt.Errorf("inspect table inventory on Burrow %s: %w", shard.name, err)
		}
		var allTables []string
		for inventoryRows.Next() {
			var table string
			if err := inventoryRows.Scan(&table); err != nil {
				inventoryRows.Close()
				return schema.Registry{}, err
			}
			allTables = append(allTables, table)
		}
		if err := inventoryRows.Err(); err != nil {
			inventoryRows.Close()
			return schema.Registry{}, err
		}
		inventoryRows.Close()
		for table, value := range generated {
			keys := shardKeys[table]
			if len(keys) != 1 || keys[0] != value.Column {
				delete(generated, table)
			}
		}
		registry := schema.NewWithGeneratedAndTypes(shardKeys, generated, shardKeyTypes).WithAllTables(allTables)
		if index == 0 {
			expected = registry
			continue
		}
		if err := expected.Equal(registry); err != nil {
			return schema.Registry{}, fmt.Errorf("schema registry mismatch at Burrow %s: %w", shard.name, err)
		}
	}
	return expected, nil
}

func appendShardKey(keys, types map[string][]string, namespace, table, column, dataType string) {
	qualified := namespace + "." + table
	keys[qualified] = append(keys[qualified], column)
	types[qualified] = append(types[qualified], dataType)
	if namespace == "public" {
		keys[table] = append(keys[table], column)
		types[table] = append(types[table], dataType)
	}
}

// Session owns lazy PostgreSQL affinity connections for the lifetime of a
// frontend session. PostgreSQL prepared statements and portals live on a
// backend connection, so a Burrow is acquired only when that Burrow first
// participates and is then kept stable until the session is released.
type Session struct {
	shards           map[string]*sessionShard
	runtimeParams    map[string]string
	replaySQL        []string
	fleetWriteGate   chan struct{}
	fleetWriteLocked bool
	ctx              context.Context
	manager          *Manager
}

type sessionShard struct {
	name            string
	conn            *pgconn.PgConn
	pooled          *pgxpool.Conn
	stopCancelWatch chan struct{}
	cancelWatchDone chan struct{}
}

// NewSession creates an empty affinity session. Connections are acquired on
// first use by SendTo or SendBatchTo, so parsing, validation, and routed work do
// not contact unrelated Burrows. The caller must close the returned session.
func (m *Manager) NewSession(ctx context.Context, runtimeParams map[string]string, replaySQL ...[]string) (*Session, error) {
	var replay []string
	if len(replaySQL) > 0 {
		replay = append([]string(nil), replaySQL[0]...)
	}
	return &Session{shards: make(map[string]*sessionShard), runtimeParams: runtimeParams, replaySQL: replay, fleetWriteGate: m.fleetWriteGate, ctx: ctx, manager: m}, nil
}

func (s *Session) acquire(name string) (*sessionShard, error) {
	if shard := s.shardByName(name); shard != nil {
		return shard, nil
	}
	if s.manager == nil {
		return nil, fmt.Errorf("unknown Burrow %s", name)
	}
	var target *shard
	for _, candidate := range s.manager.shards {
		if candidate.name == name {
			target = candidate
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("unknown Burrow %s", name)
	}
	pooled, err := target.pool.Acquire(s.Context())
	if err != nil {
		s.manager.RecordOperation("backend_connection", "failure")
		slog.Error("Burrow session connection failed", "event", "backend_connection_failed", "component", "hamstergres-proxy", "burrow", target.name, "error_category", "burrow_unavailable", "error", err)
		return nil, fmt.Errorf("connect session to Burrow %s: %w", target.name, err)
	}
	if err := s.manager.syncPreparedStatements(s.Context(), target.name, pooled.Conn()); err != nil {
		s.manager.discardPooledConnection(target.name, pooled)
		s.manager.RecordOperation("backend_connection", "failure")
		return nil, fmt.Errorf("synchronize prepared statements on Burrow %s: %w", target.name, err)
	}
	// Reconcile prepared state while the Tunnel still has its clean pool
	// baseline. Some frontend settings (notably standard_conforming_strings=off)
	// make pgx's simple-protocol helpers intentionally unavailable.
	if err := s.manager.applyRuntimeParams(s.Context(), target.name, pooled, s.runtimeParams); err != nil {
		s.manager.RecordOperation("backend_connection", "failure")
		return nil, err
	}
	if err := replaySessionSettings(s.Context(), pooled.Conn().PgConn(), s.replaySQL); err != nil {
		s.manager.discardPooledConnection(target.name, pooled)
		s.manager.RecordOperation("backend_connection", "failure")
		return nil, fmt.Errorf("replay frontend session state on Burrow %s: %w", target.name, err)
	}
	connection := &sessionShard{name: target.name, conn: pooled.Conn().PgConn(), pooled: pooled}
	connection.watchCancellation(s.Context())
	s.shards[target.name] = connection
	s.manager.RecordOperation("backend_connection", "success")
	return connection, nil
}

func (m *Manager) applyRuntimeParams(ctx context.Context, burrow string, connection *pgxpool.Conn, desired map[string]string) error {
	connectionKey := preparedConnectionKey(burrow, connection.Conn().PgConn().PID())
	if err := m.ensureRuntimeParamBaselines(ctx, connectionKey, connection, desired); err != nil {
		m.discardRuntimeParamConnection(burrow, connection)
		return fmt.Errorf("inspect frontend settings on Burrow %s: %w", burrow, err)
	}
	changed := m.changedRuntimeParams(connectionKey, desired)
	names := make([]string, 0, len(changed))
	for name := range changed {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := changed[name]
		if _, err := connection.Exec(ctx, "SELECT set_config($1, $2, false)", name, value); err != nil {
			m.discardRuntimeParamConnection(burrow, connection)
			return fmt.Errorf("apply frontend setting %s on Burrow %s: %w", name, burrow, err)
		}
		m.rememberRuntimeParam(connectionKey, name, value)
	}
	return nil
}

func (m *Manager) discardRuntimeParamConnection(burrow string, pooled *pgxpool.Conn) {
	m.discardPooledConnection(burrow, pooled)
}

func (m *Manager) discardPooledConnection(burrow string, pooled *pgxpool.Conn) {
	connection := pooled.Hijack()
	m.forgetPreparedConnection(burrow, connection.PgConn().PID())
	m.forgetRuntimeParams(burrow, connection.PgConn().PID())
	_ = connection.Close(context.Background())
}

func replaySessionSettings(ctx context.Context, connection *pgconn.PgConn, statements []string) error {
	for _, statement := range statements {
		if _, err := connection.Exec(ctx, statement).ReadAll(); err != nil {
			return fmt.Errorf("%s: %w", statement, err)
		}
	}
	return nil
}

func (m *Manager) ensureRuntimeParamBaselines(ctx context.Context, connectionKey string, connection *pgxpool.Conn, desired map[string]string) error {
	m.runtimeParamsMu.Lock()
	state := m.runtimeParams[connectionKey]
	missing := make([]string, 0, len(desired))
	for name := range desired {
		if state == nil {
			missing = append(missing, name)
			continue
		}
		if _, ok := state.baseline[name]; !ok {
			missing = append(missing, name)
		}
	}
	m.runtimeParamsMu.Unlock()
	sort.Strings(missing)
	for _, name := range missing {
		var value string
		if err := connection.QueryRow(ctx, "SELECT current_setting($1)", name).Scan(&value); err != nil {
			return fmt.Errorf("inspect default for %s: %w", name, err)
		}
		m.rememberRuntimeParamBaseline(connectionKey, name, value)
	}
	return nil
}

func (m *Manager) changedRuntimeParams(connectionKey string, desired map[string]string) map[string]string {
	m.runtimeParamsMu.Lock()
	defer m.runtimeParamsMu.Unlock()
	state := m.runtimeParams[connectionKey]
	effective := make(map[string]string, len(desired))
	if state != nil {
		for name, value := range state.baseline {
			effective[name] = value
		}
	}
	for name, value := range desired {
		effective[name] = value
	}
	changed := make(map[string]string)
	for name, value := range effective {
		current := ""
		if state != nil {
			current = state.current[name]
		}
		if current != value {
			changed[name] = value
		}
	}
	return changed
}

func (m *Manager) rememberRuntimeParamBaseline(connectionKey, name, value string) {
	m.runtimeParamsMu.Lock()
	defer m.runtimeParamsMu.Unlock()
	if m.runtimeParams == nil {
		m.runtimeParams = make(map[string]*runtimeParamState)
	}
	if m.runtimeParams[connectionKey] == nil {
		m.runtimeParams[connectionKey] = &runtimeParamState{baseline: make(map[string]string), current: make(map[string]string)}
	}
	state := m.runtimeParams[connectionKey]
	if _, exists := state.baseline[name]; !exists {
		state.baseline[name] = value
		state.current[name] = value
	}
}

func (m *Manager) rememberRuntimeParam(connectionKey, name, value string) {
	m.runtimeParamsMu.Lock()
	defer m.runtimeParamsMu.Unlock()
	if m.runtimeParams == nil {
		m.runtimeParams = make(map[string]*runtimeParamState)
	}
	if m.runtimeParams[connectionKey] == nil {
		m.runtimeParams[connectionKey] = &runtimeParamState{baseline: make(map[string]string), current: make(map[string]string)}
	}
	m.runtimeParams[connectionKey].current[name] = value
}

func (m *Manager) forgetRuntimeParams(name string, processID uint32) {
	m.runtimeParamsMu.Lock()
	defer m.runtimeParamsMu.Unlock()
	delete(m.runtimeParams, preparedConnectionKey(name, processID))
}

// Ensure acquires one Burrow affinity connection without sending a frontend
// protocol message. It is used before checking connection-local prepared state.
func (s *Session) Ensure(name string) error {
	_, err := s.acquire(name)
	return err
}

// ConnectedNames returns acquired Burrows in configured order.
func (s *Session) ConnectedNames() []string {
	if s == nil || s.manager == nil {
		return nil
	}
	names := make([]string, 0, len(s.shards))
	for _, shard := range s.manager.shards {
		if s.shards[shard.name] != nil {
			names = append(names, shard.name)
		}
	}
	return names
}

func (m *Manager) syncPreparedStatements(ctx context.Context, burrow string, connection *pgx.Conn) error {
	key := preparedConnectionKey(burrow, connection.PgConn().PID())
	m.preparedMu.Lock()
	_, synchronized := m.prepared[key]
	m.preparedMu.Unlock()
	if synchronized {
		return nil
	}

	rows, err := connection.Query(ctx, "SELECT name FROM pg_prepared_statements", pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return err
	}
	names := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		names[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	m.preparedMu.Lock()
	m.prepared[key] = names
	m.preparedMu.Unlock()
	return nil
}

func preparedConnectionKey(burrow string, pid uint32) string {
	return burrow + "\x00" + strconv.FormatUint(uint64(pid), 10)
}

func (m *Manager) forgetPreparedConnection(burrow string, pid uint32) {
	if m == nil {
		return
	}
	m.preparedMu.Lock()
	delete(m.prepared, preparedConnectionKey(burrow, pid))
	m.preparedMu.Unlock()
}

// Prepared reports whether the physical PostgreSQL connection currently has
// the canonical statement name installed.
func (s *Session) Prepared(burrow, name string) bool {
	shard := s.shardByName(burrow)
	if shard == nil || s.manager == nil {
		return false
	}
	s.manager.preparedMu.Lock()
	defer s.manager.preparedMu.Unlock()
	_, ok := s.manager.prepared[preparedConnectionKey(shard.name, shard.conn.PID())][name]
	return ok
}

// MarkPrepared records a successful lazy Parse on a physical connection.
func (s *Session) MarkPrepared(burrows []string, name string) {
	if s.manager == nil {
		return
	}
	s.manager.preparedMu.Lock()
	defer s.manager.preparedMu.Unlock()
	for _, burrow := range burrows {
		shard := s.shardByName(burrow)
		if shard == nil {
			continue
		}
		key := preparedConnectionKey(shard.name, shard.conn.PID())
		if s.manager.prepared[key] == nil {
			s.manager.prepared[key] = make(map[string]struct{})
		}
		s.manager.prepared[key][name] = struct{}{}
	}
}

// InvalidatePreparedStatements forgets connection-local statement knowledge
// after a frontend command such as DEALLOCATE ALL or DISCARD ALL. The next
// checkout reconciles the affected physical connections with PostgreSQL before
// deciding whether a canonical Parse can be skipped.
func (m *Manager) InvalidatePreparedStatements(burrows []string) {
	selected := make(map[string]struct{}, len(burrows))
	for _, burrow := range burrows {
		selected[burrow] = struct{}{}
	}
	m.preparedMu.Lock()
	defer m.preparedMu.Unlock()
	for key := range m.prepared {
		burrow, _, _ := strings.Cut(key, "\x00")
		if _, ok := selected[burrow]; ok {
			delete(m.prepared, key)
		}
	}
}

// LockFleetWrites serializes fleet-wide schema writes so every Burrow observes
// them in the same process-wide order. Routed DML transactions remain concurrent.
func (m *Manager) LockFleetWrites() func() {
	<-m.fleetWriteGate
	return func() { m.fleetWriteGate <- struct{}{} }
}

// LockFleetWrites holds the process-wide fleet-write lock for this affinity
// session until UnlockFleetWrites or Close. Repeated calls are no-ops.
func (s *Session) LockFleetWrites() {
	_ = s.LockFleetWritesContext(context.Background())
}

// LockFleetWritesContext acquires the process-wide fleet-write gate.
func (s *Session) LockFleetWritesContext(ctx context.Context) bool {
	if s.fleetWriteLocked || s.fleetWriteGate == nil {
		return true
	}
	select {
	case <-s.fleetWriteGate:
		s.fleetWriteLocked = true
		return true
	case <-ctx.Done():
		return false
	}
}

func newFleetWriteGate() chan struct{} {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return gate
}

func (s *Session) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *Session) UnlockFleetWrites() {
	if !s.fleetWriteLocked || s.fleetWriteGate == nil {
		return
	}
	s.fleetWriteLocked = false
	s.fleetWriteGate <- struct{}{}
}

// Send broadcasts one frontend protocol message to the currently connected
// Burrows. It never acquires new connections; callers choose participants with
// SendTo or SendBatchTo before a protocol phase begins.
func (s *Session) Send(message pgproto3.FrontendMessage) error {
	shards := s.connectedShards()
	for _, shard := range shards {
		shard.conn.Frontend().Send(message)
		// The frontend connection is processed one protocol message at a time.
		// PostgreSQL is allowed to hold an extended-query response until a Sync
		// or Flush arrives, so inject Flush after each forwarded message to make
		// Parse, Bind, Describe, Execute, and Close observable immediately.
		shard.conn.Frontend().Send(&pgproto3.Flush{})
	}
	for _, shard := range shards {
		if err := shard.conn.Frontend().Flush(); err != nil {
			return fmt.Errorf("send to Burrow %s: %w", shard.name, err)
		}
	}
	return nil
}

// SendCopyTo forwards a COPY data-phase message to exactly the named Burrows
// without injecting a protocol Flush. PostgreSQL only accepts CopyData,
// CopyDone, and CopyFail while it is in COPY FROM STDIN mode; the transport
// buffer is still flushed normally.
func (s *Session) SendCopyTo(names []string, message pgproto3.FrontendMessage) error {
	shards := make([]*sessionShard, 0, len(names))
	for _, name := range names {
		shard := s.shardByName(name)
		if shard == nil {
			return fmt.Errorf("Burrow %s is not connected for COPY", name)
		}
		shards = append(shards, shard)
	}
	for _, shard := range shards {
		shard.conn.Frontend().Send(message)
	}
	for _, shard := range shards {
		if err := shard.conn.Frontend().Flush(); err != nil {
			return fmt.Errorf("send COPY data to Burrow %s: %w", shard.name, err)
		}
	}
	return nil
}

// SendTo forwards one frontend protocol message to a single Burrow while
// preserving that Burrow's prepared-statement and portal state.
func (s *Session) SendTo(name string, message pgproto3.FrontendMessage) error {
	shard, err := s.acquire(name)
	if err != nil {
		return err
	}
	shard.conn.Frontend().Send(message)
	shard.conn.Frontend().Send(&pgproto3.Flush{})
	if err := shard.conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("send to Burrow %s: %w", shard.name, err)
	}
	return nil
}

// SendBatchTo forwards an extended-protocol request to one Burrow and flushes
// exactly once. PostgreSQL clients normally pipeline Bind, Execute, and Sync;
// preserving that request boundary avoids a network round trip per message.
func (s *Session) SendBatchTo(name string, messages ...pgproto3.FrontendMessage) error {
	shard, err := s.acquire(name)
	if err != nil {
		return err
	}
	for _, message := range messages {
		shard.conn.Frontend().Send(message)
	}
	if err := shard.conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("send batch to Burrow %s: %w", shard.name, err)
	}
	return nil
}

// ReceiveUntil reads each Burrow's response through its terminating message.
// The outer slice is ordered by configured Burrow name, just like QueryAll.
func (s *Session) ReceiveUntil(ctx context.Context, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	return s.ReceiveUntilFromMany(ctx, s.ConnectedNames(), done)
}

// ReceiveUntilFromMany reads exactly the named participants in caller order.
// This avoids waiting on an older affinity connection that received no message
// in the current routed protocol phase.
func (s *Session) ReceiveUntilFromMany(ctx context.Context, names []string, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	ctx = s.receiveContext(ctx)
	responses := make([][]pgproto3.BackendMessage, len(names))
	for index, name := range names {
		shard := s.shardByName(name)
		if shard == nil {
			return nil, fmt.Errorf("Burrow %s is not connected", name)
		}
		response, err := receiveUntil(ctx, shard, done)
		if err != nil {
			return nil, err
		}
		responses[index] = response
	}
	return responses, nil
}

// ReceiveUntilFrom reads one Burrow's response through its terminating message.
func (s *Session) ReceiveUntilFrom(ctx context.Context, name string, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	ctx = s.receiveContext(ctx)
	shard := s.shardByName(name)
	if shard == nil {
		return nil, fmt.Errorf("unknown Burrow %s", name)
	}
	response, err := receiveUntil(ctx, shard, done)
	if err != nil {
		return nil, err
	}
	return [][]pgproto3.BackendMessage{response}, nil
}

// ReceiveEachFrom streams one Burrow response through handle until done. The
// callback must consume the flyweight message before returning. COPY TO uses
// this path so large exports are forwarded with frontend backpressure instead
// of being retained in a response slice.
func (s *Session) ReceiveEachFrom(ctx context.Context, name string, done func(pgproto3.BackendMessage) bool, handle func(pgproto3.BackendMessage) error) error {
	ctx = s.receiveContext(ctx)
	shard := s.shardByName(name)
	if shard == nil {
		return fmt.Errorf("unknown Burrow %s", name)
	}
	for {
		message, err := shard.conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receive from Burrow %s: %w", name, err)
		}
		if err := handle(message); err != nil {
			return err
		}
		if done(message) {
			return nil
		}
	}
}

func (s *Session) receiveContext(fallback context.Context) context.Context {
	if s.ctx != nil {
		// Each checked-out connection watches the session context once. Avoid
		// pgconn installing a new context.AfterFunc for every wire message.
		return context.Background()
	}
	return fallback
}

func (s *sessionShard) watchCancellation(ctx context.Context) {
	if ctx == nil || ctx.Done() == nil {
		return
	}
	s.stopCancelWatch = make(chan struct{})
	s.cancelWatchDone = make(chan struct{})
	go func() {
		defer close(s.cancelWatchDone)
		select {
		case <-ctx.Done():
			_ = s.conn.Conn().SetDeadline(time.Now())
		case <-s.stopCancelWatch:
		}
	}()
}

func (s *sessionShard) stopCancellationWatch() {
	if s.stopCancelWatch == nil {
		return
	}
	close(s.stopCancelWatch)
	<-s.cancelWatchDone
	s.stopCancelWatch = nil
	s.cancelWatchDone = nil
}

func (s *Session) shardByName(name string) *sessionShard {
	if s == nil {
		return nil
	}
	return s.shards[name]
}

func (s *Session) connectedShards() []*sessionShard {
	if s == nil || s.manager == nil {
		return nil
	}
	shards := make([]*sessionShard, 0, len(s.shards))
	for _, target := range s.manager.shards {
		if shard := s.shards[target.name]; shard != nil {
			shards = append(shards, shard)
		}
	}
	return shards
}

func receiveUntil(ctx context.Context, shard *sessionShard, done func(pgproto3.BackendMessage) bool) ([]pgproto3.BackendMessage, error) {
	var response []pgproto3.BackendMessage
	for {
		message, err := shard.conn.ReceiveMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("receive from Burrow %s: %w", shard.name, err)
		}
		// pgproto3 reuses message storage on the next Receive call. The proxy
		// merges responses after every Burrow has replied, so retain copies.
		response = append(response, cloneBackendMessage(message))
		if done(message) {
			return response, nil
		}
	}
}

func cloneBackendMessage(message pgproto3.BackendMessage) pgproto3.BackendMessage {
	switch message := message.(type) {
	case *pgproto3.RowDescription:
		copy := *message
		copy.Fields = make([]pgproto3.FieldDescription, len(message.Fields))
		for index, field := range message.Fields {
			copy.Fields[index] = field
			copy.Fields[index].Name = append([]byte(nil), field.Name...)
		}
		return &copy
	case *pgproto3.DataRow:
		copy := *message
		copy.Values = make([][]byte, len(message.Values))
		for index, value := range message.Values {
			copy.Values[index] = append([]byte(nil), value...)
		}
		return &copy
	case *pgproto3.CommandComplete:
		copy := *message
		copy.CommandTag = append([]byte(nil), message.CommandTag...)
		return &copy
	case *pgproto3.ParameterDescription:
		copy := *message
		copy.ParameterOIDs = append([]uint32(nil), message.ParameterOIDs...)
		return &copy
	case *pgproto3.CopyData:
		copy := *message
		copy.Data = append([]byte(nil), message.Data...)
		return &copy
	case *pgproto3.ErrorResponse:
		copy := *message
		if message.UnknownFields != nil {
			copy.UnknownFields = make(map[byte]string, len(message.UnknownFields))
			for key, value := range message.UnknownFields {
				copy.UnknownFields[key] = value
			}
		}
		return &copy
	case *pgproto3.NoticeResponse:
		copy := *message
		if message.UnknownFields != nil {
			copy.UnknownFields = make(map[byte]string, len(message.UnknownFields))
			for key, value := range message.UnknownFields {
				copy.UnknownFields[key] = value
			}
		}
		return &copy
	default:
		return message
	}
}

const sessionResetTimeout = 5 * time.Second

const pooledSessionResetSQL = "SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD PLANS; DISCARD SEQUENCES; DISCARD TEMP"

// Close releases every acquired affinity connection. Reusable connections are
// returned only after PostgreSQL has discarded all frontend-owned state.
// Proxy-owned canonical prepared statements survive the ordinary reset; SQL
// PREPARE state requests a full DISCARD ALL and clears the matching pgx cache.
func (s *Session) Close(ctx context.Context, reusable ...bool) {
	s.UnlockFleetWrites()
	release := len(reusable) > 0 && reusable[0]
	discardPrepared := len(reusable) > 1 && reusable[1]
	success := true
	for _, shard := range s.connectedShards() {
		shard.stopCancellationWatch()
		if s.ctx != nil && s.ctx.Err() != nil {
			release = false
		}
		if release {
			resetContext, cancel := context.WithTimeout(ctx, sessionResetTimeout)
			resetSQL := pooledSessionResetSQL
			if discardPrepared {
				resetSQL = "DISCARD ALL"
			}
			err := resetSessionState(
				resetContext,
				shard.conn.TxStatus(),
				discardPrepared,
				shard.pooled.Conn().DeallocateAll,
				func(resetContext context.Context) error {
					_, err := shard.conn.Exec(resetContext, resetSQL).ReadAll()
					return err
				},
			)
			cancel()
			if err == nil {
				if s.manager != nil {
					if discardPrepared {
						s.manager.forgetPreparedConnection(shard.name, shard.conn.PID())
					}
					s.manager.forgetRuntimeParams(shard.name, shard.conn.PID())
				}
				shard.pooled.Release()
				continue
			}
			success = false
			slog.Error("Burrow session reset failed", "event", "backend_session_reset_failed", "component", "hamstergres-proxy", "burrow", shard.name, "error_category", "pool_safety", "error", err)
		}
		key := preparedConnectionKey(shard.name, shard.conn.PID())
		connection := shard.pooled.Hijack()
		if err := connection.Close(ctx); err != nil {
			success = false
			slog.Error("Burrow session cleanup failed", "event", "backend_session_cleanup_failed", "component", "hamstergres-proxy", "burrow", shard.name, "error_category", "client_disconnect_cleanup", "error", err)
		}
		if s.manager != nil {
			s.manager.preparedMu.Lock()
			delete(s.manager.prepared, key)
			s.manager.preparedMu.Unlock()
			s.manager.forgetRuntimeParams(shard.name, shard.conn.PID())
		}
	}
	if s.manager != nil {
		outcome := "success"
		if !success {
			outcome = "failure"
		}
		s.manager.RecordOperation("frontend_session_cleanup", outcome)
	}
}

func resetSessionState(ctx context.Context, txStatus byte, discardPrepared bool, deallocate func(context.Context) error, discard func(context.Context) error) error {
	if txStatus != 'I' {
		return fmt.Errorf("Tunnel transaction status is %q, expected idle", txStatus)
	}
	if discardPrepared {
		if err := deallocate(ctx); err != nil {
			return fmt.Errorf("clear prepared statement caches: %w", err)
		}
	}
	if err := discard(ctx); err != nil {
		return fmt.Errorf("discard frontend session state: %w", err)
	}
	return nil
}

func (m *Manager) Close() {
	for _, shard := range m.shards {
		shard.pool.Close()
	}
}

// QueryAll runs sql against every configured shard concurrently, then appends
// compatible result rows in deterministic shard-name order. This is deliberately
// a temporary fan-out strategy, not transactional distributed SQL.
func (m *Manager) QueryAll(ctx context.Context, sql string) (Result, error) {
	started := time.Now()
	targets := m.shardNames()
	success := false
	errorCategory := "query_execution"
	defer func() {
		m.metrics.Record(statistics.QueryEvent{SQL: sql, Success: success, Duration: time.Since(started), Shards: targets, ErrorCategory: errorCategory})
	}()

	type outcome struct {
		name   string
		result Result
		err    error
	}
	results := make(chan outcome, len(m.shards))
	for _, s := range m.shards {
		go func(s *shard) {
			result, err := m.queryShard(ctx, s, sql)
			results <- outcome{name: s.name, result: result, err: err}
		}(s)
	}

	byName := make(map[string]Result, len(m.shards))
	var firstErr error
	for range m.shards {
		outcome := <-results
		if outcome.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("shard %s: %w", outcome.name, outcome.err)
			}
			continue
		}
		byName[outcome.name] = outcome.result
	}
	if firstErr != nil {
		errorCategory = classifyQueryError(firstErr)
		m.RecordOperation("backend_query", "failure")
		return Result{}, firstErr
	}

	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	ordered := make([]Result, 0, len(names))
	for _, name := range names {
		ordered = append(ordered, byName[name])
	}
	merged, err := merge(ordered)
	success = err == nil
	if err != nil {
		errorCategory = "result_mismatch"
		m.RecordOperation("backend_query", "failure")
	} else {
		m.RecordOperation("backend_query", "success")
	}
	return merged, err
}

// QueryOne runs sql against one Burrow and records it as a single-shard query.
func (m *Manager) QueryOne(ctx context.Context, sql, name string) (Result, error) {
	started := time.Now()
	targets := []string{name}
	success := false
	errorCategory := "query_execution"
	defer func() {
		m.metrics.Record(statistics.QueryEvent{SQL: sql, Success: success, Duration: time.Since(started), Shards: targets, ErrorCategory: errorCategory})
	}()

	for _, shard := range m.shards {
		if shard.name != name {
			continue
		}
		result, err := m.queryShard(ctx, shard, sql)
		success = err == nil
		if err != nil {
			errorCategory = classifyQueryError(err)
			m.RecordOperation("backend_query", "failure")
		} else {
			m.RecordOperation("backend_query", "success")
		}
		return result, err
	}
	errorCategory = "configuration"
	return Result{}, fmt.Errorf("unknown Burrow %s", name)
}

func classifyQueryError(err error) string {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		if len(postgresError.Code) >= 2 {
			switch postgresError.Code[:2] {
			case "22", "23":
				return "data_error"
			case "25", "40":
				return "transaction_error"
			case "42":
				return "sql_error"
			case "53":
				return "resource_exhausted"
			}
		}
		return "postgres_error"
	}
	return "backend_connection"
}

// ShardNames returns the configured Burrow names in routing order.
func (m *Manager) ShardNames() []string {
	return m.shardNames()
}

func (m *Manager) UnshardedMode() string { return m.unshardedMode }
func (m *Manager) PrimaryBurrow() string { return m.primaryBurrow }

type ShardingInventory struct {
	Source           string                  `json:"source"`
	SchemaRevision   uint64                  `json:"schema_revision"`
	TopologyRevision uint64                  `json:"topology_revision"`
	UnshardedMode    string                  `json:"unsharded_mode"`
	PrimaryBurrow    string                  `json:"primary_burrow,omitempty"`
	VirtualShards    int                     `json:"virtual_shards"`
	Tables           []schema.TableInventory `json:"tables"`
}

// ShardingInventory returns the process-owned copy of the Nest-validated
// catalog. It performs no PostgreSQL or Nest request on the status path.
func (m *Manager) ShardingInventory() ShardingInventory {
	m.schemaMu.RLock()
	defer m.schemaMu.RUnlock()
	return ShardingInventory{Source: "hamstergres-nest", SchemaRevision: m.schema.Revision(), TopologyRevision: m.topologyRevision, UnshardedMode: m.unshardedMode, PrimaryBurrow: m.primaryBurrow, VirtualShards: m.schema.MaximumVShardCount(), Tables: m.schema.Inventory()}
}

// Schema returns the shard-key registry validated across all Burrows at
// startup. The Proxy uses it to make routing decisions.
func (m *Manager) Schema() schema.Registry {
	m.schemaMu.RLock()
	defer m.schemaMu.RUnlock()
	return m.schema
}

// RefreshSchema validates the post-DDL catalogs on every Burrow, publishes the
// intentional transition to Nest, and makes it immediately routable here.
func (m *Manager) RefreshSchema(ctx context.Context) error {
	registry, err := m.loadSchema(ctx)
	if err != nil {
		m.recordSchemaRefreshFailure(err)
		return err
	}
	if m.schemaUnchanged(registry) {
		m.RecordOperation("schema_registry_refresh", "success")
		return nil
	}
	if m.registryStore != nil {
		registry, err = m.registryStore.ReplaceVersioned(ctx, registry)
		if err != nil {
			m.RecordOperation("nest_registry_write", "failure")
			m.RecordOperation("schema_registry_refresh", "failure")
			slog.Error("Nest schema registry write failed", "event", "nest_request_failed", "component", "hamstergres-proxy", "error_category", "nest_unavailable", "operation", "schema_registry_write", "error", err)
			return err
		}
		m.RecordOperation("nest_registry_write", "success")
		updatedTopology, err := m.topologyStore.ReconcileSchema(ctx, registry)
		if err != nil {
			m.RecordOperation("topology_reconcile", "failure")
			return err
		}
		if err := updatedTopology.Catalog.ValidateSchema(registry); err != nil {
			m.RecordOperation("topology_reconcile", "failure")
			return err
		}
		placements, err := updatedTopology.Catalog.TablePlacements()
		if err != nil {
			m.RecordOperation("topology_reconcile", "failure")
			return err
		}
		registry = registry.WithTableVShards(placements)
		m.topologyRevision = updatedTopology.Catalog.Revision
		m.RecordOperation("topology_reconcile", "success")
	} else {
		registry = registry.WithVShards(topology.LegacyModuloOwners(m.names, topology.DefaultVShardCount))
	}
	m.schemaMu.Lock()
	m.schema = registry
	m.schemaMu.Unlock()
	m.RecordOperation("schema_registry_refresh", "success")
	return nil
}

func (m *Manager) schemaUnchanged(registry schema.Registry) bool {
	m.schemaMu.RLock()
	defer m.schemaMu.RUnlock()
	return m.schema.EqualSchema(registry) == nil
}

func (m *Manager) recordSchemaRefreshFailure(err error) {
	m.RecordOperation("schema_registry_refresh", "failure")
	if strings.Contains(err.Error(), "schema registry mismatch") {
		m.RecordOperation("schema_registry_mismatch", "detected")
		slog.Error("schema registry mismatch detected", "event", "schema_registry_mismatch", "component", "hamstergres-proxy", "error_category", "schema_registry_mismatch", "error", err)
		return
	}
	slog.Error("schema registry refresh failed", "event", "schema_registry_refresh_failed", "component", "hamstergres-proxy", "error_category", "schema_registry", "error", err)
}

// NextGlobalID allocates through Hamstergres Nest. A Proxy without a Nest
// endpoint deliberately cannot generate fleet-wide keys.
func (m *Manager) NextGlobalID(ctx context.Context) (int64, error) {
	if m.globalIDs == nil {
		m.RecordOperation("generated_id_allocation", "failure")
		m.RecordOperation("nest_request", "failure")
		slog.Error("generated ID allocation requires Nest", "event", "nest_request_failed", "component", "hamstergres-proxy", "error_category", "nest_unavailable", "operation", "generated_id_allocation")
		return 0, fmt.Errorf("generated primary keys require Hamstergres Nest")
	}
	id, err := m.globalIDs.Next(ctx)
	if err != nil {
		m.RecordOperation("generated_id_allocation", "failure")
		m.RecordOperation("nest_request", "failure")
		slog.Error("generated ID allocation failed", "event", "nest_request_failed", "component", "hamstergres-proxy", "error_category", "nest_unavailable", "operation", "generated_id_allocation", "error", err)
		return 0, err
	}
	m.RecordOperation("generated_id_allocation", "success")
	m.RecordOperation("nest_request", "success")
	return id, nil
}

func (m *Manager) shardNames() []string {
	return m.names
}

func (m *Manager) queryShard(ctx context.Context, shard *shard, sql string) (Result, error) {
	connection, err := shard.pool.Acquire(ctx)
	if err != nil {
		return Result{}, err
	}
	if err := m.applyRuntimeParams(ctx, shard.name, connection, nil); err != nil {
		return Result{}, err
	}
	defer connection.Release()
	rows, err := connection.Query(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()

	result := Result{Fields: fieldsToWire(rows.FieldDescriptions())}
	for rows.Next() {
		values := rows.RawValues()
		copied := make([][]byte, len(values))
		for i, value := range values {
			copied[i] = append([]byte(nil), value...)
		}
		result.Rows = append(result.Rows, copied)
	}
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	result.CommandTag = rows.CommandTag().String()
	return result, nil
}

func fieldsToWire(fields []pgconn.FieldDescription) []pgproto3.FieldDescription {
	converted := make([]pgproto3.FieldDescription, len(fields))
	for i, field := range fields {
		converted[i] = pgproto3.FieldDescription{
			Name:                 []byte(field.Name),
			TableOID:             field.TableOID,
			TableAttributeNumber: field.TableAttributeNumber,
			DataTypeOID:          field.DataTypeOID,
			DataTypeSize:         field.DataTypeSize,
			TypeModifier:         field.TypeModifier,
			Format:               field.Format,
		}
	}
	return converted
}

func merge(results []Result) (Result, error) {
	if len(results) == 0 {
		return Result{}, fmt.Errorf("no shards configured")
	}
	merged := Result{Fields: results[0].Fields}
	for index, result := range results {
		if !sameFields(merged.Fields, result.Fields) {
			return Result{}, fmt.Errorf("incompatible row descriptions from shard %d", index+1)
		}
		merged.Rows = append(merged.Rows, result.Rows...)
	}
	merged.CommandTag = mergedTag(results, len(merged.Rows))
	return merged, nil
}

func sameFields(left, right []pgproto3.FieldDescription) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if string(left[i].Name) != string(right[i].Name) || left[i].DataTypeOID != right[i].DataTypeOID || left[i].Format != right[i].Format {
			return false
		}
	}
	return true
}

func mergedTag(results []Result, rowCount int) string {
	if len(results) > 0 && strings.HasPrefix(results[0].CommandTag, "SELECT") {
		return fmt.Sprintf("SELECT %d", rowCount)
	}
	if tag, ok := mergeRowCountTags(resultTags(results)); ok {
		return tag
	}
	first := results[0].CommandTag
	for _, result := range results[1:] {
		if result.CommandTag != first {
			return "FANOUT"
		}
	}
	return first
}

func resultTags(results []Result) []string {
	tags := make([]string, 0, len(results))
	for _, result := range results {
		tags = append(tags, result.CommandTag)
	}
	return tags
}

func mergeRowCountTags(tags []string) (string, bool) {
	if len(tags) == 0 {
		return "", false
	}
	prefix, rows, ok := splitRowCountTag(tags[0])
	if !ok {
		return "", false
	}
	for _, tag := range tags[1:] {
		nextPrefix, nextRows, ok := splitRowCountTag(tag)
		if !ok || nextPrefix != prefix {
			return "", false
		}
		rows += nextRows
	}
	return fmt.Sprintf("%s %d", prefix, rows), true
}

func splitRowCountTag(tag string) (string, int64, bool) {
	parts := strings.Fields(tag)
	if len(parts) < 2 {
		return "", 0, false
	}
	rows, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return strings.Join(parts[:len(parts)-1], " "), rows, true
}

// Statistics reports process-lifetime gateway query counts.
type Statistics = statistics.Statistics

// QueryMetrics includes process-lifetime and rolling query routing telemetry.
type QueryMetrics = statistics.Snapshot

func (m *Manager) Statistics() Statistics {
	return m.metrics.Snapshot().Total
}

func (m *Manager) QueryMetrics() QueryMetrics {
	return m.metrics.Snapshot()
}

// RecordQuery records a query executed through an affinity Session. QueryAll
// records its own work; this is for extended-query executions whose backend
// state must stay pinned to a frontend session.
func (m *Manager) RecordQuery(sql string, success bool, duration time.Duration) {
	m.RecordQueryTargets(sql, success, duration, m.shardNames())
}

// RecordQueryTargets records an affinity-session query with the selected Burrows.
func (m *Manager) RecordQueryTargets(sql string, success bool, duration time.Duration, shards []string) {
	m.RecordQueryTargetsCategory(sql, success, duration, shards, "")
}

func (m *Manager) RecordQueryTargetsCategory(sql string, success bool, duration time.Duration, shards []string, errorCategory string) {
	m.metrics.Record(statistics.QueryEvent{
		SQL: sql, Success: success, Duration: duration, Shards: shards, ErrorCategory: errorCategory,
	})
}

func (m *Manager) RecordOperation(operation, outcome string) {
	m.metrics.RecordOperation(operation, outcome)
}

// ShardStatus is a safe, presentation-ready snapshot of one backend pool.
type ShardStatus struct {
	Name          string    `json:"name"`
	Healthy       bool      `json:"healthy"`
	LastCheckedAt time.Time `json:"last_checked_at"`
	LastError     string    `json:"last_error,omitempty"`
	TotalConns    int32     `json:"total_connections"`
	AcquiredConns int32     `json:"acquired_connections"`
	IdleConns     int32     `json:"idle_connections"`
	MaxConns      int32     `json:"max_connections"`
	AcquireCount  int64     `json:"acquire_count"`
	AcquireWaits  int64     `json:"acquire_wait_count"`
	AcquireErrors int64     `json:"acquire_error_count"`
	AcquireTime   float64   `json:"acquire_duration_seconds"`
}

// ShardStatuses pings every shard before returning connection and health data.
func (m *Manager) ShardStatuses(ctx context.Context) []ShardStatus {
	var wg sync.WaitGroup
	for _, s := range m.shards {
		wg.Add(1)
		go func(s *shard) {
			defer wg.Done()
			checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := s.pool.Ping(checkCtx)
			cancel()
			m.mu.Lock()
			wasHealthy := s.last.error == ""
			s.last.checkedAt = time.Now().UTC()
			if err != nil {
				s.last.error = err.Error()
			} else {
				s.last.error = ""
			}
			m.mu.Unlock()
			if err != nil && wasHealthy {
				slog.Error("Burrow health check failed", "event", "burrow_health_check_failed", "component", "hamstergres-proxy", "burrow", s.name, "error_category", "burrow_unavailable", "error", err)
			}
		}(s)
	}
	wg.Wait()

	m.mu.RLock()
	defer m.mu.RUnlock()
	statuses := make([]ShardStatus, 0, len(m.shards))
	for _, shard := range m.shards {
		stat := shard.pool.Stat()
		statuses = append(statuses, ShardStatus{
			Name: shard.name, Healthy: shard.last.error == "", LastCheckedAt: shard.last.checkedAt,
			LastError: shard.last.error, TotalConns: stat.TotalConns(), AcquiredConns: stat.AcquiredConns(), IdleConns: stat.IdleConns(),
			MaxConns: stat.MaxConns(), AcquireCount: stat.AcquireCount(), AcquireWaits: stat.EmptyAcquireCount(),
			AcquireErrors: stat.CanceledAcquireCount(), AcquireTime: stat.AcquireDuration().Seconds(),
		})
	}
	return statuses
}
