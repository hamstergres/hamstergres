package backend

import (
	"context"
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
	shards        []*shard
	mu            sync.RWMutex
	writeMu       sync.Mutex
	metrics       *statistics.Collector
	schemaMu      sync.RWMutex
	schema        schema.Registry
	globalIDs     *nest.SequenceStore
	registryStore *nest.RegistryStore
}

func New(ctx context.Context, cfg config.Config) (*Manager, error) {
	m := &Manager{metrics: statistics.NewCollector()}
	for _, name := range cfg.ShardNames() {
		poolConfig, err := pgxpool.ParseConfig(cfg.Sharding.PhysicalShards[name].DSN)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("parse dsn for shard %q: %w", name, err)
		}
		poolConfig.MaxConns = 8
		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("create pool for shard %q: %w", name, err)
		}
		m.shards = append(m.shards, &shard{name: name, dsn: cfg.Sharding.PhysicalShards[name].DSN, pool: pool})
	}
	registry, err := m.loadSchema(ctx)
	if err != nil {
		m.Close()
		return nil, err
	}
	m.schema = registry
	if cfg.Nest.Endpoint != "" {
		m.registryStore = nest.NewRegistryStore(cfg.Nest.Endpoint, cfg.Nest.RegistryKey)
		if err := m.registryStore.VerifyOrSeed(ctx, registry); err != nil {
			m.Close()
			return nil, err
		}
		m.globalIDs = nest.NewSequenceStore(cfg.Nest.Endpoint, cfg.Nest.SequenceKey)
	}
	return m, nil
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
WHERE i.indisprimary AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname, key_column.ordinality`

func (m *Manager) loadSchema(ctx context.Context) (schema.Registry, error) {
	var expected schema.Registry
	for index, shard := range m.shards {
		rows, err := shard.pool.Query(ctx, primaryKeyQuery)
		if err != nil {
			return schema.Registry{}, fmt.Errorf("inspect schema on Burrow %s: %w", shard.name, err)
		}
		tables := make(map[string][]string)
		generated := make(map[string]schema.GeneratedPrimary)
		for rows.Next() {
			var namespace, table, column, identity, defaultExpression, dataType string
			var ordinality int
			if err := rows.Scan(&namespace, &table, &column, &ordinality, &identity, &defaultExpression, &dataType); err != nil {
				rows.Close()
				return schema.Registry{}, fmt.Errorf("read schema on Burrow %s: %w", shard.name, err)
			}
			qualified := namespace + "." + table
			tables[qualified] = append(tables[qualified], column)
			if namespace == "public" {
				tables[table] = append(tables[table], column)
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
			if len(tables[table]) != 1 {
				delete(generated, table)
			}
		}
		registry := schema.NewWithGenerated(tables, generated)
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

// Session owns one PostgreSQL connection to every Burrow for the lifetime of a
// frontend session. PostgreSQL prepared statements and portals live on a
// backend connection, so extended-query messages must not be sent through the
// regular query pools independently.
type Session struct {
	shards      []*sessionShard
	writeMu     *sync.Mutex
	writeLocked bool
}

type sessionShard struct {
	name string
	conn *pgconn.PgConn
}

// NewSession establishes an affinity connection to each Burrow. The caller
// must close the returned session.
func (m *Manager) NewSession(ctx context.Context) (*Session, error) {
	session := &Session{shards: make([]*sessionShard, 0, len(m.shards)), writeMu: &m.writeMu}
	for _, shard := range m.shards {
		conn, err := pgconn.Connect(ctx, shard.dsn)
		if err != nil {
			session.Close(context.Background())
			return nil, fmt.Errorf("connect session to Burrow %s: %w", shard.name, err)
		}
		session.shards = append(session.shards, &sessionShard{name: shard.name, conn: conn})
	}
	return session, nil
}

// LockWrites serializes scattered writes so every Burrow observes them in the
// same process-wide order. The returned function must be called when the write
// is complete.
func (m *Manager) LockWrites() func() {
	m.writeMu.Lock()
	return m.writeMu.Unlock
}

// LockWrites holds the process-wide write lock for this affinity session until
// UnlockWrites or Close. Repeated calls while already locked are no-ops.
func (s *Session) LockWrites() {
	if s.writeLocked || s.writeMu == nil {
		return
	}
	s.writeMu.Lock()
	s.writeLocked = true
}

// UnlockWrites releases the process-wide write lock if this session holds it.
func (s *Session) UnlockWrites() {
	if !s.writeLocked || s.writeMu == nil {
		return
	}
	s.writeLocked = false
	s.writeMu.Unlock()
}

// Send broadcasts one frontend protocol message to every Burrow before
// flushing, keeping their prepared-statement and portal state in lockstep.
func (s *Session) Send(message pgproto3.FrontendMessage) error {
	for _, shard := range s.shards {
		shard.conn.Frontend().Send(message)
		// The frontend connection is processed one protocol message at a time.
		// PostgreSQL is allowed to hold an extended-query response until a Sync
		// or Flush arrives, so inject Flush after each forwarded message to make
		// Parse, Bind, Describe, Execute, and Close observable immediately.
		shard.conn.Frontend().Send(&pgproto3.Flush{})
	}
	for _, shard := range s.shards {
		if err := shard.conn.Frontend().Flush(); err != nil {
			return fmt.Errorf("send to Burrow %s: %w", shard.name, err)
		}
	}
	return nil
}

// SendCopy broadcasts a COPY data-phase message without injecting a protocol
// Flush. PostgreSQL only accepts CopyData, CopyDone, and CopyFail while it is
// in COPY FROM STDIN mode; the transport buffer is still flushed normally.
func (s *Session) SendCopy(message pgproto3.FrontendMessage) error {
	for _, shard := range s.shards {
		shard.conn.Frontend().Send(message)
	}
	for _, shard := range s.shards {
		if err := shard.conn.Frontend().Flush(); err != nil {
			return fmt.Errorf("send COPY data to Burrow %s: %w", shard.name, err)
		}
	}
	return nil
}

// SendTo forwards one frontend protocol message to a single Burrow while
// preserving that Burrow's prepared-statement and portal state.
func (s *Session) SendTo(name string, message pgproto3.FrontendMessage) error {
	shard := s.shardByName(name)
	if shard == nil {
		return fmt.Errorf("unknown Burrow %s", name)
	}
	shard.conn.Frontend().Send(message)
	shard.conn.Frontend().Send(&pgproto3.Flush{})
	if err := shard.conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("send to Burrow %s: %w", shard.name, err)
	}
	return nil
}

// ReceiveUntil reads each Burrow's response through its terminating message.
// The outer slice is ordered by configured Burrow name, just like QueryAll.
func (s *Session) ReceiveUntil(ctx context.Context, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	responses := make([][]pgproto3.BackendMessage, len(s.shards))
	for index, shard := range s.shards {
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

func (s *Session) shardByName(name string) *sessionShard {
	for _, shard := range s.shards {
		if shard.name == name {
			return shard
		}
	}
	return nil
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
	default:
		return message
	}
}

// Close releases every affinity connection. It is safe after a partial
// NewSession failure.
func (s *Session) Close(ctx context.Context) {
	s.UnlockWrites()
	for _, shard := range s.shards {
		_ = shard.conn.Close(ctx)
	}
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
	defer func() {
		m.metrics.Record(statistics.QueryEvent{SQL: sql, Success: success, Duration: time.Since(started), Shards: targets})
	}()

	type outcome struct {
		name   string
		result Result
		err    error
	}
	results := make(chan outcome, len(m.shards))
	for _, s := range m.shards {
		go func(s *shard) {
			result, err := queryShard(ctx, s.pool, sql)
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
	return merged, err
}

// QueryOne runs sql against one Burrow and records it as a single-shard query.
func (m *Manager) QueryOne(ctx context.Context, sql, name string) (Result, error) {
	started := time.Now()
	targets := []string{name}
	success := false
	defer func() {
		m.metrics.Record(statistics.QueryEvent{SQL: sql, Success: success, Duration: time.Since(started), Shards: targets})
	}()

	for _, shard := range m.shards {
		if shard.name != name {
			continue
		}
		result, err := queryShard(ctx, shard.pool, sql)
		success = err == nil
		return result, err
	}
	return Result{}, fmt.Errorf("unknown Burrow %s", name)
}

// ShardNames returns the configured Burrow names in routing order.
func (m *Manager) ShardNames() []string {
	return m.shardNames()
}

// Schema returns the primary-key registry validated across all Burrows at
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
		m.RecordOperation("schema_registry_refresh", "failure")
		slog.Error("schema registry refresh failed", "event", "schema_registry_refresh_failed", "component", "hamstergres-proxy", "error_category", "schema_registry", "error", err)
		return err
	}
	if m.registryStore != nil {
		if err := m.registryStore.Replace(ctx, registry); err != nil {
			m.RecordOperation("nest_registry_write", "failure")
			m.RecordOperation("schema_registry_refresh", "failure")
			slog.Error("Nest schema registry write failed", "event", "nest_request_failed", "component", "hamstergres-proxy", "error_category", "nest_unavailable", "operation", "schema_registry_write", "error", err)
			return err
		}
		m.RecordOperation("nest_registry_write", "success")
	}
	m.schemaMu.Lock()
	m.schema = registry
	m.schemaMu.Unlock()
	m.RecordOperation("schema_registry_refresh", "success")
	return nil
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
	names := make([]string, 0, len(m.shards))
	for _, shard := range m.shards {
		names = append(names, shard.name)
	}
	return names
}

func queryShard(ctx context.Context, pool *pgxpool.Pool, sql string) (Result, error) {
	rows, err := pool.Query(ctx, sql, pgx.QueryExecModeSimpleProtocol)
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
	m.metrics.Record(statistics.QueryEvent{
		SQL: sql, Success: success, Duration: duration, Shards: shards,
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
