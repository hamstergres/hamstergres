package backend

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jruszo/hamstergres/internal/config"
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
	pool *pgxpool.Pool
	last shardHealth
}

type shardHealth struct {
	checkedAt time.Time
	error     string
}

// Manager owns one connection pool per physical shard.
type Manager struct {
	shards  []*shard
	mu      sync.RWMutex
	metrics *statistics.Collector
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
		m.shards = append(m.shards, &shard{name: name, pool: pool})
	}
	return m, nil
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
	first := results[0].CommandTag
	for _, result := range results[1:] {
		if result.CommandTag != first {
			return "FANOUT"
		}
	}
	return first
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

// ShardStatus is a safe, presentation-ready snapshot of one backend pool.
type ShardStatus struct {
	Name          string    `json:"name"`
	Healthy       bool      `json:"healthy"`
	LastCheckedAt time.Time `json:"last_checked_at"`
	LastError     string    `json:"last_error,omitempty"`
	TotalConns    int32     `json:"total_connections"`
	AcquiredConns int32     `json:"acquired_connections"`
	IdleConns     int32     `json:"idle_connections"`
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
			s.last.checkedAt = time.Now().UTC()
			if err != nil {
				s.last.error = err.Error()
			} else {
				s.last.error = ""
			}
			m.mu.Unlock()
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
		})
	}
	return statuses
}
