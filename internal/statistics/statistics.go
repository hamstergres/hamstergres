// Package statistics collects bounded, process-local gateway query telemetry.
package statistics

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	rollingBuckets    = 10 * 60
	maxQuerySummaries = 100
	maxSummaryLabels  = 100
)

var windows = []Window{
	{Name: "10 seconds", Duration: 10 * time.Second},
	{Name: "1 minute", Duration: time.Minute},
	{Name: "5 minutes", Duration: 5 * time.Minute},
	{Name: "10 minutes", Duration: 10 * time.Minute},
}

// LatencyBuckets are deliberately fixed and shared by every exporter. Values
// are seconds, following Prometheus base-unit conventions.
var LatencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var (
	blockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	lineComment  = regexp.MustCompile(`--[^\n]*`)
	stringValue  = regexp.MustCompile(`'(?:''|[^'])*'`)
	numberValue  = regexp.MustCompile(`\b\d+(?:\.\d+)?\b`)
	whitespace   = regexp.MustCompile(`\s+`)
)

// QueryEvent describes one completed gateway query. Shards lists all targets
// selected by the router, even if the query eventually failed on a target.
type QueryEvent struct {
	Duration      time.Duration
	Success       bool
	Shards        []string
	SQL           string
	ErrorCategory string
}

type timeBucket struct {
	second     int64
	statistics Statistics
	shards     map[string]int64
}

// Statistics contains aggregate counts and duration information.
type Statistics struct {
	Queries               int64 `json:"queries"`
	FailedQueries         int64 `json:"failed_queries"`
	ScatteredQueries      int64 `json:"scattered_queries"`
	SingleShardQueries    int64 `json:"single_shard_queries"`
	TotalDurationMillis   int64 `json:"total_duration_ms"`
	AverageDurationMillis int64 `json:"average_duration_ms"`
}

type ShardCount struct {
	Name    string `json:"name"`
	Queries int64  `json:"queries"`
}

type Window struct {
	Name     string        `json:"name"`
	Duration time.Duration `json:"-"`
}

type WindowStatistics struct {
	Name            string       `json:"name"`
	Seconds         int64        `json:"seconds"`
	Statistics      Statistics   `json:"statistics"`
	ShardExecutions []ShardCount `json:"burrow_executions"`
}

// QuerySummary is a bounded, normalized statement label. It is a metrics label
// rather than a full SQL parser, so it deliberately replaces string and number
// literals before retaining a query shape.
type QuerySummary struct {
	QueryShape      string       `json:"query_shape"`
	Fingerprint     string       `json:"fingerprint"`
	Statement       string       `json:"statement"`
	Statistics      Statistics   `json:"statistics"`
	ShardExecutions []ShardCount `json:"burrow_executions"`
	LastSeenAt      time.Time    `json:"last_seen_at"`
}

type Snapshot struct {
	Total           Statistics         `json:"total"`
	ShardExecutions []ShardCount       `json:"burrow_executions"`
	Windows         []WindowStatistics `json:"windows"`
	QuerySummaries  []QuerySummary     `json:"query_summaries"`
	Latency         Histogram          `json:"latency"`
	Operations      []OperationCount   `json:"operations"`
	Failures        []FailureCount     `json:"failures"`
}

type FailureCount struct {
	Category string `json:"category"`
	Count    int64  `json:"count"`
}

type OperationCount struct {
	Operation string `json:"operation"`
	Outcome   string `json:"outcome"`
	Count     int64  `json:"count"`
}

type Histogram struct {
	Buckets []Bucket `json:"buckets"`
	Count   int64    `json:"count"`
	Sum     float64  `json:"sum_seconds"`
}

type Bucket struct {
	UpperBound float64 `json:"upper_bound_seconds"`
	Count      int64   `json:"count"`
}

type summary struct {
	statistics Statistics
	shards     map[string]int64
	lastSeenAt time.Time
}

type operationKey struct {
	operation string
	outcome   string
}

// Collector keeps ten minutes of one-second aggregates plus a bounded set of
// process-lifetime query shapes. It has no external dependency or persistence.
type Collector struct {
	mu         sync.Mutex
	now        func() time.Time
	buckets    []timeBucket
	total      Statistics
	shards     map[string]int64
	summaries  map[string]*summary
	labels     map[string]string
	labelOrder []string
	labelNext  int
	latency    Histogram
	operations map[operationKey]int64
	failures   map[string]int64
}

func NewCollector() *Collector {
	return newCollector(time.Now)
}

func newCollector(now func() time.Time) *Collector {
	buckets := make([]Bucket, len(LatencyBuckets))
	for i, upper := range LatencyBuckets {
		buckets[i].UpperBound = upper
	}
	return &Collector{now: now, buckets: make([]timeBucket, rollingBuckets), shards: make(map[string]int64), summaries: make(map[string]*summary), labels: make(map[string]string), latency: Histogram{Buckets: buckets}, operations: make(map[operationKey]int64), failures: make(map[string]int64)}
}

// RecordOperation records a bounded operational event. Callers must use stable
// operation and outcome constants; dynamic errors and identifiers belong in logs.
func (c *Collector) RecordOperation(operation, outcome string) {
	c.mu.Lock()
	c.operations[operationKey{operation: operation, outcome: outcome}]++
	c.mu.Unlock()
}

func (c *Collector) Record(query QueryEvent) {
	now := c.now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()
	summaryLabel := c.summaryLabel(query.SQL)

	add(&c.total, query)
	c.recordWindow(now.Unix(), query)
	if !query.Success {
		category := query.ErrorCategory
		if category == "" {
			category = "unknown"
		}
		c.failures[category]++
	}
	c.latency.Count++
	c.latency.Sum += query.Duration.Seconds()
	for i := range c.latency.Buckets {
		if query.Duration.Seconds() <= c.latency.Buckets[i].UpperBound {
			c.latency.Buckets[i].Count++
		}
	}
	for _, shard := range query.Shards {
		c.shards[shard]++
	}
	c.recordSummary(summaryLabel, now, query)
}

func (c *Collector) summaryLabel(sql string) string {
	if label, ok := c.labels[sql]; ok {
		return label
	}
	label := Normalize(sql)
	if len(c.labels) < maxSummaryLabels {
		c.labels[sql] = label
		c.labelOrder = append(c.labelOrder, sql)
	} else if maxSummaryLabels > 0 {
		delete(c.labels, c.labelOrder[c.labelNext])
		c.labels[sql] = label
		c.labelOrder[c.labelNext] = sql
		c.labelNext = (c.labelNext + 1) % maxSummaryLabels
	}
	return label
}

func (c *Collector) recordWindow(second int64, query QueryEvent) {
	index := int(second % int64(len(c.buckets)))
	if index < 0 {
		index += len(c.buckets)
	}
	bucket := &c.buckets[index]
	if bucket.second != second {
		bucket.second = second
		bucket.statistics = Statistics{}
		if bucket.shards == nil {
			bucket.shards = make(map[string]int64)
		} else {
			clear(bucket.shards)
		}
	}
	add(&bucket.statistics, query)
	for _, shard := range query.Shards {
		bucket.shards[shard]++
	}
}

func (c *Collector) recordSummary(label string, now time.Time, query QueryEvent) {
	item := c.summaries[label]
	if item == nil {
		if len(c.summaries) >= maxQuerySummaries {
			c.evictSummary()
		}
		item = &summary{shards: make(map[string]int64)}
		c.summaries[label] = item
	}
	add(&item.statistics, query)
	for _, shard := range query.Shards {
		item.shards[shard]++
	}
	item.lastSeenAt = now
}

func (c *Collector) evictSummary() {
	var candidate string
	var candidateSummary *summary
	for label, item := range c.summaries {
		if candidateSummary == nil || item.statistics.Queries < candidateSummary.statistics.Queries || (item.statistics.Queries == candidateSummary.statistics.Queries && item.lastSeenAt.Before(candidateSummary.lastSeenAt)) {
			candidate, candidateSummary = label, item
		}
	}
	delete(c.summaries, candidate)
}

func (c *Collector) Snapshot() Snapshot {
	now := c.now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := Snapshot{Total: finalize(c.total), ShardExecutions: sortedShardCounts(c.shards), Latency: copyHistogram(c.latency)}
	for key, count := range c.operations {
		snapshot.Operations = append(snapshot.Operations, OperationCount{Operation: key.operation, Outcome: key.outcome, Count: count})
	}
	for category, count := range c.failures {
		snapshot.Failures = append(snapshot.Failures, FailureCount{Category: category, Count: count})
	}
	sort.Slice(snapshot.Failures, func(i, j int) bool { return snapshot.Failures[i].Category < snapshot.Failures[j].Category })
	sort.Slice(snapshot.Operations, func(i, j int) bool {
		if snapshot.Operations[i].Operation == snapshot.Operations[j].Operation {
			return snapshot.Operations[i].Outcome < snapshot.Operations[j].Outcome
		}
		return snapshot.Operations[i].Operation < snapshot.Operations[j].Operation
	})
	for _, window := range windows {
		statistics, shards := summarizeBuckets(c.buckets, now.Unix(), window.Duration)
		snapshot.Windows = append(snapshot.Windows, WindowStatistics{
			Name: window.Name, Seconds: int64(window.Duration.Seconds()), Statistics: finalize(statistics), ShardExecutions: sortedShardCounts(shards),
		})
	}
	for label, item := range c.summaries {
		snapshot.QuerySummaries = append(snapshot.QuerySummaries, QuerySummary{
			QueryShape: label, Fingerprint: Fingerprint(label), Statement: statement(label), Statistics: finalize(item.statistics), ShardExecutions: sortedShardCounts(item.shards), LastSeenAt: item.lastSeenAt,
		})
	}
	sort.Slice(snapshot.QuerySummaries, func(i, j int) bool {
		if snapshot.QuerySummaries[i].Statistics.Queries == snapshot.QuerySummaries[j].Statistics.Queries {
			return snapshot.QuerySummaries[i].LastSeenAt.After(snapshot.QuerySummaries[j].LastSeenAt)
		}
		return snapshot.QuerySummaries[i].Statistics.Queries > snapshot.QuerySummaries[j].Statistics.Queries
	})
	return snapshot
}

func copyHistogram(source Histogram) Histogram {
	result := source
	result.Buckets = append([]Bucket(nil), source.Buckets...)
	return result
}

// Fingerprint returns a stable 64-bit label for a normalized query shape. The
// status API intentionally exposes this identifier rather than SQL text, so an
// operator can search and correlate a statement without leaking its contents.
func Fingerprint(normalizedQuery string) string {
	digest := sha256.Sum256([]byte(normalizedQuery))
	return hex.EncodeToString(digest[:8])
}

func summarizeBuckets(buckets []timeBucket, now int64, window time.Duration) (Statistics, map[string]int64) {
	statistics := Statistics{}
	shards := make(map[string]int64)
	cutoff := now - int64(window/time.Second) + 1
	for _, bucket := range buckets {
		if bucket.second < cutoff || bucket.second > now {
			continue
		}
		statistics.Queries += bucket.statistics.Queries
		statistics.FailedQueries += bucket.statistics.FailedQueries
		statistics.ScatteredQueries += bucket.statistics.ScatteredQueries
		statistics.SingleShardQueries += bucket.statistics.SingleShardQueries
		statistics.TotalDurationMillis += bucket.statistics.TotalDurationMillis
		for shard, queries := range bucket.shards {
			shards[shard] += queries
		}
	}
	return statistics, shards
}

func add(statistics *Statistics, query QueryEvent) {
	statistics.Queries++
	if !query.Success {
		statistics.FailedQueries++
	}
	if len(query.Shards) > 1 {
		statistics.ScatteredQueries++
	} else if len(query.Shards) == 1 {
		statistics.SingleShardQueries++
	}
	statistics.TotalDurationMillis += query.Duration.Milliseconds()
}

func finalize(statistics Statistics) Statistics {
	if statistics.Queries > 0 {
		statistics.AverageDurationMillis = statistics.TotalDurationMillis / statistics.Queries
	}
	return statistics
}

func sortedShardCounts(counts map[string]int64) []ShardCount {
	result := make([]ShardCount, 0, len(counts))
	for name, queries := range counts {
		result = append(result, ShardCount{Name: name, Queries: queries})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Normalize returns a safe, bounded label for grouping similar simple queries.
func Normalize(sql string) string {
	normalized := blockComment.ReplaceAllString(sql, " ")
	normalized = lineComment.ReplaceAllString(normalized, " ")
	normalized = stringValue.ReplaceAllString(normalized, "?")
	normalized = numberValue.ReplaceAllString(normalized, "?")
	normalized = strings.TrimSpace(whitespace.ReplaceAllString(normalized, " "))
	if len(normalized) > 300 {
		normalized = normalized[:300] + "…"
	}
	if normalized == "" {
		return "<empty query>"
	}
	return normalized
}

func statement(query string) string {
	parts := strings.Fields(query)
	if len(parts) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(parts[0])
}
