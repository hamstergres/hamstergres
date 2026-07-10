// Package status serves the human and machine readable gateway status views.
package status

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/jruszo/hamstergres/internal/backend"
	"github.com/jruszo/hamstergres/internal/proxy"
)

type Snapshot struct {
	Now           time.Time             `json:"now"`
	StartedAt     time.Time             `json:"started_at"`
	UptimeSeconds int64                 `json:"uptime_seconds"`
	Queries       backend.Statistics    `json:"queries"`
	QueryMetrics  backend.QueryMetrics  `json:"query_metrics"`
	Frontend      proxy.Statistics      `json:"frontend"`
	Burrows       []backend.ShardStatus `json:"burrows"`
}

// Collector is the gateway's in-process source of operational state. Future
// metrics can be added here without making the status UI or CLI inspect
// PostgreSQL directly.
type Collector struct {
	backends *backend.Manager
	frontend *proxy.Server
	started  time.Time
}

func NewCollector(backends *backend.Manager, frontend *proxy.Server) *Collector {
	return &Collector{backends: backends, frontend: frontend, started: time.Now().UTC()}
}

// Snapshot collects connection, query, and Burrow-health data directly from
// this gateway process and its managed backend pools.
func (c *Collector) Snapshot(ctx context.Context) Snapshot {
	now := time.Now().UTC()
	metrics := c.backends.QueryMetrics()
	return Snapshot{Now: now, StartedAt: c.started, UptimeSeconds: int64(now.Sub(c.started).Seconds()), Queries: metrics.Total, QueryMetrics: metrics, Frontend: c.frontend.Statistics(), Burrows: c.backends.ShardStatuses(ctx)}
}

type Server struct {
	collector *Collector
}

func New(backends *backend.Manager, frontend *proxy.Server) *Server {
	return &Server{collector: NewCollector(backends, frontend)}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleHTML)
	mux.HandleFunc("/api/v1/status", s.handleJSON)
	mux.HandleFunc("/healthz", s.handleHealth)
	return mux
}

func (s *Server) handleJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(s.collector.Snapshot(r.Context()))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	for _, burrow := range s.collector.Snapshot(r.Context()).Burrows {
		if !burrow.Healthy {
			http.Error(w, "a Burrow is unhealthy", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleHTML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pageTemplate.Execute(w, s.collector.Snapshot(r.Context())); err != nil {
		http.Error(w, fmt.Sprintf("render status page: %v", err), http.StatusInternalServerError)
	}
}

var pageTemplate = template.Must(template.New("status").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Hamstergres status</title>
<style>body{font:16px system-ui,sans-serif;max-width:1050px;margin:3rem auto;padding:0 1rem;color:#1f2937}table{border-collapse:collapse;width:100%;margin-top:1rem}th,td{text-align:left;border-bottom:1px solid #d1d5db;padding:.65rem;vertical-align:top}.healthy{color:#047857}.unhealthy{color:#b91c1c}code{background:#f3f4f6;padding:.15rem .3rem;border-radius:3px}.muted{color:#6b7280}.shards{margin:0;padding:0;list-style:none}</style></head>
<body><h1>Hamstergres</h1><p>Running for {{.UptimeSeconds}} seconds · updated <code>{{.Now}}</code></p>
<h2>Gateway</h2><p>{{.Frontend.ActiveConnections}} active / {{.Frontend.Connections}} total frontend connections · {{.Queries.Queries}} queries · {{.Queries.FailedQueries}} failed queries · {{.Queries.AverageDurationMillis}}ms average query duration</p>
<h2>Routing</h2><p>{{.QueryMetrics.Total.ScatteredQueries}} scattered queries · {{.QueryMetrics.Total.SingleShardQueries}} single-shard queries</p>
<h2>Rolling query traffic</h2><table><thead><tr><th>Window</th><th>Queries</th><th>Failures</th><th>Routing</th><th>Average</th><th>Burrow executions</th></tr></thead><tbody>{{range .QueryMetrics.Windows}}<tr><td>{{.Name}}</td><td>{{.Statistics.Queries}}</td><td>{{.Statistics.FailedQueries}}</td><td>{{.Statistics.ScatteredQueries}} scattered<br>{{.Statistics.SingleShardQueries}} single-shard</td><td>{{.Statistics.AverageDurationMillis}}ms</td><td><ul class="shards">{{range .ShardExecutions}}<li>{{.Name}}: {{.Queries}}</li>{{end}}</ul></td></tr>{{end}}</tbody></table>
<h2>Query summaries</h2><p class="muted">Query shapes retain SQL structure but replace string and numeric values with <code>?</code>. Fingerprints are stable identifiers for searching and correlation.</p><table><thead><tr><th>Query shape</th><th>Fingerprint</th><th>Statement</th><th>Queries</th><th>Failures</th><th>Routing</th><th>Burrow executions</th><th>Last seen</th></tr></thead><tbody>{{range .QueryMetrics.QuerySummaries}}<tr><td><code>{{.QueryShape}}</code></td><td><code>{{.Fingerprint}}</code></td><td>{{.Statement}}</td><td>{{.Statistics.Queries}}</td><td>{{.Statistics.FailedQueries}}</td><td>{{.Statistics.ScatteredQueries}} scattered<br>{{.Statistics.SingleShardQueries}} single-shard</td><td><ul class="shards">{{range .ShardExecutions}}<li>{{.Name}}: {{.Queries}}</li>{{end}}</ul></td><td class="muted">{{.LastSeenAt}}</td></tr>{{else}}<tr><td colspan="8" class="muted">No queries have been recorded yet.</td></tr>{{end}}</tbody></table>
<h2>Burrows</h2><table><thead><tr><th>Name</th><th>Health</th><th>Connections</th><th>Last check</th></tr></thead><tbody>{{range .Burrows}}<tr><td>{{.Name}}</td><td class="{{if .Healthy}}healthy{{else}}unhealthy{{end}}">{{if .Healthy}}healthy{{else}}unhealthy: {{.LastError}}{{end}}</td><td>{{.AcquiredConns}} acquired, {{.IdleConns}} idle, {{.TotalConns}} total</td><td>{{.LastCheckedAt}}</td></tr>{{end}}</tbody></table>
<p><a href="/api/v1/status">JSON API</a> · <a href="/healthz">health check</a></p></body></html>`))
