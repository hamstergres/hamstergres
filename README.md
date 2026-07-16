# Hamstergres

[![PostgreSQL compatibility progress](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fhamstergres%2Fhamstergres%2Fgh-pages%2Fpostgres-compatibility%2Foverall.json&cacheSeconds=300)](https://github.com/hamstergres/hamstergres/actions/workflows/postgres-compatibility.yml)
[![PostgreSQL Compatibility](https://github.com/hamstergres/hamstergres/actions/workflows/postgres-compatibility.yml/badge.svg?branch=main)](https://github.com/hamstergres/hamstergres/actions/workflows/postgres-compatibility.yml)

Hamstergres is an experimental PostgreSQL sharding system: **Tiny paws, many
shards.** Its current component is the **Hamstergres Proxy**, a development
gateway that speaks the PostgreSQL frontend protocol and routes single-key
queries to their owning **Burrow**. Reads without a usable shard key are
scattered only when appending the physical rows preserves PostgreSQL semantics.
Relation-free, catalog, and session-introspection reads execute once on a
deterministic Burrow, while unsharded reads execute once according to their
fleet policy, so the fleet looks like one PostgreSQL server. It supports simple
queries and the core extended-query lifecycle used by prepared, parameterized
PostgreSQL clients. Hamstergres Nest durably stores the versioned schema and
per-table vshard topology consumed by the Proxy. See [the architecture and
naming reference](docs/architecture.md) for the component model.

## Layout

```text
cmd/hamstergres-proxy/  PostgreSQL gateway process and status CLI
internal/backend/       Physical-Burrow connection pools and result merging
internal/config/        Gateway configuration loading
internal/proxy/         PostgreSQL frontend session handling
internal/status/        Process-owned status snapshot and HTTP views
internal/statistics/    Bounded rolling query and routing telemetry
internal/router/        PostgreSQL AST shard-key and vshard routing
internal/schema/        Versioned schema contract and routing projection
internal/topology/      Versioned Burrow membership and table distributions
internal/nest/          etcd-backed schema, topology, and sequence stores
config/                 Static routing configuration for the first PoC
db/init/                 SQL installed into every new Burrow
docker-compose.yml       Local PostgreSQL Burrow fleet
```

## Start the local Burrows

```bash
cp .env.example .env
make up
```

The two independent PostgreSQL Burrows expose these host ports:

| Burrow | Address |
| --- | --- |
| `burrow-01` | `localhost:5541` |
| `burrow-02` | `localhost:5542` |

Both use the credentials in `.env` (the development defaults are
`hamster` / `hamster` and database `hamstergres`) and start with the same
`accounts` table:

```sql
CREATE TABLE accounts (
    tenant_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    balance NUMERIC(18, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, account_id)
);
```

Connect directly to a Burrow while developing the router:

```bash
psql "postgres://hamster:hamster@localhost:5541/hamstergres?sslmode=disable"
psql "postgres://hamster:hamster@localhost:5542/hamstergres?sslmode=disable"
```

`db/init/001_schema.sql` only runs when a Burrow's data volume is first
created. To reinitialize both shards after changing it, run:

```bash
docker compose down -v
make up
```

## Routing bootstrap configuration

[`config/hamstergres.example.yaml`](config/hamstergres.example.yaml) provides
the Proxy's initial Tunnel endpoints:

- `accounts` is distributed by its PostgreSQL primary key: `(tenant_id, account_id)`.
- `hash(primary_key_tuple) % 65536` selects a vshard.
- On the first upgrade, the exact v3 owner map is imported. A fresh Nest uses
  the old one-indexed modulo layout once: odd vshards belong to `burrow-01`
  and even vshards belong to `burrow-02` in the two-Burrow fixture.

The DSNs in that file use Docker service names so a Proxy container can
connect to the Burrows. When running a Go process directly on the host, use
`localhost:5541` and `localhost:5542` instead. After bootstrap, Nest topology—not
YAML order—is the placement source of truth. The current Proxy still requires
the matching static endpoints at startup; dynamic Tunnel changes are issue #13.

## Checks

```bash
make test
docker compose config
```

`make test` runs the unit tests and a Docker-backed end-to-end test. The latter
starts (or reuses) the local Burrows, runs a compiled gateway on isolated random
ports, verifies PostgreSQL wire-protocol queries and merged results, then checks
the status JSON, HTML page, and status CLI. It performs read-only SQL only and
leaves the Docker Burrow environment running. To run just one layer:

```bash
make test-unit
make test-e2e
```

## PostgreSQL compatibility suite

The repository can run PostgreSQL's complete core regression schedule through
Hamstergres Proxy:

```bash
make test-postgres-compatibility
```

This is a long-running, Docker-backed compatibility inventory. It builds a
pinned PostgreSQL 17.10 image from the official PostgreSQL source so the
Burrows, `pg_regress`, its SQL and expected-output corpus, and the server-side
`regress.so` test module all use the same release. It then starts an isolated
Nest and the normal two-Burrow topology, runs the full upstream
`parallel_schedule` through a
host-run Proxy, and writes these files under `build/postgres-compatibility/`:

- `compatibility-report.md` — pass count, current gaps, and regressions.
- `results.json` — machine-readable per-test baseline data.
- `regression.diffs` and `regression.out` — upstream expected-versus-actual
  output for diagnosing unsupported statements and behavior.
- `proxy.log` and `pg_regress.log` — harness and Proxy evidence.

Known compatibility gaps do not make the command fail. A truncated or broken
harness does fail, as does any test that passed in the supplied baseline but no
longer passes. Compare a local run with an earlier result using:

```bash
POSTGRES_COMPATIBILITY_BASELINE=/path/to/results.json \
  make test-postgres-compatibility
```

The `PostgreSQL Compatibility` GitHub Actions workflow runs when a pull request
is opened or reopened, after changes are merged into the default branch, and
when started manually. A successful default-branch push or manual run becomes
the cached baseline used to detect regressions in later pull-request runs and
updates the workflow status badge. Every run uploads the full report and raw
diffs as an artifact. The approach is adapted to Hamstergres's two-Burrow
architecture from
[Multigres's PostgreSQL compatibility workflow](https://github.com/multigres/multigres/blob/main/.github/workflows/test-pgregress.yml)
and uses PostgreSQL's documented
[`pg_regress` suite](https://www.postgresql.org/docs/17/regress.html).

## Sysbench compatibility check

Run the PostgreSQL `oltp_read_write` compatibility check against the Docker
Burrows with:

```bash
make test-sysbench
```

The command starts or reuses the Docker Burrows, launches an isolated
Hamstergres Proxy on random local ports, and runs locally installed sysbench
1.0.20 through it for schema `prepare`, a four-thread three-second read/write
workload, and `cleanup`. Install the required version on macOS with:

```bash
brew install sysbench
```

The test validates the installed sysbench version, then checks the Proxy's
process-owned status data for both single-Burrow and scattered routes, with
both `SELECT` and `UPDATE` statements present.

## Sharded and unsharded benchmark datasets

Stock pgbench and sysbench create their tables and immediately load data. That
leaves no point for Hamstergres' explicit `hamstergres.shard_key` column comment
before the first row is routed. Use the repository workload targets to select a
dataset mode during preparation:

```bash
# Sharded is the default.
make pgbench-prepare BENCHMARK_MODE=sharded
make pgbench-run BENCHMARK_MODE=sharded
make pgbench-cleanup BENCHMARK_MODE=sharded

make sysbench-prepare BENCHMARK_MODE=sharded
make sysbench-run BENCHMARK_MODE=sharded
make sysbench-cleanup BENCHMARK_MODE=sharded

# The same workloads with no shard-key annotations.
make pgbench-prepare BENCHMARK_MODE=unsharded
make pgbench-run BENCHMARK_MODE=unsharded
make pgbench-cleanup BENCHMARK_MODE=unsharded

make sysbench-prepare BENCHMARK_MODE=unsharded
make sysbench-run BENCHMARK_MODE=unsharded
make sysbench-cleanup BENCHMARK_MODE=unsharded
```

The pgbench sharded setup marks `pgbench_accounts.aid` before loading the
100,000 accounts per scale unit. Accounts are distributed by vshard; the small
branches and tellers tables and append-only history table remain unsharded and
follow the configured unsharded-table policy. This preserves pgbench's scale
discovery and models the common fact-table/dimension-table shape. The wrapper
uses explicit-column COPY streams because shard-aware COPY deliberately
rejects an implicit column order. Set `PGBENCH_SCALE` on the make command line
to change the default scale of 1. Direct script use can pass `--scale` or set
`HAMSTERGRES_PGBENCH_SCALE`. Partitioned and foreign-key pgbench setup is not
supported in sharded mode because those layouts require a separate colocation
design.

The sysbench sharded setup wraps the standard `oltp_read_write` workload. It
marks every `sbtest<N>.id` immediately after `CREATE TABLE`, then loads explicit
single-row inserts so each write contains exactly one shard key. Unsharded mode
uses sysbench's stock bulk loader. Override `PGBENCH_OPTIONS` or
`SYSBENCH_OPTIONS` on the make command line to change run duration, clients,
table count, or table size. The connection defaults match `make run-proxy` and
can be changed with `HAMSTERGRES_BENCHMARK_HOST`,
`HAMSTERGRES_BENCHMARK_PORT`, `HAMSTERGRES_BENCHMARK_USER`,
`HAMSTERGRES_BENCHMARK_PASSWORD`, and `HAMSTERGRES_BENCHMARK_DATABASE`.

For a direct-versus-Proxy comparison in either dataset mode, start the local
Proxy and run:

```bash
make benchmark-sysbench
make benchmark-sysbench BENCHMARK_MODE=unsharded
```

The default comparison is sharded. The benchmark runs 15-second, four-thread
`oltp_read_only` and
`oltp_read_write` workloads through Hamstergres Proxy and directly against
`burrow-01`, prints the normal sysbench reports, and finishes with a JSON ratio
record. It mutates and cleans up the two local `sbtest` tables and is therefore
an explicit benchmark target rather than part of ordinary tests. Shorten or
resize an exploratory run with `HAMSTERGRES_BENCHMARK_SECONDS`,
`HAMSTERGRES_BENCHMARK_THREADS`, `HAMSTERGRES_BENCHMARK_TABLES`, and
`HAMSTERGRES_BENCHMARK_TABLE_SIZE`.

### One-CPU sharding experiment

The development Compose environment limits each PostgreSQL Burrow to one CPU.
To measure whether two one-CPU Burrows provide more keyed-read throughput than
one one-CPU Burrow, run:

```bash
make experiment-sharding-cpu
```

This experiment starts the required Compose services and manages its own
isolated Hamstergres Proxy on ports 16432 and 18080. It compares direct
PostgreSQL on `burrow-01`, a one-Burrow Proxy topology, and a two-Burrow Proxy
topology. Before timing the two-Burrow case, it verifies both Burrows contain
part of the shard-keyed dataset and verifies Docker applied a one-CPU quota to
each PostgreSQL container. It also checks the process-owned routing counters to
prove that every timed read used exactly one owning Burrow and that neither
topology scattered reads. The isolated experiment configs allow 64 pooled
connections per Burrow so the 32- and 64-client points measure Burrow CPU
capacity rather than the normal eight-connection pool default. Production
configs can tune this with `sharding.backend_pool.max_connections`.

Three 15-second rounds at 1, 4, 8, 16, 32, and 64 clients are averaged by
default. Each timed run has a three-second warmup. Proxy topology order and
concurrency order are reversed on alternating rounds to reduce order bias.
Override the concurrency curve with `HAMSTERGRES_EXPERIMENT_CONCURRENCY`, or
select a single point with `HAMSTERGRES_EXPERIMENT_THREADS`. The duration can
be changed with `HAMSTERGRES_EXPERIMENT_WARMUP_SECONDS`,
`HAMSTERGRES_EXPERIMENT_SECONDS`, `HAMSTERGRES_EXPERIMENT_ROUNDS`, or
`HAMSTERGRES_EXPERIMENT_TABLE_SIZE`.

The final JSON Lines records report average transactions per second and the
two-versus-one-Burrow Proxy ratio for every concurrency point. They also record
the CPUs available to Docker, because an undersized container VM can impose a
shared CPU ceiling across the Proxy and both Burrows. This is an end-to-end
scaling experiment, so the result includes Hamstergres Proxy routing and
protocol overhead; a ratio greater than `1` is the evidence that adding the
second one-CPU Burrow improved throughput for this workload.

The Proxy supports sysbench's Parse, Bind, Describe, Execute, Close, Sync, and
Flush protocol flow, including text and binary parameter/result formats. The
test is opt-in because it prepares, runs, and cleans up a short mutating
workload against the local Docker Burrows.

## Run the gateway

Start the Burrows first, then run the Proxy in a separate terminal:

```bash
make up
make run-proxy
```

The main example configuration is written for a proxy running *inside* Docker.
Use the host-ready local example when running the Go gateway on the host:

```bash
cp config/hamstergres.local.example.yaml config/hamstergres.local.yaml
go run ./cmd/hamstergres-proxy -config config/hamstergres.local.yaml
```

Connect a PostgreSQL client to the gateway:

```bash
psql "postgres://anything@localhost:6432/anything?sslmode=disable" -c 'SELECT * FROM accounts'
```

At startup, the Proxy reads explicit shard-key comments from every Burrow and
refuses to start if the registry differs. Mark shard-key columns with `COMMENT ON
COLUMN accounts.tenant_id IS 'hamstergres.shard_key'`. Multiple marked columns
form an ordered compound key and may contain text, numeric, or mixed PostgreSQL
types; tables without any marker are unsharded. `SELECT`, `INSERT`, `UPDATE`,
and `DELETE` route to one Burrow when they provide the complete annotated key.
Ambiguous or partial-key writes to sharded tables are rejected
instead of being duplicated across the fleet. DDL is still applied to every
Burrow. The `sharding.unsharded_tables` configuration selects either one
primary Burrow for all unsharded traffic or replicated writes with randomly
load-balanced reads. Topology-independent reads use the primary Burrow in
`primary` mode and stable round-robin selection across sorted routable Burrows
in `replicated` mode. Extended-protocol portals with a complete bound shard key
use one Tunnel for Bind, Describe, Execute, and Close; append-safe unkeyed read
portals retain deterministic scatter behavior. Global operations over scattered
sharded rows,
including aggregates, `DISTINCT`, `ORDER BY`, `LIMIT`, `OFFSET`, joins, CTEs,
subqueries, set operations, and window functions, fail closed with SQLSTATE
`0A000` until the Proxy has a PostgreSQL-equivalent global result operator. The
Proxy preserves an extended request through
Flush or Sync, so Bind, optional Describe, Execute, and Sync use one backend
flush rather than one round trip per message. Equivalent Parse messages share
a canonical backend name derived from SQL and parameter types; each Tunnel
prepares that statement once and reuses it for frontend aliases. Clean idle
Tunnels return to the per-Burrow connection pools after Sync and can be
multiplexed across frontend sessions; transactions, COPY, and incomplete
protocol batches remain pinned. Multi-Burrow write transactions use PostgreSQL
two-phase commit by default, while read-only and single-Burrow transactions use
ordinary commit. DML transactions run concurrently without a Proxy-wide write
lock. Two-phase commit makes commit atomic but does not provide global
cross-Burrow isolation; applications must retry complete transactions after
PostgreSQL deadlock, serialization, or lock-timeout failures. The default
`transactions.lock_timeout: 1s` prevents undetectable cross-Burrow lock cycles
from blocking indefinitely. Operators who accept partial cross-Burrow commit risk may set
`transactions.two_phase_commit: false` to use sequential ordinary commits.
Prepared statements and portals are pinned to the frontend session and retain
supplied text or binary parameter and result formats. Unsharded `COPY FROM
STDIN` follows the configured table policy: `primary` sends each row only to the
primary Burrow, while `replicated` sends it once to every Burrow. Unsharded
`COPY TO STDOUT` likewise reads from one policy-selected Burrow. Sharded `COPY
FROM STDIN` requires an explicit column list containing the complete annotated
shard key, decodes text, CSV, and supported binary rows, and sends each row
exactly once to its vshard's owning Burrow. Compound and quoted key columns are
supported; NULL, omitted, generated, or undecodable key values fail closed.
CSV headers and binary envelopes reach every input Burrow without being counted
as rows. The Proxy retains at most one incomplete input row, enforces a 16 MiB
per-row limit, and flushes routed chunks synchronously for backpressure.

Sharded `COPY TO STDOUT` appends Burrows in configured order. CSV output has one
header and binary output has one valid header/trailer envelope. This is a
deterministic concatenation, not a global sort; use an explicitly ordered query
when global row order matters. COPY command tags report logical rows rather
than replicated physical writes. A row-routing error aborts and drains every
active Burrow COPY before the frontend returns to ready state. Use an explicit
transaction when a multi-Burrow COPY must commit atomically through two-phase
commit; without one, a transport failure during final completion can leave an
already-completed Burrow committed and requires reconciliation. `COPY BOTH`
remains unsupported. Authentication, TLS, and cancellation requests remain
outside this initial gateway milestone.

Single-column integer identity and `serial`/`bigserial` primary keys may be
omitted in a single-row insert. Hamstergres Proxy allocates a fleet-wide,
increasing ID through Nest, injects it before routing, and fails closed if Nest
is unavailable. See the generated-key contract and migration guidance in
[the architecture document](docs/architecture.md#globally-generated-primary-keys).
The Proxy parses `CREATE TABLE` and `ALTER TABLE` statements and normalizes
auto-incrementing integer keys to `BIGINT GENERATED BY DEFAULT AS IDENTITY`
before applying the schema to every Burrow, then refreshes the Nest registry.

The Proxy defaults to four Go scheduler execution threads to preserve routing
concurrency without scaling to every host CPU. Set `runtime.max_procs` in the
configuration, or set `GOMAXPROCS` when `runtime.max_procs` is omitted, after
benchmarking the deployment.

### Versioned schema and topology in Hamstergres Nest

The development Compose environment includes an etcd-backed **Hamstergres
Nest** on port 2379. On the first successful Proxy startup, it stores two
independent records: the versioned schema contract at
`/hamstergres/schema-registry/v3` and the versioned routing topology at
`/hamstergres/topology/v1`. The topology owns immutable Burrow identities,
lifecycle state, Tunnel endpoint fingerprints, and complete per-table vshard
placement. It is published with etcd compare-and-swap, so concurrent writers
cannot replace one another or expose a partial map.

An upgrade imports schema-registry v3's existing owner map exactly. If there is
no legacy owner map, the Proxy commits the old modulo placement once. Later YAML
reordering or an extra configured Burrow cannot silently remap rows. A missing
configured owner, incomplete distribution, unknown owner, incompatible schema,
or stale topology update fails closed. The current Proxy loads topology at
startup; live watches and dynamic Tunnel reconfiguration are the next phase.

Later Proxy startups compare the live Burrows with the schema snapshot and fail
closed if either has drifted. DDL sent through the Proxy is an intentional
transition and advances compatible schema and topology revisions after all
Burrows agree. Out-of-band schema changes still require an explicit Hamstergres
Migrations workflow.

### Nest storage maintenance

Docker-backed tests keep their isolated schema registry, topology, and
generated-ID sequence below `/hamstergres/tests/<test-key>/`. Each test deletes that exact
namespace during `t.Cleanup` and logically compacts through its cleanup
revision, keeping repeated suites from retaining the large prior values. The
default schema registry, sequence, benchmark, and experiment namespaces are
outside the deletion range. An abrupt machine or container stop can prevent
lifecycle cleanup, so remove previously accumulated test state with:

```sh
make nest-status
make clean-nest-tests
```

`nest-status` reports etcd database size, database size in use, Hamstergres and
test key counts, and active alarms. `clean-nest-tests` deletes only
`/hamstergres/tests/`, compacts through the resulting current revision, and
defragments the local single-member Nest so BoltDB pages return to the
filesystem. Run bulk cleanup while no integration suite is active. Online
compaction is safe for current values, but it removes access to older revisions;
defragmentation briefly blocks Nest reads and writes. Hamstergres Nest does not
need a restart afterward.

The demo Nest enables periodic one-hour auto-compaction and a 1 GiB backend
quota. Override them with `NEST_AUTO_COMPACTION_RETENTION` and
`NEST_QUOTA_BACKEND_BYTES` when Compose starts. These are bounded local-demo
defaults, not production retention, capacity, backup, or multi-member
maintenance guidance.

After a `NOSPACE` alarm, stop test writers, run `make clean-nest-tests`, verify
the reclaimed size with `make nest-status`, then run
`docker compose exec -T hamstergres-nest etcdctl alarm disarm`. If the Nest was
OOM-killed, first restart it with `docker compose up -d --wait
hamstergres-nest`; etcd reopens the durable volume, after which the same cleanup
reclaims the test history. If the backend quota prevents recovery, temporarily
start Compose with a larger `NEST_QUOTA_BACKEND_BYTES`, clean and defragment,
then return to the normal bound.

This is a local-development gateway. Its frontend currently accepts every
startup user without a password and the example listener is reachable on all
interfaces. Do not expose it outside a trusted development network.

## Status UI and CLI

The status page listens on `http://127.0.0.1:8080/` by default. It reports
gateway uptime, query and connection counts, plus a fresh health check and pool
counts for every Burrow. The same data is available from `/api/v1/status`, and
`/healthz` returns `503` whenever a Burrow cannot be reached.

It also records the last 10 seconds, 1 minute, 5 minutes, and 10 minutes of
query traffic. Every window includes total and failed queries, scatter versus
single-shard routing, average execution time, and per-Burrow execution counts.
The page and JSON API keep up to 100 normalized query-shape summaries for the
current process lifetime. They show SQL structure such as `SELECT * FROM
accounts` while replacing string and numeric literals with `?`; for example,
`WHERE tenant_id = 42` becomes `WHERE tenant_id = ?`. Every shape also has a
stable 16-character fingerprint for searching and correlation.

Status is self-contained: the gateway's `internal/status.Collector` reads the
frontend's in-memory counters and its managed `pgx` pools directly. It never
uses `psql` or asks a client process to calculate statistics. The HTML page,
JSON API, and the CLI are presentation layers over that one process-owned
snapshot, leaving a single place to add later internal metrics.

### Prometheus / OpenMetrics

Hamstergres Proxy exposes process-owned metrics at `GET /metrics` on the same
operator listener as the status page (by default
`http://127.0.0.1:8080/metrics`). The response is OpenMetrics 1.0 text and can
be scraped directly by Prometheus. No Grafana dependency or external command is
required.

The endpoint currently exports query success/failure counters, a query latency
histogram in seconds, single-Burrow and scatter routing decisions, per-Burrow
Tunnel executions, frontend connection counts, Burrow health, and backend pool
capacity/use/acquisition signals. Metric names use the
`hamstergres_proxy_` prefix. Labels are limited to fixed states, outcomes,
routing decisions, and configured Burrow names. Raw SQL, query shapes,
fingerprints, credentials, and bound values are deliberately never labels.

| Metric | Unit | Labels |
| --- | --- | --- |
| `hamstergres_proxy_uptime_seconds` | seconds | none |
| `hamstergres_proxy_frontend_connections` | connections | `state`: `active`, `total` |
| `hamstergres_proxy_queries_total` | queries | `outcome`: `success`, `failure` |
| `hamstergres_proxy_query_failures_total` | failures | bounded `category` values |
| `hamstergres_proxy_query_routes_total` | queries | `route`: `single_burrow`, `scatter` |
| `hamstergres_proxy_query_duration_seconds` | seconds | histogram bucket `le` only |
| `hamstergres_proxy_burrow_executions_total` | executions | configured `burrow` |
| `hamstergres_proxy_burrow_up` | boolean | configured `burrow` |
| `hamstergres_proxy_schema_revision` | revision | process-applied Nest schema revision |
| `hamstergres_proxy_topology_revision` | revision | process-applied Nest topology revision |
| `hamstergres_proxy_backend_pool_connections` | connections | configured `burrow`; `state`: `capacity`, `in_use`, `idle` |
| `hamstergres_proxy_backend_pool_acquire_total` | acquisitions | configured `burrow`; `outcome`: `success`, `canceled` |
| `hamstergres_proxy_backend_pool_wait_total` | waits | configured `burrow` |
| `hamstergres_proxy_backend_pool_acquire_duration_seconds_total` | seconds | configured `burrow` |
| `hamstergres_proxy_operations_total` | operations | bounded `operation` and `outcome` values below |

Operational values are fixed in code: `backend_connection`, `backend_query`,
`copy`, `generated_id_allocation`, `nest_request`, `nest_registry_write`,
`schema_registry_mismatch`, `schema_registry_refresh`, and `two_phase_commit`.
Outcomes are bounded values such as `success`, `failure`, `detected`,
`prepare_failure`, or `uncertain`. Query failure categories are likewise fixed,
including `sql_error`, `data_error`, `transaction_error`, `unsafe_routing`,
`burrow_transport`, `resource_exhausted`, and `schema_registry`. Transaction
IDs, errors, query shapes, and other runtime values never appear in metric
labels.

Keep the status listener on a private operator network or place an
authenticated reverse proxy in front of it. `/metrics`, `/api/v1/status`, and
the HTML status page expose topology and traffic volumes and do not implement
authentication themselves. Observability is local and pull-based by default;
a slow or unavailable scraper cannot block PostgreSQL query processing.

Go runtime profiles are disabled by default. Set `status.profiling: true` only
on a private diagnostic listener to expose `/debug/pprof/` CPU, allocation,
heap, goroutine, mutex, block, and trace profiles. The checked-in CPU-scaling
experiment enables profiling on its isolated localhost-only status port.

Example Prometheus scrape configuration:

```yaml
scrape_configs:
  - job_name: hamstergres-proxy
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

### Reproducible local dashboards

The standard Compose environment starts Prometheus, Grafana, Hamstergres Nest,
both Burrows, and one PostgreSQL exporter per Burrow:

```sh
make up
make run-proxy
```

`make run-proxy` runs Hamstergres Proxy directly on the host with
`config/hamstergres.local.example.yaml`. Prometheus reaches its metrics endpoint
through `host.docker.internal:8080`, so rebuilding or containerizing the Proxy
is not required. On Linux, Compose supplies the equivalent `host-gateway`
mapping automatically.

Open Grafana at <http://127.0.0.1:3000> and sign in with `admin` / `admin`.
The provisioned **Hamstergres Demo Overview** dashboard is ready without any
manual data-source or dashboard import. Prometheus is available at
<http://127.0.0.1:9090>, Proxy status at <http://127.0.0.1:8080>, and the
PostgreSQL client endpoint at `127.0.0.1:6432`. All published ports bind to
loopback by default and can be changed with `GRAFANA_PORT`, `PROMETHEUS_PORT`,
`NEST_PORT`, `BURROW_01_PORT`, and `BURROW_02_PORT`. The host-run Proxy listens
on ports `6432` and `8080` as configured in
`config/hamstergres.local.example.yaml`.

Generate traffic and verify all four Prometheus scrape targets plus the
provisioned dashboard with:

```sh
make test-observability
```

The smoke check requires `curl` and `psql`. Dashboard JSON, the Prometheus
scrape and alert rules, and Grafana provisioning live under `observability/`.
The PostgreSQL exporters use the `pg_monitor` role created by
`db/init/002_monitoring.sh`; override `POSTGRES_MONITORING_USER` and
`POSTGRES_MONITORING_PASSWORD` before the first initialization if desired. If
the Burrow data volumes predate that initialization script, run
`docker compose down --volumes` once to recreate this disposable demo data.

This Compose environment is a local demonstration, not a production-hardened monitoring
deployment. Its defaults intentionally use well-known development passwords,
no TLS, no Grafana secret management, no metrics authentication, and local
named volumes. Use managed secrets, authentication, TLS, retention/resource
limits, backups, and your normal access controls in a real environment.

Structured operational events use stable `event`, `component`, `burrow`,
`transaction_id`, and `error_category` fields where applicable. Set
`observability.log_file` to append JSON logs to a local file created with mode
`0600`. Leaving it empty keeps the normal stderr logger. Hamstergres Proxy does
not submit or export logs to an external service. If the configured file cannot
be opened, the Proxy emits `logging_configuration_failed`, falls back to JSON
on stderr, and continues serving queries.

Tracing hooks cover each frontend query and its selected Tunnel/Burrow
executions. Export is disabled by default. To opt in, configure the standard
OpenTelemetry OTLP/HTTP environment variables, for example:

```sh
OTEL_TRACES_EXPORTER=otlp \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector:4318/v1/traces \
hamstergres-proxy --config config/hamstergres.example.yaml
```

`OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` always disables export.
Spans include the statement operation, routing decision, Burrow name, and error
status. They never contain raw SQL or bound parameter values.

```bash
make proxy-status
# or: go run ./cmd/hamstergres-proxy status --status-url http://host:8080/api/v1/status
```

The binary is intentionally structured as a CLI (`hamstergres-proxy status`)
so `add-shard` and `remove-shard` commands can be added without changing the
gateway process contract.

## License

Hamstergres is licensed under the [GNU Affero General Public License v3.0](LICENSE)
only (`AGPL-3.0-only`).
