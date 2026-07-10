# Hamstergres

Hamstergres is an experimental PostgreSQL sharding system: **Tiny paws, many
shards.** Its current component is the **Hamstergres Proxy**, a development
gateway that speaks the PostgreSQL frontend protocol, fans each simple query
out to every configured **Burrow**, and appends compatible result rows. Real
shard-key routing and Nest-backed metadata management come next; this fan-out
behavior is intentionally temporary. See [the architecture and naming
reference](docs/architecture.md) for the component model.

## Layout

```text
cmd/hamstergres-proxy/  PostgreSQL gateway process and status CLI
internal/backend/       Physical-Burrow connection pools and result merging
internal/config/        Gateway configuration loading
internal/proxy/         PostgreSQL frontend session handling
internal/status/        Process-owned status snapshot and HTTP views
internal/statistics/    Bounded rolling query and routing telemetry
internal/router/        Future shard-key, vshard, and Burrow routing
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

## PoC routing configuration

[`config/hamstergres.example.yaml`](config/hamstergres.example.yaml) records
the initial routing contract for the future Go proxy:

- `accounts` is distributed by `tenant_id`.
- `tenant_id % 16` selects a virtual shard.
- Virtual shards `0` through `7` belong to `burrow-01`; `8` through `15`
  belong to `burrow-02`.

The DSNs in that file use Docker service names so a future Proxy container can
connect to the Burrows. When running a Go process directly on the host, use
`localhost:5541` and `localhost:5542` instead. The configuration is a static
development fixture, not a design for the later dynamic metadata/control
plane.

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

Connect a normal PostgreSQL simple-query client to the gateway:

```bash
psql "postgres://anything@localhost:6432/anything?sslmode=disable" -c 'SELECT * FROM accounts'
```

Every query currently executes on both Burrows concurrently. `SELECT` rows are
appended in Burrow-name order. Each Burrow must return the same columns and data
types. Writes and DDL are also fanned out, but they are not a distributed
transaction: a failed Burrow can leave another Burrow changed. Prepared
statements, extended-query protocol, authentication, TLS, cancellation, and
transaction semantics are deliberately outside this initial gateway milestone.

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

```bash
make proxy-status
# or: go run ./cmd/hamstergres-proxy status --status-url http://host:8080/api/v1/status
```

The binary is intentionally structured as a CLI (`hamstergres-proxy status`)
so `add-shard` and `remove-shard` commands can be added without changing the
gateway process contract.
