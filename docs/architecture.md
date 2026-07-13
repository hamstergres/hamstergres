# Hamstergres architecture and naming

Hamstergres is the PostgreSQL sharding system with the tagline **Tiny paws,
many shards.** This document defines the names used by the codebase and its
operators.

## Components

| Component | Responsibility | Binary or service name |
| --- | --- | --- |
| Hamstergres Proxy | Accepts PostgreSQL connections and routes SQL by shard key. | `hamstergres-proxy` |
| Hamstergres Keeper | Coordinates topology, health, reconciliation, and failover awareness. | `hamstergres-keeper` |
| Hamstergres Nest | etcd-backed control-plane metadata: the shard map, routing rules, schema registry, and migration state. | `hamstergres-nest` |
| Hamstergres Migrations | Changes schemas and records migration state. | `hamstergres-migrations` |
| Hamstergres Mover | Moves data during resharding. | `hamstergres-mover` |
| Hamstergres Burrows | The fleet of physical PostgreSQL shards. | infrastructure |

A **Burrow** is one physical PostgreSQL shard or shard cluster, for example
`burrow-01`. A cluster may contain `burrow-01-primary`,
`burrow-01-replica-01`, and `burrow-01-replica-02`. A **Tunnel** is the logical
routing path from a Proxy to a Burrow.

```text
Application
    ↓
Hamstergres Proxy
    ↓ Tunnel
Hamstergres Burrow-01
```

The Nest is separate from the data path:

```text
Hamstergres Proxy
    ↓ reads metadata
Hamstergres Nest
```

Nest is not a shard. A Burrow is a shard. Burrows are all shards together. The
Nest contains the map that says which data belongs to which Burrow.

## Routing vocabulary

Use the familiar technical term `vshard` for virtual shards, named like
`vshard-00001`. A vshard maps to a Burrow. At startup, each Proxy reads the
shard-key registry from every Burrow's PostgreSQL catalogs and refuses to start
if they disagree. A table is sharded only when one or more columns have the
exact comment `hamstergres.shard_key`. Multiple marked columns form a compound
key in PostgreSQL attribute order, so numeric, text, and mixed-type tuples are
supported. For example, mark both `accounts.tenant_id` and
`accounts.region`. It routes reads and writes using the complete tuple and the
64k-vshard ownership catalog persisted in Nest. It scatters only reads without
a usable shard key and schema commands that must
reach every Burrow. A simple-query write without one unambiguous primary key is
rejected; it is never broadcast as a substitute for distributed transactions.
Within a simple-query transaction, statements may route to different Burrows.
If a write touches more than one Burrow, Hamstergres Proxy prepares the transaction
on the fleet and then issues `COMMIT PREPARED` to each Burrow. A preparation
failure is rolled back. A commit-phase failure is reported with SQLSTATE
`40003` and the generated transaction ID because manual reconciliation may be
required. The current coordinator does not yet persist commit decisions, so an
operator must inspect `pg_prepared_xacts` after a Proxy crash during commit.

Extended-query statements are parsed on every affinity connection, but a portal
whose bound parameters contain a complete primary key uses one Tunnel for its
Bind, Describe, Execute, and Close lifecycle. Unkeyed reads scatter in stable
Burrow order, and unroutable writes fail closed. Read-only and single-Burrow
transactions avoid prepared transactions. Multi-Burrow writes use the same
two-phase commit path as simple queries.

The Proxy buffers an extended request through the frontend Flush or Sync
boundary and forwards Bind, optional portal Describe, Execute, and Sync in one
backend flush. It does not inject a network flush between those messages.
Normalized SQL plus parameter OIDs produce a stable internal statement name.
Each Tunnel records the names already prepared on its PostgreSQL connection,
reuses them for equivalent frontend aliases, and retains the backend statement
when an alias closes. Cache hits and misses are exposed as the
`prepared_statement_cache` operational metric. PostgreSQL still performs Parse
on a Tunnel cache miss; subsequent equivalent frontend Parse messages are
answered from the cache without repeating backend preparation.

Tunnel connections come from the per-Burrow pools. A frontend keeps its
connections pinned while a transaction, COPY stream, portal request, or
unsynchronized protocol batch is active. After a clean idle Sync, the
connections return to their pools and may serve another frontend. On checkout,
the Proxy reconciles its per-connection cache with `pg_prepared_statements`;
frontend statement definitions remain virtual in the Proxy, and any missing
canonical Parse is injected before Bind on the newly selected connection. A
disconnect in an unsafe state closes the physical connections instead of
returning uncertain transaction or protocol state to the pool.

Two-phase commit is enabled by default. Setting
`transactions.two_phase_commit: false` selects best-effort sequential COMMIT
for operators who knowingly accept partial cross-Burrow commits. A failure in
that mode is not atomic: earlier Burrows may already be committed, and the
operator must reconcile application data.

## COPY protocol

Hamstergres Proxy supports the PostgreSQL streaming COPY protocol for `COPY
FROM STDIN` and `COPY TO STDOUT`. COPY input is deliberately fanned out to every
Burrow, matching the current extended-query contract. COPY output is appended
in configured Burrow order. Format metadata must agree across the fleet, and
`COPY BOTH` remains unsupported. COPY inside a multi-Burrow transaction joins
the same two-phase commit.

## Schema registry contract

Hamstergres Nest persists the validated table inventory, shard-key, and vshard
registry at `/hamstergres/schema-registry/v3` by default. On first startup, a Proxy seeds
an empty registry from the live Burrow catalogs. Later startup compares the
live registry with both every other Burrow and the Nest snapshot. Any mismatch
is a startup error: the Proxy never guesses which schema is authoritative or
applies DDL itself. Hamstergres Migrations is responsible for an intentional
schema change and for updating the Nest snapshot as part of that workflow.

The snapshot is also Hamstergres' sharding catalog: it records each sharded
table and ordered column list plus the owning Burrow for every vshard. Tables absent from the
catalog are unsharded. The configuration-wide
`sharding.unsharded_tables.mode` chooses `primary` (all traffic uses one
configured Burrow) or `replicated` (writes reach all Burrows and reads choose
one Burrow). This policy is deliberately not selectable per table.

Hamstergres Proxy parses DML with PostgreSQL's AST before it makes a routing
decision. Simple and extended-query execution use the same plan. Relation and
column aliases, schema qualification, quoted identifiers, comments,
parentheses, casts, constants, and bound parameters are resolved from syntax
nodes rather than SQL text. A sharded statement targets one Burrow only when
the AST proves every component of one shard-key tuple. `INSERT` routing
requires exactly one `VALUES` row; keyed `SELECT`, `UPDATE`, and `DELETE`
routing requires unambiguous equality or tuple-equality predicates.

Shapes that can span routing domains stay conservative. Reads with `OR`,
ranges, joins, CTEs, subqueries, set operations, or multiple physical
relations scatter. Sharded writes with those shapes, multi-row `VALUES`,
`INSERT ... SELECT`, `ON CONFLICT`, a shard-key-changing `UPDATE`, or multiple
SQL statements are rejected before opening a Tunnel. A parser failure never
falls back to text matching or permissive routing.

Quoted identifiers retain their PostgreSQL catalog case and are never folded
together with unquoted names. Nest schema metadata also records each shard-key
type. The Proxy canonicalizes equivalent typed values before hashing (for
example, `1`, `'01'::bigint`, and a text-format bound bigint all route alike);
types whose equality representation is not understood remain unrouted.

Extended-query affinity is lazy. Parse and AST validation open no Burrow
connections, statement Describe uses one representative Burrow, and Bind or
Execute acquires only its selected participants. A routed execution therefore
does not create affinity connections to every configured Burrow; scatter and
COPY acquire the fleet only when their execution semantics require it.

The Proxy keeps the validated Nest inventory in process and exposes it through
the status HTML, JSON API, CLI, and Prometheus metrics. Status requests never
query Burrows, invoke `psql`, or fetch Nest synchronously.

## Globally generated primary keys

Hamstergres Proxy replaces a backend-generated, single-column integer primary
key with a value from the fleet-wide sequence stored at
`/hamstergres/sequences/global-id/v1`. The schema registry recognizes PostgreSQL
identity columns and `serial`/`bigserial` sequence defaults. For a single-row
`INSERT` that omits such a key or supplies `DEFAULT`, the Proxy atomically
allocates a `BIGINT`, makes it explicit in the statement, hashes that value to
choose a vshard, and sends the statement to one Burrow. This applies to simple
queries and prepared extended-query binds. Explicit primary keys are preserved.

Before schema statements reach the Burrows, the Proxy parses `CREATE TABLE` and
`ALTER TABLE` with PostgreSQL's parser. `serial`, `smallserial`, and `bigserial`
definitions are normalized to `BIGINT GENERATED BY DEFAULT AS IDENTITY`, as are
integer identity columns. `GENERATED ALWAYS` is changed to `BY DEFAULT` so the
Proxy's explicit fleet-wide value is accepted. After the DDL succeeds on every
Burrow, the Proxy rereads all catalogs, updates Nest, and replaces its live
routing registry. The application can therefore use the new table immediately
without restarting that Proxy.

The normalization also handles pg_dump's split identity form, where
`CREATE TABLE` defines an ordinary column and a later `ALTER TABLE ... ADD
GENERATED ALWAYS AS IDENTITY` attaches generation. This mirrors the useful
parts of external proxy's pg_dump AST pipeline, especially normalizing integer keys to
`bigint`, while retaining a `BY DEFAULT` identity marker so Hamstergres can
rediscover generated-key intent from each Burrow's catalogs.

Allocation uses an etcd compare-and-swap transaction, so concurrent Proxies
cannot receive the same ID. Allocation order is increasing, but aborted
transactions, prepared portals that are never executed, and failed Burrow
writes leave gaps, as PostgreSQL sequences do. The Proxy fails the insert before
touching a Burrow when Nest is unavailable; it never falls back to a local or
per-Burrow sequence. Adding or removing Burrows does not change the sequence.

The initial contract deliberately excludes generated composite keys,
non-integer generated keys, multi-row `VALUES`, `INSERT ... SELECT`, and inserts
without a column list. These shapes are rejected unless they contain a complete
explicit primary key.

Existing `serial`, `bigserial`, and integer identity tables remain detectable,
but applications must write through the Proxy and operators
must set the Nest counter above the largest existing fleet-wide key before
enabling generated inserts. First audit and repair duplicates across all
Burrows. Direct Burrow writes and resetting a Burrow-local sequence can still
violate fleet-wide uniqueness and are unsupported. For new schemas, prefer a
`BIGINT GENERATED BY DEFAULT AS IDENTITY` primary key. DDL sent through the
Proxy is normalized to this shape automatically.

## Naming rules

- Use the formal names above in documentation, configuration IDs, status pages,
  and service names.
- Use `burrow-01`, `burrow-02`, and so on for physical data infrastructure.
- `shard` remains valid as a generic technical term in routing algorithms and
  PostgreSQL code. Do not use it as the operator-facing physical service name.
- “Food Stash” and “Moving Bedding” are informal metaphors only; use Nest and
  Mover in source, configuration, and operational documentation.

In short: **Proxy routes, Keeper coordinates, Nest remembers, Migrations
changes schemas, Mover moves data, and Burrows store the data.**
