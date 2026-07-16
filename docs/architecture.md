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

The application-facing PostgreSQL target and the features that intentionally
fail closed are defined in the
[PostgreSQL compatibility contract](postgresql-compatibility.md).

## Routing vocabulary

Use the familiar technical term `vshard` for virtual shards, named like
`vshard-00001`. A vshard maps to a Burrow. At startup, each Proxy reads the
shard-key registry from every Burrow's PostgreSQL catalogs and refuses to start
if they disagree. A table is sharded only when one or more columns have the
exact comment `hamstergres.shard_key`. Multiple marked columns form a compound
key in PostgreSQL attribute order, so numeric, text, and mixed-type tuples are
supported. For example, mark both `accounts.tenant_id` and
`accounts.region`. It routes reads and writes using the complete tuple and the
64k-vshard ownership catalog persisted in Nest. It scatters append-safe reads
without a usable shard key and schema commands that must reach every Burrow. A
simple-query write to a sharded table without one complete, unambiguous
annotated shard key is rejected; unsharded writes follow the configured fleet
policy. Relation-free `SELECT`, PostgreSQL catalog reads, and session
introspection execute once on one Burrow. In `primary` mode that is the
configured primary Burrow; in `replicated` mode a stable round-robin selection
distributes these reads across the sorted routable Burrows. Reads
whose referenced user tables are all unsharded also execute once, using the
configured primary or one selected replica according to the fleet policy. This
keeps physical catalog copies and Burrow count invisible to PostgreSQL clients.
Aggregates, grouping, `DISTINCT`, ordering, limits, offsets, joins, CTEs,
subqueries, set operations, windows, and row locking fail before a Tunnel is
opened with SQLSTATE `0A000` unless a complete shard key first reduces the query
to one Burrow. Simple and extended-query execution share this planner.
Within a simple-query transaction, statements may route to different Burrows.
Transactions execute DML concurrently with other frontend transactions and do
not take a Proxy-wide write lock. If a write touches more than one Burrow,
Hamstergres Proxy prepares the transaction on the participating Burrows and
then issues `COMMIT PREPARED` to each Burrow. A preparation
failure is rolled back. A commit-phase failure is reported with SQLSTATE
`40003` and the generated transaction ID because manual reconciliation may be
required. The current coordinator does not yet persist commit decisions, so an
operator must inspect `pg_prepared_xacts` after a Proxy crash during commit.

Two-phase commit makes the commit decision atomic, but it does not create
global isolation between Burrows. Concurrent transactions can observe changes
at different times and PostgreSQL can report deadlock or serialization errors
when their participant operations conflict. Applications must retry the
complete transaction for retryable SQLSTATEs and should keep cross-Burrow
transactions short. Fleet-wide schema statements remain serialized so every
Burrow observes them in one order.

Independent Burrows cannot detect a lock cycle that crosses their transaction
managers. Every Tunnel therefore sets PostgreSQL `lock_timeout` from
`transactions.lock_timeout`, which defaults to one second. A bounded lock wait
fails with SQLSTATE `55P03`; applications must retry the complete transaction.

Extended-query statements are parsed on every selected affinity connection,
but a portal whose bound parameters contain a complete annotated shard key uses
one Tunnel for its Bind, Describe, Execute, and Close lifecycle. Unkeyed reads
scatter in stable Burrow order only when their results are append-safe;
topology-independent reads use one Tunnel, and unroutable writes fail closed.
Read-only and single-Burrow transactions avoid prepared transactions.
Multi-Burrow writes use the same two-phase commit path as simple queries.

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

The Proxy defaults to four Go scheduler execution threads, preserving routing
concurrency without scaling scheduler width to every host CPU. Set
`runtime.max_procs` to a positive integer after measuring the deployment. When
that setting is omitted, an explicit `GOMAXPROCS` environment value takes
precedence over the default.

## COPY protocol

Hamstergres Proxy supports the PostgreSQL streaming COPY protocol for `COPY
FROM STDIN` and `COPY TO STDOUT`. For an unsharded table in `primary` mode,
COPY uses only the configured primary Burrow; the table definition still exists
on every Burrow because DDL remains fleet-wide. In `replicated` mode, COPY input
is sent once to every Burrow and COPY output uses one read Burrow.

Server-side `COPY table FROM 'path'` and `COPY table TO 'path'` are supported
only for a known unsharded table in `primary` mode. The statement executes once
on the configured primary Burrow, which owns PostgreSQL's path, permission,
option, command-tag, and error semantics. Server-side COPY is rejected before a
Tunnel is opened for sharded tables and for replicated unsharded tables, because
a path is local to each Burrow and therefore cannot safely describe one logical
operation. `COPY PROGRAM` is always rejected. Each rejection directs clients to
the topology-independent `STDIN` or `STDOUT` streaming form.

Sharded `COPY FROM STDIN` is planned from PostgreSQL's COPY AST and the
Nest-validated schema registry before any Burrow enters COPY mode. The command
must name the relation and an explicit column list containing every annotated
shard-key component. Each completed row is decoded, its typed compound key is
canonicalized through the ordinary router, and the row is sent exactly once to
the vshard owner. Quoted identifiers retain catalog case. A generated shard key
must be supplied explicitly with a fleet-wide value; the Proxy never consumes
Burrow-local sequences during COPY. Missing, duplicate, NULL, unsupported, or
undecodable key shapes fail closed.

Text and CSV input support delimiter, NULL marker, header, quote, escape, and
UTF8 encoding options. CSV headers are copied to every participant because each
backend validates its own stream. Binary input supports integer, OID,
floating-point, boolean, UUID, and text-family shard keys; binary numeric and
other equality representations that the router cannot canonicalize are
rejected before streaming. Options whose row semantics are not implemented,
including `FORCE_NULL`, `FORCE_NOT_NULL`, and error-skipping modes, are rejected
for sharded input. `COPY BOTH` remains unsupported.

The input router buffers only an incomplete row, caps it at 16 MiB, and flushes
each frontend frame's per-Burrow batches synchronously so backend pressure
propagates to the client. A routing or cancellation failure sends `CopyFail` to
every participant, drains their responses, and returns the frontend to a known
ready state; inside a transaction the state is failed and the client must roll
back. COPY in an explicit multi-Burrow write transaction joins the normal
two-phase commit path. Outside an explicit transaction, each Burrow's COPY
statement is atomic, but a transport failure while final completions are being
collected can still leave an earlier Burrow committed; the error is reported
and operators must reconcile that documented partial-failure case.

Sharded `COPY TO STDOUT` streams Burrows in configured order without buffering
the complete export. CSV headers from later Burrows and redundant binary stream
envelopes are removed, yielding one valid frontend stream. The merged command
tag sums physical row counts for sharded data and reports one logical count for
replicated input. COPY output does not promise global row ordering; applications
that require it must use an explicitly ordered query-based alternative. Format
metadata must agree across all participants.

## Schema and topology contracts

Hamstergres Nest keeps schema and topology as separately versioned records. The
schema registry remains at `/hamstergres/schema-registry/v3` during the in-place
upgrade, but its version 4 payload contains only the validated table inventory,
ordered shard keys and types, generated-key metadata, and a monotonic schema
revision. On first startup, a Proxy seeds that schema contract from the live
Burrow catalogs. Later startups compare every Burrow with the Nest snapshot and
fail closed on drift. Hamstergres Migrations remains responsible for intentional
schema transitions and registry publication.

The independent topology catalog is stored at `/hamstergres/topology/v1`. One
atomic catalog value contains a monotonic topology revision, schema compatibility
revision and fingerprint, immutable Burrow IDs, operator-facing names, Tunnel
addresses and configuration fingerprints, lifecycle states, capacity and
placement labels, routing distributions, and the table-to-distribution map.
Credentials remain in Proxy configuration; topology records do not expose
Tunnel passwords. Each distribution has a fixed vshard count and a complete
owner map encoded as a Burrow-ID dictionary plus one numeric owner index per
vshard. Tables may share a distribution initially and later move to independent
distributions without remapping unrelated tables.

Topology publication uses one etcd compare-and-swap transaction. A writer must
present the current etcd modification revision and publish the next logical
revision as a complete value. Stale writers lose without replacing the winner;
unknown owners, duplicate IDs or names, incomplete coverage, invalid lifecycle
transitions, and removal of an owner with placements all fail validation before
publication. Existing distributions cannot change their vshard count in place.

The first upgraded Proxy imports the exact owner array from schema-registry v3.
If no v3 owner array exists, it calculates the old sorted-Burrow modulo map once
and commits that result as topology revision 1. It then rewrites the accepted
schema snapshot without placement. Reordering YAML cannot affect routing after
that point. A configured Burrow absent from the ready or draining topology set
is not routable, and a topology owner missing from configuration makes startup
fail closed. Adding a Burrow starts with no ownership; an explicit later
placement transaction is required before any vshard moves.

The current Proxy loads one immutable topology snapshot during startup. DDL that
successfully creates a new sharded table advances the schema revision and uses a
CAS topology revision to attach the empty table to the bootstrap distribution
without changing any existing owners. Watching topology, preparing new pools,
atomically switching live Proxies, and retiring old Tunnels belong to issue #13.

Tables absent from the schema's sharded-table set are unsharded. The
configuration-wide `sharding.unsharded_tables.mode` chooses `primary` (all
traffic uses one configured Burrow) or `replicated` (writes reach all routable
Burrows and reads choose one Burrow). This policy is deliberately not selectable
per table yet.

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
