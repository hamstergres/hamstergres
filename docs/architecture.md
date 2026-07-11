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
primary-key registry from every Burrow's PostgreSQL catalogs and refuses to
start if they disagree. It routes reads and writes using the discovered primary
key (all columns for a composite key), with the static 64k-vshard map. It
scatters only reads without a usable primary key and schema commands that must
reach every Burrow. A write without one unambiguous primary key is rejected;
it is never broadcast as a substitute for distributed transactions. Within a
transaction, the first routed statement pins the session to one Burrow and a
statement for another Burrow (or a scatter query) is rejected. The future Proxy
will use Nest metadata to choose a Tunnel and target the appropriate Burrow.

## Schema registry contract

Hamstergres Nest persists the validated primary-key registry at
`/hamstergres/schema-registry/v1` by default. On first startup, a Proxy seeds
an empty registry from the live Burrow catalogs. Later startup compares the
live registry with both every other Burrow and the Nest snapshot. Any mismatch
is a startup error: the Proxy never guesses which schema is authoritative or
applies DDL itself. Hamstergres Migrations is responsible for an intentional
schema change and for updating the Nest snapshot as part of that workflow.

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
