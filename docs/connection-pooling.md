# Connection pooling and multiplexing

Hamstergres Proxy owns a PostgreSQL connection pool for every configured
Burrow. These pools let many frontend connections share a smaller set of
physical PostgreSQL connections, but only when the Proxy can prove that moving
the next request to another physical connection preserves PostgreSQL session
semantics.

This document describes the current pooling contract, the state that the Proxy
can replay, the cases that require a pinned Tunnel, and the cases where a
physical connection must be destroyed rather than returned to a pool.

## Terminology

- A **frontend connection** is the PostgreSQL protocol connection between an
  application and Hamstergres Proxy.
- A **physical connection** is a real PostgreSQL connection from the Proxy to
  one Burrow.
- A **Tunnel** is the logical routing path from a frontend connection to a
  Burrow. A Tunnel owns a checked-out physical connection while it is active.
- **Multiplexing** means returning a safe, idle physical connection to its
  Burrow pool so a different frontend can use it later. Two frontends never use
  the same physical connection concurrently.
- **Pinning** means keeping a frontend associated with the same Tunnel because
  its state cannot safely move to another physical connection yet.

Pools and all of their bookkeeping are local to one Proxy process. They are not
shared between Proxy replicas, and session state is never transferred between
Proxy processes.

## Pool topology and sizing

The Proxy creates one independent pool per configured Burrow. The default
maximum is eight physical connections per pool:

```yaml
sharding:
  backend_pool:
    max_connections: 8
  physical_shards:
    burrow-01:
      dsn: "postgres://hamster:hamster@localhost:5541/hamstergres?sslmode=disable"
    burrow-02:
      dsn: "postgres://hamster:hamster@localhost:5542/hamstergres?sslmode=disable"
```

The setting applies to each Burrow in each Proxy process. A useful upper bound
when planning PostgreSQL capacity is therefore:

```text
Proxy processes x Burrows x max_connections
```

Connections are acquired as work reaches a Burrow, so this is a capacity bound
rather than a promise that every slot is always open. A request that needs more
than one Burrow may hold one physical connection in several pools at the same
time. Size the pools for routed concurrency and watch acquisition waits before
raising the limit; increasing it moves concurrency and resource use to the
PostgreSQL Burrows.

## Checkout and release lifecycle

When a frontend needs a session-aware backend path, the Proxy performs these
steps for each required Burrow:

1. Acquire a physical connection from that Burrow's pool.
2. Reconcile the connection's Proxy-owned prepared statements with
   `pg_prepared_statements` while the connection is still at its clean pool
   baseline.
3. Apply PostgreSQL startup parameters associated with the frontend.
4. Replay the frontend's tracked session settings.
5. Execute the frontend protocol cycle.
6. At a safe boundary, either reset and release the connection, keep it pinned,
   or destroy it.

The safe release boundaries are the end of a simple-query cycle and a clean
extended-protocol `Sync`. At that point the Proxy releases the Tunnel only when
there is no active transaction, COPY stream, pending extended request, failed
protocol cycle, or session-affinity requirement.

Some stateless simple queries use a short-lived pooled query path directly.
They still acquire the required Burrow connections for the duration of the
query, but do not create a persistent frontend-to-Tunnel association.

## When multiplexing is safe

Provided no other feature requires affinity, the current implementation can
multiplex these cases:

| Frontend behavior                                             | Why it can be multiplexed                                                                                       |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Ordinary autocommit simple queries                            | The query finishes at an idle protocol boundary and leaves no tracked session affinity.                         |
| Extended Parse/Bind/Describe/Execute                          | Frontend prepared statements are virtualized by the Proxy, and the Tunnel can be released after a clean `Sync`. |
| `SET`, `RESET`, and related GUC changes outside a transaction | The Proxy records the latest command for each setting and replays it on the next Tunnel.                        |
| Startup parameters such as `TimeZone`                         | The desired frontend values are applied on every checkout.                                                      |
| A completed transaction with no remaining affinity            | The transaction stays pinned while active and becomes releasable after a clean completion boundary.             |

Multiplexing is a statement about connection reuse, not query routing. A query
may still target one Burrow or scatter to several Burrows according to the
normal routing rules described in the [architecture](architecture.md).

## Replayable session settings

For ordinary session settings changed outside a transaction, the Proxy keeps a
logical frontend-session record rather than keeping a physical connection
permanently pinned. It records the latest successful command by setting name,
including settings such as GUCs, `search_path`, role, and session
authorization. The recorded commands are replayed whenever the frontend checks
out a different Tunnel.

Outside a transaction, successful `RESET ALL` and `DISCARD ALL` clear the
tracked setting list. `DISCARD ALL` is the intended explicit recovery point
when an application wants to discard other resettable session affinity as
well. Applications should not use `RESET ALL` as a substitute for cleaning
non-GUC state such as temporary objects or notifications.

PostgreSQL can roll back `SET` and `RESET` executed inside a transaction or at a
savepoint. The Proxy does not try to reconstruct those semantics from a replay
log. A transactional setting change therefore pins the frontend to its current
Tunnel. This conservative affinity remains after transaction completion until
the frontend explicitly resets its session state.

`ParameterStatus` messages are forwarded or generated so the frontend's view
of settings remains consistent with the selected backend state.

## When a Tunnel must stay pinned

The following behaviors require one frontend to retain its Tunnel instead of
sharing the physical connection with another frontend:

| Behavior                                                | Reason                                                                                                                          | How affinity ends                                                                                                    |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Explicit transaction                                    | Statements, errors, savepoints, and transaction-local state must use the same PostgreSQL session.                               | Successful `COMMIT` or `ROLLBACK`, including correct handling of `AND CHAIN`, followed by a clean protocol boundary. |
| COPY stream                                             | COPY data and completion messages belong to the backend protocol exchange that started the stream.                              | The COPY exchange finishes successfully and reaches a clean boundary.                                                |
| Unsynchronized extended-protocol batch or active portal | Later protocol messages depend on backend-local portal and error state.                                                         | The batch completes at `Sync` without leaving other affinity.                                                        |
| Temporary objects                                       | Temporary schemas and relations are physical-session state.                                                                     | Explicit session reset; the Proxy remains conservative even if application SQL appears to drop the object.           |
| `LISTEN` or `UNLISTEN`                                  | Notification registrations belong to a PostgreSQL session.                                                                      | Explicit session reset.                                                                                              |
| SQL `PREPARE`                                           | SQL-level prepared statements are backend-session objects and are distinct from Proxy-virtualized extended-protocol statements. | Explicit session reset; the physical connection receives a full prepared-state cleanup before reuse.                 |
| Declared cursors                                        | Cursor position and lifetime are backend-session state.                                                                         | Explicit session reset.                                                                                              |
| Session advisory locks                                  | Locks must stay associated with the acquiring PostgreSQL session.                                                               | Explicit session reset; cleanup calls `pg_advisory_unlock_all()`.                                                    |
| `set_config(...)`, especially `set_config(..., false)`  | The function can change session state in an expression rather than a top-level `SET`.                                           | Explicit session reset.                                                                                              |
| `UPDATE pg_settings`                                    | The update can change settings without using ordinary replayable `SET` syntax.                                                  | Explicit session reset.                                                                                              |
| `SET` or `RESET` inside a transaction                   | Rollback and savepoint effects cannot be reconstructed safely for a future Tunnel.                                              | Explicit session reset after the transaction.                                                                        |
| Mixed multi-statement batch containing session state    | The Proxy cannot safely split replayable state from the rest of the batch.                                                      | Explicit session reset.                                                                                              |

Pinning is deliberately conservative. Executing the apparent inverse command,
such as `UNLISTEN`, dropping a temporary object, or unlocking one advisory lock,
does not prove that all backend-session state is clean. Applications that want
to return a long-lived frontend connection to a multiplexable state should use
`DISCARD ALL` at a safe idle boundary.

Pinning also does not turn session-local PostgreSQL features into fleet-global
features. For example, notification and advisory-lock behavior remains scoped
to the selected Burrow and its physical PostgreSQL session.

## When the physical connection is destroyed

Some state is too opaque to reset safely. The Proxy marks the Tunnel for
destruction in these cases and never returns its physical connection to a pool:

- an opaque `DO` block, because its procedural body may create hidden
  process-local or session-local effects;
- `LOAD`, because a loaded library changes the PostgreSQL backend process;
- a parse failure whose leading keyword indicates session-state SQL, because
  the Proxy cannot safely classify the statement;
- failure while reconciling prepared statements, applying startup parameters,
  replaying settings, or resetting the session;
- frontend cancellation, disconnect, or protocol failure while transaction or
  backend state is uncertain; or
- any attempted release when PostgreSQL does not report idle transaction status.

`DISCARD ALL` cannot make a Tunnel marked by `DO` or `LOAD` reusable. It resets
PostgreSQL-managed session objects, but cannot prove that arbitrary procedural
or process-local effects were undone.

## Reset contract before pool return

Before an ordinary reusable Tunnel returns to a pool, PostgreSQL must report
idle transaction status. The Proxy then runs this reset with a five-second
timeout:

```sql
SET SESSION AUTHORIZATION DEFAULT;
RESET ALL;
CLOSE ALL;
UNLISTEN *;
SELECT pg_advisory_unlock_all();
DISCARD PLANS;
DISCARD SEQUENCES;
DISCARD TEMP;
```

This removes frontend-owned state while preserving canonical prepared
statements owned by the Proxy. If the frontend used SQL `PREPARE`, the Proxy
instead clears its matching cache and performs a full `DISCARD ALL`. Any reset
failure closes the physical connection and forgets its prepared-statement and
runtime-parameter bookkeeping.

As a final guard, a physical connection is rejected from the pool if
`standard_conforming_strings` is not `on`. This prevents parser state that
conflicts with the value advertised to frontends from leaking between sessions.

## Prepared statements

Extended-protocol prepared statements do not by themselves require permanent
pinning. The Proxy stores frontend statement definitions logically, derives a
stable canonical backend name from SQL and parameter OIDs, and tracks which
canonical names exist on each physical connection. After a later checkout, it
reconciles that cache and injects any missing backend Parse before Bind.

This design preserves prepared-statement reuse while allowing the next Execute
to use a different physical connection. Named portals and incomplete protocol
batches still require pinning until `Sync` because their execution state is not
virtualized across Tunnels.

SQL `PREPARE` is intentionally treated differently: it creates a native
backend-session object, pins the frontend, and requires full prepared-state
cleanup before the physical connection can be reused.

## Limitations and non-guarantees

- Session-state classification is syntax-aware but not omniscient. New
  extensions or functions can introduce state-changing SQL that the classifier
  does not yet recognize. Every newly supported session-mutating construct must
  define whether it is replayable, pinned, or destructive and add isolation
  regressions.
- The Proxy preserves session association for recognized notification,
  advisory-lock, cursor, and temporary-object operations, but their semantics
  remain those of the selected Burrow rather than a single fleet-wide
  PostgreSQL server.
- Transactional `SET` and `RESET` are intentionally more conservative than
  PostgreSQL. The frontend remains pinned rather than having commit, rollback,
  and savepoint changes inferred and replayed.
- A frontend connection can remain long-lived while its physical Tunnel changes
  between clean protocol cycles. Applications must not depend on backend PID or
  other unclassified physical-session identity remaining stable.
- Pool capacity is per Proxy process and per Burrow. Adding Proxy replicas or
  Burrows increases the possible PostgreSQL connection count.
- An application-side connection pool can still be used in front of the Proxy.
  Its logical connections retain Hamstergres frontend-session state, so the
  application pool should reset sessions before giving them to another tenant
  or request context.

## Observability and troubleshooting

The status UI and `GET /api/v1/status` report each Burrow pool's maximum, total,
acquired, and idle connections, plus acquisition counts, waits, errors, and
total acquisition time. The same data is exported from `GET /metrics`:

| Metric                                                                  | Use                                                                 |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `hamstergres_proxy_backend_pool_connections{burrow,state}`              | Compare `in_use` and `idle` connections with configured `capacity`. |
| `hamstergres_proxy_backend_pool_acquire_total{burrow,outcome}`          | Track successful and canceled acquisitions.                         |
| `hamstergres_proxy_backend_pool_wait_total{burrow}`                     | Detect requests that had to wait for a pool slot.                   |
| `hamstergres_proxy_backend_pool_acquire_duration_seconds_total{burrow}` | Measure aggregate time spent acquiring physical connections.        |

Operational events also record `backend_connection_multiplex` releases,
prepared-statement cache outcomes, and frontend-session cleanup outcomes.

When waits rise, first determine whether the workload contains long
transactions, COPY streams, or session-affinity features. Raising
`max_connections` may reduce waits, but it does not correct a workload that
holds Tunnels indefinitely and it increases PostgreSQL resource consumption.

## Adding a new session-state feature

Any change that supports new PostgreSQL session behavior should answer all of
these questions in code, tests, and this document:

1. Can the state be represented as deterministic SQL and replayed on another
   Tunnel?
2. If not, can the frontend safely stay pinned to one physical connection?
3. What exact command proves the state has been cleared?
4. Can the ordinary reset sequence remove it, or must the physical connection
   be destroyed?
5. Does it interact with transaction rollback, savepoints, `AND CHAIN`, COPY,
   portals, prepared statements, or multi-Burrow routing?
6. Is there a forced-connection-reuse regression proving that one frontend's
   state cannot contaminate another frontend?

When there is doubt, prefer pinning. When cleanup cannot be proven, destroy the
physical connection rather than returning uncertain state to a pool.
