# PostgreSQL compatibility contract

Hamstergres aims to look like one PostgreSQL database at the application
boundary even when data lives in many Burrows. Compatibility means more than
accepting PostgreSQL syntax: rows, command tags, transaction state, errors,
session settings, and side effects must not reveal the number or identity of
Burrows.

This document separates three things that must not be confused:

1. PostgreSQL behavior Hamstergres intends to reproduce.
2. Temporary gaps that should become compatible.
3. Features that the Proxy intentionally rejects because accepting them would
   promise unsafe or server-local semantics.

The implementation does not yet enforce every policy below. A rule marked
`intentional rejection` is the target fail-closed contract; its linked issue
tracks any remaining enforcement work.

## How compatibility is measured

`make test-postgres-compatibility` runs PostgreSQL 17.10's complete 225-test
core regression schedule through the normal two-Burrow topology. The raw score
is deliberately strict: a safe intentional rejection is not an upstream pass.

The measured 2026-07-16 baseline is **71/225 passing**, 154 gaps, and zero Proxy
crashes. The first high-impact blocker is fleet-wide DDL failure atomicity. A
data-dependent partition DDL succeeded on one Burrow and failed on the other,
after which the run recorded 6,924 schema-registry mismatch errors. This is
tracked in [#50](https://github.com/hamstergres/hamstergres/issues/50).

The long-term report must show both the unchanged raw PostgreSQL score and the
Hamstergres contract score. The contract score will distinguish compatible
behavior, verified intentional rejection, unresolved gaps, regressions, and
missing tests without inflating the upstream pass count. That reporting work is
tracked in [#56](https://github.com/hamstergres/hamstergres/issues/56).

## Compatibility levels

| Level | Meaning |
| --- | --- |
| Compatible | Client-visible behavior matches PostgreSQL for the documented topology and table policy. |
| Partial | A safe subset works; other shapes fail closed or remain a tracked gap. |
| Intentional rejection | The Proxy must return a stable PostgreSQL-compatible error before opening a Tunnel or changing a Burrow or Nest value. |
| Gap | PostgreSQL-compatible behavior is a product goal but is not implemented yet. |
| Outside the application endpoint | The operation administers one physical server or the Burrow fleet and belongs to operator tooling, not application SQL through the Proxy. |

## Target application compatibility

Hamstergres intends to support these PostgreSQL application behaviors:

- Simple query and the core extended-query lifecycle, including prepared and
  parameterized statements, portals, text and binary formats, Flush, Sync,
  cancellation, authentication, and TLS.
- PostgreSQL scalar types, casts, expressions, functions, collations, and
  client-visible formatting.
- Session-local settings and objects without leakage between frontends or
  pooled Tunnels.
- Keyed and unkeyed CRUD with one logical command result and no topology
  details in rows or errors.
- Global query semantics over sharded data, including aggregates and grouping
  ([#51](https://github.com/hamstergres/hamstergres/issues/51)), ordering,
  duplicate elimination and pagination
  ([#52](https://github.com/hamstergres/hamstergres/issues/52)), and joins,
  nested queries, set operations and windows
  ([#53](https://github.com/hamstergres/hamstergres/issues/53)).
- PostgreSQL transaction state, atomic commit, savepoints, retryable errors,
  and every isolation level Hamstergres advertises. Durable cross-Burrow commit
  recovery is tracked in [#8](https://github.com/hamstergres/hamstergres/issues/8),
  while global isolation and honest rejection of unsupported levels are tracked
  in [#57](https://github.com/hamstergres/hamstergres/issues/57).
- Transactional, failure-atomic schema changes across every Burrow, followed by
  one live and Nest registry publication. Hamstergres Migrations will own
  versioned production transitions.
- Streaming `COPY FROM STDIN` and `COPY TO STDOUT` with one logical stream and
  command result.
- Ordinary PostgreSQL constraints that can be enforced in the row's routing
  domain, plus explicitly documented global implementations where they exist.
- PostgreSQL-compatible structured errors and notices without Burrow names,
  Tunnel addresses, or flattened ErrorResponse fields.

## Intentional rejections and scope boundaries

### Foreign keys

Foreign keys are intentionally unsupported for now. Hamstergres Proxy will
reject column-level `REFERENCES`, table-level `FOREIGN KEY`, `ALTER TABLE ...
ADD CONSTRAINT`, and validation forms before any physical or Nest mutation.
Local constraints on each Burrow cannot be presented as fleet-wide referential
integrity. Enforcement is tracked in
[#44](https://github.com/hamstergres/hamstergres/issues/44).

### Uniqueness outside the shard key

For a sharded table, a primary-key, unique, or exclusion constraint is safe
only when every possible conflict is guaranteed to route to the same Burrow.
The normal safe case is a key that contains the complete active shard-key tuple.
Constraints that cannot prove co-location will be rejected; they will not be
forwarded as misleading per-Burrow constraints. The exact AST policy and
enforcement are tracked in
[#54](https://github.com/hamstergres/hamstergres/issues/54).

### Placement-changing schema operations

Dropping, renaming, retyping, or otherwise changing an active shard-key column
cannot be a metadata-only `ALTER`. Until Hamstergres Migrations and Hamstergres
Mover can perform an explicit data-placement transition, unsafe or unknown
shard-key changes will be rejected. This is tracked in
[#45](https://github.com/hamstergres/hamstergres/issues/45).

Non-transactional or otherwise unsafe fleet DDL is also rejected until an
operator workflow exists. Transactional DDL must be failure-atomic under
[#50](https://github.com/hamstergres/hamstergres/issues/50).

### Server-local operations

The Proxy endpoint is not a replacement for physical-server administration.
The following belong to Hamstergres Keeper, Migrations, Mover, or direct
operator tooling and are intentionally outside the application endpoint:

- physical or logical replication connections, base backup, replication slots,
  and replication-protocol commands;
- publication and subscription administration;
- tablespaces, `ALTER SYSTEM`, server configuration files, and server
  filesystem or program execution;
- unallowlisted extensions, foreign-data integrations, or other operations
  whose external state cannot be proven identical and safe across Burrows.

The Proxy will reject these operations rather than blindly fan them out. The
boundary is tracked in
[#58](https://github.com/hamstergres/hamstergres/issues/58).

`COPY PROGRAM` is always rejected. Server-side file `COPY` is supported only
for a known unsharded table in `primary` mode, where exactly one Burrow owns the
operation. Sharded and replicated tables must use the topology-independent
STDIN or STDOUT streaming forms. `COPY BOTH` remains outside the SQL gateway
contract.

### Direct Burrow access

Writes directly to a Burrow bypass routing, global IDs, schema validation,
transaction coordination, and Nest. Their effects are outside the Hamstergres
compatibility contract. Operator reads and maintenance may use direct access,
but applications must use Hamstergres Proxy.

## Important partial areas

| Area | Current contract | Direction |
| --- | --- | --- |
| Wire protocol | Simple query and the core extended lifecycle work. Authentication, TLS, and cancellation remain incomplete. | Complete under [#9](https://github.com/hamstergres/hamstergres/issues/9). |
| Session state | Common runtime settings are relayed and pooled settings are reset, but full affinity and isolation are incomplete. | Finish [#1](https://github.com/hamstergres/hamstergres/issues/1) and [#35](https://github.com/hamstergres/hamstergres/issues/35). |
| Errors | Backend ErrorResponse fields are preserved in important paths; Proxy parser and multi-Burrow error behavior still have gaps. | Finish [#36](https://github.com/hamstergres/hamstergres/issues/36). |
| DDL | Schema DDL fans out and refreshes Nest, but is not yet failure-atomic. | Do [#50](https://github.com/hamstergres/hamstergres/issues/50) next, then move orchestration to [#11](https://github.com/hamstergres/hamstergres/issues/11). |
| Global reads | Single-Burrow and append-safe reads work; global operators fail closed. | Implement [#51](https://github.com/hamstergres/hamstergres/issues/51), [#52](https://github.com/hamstergres/hamstergres/issues/52), and [#53](https://github.com/hamstergres/hamstergres/issues/53). |
| Transactions | Multi-Burrow writes use two-phase commit, but recovery and fleet-wide isolation are incomplete. | Finish [#8](https://github.com/hamstergres/hamstergres/issues/8) and [#57](https://github.com/hamstergres/hamstergres/issues/57). |
| Constraints | PostgreSQL checks execute locally. Foreign keys are rejected by policy; uniqueness must prove co-location. | Enforce [#44](https://github.com/hamstergres/hamstergres/issues/44) and [#54](https://github.com/hamstergres/hamstergres/issues/54). |
| Sequences | A single generated integer primary key can use the Nest allocator. Arbitrary sequences are not fleet-compatible yet. | Implement or fail closed under [#55](https://github.com/hamstergres/hamstergres/issues/55). |
| COPY | Streaming input/output works with documented routing and format limits. | Keep server-local and unsupported formats fail-closed. |

## Decision rule for new PostgreSQL features

Before accepting a feature, answer these questions:

1. Can its complete client-visible semantics be produced independently of
   Burrow count and topology order?
2. Can every side effect be routed or coordinated without partial mutation?
3. Can errors, notices, command tags, session state, and transaction state match
   PostgreSQL?
4. Can restart, Proxy concurrency, and topology change preserve the contract?
5. Is there a Docker-backed comparison against direct PostgreSQL?

If any answer is unknown, the feature must fail closed with the most specific
PostgreSQL-compatible SQLSTATE. A local per-Burrow implementation is not enough
to call a feature compatible.
