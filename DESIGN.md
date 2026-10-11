# Design: low-churn transactional outboxes

This document explains what the prototype is meant to do, how it differs from a
conventional outbox table, and the trade-offs. It is written for an engineering
audience deciding whether to take the approach further. It covers two families: the
log-message transports, which remove the table and need logical decoding, and the
cursor transport, which keeps a table but never deletes from it and needs nothing
beyond an ordinary Postgres.

## The problem with a table outbox

A conventional outbox is a table. Producers insert a row in the same transaction as
their business write. A relay selects a batch, publishes it to the broker, deletes the
rows and commits. This gives atomic enqueue and at-least-once delivery, and it is the
right design.

The cost is churn. Every event is one INSERT and one DELETE, so every event leaves a dead
tuple and dead index entries behind. Postgres has to vacuum them away, and under bursty
load vacuum falls behind: the relay's selects walk over thousands of dead entries, the
index bloats, and the table needs aggressive autovacuum settings and periodic reindexing
just to hold steady. The write pattern is already minimal, so no change to the relay
query fixes this. The only way to remove the churn is to stop writing rows, or at least
to stop deleting them.

## The table and the two log-message transports

```mermaid
flowchart TB
  subgraph txn["producer transaction (through a transaction-mode pooler)"]
    direction LR
    biz["INSERT business rows"] --> emit
  end
  emit{{"emit event"}}
  emit -->|"mode=table"| T[("outbox_items<br/>insert, later delete")]
  emit -->|"mode=wal"| W["pg_logical_emit_message()<br/>WAL record, no table"]
  emit -->|"mode=journal"| J[("outbox_journal<br/>insert only, lsn column")]
  J -. "same statement" .-> W
  T --> TR["table relay<br/>poll, SKIP LOCKED, publish, delete, commit"]
  W --> S["replication slot<br/>retains WAL until acked"]
  S --> R["relay<br/>decode, batch, publish, ack commit LSN"]
  J -. "replay (last acked, new slot start]<br/>only after slot loss" .-> R
  TR --> PS[["sink per target: Pub/Sub topic,<br/>or simulated fixed latency per batch"]]
  R --> PS
```

**`table`** is the baseline: the conventional outbox.

**`wal`** removes the table. `pg_logical_emit_message(true, target, payload)` writes the
payload into the write-ahead log as part of the open transaction. It touches no table.
A rolled-back transaction never produces the record. A logical replication slot on the
primary keeps every log record after the slot's confirmed position; the relay reads the
stream over a replication connection, keeps messages whose prefix equals its target,
batches them, publishes, and only then advances the slot to the end of the last fully
published source transaction.

**`journal`** is `wal` plus a safety net, in the spirit of LISTEN/NOTIFY backed by
polling: the log stream is the fast path, an append-only table is the recovery path.
The producer writes both in one statement, storing the message's log position in the
row:

```sql
INSERT INTO outbox_journal (target, payload, lsn)
VALUES ($1, $2, pg_logical_emit_message(true, $1, $2));
```

In steady state the journal is never read. Nobody updates or deletes it. It is
partitioned by day and old partitions are dropped after a retention period, so it costs
one insert per event and produces no dead tuples. When a relay reconnects and finds its
slot gone, it creates a new one, which returns the position the new stream starts
from, replays the journal rows for its target between its last acknowledged position
and that start, then streams. The relay keeps its last acknowledged position in a
state file outside Postgres for exactly this purpose; the README's "Journal recovery
and position tracking" section walks through the start-up branches, the replay query,
the boundary edge and what to persist where.

## Targets

Every event names a target. In `wal` and `journal` the target is the message prefix; in
`table` it is a column. One relay serves one target: it has its own slot, keeps only
that prefix, and publishes to that target's topic. Lag, restarts and throughput are
independent per destination, and a slow consumer for one target never holds back
another. The cost is that each relay decodes the whole stream and discards other
targets' messages, so server-side decoding work scales with the number of targets. For
a handful of targets that is negligible; for dozens, one relay fanning out from one
slot is the alternative, at the cost of shared lag.

## The cursor transport: no slot, no deletes

Enabling `wal_level = logical` is a cluster-wide decision with a restart, and on a
shared instance it is often not one the outbox owner gets to make. The cursor transport
is the answer for that case. It asks two things of the design: keep the producer's
transactional guarantee, and leave as little behind as possible. It gives up the one
thing only logical decoding can provide, commit order.

```mermaid
flowchart LR
  P[producer txn<br/>INSERT business rows<br/>INSERT outbox_events] --> B[PgBouncer]
  B --> PG[(outbox_events<br/>append-only, partitioned by day<br/>xid = writing txn, shard = hash key % 32)]
  PG --- C[(outbox_cursor<br/>per target × shard:<br/>frontier xid,id + pending xids)]
  R1[relay replica 1<br/>N workers] -->|lock a shard cursor<br/>FOR UPDATE SKIP LOCKED| C
  R2[relay replica 2<br/>N workers] --> C
  R1 -->|read forward, publish, advance| PS[[Pub/Sub topic]]
  R2 --> PS
```

**Where the churn goes.** The producer inserts one row. The relay reads rows and
updates one cursor row per batch, a HOT update on a one-row-per-shard table with a low
fillfactor. Nothing deletes from the event table; daily partitions are dropped after a
retention period. Dead tuples per event: zero. The relay also never locks or dirties a
producer page, since its reads are ordinary snapshot reads, so the contention that
collapsed the delete-based relay under concurrent load does not arise.

**Why a plain id cursor is wrong, and what fixes it.** Sequence ids are assigned at
insert time, not commit time. A slow transaction can hold a low id and commit after a
fast one with a higher id has been read; a cursor that moved past the high id would
never see the low one. Delete-based outboxes exist to avoid exactly this. The fix
here is to record the writing transaction's id, `pg_current_xact_id()`, order by it, and
let the cursor remember which transactions were still in flight when it passed them:

- Every poll runs under one repeatable-read snapshot and reads the snapshot's in-flight
  transaction list with `pg_current_snapshot()`.
- The frontier moves forward through visible rows in `(xid, id)` order. Visible means
  committed, so nothing uncommitted is ever published.
- In-flight transaction ids below the new frontier go into the cursor's `pending` set.
  On a later poll, pending ids no longer in flight have resolved; their rows, if they
  committed, are read by a separate catch-up query and published first.

The invariant: every committed row at or below the frontier has been published unless
its transaction is in `pending`. A transaction held open for five minutes costs nothing
while it runs and its rows arrive on the first poll after it commits. The simpler
alternative, reading only below the snapshot's `xmin`, would stall publishing for the
whole five minutes on every such transaction, which rules it out for any cluster that
has them.

**Shards are how replicas help.** One cursor per target would mean one active
publisher, however many replicas you run. So the key is hashed into a fixed number of
shards at insert time, in a stored generated column, and there is one cursor per
shard. A key always lands in one shard and a shard has one lock holder at a time, so
per-key order is as good as with a single cursor. Workers in every replica race over
the shard cursors with `FOR UPDATE SKIP LOCKED`, holding the lock for exactly one
batch. There is no assignment and nothing to rebalance: shards flow to free workers and
a dead replica releases its locks on rollback. The shard count is the parallelism
ceiling. It is fixed in the schema; changing it is a drain, a remap of the generated
expression, and fresh cursor rows, with no data movement, because a shard owns a cursor
position and not data. Postgres has no consistent hashing, and this design does not
need it.

**What it gives up.**

- *Commit order.* Rows leave in transaction-id order, the order transactions first
  wrote. Two transactions on the same key normally serialise on that key's business
  row, so per-key order holds when that row is the transaction's first write. A
  transaction that writes other rows first can take its id early and commit late. The
  delete-based table has the same property; only logical decoding gives commit order.
- *Push.* The relay polls. An idle deployment costs workers ÷ poll interval tiny
  transactions per second, and new work is noticed within shards ÷ workers poll
  intervals. LISTEN/NOTIFY cannot help through a transaction-mode pooler.
- *A connection per in-flight batch.* Each worker holds a pooled connection, idle in
  transaction, while it waits for Pub/Sub acks. Size the pool for replicas × workers,
  give the relay an explicit idle-in-transaction timeout and a matching publish
  deadline. If that ever pinches, a lease column on the cursor row would let the read
  and the advance be two short transactions.
- *Plan cache.* With no server-side prepared statements, every poll is planned, and
  planning over a partitioned table touches the catalog for each partition. Keep the
  partition count small, and let the lookback bound prune the rest.

**Failure and dead letters.** A crash after publish and before commit republishes the
batch; consumers dedupe on the outbox id. Transient publish errors roll the poll back
and the cursor waits, which is the right behaviour for a broker outage. Permanent
per-message rejections, such as a Pub/Sub schema violation, need a policy, because a
cursor cannot skip a row the way a delete-based relay can: write the message to a
dead-letter table in the same transaction that advances the cursor, resume the ordering
key, and alert. The prototype does not implement that path; it is designed for.

**Deployment.** A Deployment per target, exactly as a table-outbox publisher is run
today, with topic, target, shard count and worker count from the environment. Replicas
is the throughput knob. No volume, no StatefulSet, no leader election, no direct
primary connection. Autoscale on the age of the oldest unpublished row, which is one
index probe past each cursor, not on CPU.

## Benefits over the table

- **Zero or append-only churn.** `wal` writes no rows. `journal` writes one row per
  event and never touches it again; dropping a partition costs nothing and leaves no
  dead tuples. Either way there is no vacuum pressure, no index bloat and no reindexing.
- **Same atomicity as a table.** The event is committed or discarded with the business
  write. There is no window where the two disagree.
- **Commit order for free.** Logical decoding delivers transactions in commit order, so
  the id-gap problem that forces table relays to delete rather than track a cursor does
  not exist.
- **Same restart semantics as a table.** The slot position replaces the relay's
  transaction. A crash before acknowledgement replays, never drops. Delivery is
  at-least-once in every mode and consumers dedupe on the message id in every mode.
- **Fewer relay queries.** No polling and no batch selects. The relay receives a push
  stream and issues one status update per acknowledged batch.
- **Pooler friendly on the producer side.** Nothing session-scoped is used, so
  producers can sit behind a transaction-mode pooler. Only the relay needs a direct
  connection.
- **Self-healing with `journal`.** A failover that loses the slot is recovered
  automatically, as long as journal retention exceeds the outage.

## Downsides and risks

- **`wal` has nothing to look at.** Pending events are opaque log bytes until the relay
  decodes them. Replay is bounded by retained log and the slot position; a true
  backfill means re-deriving events from business state. `journal` restores SQL
  visibility and replay for the retention window.
- **The buffer is bounded by disk, not by table size.** A table grows while the relay is
  down. Retained log grows too, on the primary's disk, until `max_slot_wal_keep_size`
  or the disk is exhausted. When the keep size is hit Postgres invalidates the slot and
  everything unacknowledged is lost in `wal`, or recovered from the journal in
  `journal`. Either way it needs a paging alert on slot lag and disk headroom.
- **Slot loss on failover.** Logical slots live on the primary. Whether they survive a
  failover depends on how the standby is built:
  - A standby that shares the primary's disk, as some managed services implement
    regional high availability, carries the slot and its retained log with it. Verify
    by forcing a failover with a deliberately lagging slot.
  - A streaming replica, which is what most self-managed clusters and cross-region
    disaster recovery replicas use, does not get the slot. Postgres 17 adds failover
    slots that are synchronised to standbys, if the cluster manager or hosting
    platform supports the required settings; before 17 the pg_failover_slots extension
    does the same job where extensions can be installed.
  - Where neither applies, `journal` turns the loss into a replay, and `wal` should
    only carry events that can be rebuilt from business state.
- **Per-instance prerequisites.** `wal_level = logical`, a free replication slot, a role
  with the replication attribute, and on managed platforms a flag that restarts the
  instance. Logical write-ahead logging also increases log volume somewhat on busy
  tables.
- **One consumer per slot.** A second relay attaching to an active slot is refused, so
  deploys must stop the old instance before starting the new one, and scaling out means
  more targets rather than competing workers on one target. The cursor transport has
  the opposite shape: any number of replicas, no slot.
- **Bytes are still written.** The payload goes into the write-ahead log, which is
  fsynced, physically replicated and archived. The heap, index and vacuum costs are
  gone; the write itself is not. `journal` adds the row's own log records back.
- **Duplicates are per transaction.** Acknowledgement moves only at commit boundaries,
  so a crash replays whole source transactions, including the already-published part of
  one that straddled a batch. Non-transactional messages have no commit record and are
  replayed on every restart until a later commit moves the position past them.
- **Journal replay window edge.** The stored `lsn` is where the message was written, not
  where its transaction committed. A long transaction can hold a low position and commit
  after a higher one was acknowledged. Replaying from a margin below the last
  acknowledged position, and dedupe on the consumer, covers it.
- **Thin tooling outside Go and Java.** Debezium handles these messages natively and
  Debezium Server has a Pub/Sub sink. Go has pglogrepl. Most other CDC tools and every
  surveyed Ruby outbox library handle row changes only, so a custom relay is Go or
  Java.
- **The relay must not create a slot silently in production.** The prototype does, for
  convenience. A missing slot with existing state should page someone in `wal` mode
  and trigger the journal replay in `journal` mode, which is what the prototype logs.

## Operational checklist before production

- Slot lag and retained log size exported as metrics, with alerts well below the keep
  size.
- `max_slot_wal_keep_size` set from measured disk headroom, not left unlimited.
- A forced failover test, with a lagging slot, per platform and edition, with the
  result recorded.
- Relay deployed as a singleton per target, stop-before-start, with a final flush on
  shutdown to avoid duplicates on routine restarts, and its state file on durable
  storage.
- Journal partition creation and retention automated, with retention longer than the
  longest outage you intend to survive.
- A written decision per event type: `wal`, `journal`, `cursor` or `table`, based on
  whether logical decoding is available, whether the event can be re-derived after a
  slot loss, and on the platform's failover behaviour.
- For `cursor`: a dead-letter policy per target, pool sizing for replicas × workers,
  an idle-in-transaction timeout for the relay role, and partition retention longer
  than the longest relay outage you intend to survive.
- Consumers idempotent on the message id.

## Open questions this prototype does not answer

- Log volume per event under representative payloads on the real write-ahead log
  settings, beyond what the benchmark measures with the simulated sink.
- Decoding throughput of a single slot at sustained peak rate with many targets.
- Whether the hosting platform preserves slots on standard failover, and on which
  editions.
