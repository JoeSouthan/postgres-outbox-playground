# outbox: low-churn transactional outboxes on Postgres

A prototype of two ways to run a transactional outbox without the insert-and-delete
churn of a conventional outbox table, benchmarked against that table.

- **Log messages.** Producers write events into the Postgres write-ahead log with
  `pg_logical_emit_message` inside their business transaction; a relay decodes them from
  a logical replication slot and publishes batches to Google Pub/Sub. No outbox table at
  all. A variant adds an append-only journal so a relay can recover a lost slot.
  Requires `wal_level = logical`.
- **Append-only table with a cursor.** Producers insert into a partitioned, insert-only
  table; relays read it forward with a per-shard cursor and never delete. Old partitions
  are dropped. Needs nothing beyond a normal Postgres, runs through PgBouncer, and
  scales publishing by adding relay replicas. This is the one to pick when enabling
  logical decoding on the cluster is not on the table.

Both publish to Pub/Sub or to a simulated sink that costs a fixed latency per batch.
See [DESIGN.md](DESIGN.md) for the reasoning and trade-offs and
[BENCHMARK.md](BENCHMARK.md) for measurements.

```mermaid
flowchart LR
  P[producer<br/>Go, N workers] -->|txn: INSERT payments<br/>+ emit event| B[PgBouncer<br/>transaction mode]
  B --> PG[(Postgres 18<br/>wal_level=logical)]
  PG -->|slot outbox_relay_target_1<br/>pgoutput, empty publication| R1[relay target_1]
  PG -->|slot outbox_relay_target_2| R2[relay target_2]
  R1 -->|batches, ack LSN after publish| T1[[sink: simulated 200ms/batch<br/>or Pub/Sub topic outbox-target_1]]
  R2 --> T2[[sink for target_2]]
  T1 -. real Pub/Sub only .-> C1[consumer target_1]
```

Every event carries a **target**. One relay serves one target: it has its own replication
slot, keeps only that prefix, and publishes to that target's topic. Adding a destination
is adding a relay.

## Transports

| mode | producer statement | delivery | recovery after slot loss | table churn |
|---|---|---|---|---|
| `table` | `INSERT INTO outbox_items` | relay polls, `FOR UPDATE SKIP LOCKED`, publish, `DELETE`, commit | n/a, rows wait in the table | insert + delete per event: dead tuples, vacuum, bloat |
| `wal` | `SELECT pg_logical_emit_message(true, target, payload)` | relay streams the slot, publishes, acks commit LSN | none: the unacknowledged window is lost | none |
| `journal` | `INSERT INTO outbox_journal (…, lsn) VALUES (…, pg_logical_emit_message(…))` | same as `wal` | relay replays `outbox_journal` between its last acked LSN and the new slot's start | insert only; daily partitions dropped, never deleted, no dead tuples |
| `cursor` | `INSERT INTO outbox_events (target, key, payload)` | relays poll per shard: lock the shard's cursor row, read forward from it, publish, advance, commit | n/a, rows wait in the table; no slot involved | insert only; daily partitions dropped; one HOT update per batch on the cursor row |

Properties this demonstrates:

- **Atomic with business data.** A rolled-back transaction's events are never decoded.
- **Commit order.** Events arrive in commit order, so there is no id-gap problem.
- **Zero or append-only churn.** `wal` writes no rows; `journal` writes one row per event
  and never updates or deletes it.
- **Pooler friendly on the producer side.** The producer goes through PgBouncer in
  transaction mode. Relays connect directly (replication protocol).
- **At-least-once.** The relay acknowledges WAL only after Pub/Sub accepts a batch, and
  only up to whole-transaction boundaries. A crash replays, never drops. Consumers must
  dedupe on the message `id`. Non-transactional messages have no COMMIT record, so they
  never advance the ack and are replayed on restart until a later commit passes them.
- **Self-healing with `journal`.** After a failover that loses the slot, the relay
  recreates it and replays the journal for the lost window, then streams.
- **No logical decoding with `cursor`.** The cursor transport needs no `wal_level`
  change, no replication slot and no direct connection: everything is one ordinary
  transaction per batch, so it runs through PgBouncer. Any number of relay replicas
  share the work. The price is transaction-id order rather than commit order, which
  still preserves per-key order in the normal case. See [Cursor transport](#cursor-transport).

## Layout

| Path | What |
|---|---|
| `docker-compose.yml` | one stack per transport: `postgres-<mode>` (Postgres 18; `wal_level=logical` only for `wal` and `journal`), `pgbouncer-<mode>` (transaction mode), `outbox-<mode>` (the publisher) and `producer-<mode>`; no Pub/Sub emulator |
| `postgres/*.sql` | per-transport schema, mounted into each database's init directory: `00-common.sql` (`payments`), `10-table.sql` (`outbox_items`), `10-publication.sql` (empty publication `outbox_pub`), `20-journal.sql` (`outbox_journal`, partitioned by day), `10-cursor.sql` (`outbox_events` + `outbox_cursor`) |
| `relay/` | Go: `/relay` (slot management, pgoutput decoding, batching, publish, LSN ack, journal recovery), `/cursorrelay` (per-shard cursors over the append-only table, pending set for long transactions, N workers) and `/tablerelay` (table-outbox baseline); all publish to Pub/Sub or to a simulated sink |
| `producer/` | Go load generator: worker pool, pipelined transactions, four transports, latency percentiles |
| `consumer/` | Go: subscribes to a target's topic on a real Pub/Sub project and prints what arrives |
| `bench/run.sh` | benchmark harness; `bench/report.py` renders results; findings in [BENCHMARK.md](BENCHMARK.md) |

Each service has its own Dockerfile; nothing is published on host ports and nothing
runs outside containers. `mise.toml` pins Go for development only. The four stacks are
independent, so each transport is measured against a database configured with only
what it needs: the `table` and `cursor` databases never enable logical decoding.

## Sinks

There is no Pub/Sub emulator in the stack: its single Java process became the bottleneck
at a few hundred messages per second with ordering keys. Both relays therefore have two
sinks. `simulated` (the default) costs `RELAY_SINK_LATENCY` per publish batch, 200 ms by
default, and sends nothing; it stands in for a Pub/Sub round trip so the relay's
behaviour and the benchmark reflect a realistic publish cost without a broker.
`pubsub` publishes to a real project: set `RELAY_SINK=pubsub`, `PUBSUB_PROJECT_ID`, and
mount credentials via `GOOGLE_APPLICATION_CREDENTIALS`. The consumer only works with the
real sink.

## Run it

Every transport is a stack of four services named after it: `postgres-<mode>`,
`pgbouncer-<mode>`, `outbox-<mode>` (the publisher) and `producer-<mode>`, for `table`,
`wal`, `journal` and `cursor`.

```sh
docker compose up -d                                     # the four databases and poolers
docker compose --profile bench build                     # publisher, producer, consumer images

docker compose run --rm outbox-journal &                 # publisher for target_1 with journal recovery
docker compose run --rm producer-journal -events 10000 -workers 16 -rollback-every 10
# committed=9000 rolled_back=1000 ... expect the target_1 relay to publish exactly 9000 messages
# relay log: published 1000 (size) total=... up to total=9000, ack=0/...
```

Simulate a failover that loses the slot, then watch the relay recover from the journal:

```sh
docker stop $(docker ps -q --filter ancestor=outbox-outbox-journal)
docker compose run --rm producer-journal -events 500 -workers 8          # committed while no relay
docker compose exec postgres-journal psql -U outbox -d outbox -c "select pg_drop_replication_slot('outbox_relay_target_1')"
docker compose run --rm outbox-journal
# created slot outbox_relay_target_1 at 0/…
# replayed 500 from outbox_journal for window (0/…, 0/…] total=500
```

The cursor transport, with two publisher replicas sharing one target and a transaction
held open across the run:

```sh
docker compose run --rm outbox-cursor &                  # 8 workers each, through pgbouncer-cursor
docker compose run --rm outbox-cursor &
docker compose exec postgres-cursor psql -U outbox -d outbox -c \
  "BEGIN; INSERT INTO outbox_events (target, key, payload) VALUES ('target_1','LONG','{}'); SELECT pg_sleep(30); COMMIT;" &
docker compose run --rm producer-cursor -events 10000 -workers 16 -rollback-every 10
# both replicas publish batches straight away; totals across them reach 9000 while the
# transaction is still open, then 9001 the moment it commits: "published 1 (... catchup=1 ...)"
```

Other combinations:

```sh
docker compose run --rm -e RELAY_TARGET=target_2 outbox-wal &            # second target: own slot, own topic
docker compose run --rm producer-wal -targets target_1,target_2 -events 1000
docker compose run --rm outbox-table &                                   # the conventional baseline
docker compose run --rm producer-table -events 1000
docker compose run --rm -e RELAY_WORKERS=2 outbox-cursor                 # fewer pollers per replica
docker compose run --rm producer-cursor -h                               # all flags
```

Useful queries while it runs:

```sql
-- docker compose exec postgres-journal psql -U outbox -d outbox   (or postgres-<mode>)
select slot_name, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
from pg_replication_slots;
select relname, n_tup_ins, n_tup_del, n_dead_tup from pg_stat_user_tables
where relname in ('payments', 'outbox_items') or relname like 'outbox_journal%';
```

Stop a log-message relay, produce more, and watch `lag` grow: that WAL is retained for
the slot until the relay returns and acks. `max_slot_wal_keep_size` in the compose file caps it,
after which Postgres invalidates the slot. With `wal` those events are lost; with
`journal` the relay recovers them.

## Benchmark

`bench/run.sh [events] [per_txn] [workers]` (defaults 100000, 10, 32) runs the same
producer workload through each transport in a steady-state scenario (relay running), a
burst scenario (relay stopped while producing, then draining), for `journal` a
slot-loss scenario (slot dropped before the relay restarts) and, for `cursor`, a
long-transaction scenario (a transaction that wrote one event is held open for the
whole run). `BENCH_CURSOR_WORKERS` and `BENCH_CURSOR_REPLICAS` set how many pollers and
how many relay containers the cursor runs use. Relays use the simulated
sink (`BENCH_SINK`, `BENCH_SINK_LATENCY`). It records producer throughput and latency,
relay drain time, write-ahead log volume, dead tuples, outbox heap and index size,
index leaf density and fragmentation (pgstatindex), heap dead and free space
(pgstattuple), vacuum activity, sampled Postgres CPU, and per-statement costs from
pg_stat_statements attributed to the producer and relay sides. One JSON line per run goes to
`bench/results.jsonl`; `bench/report.py` renders the tables. The harness is a shell
script on the host that only drives `docker compose run`. Findings are in
[BENCHMARK.md](BENCHMARK.md).

## How the relay works

```mermaid
sequenceDiagram
  participant PG as Postgres
  participant R as relay (target_1)
  participant PS as sink (Pub/Sub or simulated)
  R->>PG: CREATE_REPLICATION_SLOT outbox_relay_target_1 LOGICAL pgoutput (or resume)
  R->>PG: START_REPLICATION proto_version 2, publication outbox_pub, messages true
  loop stream
    PG-->>R: BEGIN xid
    PG-->>R: MESSAGE prefix=target_1 payload
    PG-->>R: COMMIT lsn
    R->>R: buffer, flush on batch size or interval
    R->>PS: publish batch (ordering key = payload.key)
    PS-->>R: all acked
    R->>PG: standby status: last fully published COMMIT lsn
    R->>R: write state file
  end
```

1. Create the slot, or start at LSN `0/0` to resume from `confirmed_flush_lsn`. With
   `-journal`, a slot that had to be recreated while a state file exists triggers a
   replay of `outbox_journal` rows with `lsn` in (last acked, new slot start].
2. Stream with `proto_version '2'`, `publication_names 'outbox_pub'`, `messages 'true'`.
   The publication has no tables, so only BEGIN, MESSAGE and COMMIT records arrive.
3. Each MESSAGE whose prefix equals the relay's target becomes a Pub/Sub message on
   `outbox-<target>`; other prefixes are skipped. `target`, `type`, `key`, LSN and xid
   become attributes.
4. Flush on `-batch` size or `-flush` interval. After every result is confirmed, send a
   standby status update with the last COMMIT LSN fully contained in what has been
   published, and persist it to the state file.

Requirements: Postgres 14+ for pgoutput messages (the function itself is 9.6+), a
role with `REPLICATION`, `wal_level=logical`, a free `max_replication_slots`.

## Journal recovery and position tracking

The journal exists for one case: the relay comes back and its slot is gone. Everything
else about it is designed so that case can be handled exactly, without reading the
journal at any other time.

**What the producer writes.** One statement per event:

```sql
INSERT INTO outbox_journal (target, payload, lsn)
VALUES ($1, $2, pg_logical_emit_message(true, $1, $2));
```

`pg_logical_emit_message` returns the log position of the record it just wrote, and the
INSERT stores it in the row. Both happen inside the business transaction, so a rollback
discards both. Nothing ever updates or deletes a journal row. The table is partitioned by
day on `created_at` and old partitions are dropped after a retention period, so it costs
one insert per event and never produces a dead tuple.

**What the relay tracks.** Two positions, kept in two places:

| position | where it lives | what it means |
|---|---|---|
| `confirmed_flush_lsn` | the slot, in Postgres | Postgres may discard log before this; the relay advances it with a standby status update after a batch is published |
| last acknowledged LSN | the relay's state file, `state/relay_<target>.lsn` | the relay's own copy of the same number, written atomically (temp file and rename) every time it acknowledges |

In steady state the two agree. The state file exists because the slot is the thing that
disappears in a failover, and the number it held is the only thing that defines the
lost window. In production this file belongs on durable storage outside the database
that just failed over: a small table in another database, an object store, or the relay's
own persistent volume.

**The acknowledgement rule.** The relay only acknowledges up to the end of a source
transaction whose messages have all been published. A batch that flushes in the middle
of a transaction publishes those messages but leaves the acknowledged position at the
previous commit, so a crash replays that whole transaction. Non-transactional messages
have no commit record and never advance the position. This is what makes delivery
at-least-once rather than at-most-once, and why consumers dedupe on the message id.

**What happens on start.** The relay tries to create its slot, then branches on two
facts: did the slot already exist, and does a state file exist.

```mermaid
flowchart TD
  S([relay starts]) --> C{slot exists?}
  C -->|yes| R[resume at 0/0, i.e. the slot's confirmed_flush_lsn]
  C -->|no, created now| F{state file exists?}
  F -->|no| N[first run, nothing before the slot's start can be streamed, write state = start]
  F -->|yes, journal mode| J[replay journal rows with lsn in last acked to slot start, then stream]
  F -->|yes, wal mode| W[log a warning, the window is lost, then stream]
  R --> T[stream, publish, ack, write state]
  N --> T
  J --> T
  W --> T
```

The slot-exists path is the normal restart. The created-now path with a state file is a
failover or an operator dropping the slot: the gap between the state file's position
and the new slot's consistent point is exactly what was committed while no slot was
retaining log, and the journal holds it.

**The replay itself.** Over an ordinary connection, not the replication one:

```sql
SELECT payload, lsn::text
FROM outbox_journal
WHERE target = $1 AND lsn > $2::pg_lsn AND lsn <= $3::pg_lsn
ORDER BY lsn, id;
```

`$2` is the state file's position and `$3` the new slot's consistent point. The rows go
through the same sink in relay-sized batches, with a `replayed=true` attribute, and the
state file is then set to the consistent point. Only after that does streaming begin, so
a crash during replay simply replays again from the same position. The benchmark's
slot-loss run replayed exactly 100,000 rows this way in 24.4 seconds.

**The boundary edge.** The stored `lsn` is where the message was written, which is
before its transaction's commit record. A transaction that wrote its message early and
committed late can have a position below one the relay has already acknowledged, while
still being streamed correctly in normal operation because streaming is commit-ordered.
After a slot loss, though, replaying strictly from the acknowledged position would miss
it. Two mitigations: widen the window downward by a margin at least as large as the
longest transaction the producers run, and rely on consumer dedupe for the duplicates
that margin creates. The prototype replays from the exact position; a production relay
should apply the margin.

**Per target.** Each target has its own slot, its own state file and its own replay
query, because each relay acknowledges independently. A failover therefore produces one
recovery per relay, each bounded by its own last acknowledgement.

**Retention is the one knob.** The journal can only recover what it still holds. Keep
partitions for longer than the longest relay outage you intend to survive, and alert on
journal partition age against that, the same way you alert on slot lag.

## Cursor transport

The cursor transport exists for clusters where `wal_level = logical` is not an option.
It keeps the two properties that matter, atomic enqueue and an insert-only table, and
gives up the slot. Producers write one row:

```sql
INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3);
```

The table fills in two more columns. `xid` is `pg_current_xact_id()`, the top-level id
of the writing transaction. `shard` is a hash of the ordering key modulo 32, computed
as a stored generated column, so producers never see it. The table is partitioned by
day and old partitions are dropped. Nothing ever updates or deletes a row.

**One cursor per target and shard.** `outbox_cursor` holds, per `(target, shard)`, the
frontier `(xid, id)` of the last row published and a `pending` array of transaction ids.
A relay replica runs N workers; each worker picks a shard and runs one transaction:

```mermaid
sequenceDiagram
  participant W as worker (any replica)
  participant PG as Postgres (via PgBouncer)
  participant PS as sink
  W->>PG: BEGIN REPEATABLE READ
  W->>PG: SELECT xid, id, pending, in-flight xids FROM outbox_cursor<br/>WHERE target, shard FOR UPDATE SKIP LOCKED
  Note over W: locked by another worker → no row → try the next shard
  W->>PG: catch-up: rows WHERE xid = ANY(pending that are no longer in flight)
  W->>PG: frontier: rows WHERE (xid, id) > frontier ORDER BY xid, id LIMIT batch
  W->>PS: publish catch-up rows, then frontier rows (ordering key = key)
  PS-->>W: all acked
  W->>PG: UPDATE outbox_cursor SET frontier = last row,<br/>pending = still in flight ∪ in-flight xids below the new frontier
  W->>PG: COMMIT
```

**Why the frontier is safe to move.** Every row a snapshot can see is committed, so the
relay never publishes uncommitted data. The hazard is the opposite one: a transaction
that is still running when the frontier passes its position can commit later and leave
a committed row *behind* the cursor. The `pending` set is the list of transactions that
could do that, taken from the snapshot's in-flight list. On each poll, the pending ids
that are no longer in flight have resolved, and their rows, if they committed, are read
by the catch-up query and published first. The invariant is: every committed row at or
below the frontier has been published, unless its transaction is in `pending`. A
transaction held open for minutes therefore costs nothing while it runs; its rows are
published on the first poll after it commits, with a `catchup=true` attribute.

**Replicas.** The cursor row lock is held for exactly one batch, so shards flow to
whichever worker is free, across any number of replicas. There is no leader, no
assignment, no state in the pod and nothing to rebalance. A replica dying releases its
locks on rollback. Publishing rate scales with workers up to the shard count; 32 shards
is the ceiling, and changing it is a drain-and-remap (see DESIGN.md).

**Failure.** A crash after publish and before commit leaves the cursor where it was,
so the batch is republished. Delivery is at-least-once and consumers dedupe on the
`outbox_id` attribute, as with every transport here.

**Ordering.** Rows leave in `(xid, id)` order, which is the order transactions first
wrote, not the order they committed. Two transactions on the same key usually serialise
on that key's business row, so per-key order holds when that row is the transaction's
first write. Commit order needs logical decoding; this transport trades it away.

**PgBouncer.** The whole poll is one transaction with nothing session-scoped, so the
relay connects through the pooler in transaction mode like the producers. Each worker
holds one pooled connection for the length of a batch, mostly waiting on Pub/Sub acks,
so size the pool for replicas × workers.

**Idle cost.** A worker sleeps for `-poll` after an empty poll, so an idle deployment
runs about workers ÷ poll interval tiny transactions per second and notices new work on
any shard within roughly shards ÷ workers poll intervals.
