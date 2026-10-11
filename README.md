# postgres-outbox-playground

Four ways to run a transactional outbox on Postgres, each in its own Docker Compose
stack with its own database, driven by the same producer and measured by the same
benchmark. The question they answer: how to keep the producer's transactional
guarantee while getting rid of the churn a delete-based outbox table leaves behind, on
databases where logical decoding cannot be turned on, behind PgBouncer, with
publishers that scale by adding replicas.

| strategy | one line | page |
|---|---|---|
| `table` | The conventional outbox. Insert a row, relay claims and deletes in batches. The baseline. | [docs/table.md](docs/table.md) |
| `wal` | No table. `pg_logical_emit_message` writes the event into the write-ahead log; a relay decodes it from a replication slot. | [docs/wal.md](docs/wal.md) |
| `journal` | `wal` plus an append-only journal table so a relay can recover a lost slot. | [docs/journal.md](docs/journal.md) |
| `cursor` | Append-only partitioned table read forward by per-shard cursors. No slot, no deletes, any number of replicas, through PgBouncer. | [docs/cursor.md](docs/cursor.md) |

Measurements for all four are in [docs/benchmark.md](docs/benchmark.md).

## Requirements

These are the requirements the strategies are judged against. Each strategy page ends
with a section that takes them one by one.

| | requirement | why |
|---|---|---|
| **R1** | **Atomic with the business write.** An event exists if and only if the transaction that produced it committed. | This is the point of an outbox. |
| **R2** | **Low churn.** The outbox must not create dead tuples, index bloat or vacuum pressure in proportion to traffic. | A delete-based outbox table pays a dead tuple per event, and that churn is the problem being solved. |
| **R3** | **Simple to reason about.** Ordinary SQL, pending events visible and replayable, as few moving parts as possible. | A clever transport that nobody can debug at 3 a.m. is a net loss. |
| **R4** | **No logical decoding required.** Must work without `wal_level = logical` on databases that do not otherwise need it. | That setting is cluster-wide, needs a restart, and is not the outbox owner's call on a shared instance. |
| **R5** | **Runs behind PgBouncer in transaction mode**, producer and publisher alike. | Transaction-mode pooling is the common deployment, and nothing session-scoped survives it. |
| **R6** | **Long transactions do not stall publishing.** | Real systems have transactions that stay open for minutes; one of them must not hold back everyone else's events. |
| **R7** | **One deployment per target, scaled by replicas.** The publisher is given the target and the topic; adding replicas increases publish rate; no StatefulSets, volumes or leader election. | A stateless Deployment is the simplest thing to run on Kubernetes, and a replica count is the simplest throughput knob. |
| **R8** | **At-least-once, never lose an event.** A crash anywhere replays; consumers dedupe on the message id. | Losing events is worse than duplicating them. |
| **R9** | **Cannot become stuck.** No event, payload or error may hold up publishing. Anything that cannot be published, such as a Pub/Sub schema rejection, is dead-lettered and trivially republished. | A conventional outbox table can carry a dead-letter flag that makes this a one-line update; a replacement has to be at least that good. |
| **R10** | **Ordering is not required.** Commit order is not needed because the target is Pub/Sub; per-key order is desirable. | Listed so that no strategy is penalised for giving up something that was never needed. |

### At a glance

✅ met · ◐ met with caveats, see the page · ✗ not met

| | `table` | `wal` | `journal` | `cursor` |
|---|---|---|---|---|
| R1 atomic | ✅ | ✅ | ✅ | ✅ |
| R2 low churn | ✗ | ✅ | ✅ | ✅ |
| R3 simple | ✅ | ✗ | ✗ | ◐ |
| R4 no logical decoding | ✅ | ✗ | ✗ | ✅ |
| R5 PgBouncer | ✅ | ◐ | ◐ | ✅ |
| R6 long transactions | ✅ | ✅ | ✅ | ✅ |
| R7 deployment per target, replicas | ◐ | ✗ | ✗ | ✅ |
| R8 at-least-once, never lose | ✅ | ✗ | ✅ | ✅ |
| R9 cannot get stuck | ✅ | ◐ | ◐ | ◐ |
| R10 ordering | insert order | commit order | commit order | per key |

The short version: `wal` and `journal` solve the churn completely but fail the two
constraints that matter most here, no logical decoding and replica scaling. `cursor`
meets everything the table meets except that its dead-letter path is designed and not
yet built, and it removes the churn.

## Layout

| path | what |
|---|---|
| `docker-compose.yml` | one stack per strategy: `postgres-<mode>` (Postgres 18, `wal_level=logical` only for `wal` and `journal`), `pgbouncer-<mode>` (transaction mode), `outbox-<mode>` (the publisher) and `producer-<mode>`; no host ports, no Pub/Sub emulator |
| `postgres/*.sql` | per-strategy schema mounted into each database's init directory: `00-common.sql` (`payments`), `10-table.sql`, `10-publication.sql`, `20-journal.sql`, `10-cursor.sql` |
| `relay/` | Go: `/tablerelay`, `/relay` (slot streaming, with `-journal` recovery) and `/cursorrelay`; one shared sink package that publishes to Pub/Sub or to a simulated sink |
| `producer/` | Go load generator: worker pool, pipelined transactions, all four modes, latency percentiles |
| `consumer/` | Go: subscribes to a target's topic on a real Pub/Sub project and prints what arrives |
| `bench/` | `run.sh` drives the benchmark, `report.py` renders `results.jsonl` into the tables in [docs/benchmark.md](docs/benchmark.md) |
| `docs/` | one page per strategy, the dead-letter design for `cursor`, and the benchmark |

Every event carries a **target**. One publisher serves one target and publishes to that
target's topic; adding a destination is adding a publisher. `mise.toml` pins Go for
development only. Nothing runs outside containers.

## Sinks

There is no Pub/Sub emulator in the stack: its single Java process became the
bottleneck at a few hundred messages per second with ordering keys. Every publisher has
two sinks. `simulated` (the default) costs `RELAY_SINK_LATENCY` per publish batch,
200 ms by default, and sends nothing; it stands in for a Pub/Sub round trip. `pubsub`
publishes to a real project: set `RELAY_SINK=pubsub`, `PUBSUB_PROJECT_ID`, and mount
credentials via `GOOGLE_APPLICATION_CREDENTIALS`. The consumer only works with the real
sink.

## Run it

```sh
docker compose up -d                                  # the four databases and their poolers
docker compose --profile bench build                  # publisher, producer and consumer images

docker compose run --rm outbox-cursor &               # a publisher for target_1 ...
docker compose run --rm producer-cursor -events 10000 -workers 16 -rollback-every 10
# committed=9000 rolled_back=1000 ... the publisher reaches total=9000 and stops there

docker compose run --rm producer-cursor -h            # all producer flags
docker compose run --rm -e RELAY_TARGET=target_2 outbox-wal &   # a second target, own slot, own topic
docker compose exec postgres-cursor psql -U outbox -d outbox    # poke at a database
```

Swap `cursor` for `table`, `wal` or `journal`; each page has the walkthroughs specific
to its strategy, such as slot-loss recovery for `journal` and multi-replica publishing
with a held-open transaction for `cursor`.

`bench/run.sh [events] [per_txn] [workers]` (defaults 100000, 10, 32) runs every
strategy through the same scenarios and appends one JSON line per run to
`bench/results.jsonl`; `bench/report.py` renders the tables. The method, the numbers and
what they mean are in [docs/benchmark.md](docs/benchmark.md).

## Open questions

- Write-ahead log volume per event under representative payloads and real settings,
  beyond what the benchmark measures with the simulated sink.
- Whether the hosting platform preserves replication slots on a standard failover, and
  on which editions. This only matters if `wal` or `journal` is ever chosen.
- The cursor dead-letter path is designed ([docs/cursor-dead-letters.md](docs/cursor-dead-letters.md))
  and not yet built or benchmarked.
