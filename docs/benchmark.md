# Benchmark: table outbox vs log messages vs append-only table with a cursor

The same producer workload driven through four transports, measuring the effect on the
producer, on the relay and on the database. Numbers are from one laptop-class machine
with every component sharing the same virtual machine, so the ratios between transports
matter more than the absolute figures.

## Environment

| | |
|---|---|
| Host | MacBook Pro, Intel Core i5-1038NG7 at 2.0 GHz, 4 cores / 8 threads, 16 GB RAM, NVMe SSD, macOS 26.1 |
| Docker | Docker Desktop, engine 29.4.2, Apple Virtualization framework with VirtioFS, linux/amd64 |
| Docker VM | 8 vCPUs, 8 GB RAM, 1 GB swap, 60 GB disk, kernel 6.12 linuxkit, overlay2 |
| Postgres | 18.6, one instance per transport: `table` and `cursor` run `wal_level=replica` (the default), `wal` and `journal` run `wal_level=logical` with `max_slot_wal_keep_size=2GB`; all with `shared_buffers=512MB`, `max_connections=300`, `pg_stat_statements` loaded, default autovacuum settings |
| PgBouncer | edoburu/pgbouncer, transaction mode, pool of 64 server connections, one per Postgres |
| Sink | simulated: every publish batch costs 200 ms and sends nothing, standing in for a Pub/Sub round trip |

Postgres, PgBouncer, the relay and the producer all run inside that one VM and compete
for the same four physical cores. Only one transport's stack is busy at a time; the
other three databases sit idle. There is no network between them beyond the Docker
bridge.

## Method

`bench/run.sh 100000 10 32`: per run, 100,000 events in 10,000 transactions of 10
payment inserts plus 10 events, from 32 concurrent producer connections through
PgBouncer, each transaction pipelined as one batch. Event payload is about 190 bytes of
JSON. Relays batch up to 1,000 messages; the log relays also flush every 500 ms.

Four transports:

| mode | producer writes per event | relay |
|---|---|---|
| `table` | INSERT into `outbox_items` | claim 1,000 rows with `FOR UPDATE SKIP LOCKED` in a materialised CTE, DELETE … RETURNING, publish, commit |
| `wal` | `pg_logical_emit_message()` | stream the slot, publish, ack commit LSN |
| `journal` | INSERT into `outbox_journal` with the emit inside the VALUES | as `wal`; journal read only after slot loss |
| `cursor` | INSERT into `outbox_events` (append-only, partitioned day → target → shard, `xid` column) | N workers per replica, each: lock a shard's cursor row `FOR UPDATE SKIP LOCKED`, read forward from it in `(xid, id)` order, publish, advance the cursor, commit; no deletes, no slot, through PgBouncer |

The cursor transport is run in three shapes, labelled `w<workers>x<replicas>`: one
worker in one replica, which is a single publishing stream like the other relays; eight
workers in one replica; and eight workers in each of two replica containers.

Four scenarios:

- **concurrent**: relay running while the producer runs. Drain time is measured from
  producer start to the relay's last publish, so it includes the producer's own run time.
- **backlog**: producer runs with the relay stopped, then the relay starts and drains.
  Drain time is relay start to last publish. This is the burst case.
- **slotloss** (`journal` only): as backlog, but the replication slot is dropped before
  the relay restarts, simulating a failover that loses the slot. The relay recovers the
  window from the journal.
- **longtxn** (`cursor` only): as concurrent, but one transaction that has written an
  event is held open for 30 seconds across the whole run. The drain must not wait for
  it, and its event must be published once it commits.

Between runs the transport's outbox table and `payments` are truncated and vacuumed,
cursors are reset, statistics are reset, and for the log transports the slot is dropped
and the relay state file removed. Each run reports the producer's own throughput and
latency percentiles; the harness samples dead tuples, outbox size (including
partitions), index size, slot lag and the Postgres container's CPU every half second
and at the end reads `pg_stat_wal`, `pg_stat_user_tables`, `pg_stat_database`,
`pgstattuple` and `pgstatindex` over the outbox heap and indexes, and
`pg_stat_statements`, reset at the start of every run, with each statement attributed
to the producer side (business insert plus event write) or the relay side (anything
else touching an outbox table).

## Results

Events per run: 100,000 in transactions of 10 with 32 producer workers; runs averaged per column: table/concurrent=1, wal/concurrent=1, journal/concurrent=1, cursor w1x1/concurrent=1, cursor w8x1/concurrent=1, cursor w8x2/concurrent=1, table/backlog=1, wal/backlog=1, journal/backlog=1, cursor w1x1/backlog=1, cursor w8x1/backlog=1, cursor w8x2/backlog=1, journal/slotloss=1, cursor w1x1/longtxn=1.

### Producer (Go, via PgBouncer)

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| throughput, events/s | 12,622 | 15,087 | 13,090 | 15,947 | 13,357 | 10,146 | 14,693 | 13,960 | 12,483 | 14,693 | 15,516 | 14,988 | 14,538 | 16,440 |
| throughput, transactions/s | 1,262 | 1,509 | 1,309 | 1,595 | 1,336 | 1,015 | 1,469 | 1,396 | 1,248 | 1,469 | 1,552 | 1,499 | 1,454 | 1,644 |
| transaction latency p50, ms | 20.63 | 17.44 | 19.48 | 17.53 | 20.69 | 21.85 | 18.02 | 16.34 | 18.68 | 18.13 | 17.96 | 17.28 | 17.97 | 16.53 |
| transaction latency p95, ms | 57.99 | 48.44 | 57.51 | 39.15 | 47.05 | 77.54 | 48.88 | 56.11 | 60.23 | 44.49 | 41.05 | 45.55 | 50.07 | 40.17 |
| transaction latency p99, ms | 85.42 | 73.50 | 94.90 | 61.84 | 80.48 | 205.33 | 73.37 | 133.82 | 148.80 | 73.68 | 62.14 | 94.18 | 77.03 | 63.74 |
| errors | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

### Relay

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| drain time, s (backlog/slotloss: relay start to last publish; concurrent: producer start to last publish) | 65.70 | 25.73 | 25.81 | 31.12 | 11.50 | 15.40 | 23.98 | 26.22 | 24.46 | 29.47 | 5.52 | 4.32 | 22.67 | 33.27 |
| publish batches | 101 | 100 | 100 | 128 | 278 | 439 | 100 | 100 | 100 | 126 | 126 | 126 | 0 | 130 |
| events replayed from journal | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 100,000 | 0 |
| cursor: pollers per replica | - | - | - | 1 | 8 | 8 | - | - | - | 1 | 8 | 8 | - | 1 |
| cursor: relay replicas | - | - | - | 1 | 1 | 2 | - | - | - | 1 | 1 | 2 | - | 1 |
| longtxn: seconds the transaction was held open | - | - | - | - | - | - | - | - | - | - | - | - | - | 30 |
| longtxn: s until its event was published too (from producer start) | - | - | - | - | - | - | - | - | - | - | - | - | - | 34.23 |

### Database

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| WAL bytes written | 90.27 MB | 42.14 MB | 81.08 MB | 55.22 MB | 53.48 MB | 55.33 MB | 72.65 MB | 42.12 MB | 81.09 MB | 55.29 MB | 55.68 MB | 53.89 MB | 81.25 MB | 56.06 MB |
| WAL records | 725,005 | 313,327 | 516,854 | 417,333 | 404,294 | 418,615 | 728,522 | 314,080 | 517,913 | 417,341 | 413,201 | 407,642 | 521,089 | 418,498 |
| outbox rows inserted | 100,000 | 0 | 100,000 | 100,000 | 96,670 | 99,890 | 100,000 | 0 | 100,000 | 100,000 | 98,710 | 97,530 | 100,000 | 100,001 |
| outbox rows deleted | 100,000 | 0 | 0 | 0 | 0 | 0 | 100,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox dead tuples, peak | 91,000 | 0 | 0 | 0 | 0 | 0 | 45,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox dead tuples, end | 96,430 | 0 | 0 | 0 | 0 | 0 | 51,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox table+index size, peak | 33.63 MB | 0.00 MB | 32.56 MB | 42.81 MB | 43.70 MB | 44.68 MB | 34.09 MB | 0.00 MB | 33.56 MB | 42.89 MB | 46.80 MB | 42.79 MB | 32.42 MB | 43.04 MB |
| outbox table+index size, end | 33.63 MB | 0.00 MB | 32.56 MB | 42.81 MB | 43.70 MB | 44.68 MB | 34.09 MB | 0.00 MB | 33.56 MB | 42.89 MB | 46.80 MB | 42.79 MB | 32.42 MB | 43.04 MB |
| autovacuum runs on outbox | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 | 0 | 0 | 0 | 1 | 32 |
| buffer accesses (blks_hit) | 1,987,967 | 430,800 | 836,669 | 1,643,704 | 1,515,734 | 1,714,220 | 1,933,645 | 435,514 | 847,932 | 1,700,066 | 1,470,164 | 1,525,640 | 941,498 | 1,604,796 |
| transactions committed | 10,372 | 10,133 | 10,133 | 10,302 | 10,088 | 10,559 | 10,272 | 10,143 | 10,147 | 10,317 | 10,184 | 9,986 | 10,180 | 10,419 |
| slot lag, peak | 0.00 MB | 29.01 MB | 55.61 MB | 0.00 MB | 0.00 MB | 0.00 MB | 0.00 MB | 42.74 MB | 82.01 MB | 0.00 MB | 0.00 MB | 0.00 MB | 81.88 MB | 0.00 MB |
| postgres container CPU, avg % | 99.7 | 86.0 | 90.9 | 82.8 | 234.2 | 265.7 | 96.7 | 72.7 | 79.0 | 73.4 | 210.8 | 202.8 | 78.0 | 85.4 |
| postgres container CPU, max % | 469.2 | 411.1 | 430.3 | 487.8 | 463.8 | 491.8 | 489.8 | 449.1 | 485.5 | 493.5 | 490.4 | 480.2 | 460.6 | 476.8 |

### Heap and indexes at the end of the run (pgstattuple, pgstatindex)

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| outbox heap size | 27.34 MB | 0.00 MB | 28.38 MB | 33.24 MB | 34.13 MB | 35.11 MB | 27.79 MB | 0.00 MB | 29.34 MB | 33.32 MB | 37.23 MB | 33.22 MB | 28.19 MB | 33.21 MB |
| outbox heap dead tuple space, % | 32.5 | - | 0.0 | 0.0 | 0.0 | 0.0 | 14.9 | - | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| outbox heap free space, % | 65.8 | - | 2.7 | 14.7 | 16.9 | 19.3 | 82.4 | - | 3.3 | 14.9 | 23.8 | 14.7 | 2.3 | 14.7 |
| outbox index size, end | 6.25 MB | 0.00 MB | 4.08 MB | 6.12 MB | 6.12 MB | 6.12 MB | 6.26 MB | 0.00 MB | 4.10 MB | 6.12 MB | 6.12 MB | 6.12 MB | 4.12 MB | 6.12 MB |
| outbox index size, peak | 6.25 MB | 0.00 MB | 4.08 MB | 6.12 MB | 6.12 MB | 6.12 MB | 6.26 MB | 0.00 MB | 4.10 MB | 6.12 MB | 6.12 MB | 6.12 MB | 4.12 MB | 6.12 MB |
| outbox index count | 2 | 0 | 10 | 325 | 325 | 325 | 2 | 0 | 10 | 325 | 325 | 325 | 10 | 325 |
| outbox index leaf density, % (page-weighted) | 91.1 | - | 91.7 | 88.3 | 88.3 | 88.3 | 90.8 | - | 91.1 | 88.3 | 88.3 | 88.3 | 90.8 | 88.3 |
| outbox index leaf fragmentation, % | 0.3 | - | 0.2 | 0.0 | 0.0 | 0.0 | 0.3 | - | 0.0 | 0.0 | 0.0 | 0.0 | 0.2 | 0.0 |
| outbox index deleted + empty pages | 26 | 0 | 0 | 0 | 0 | 0 | 363 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox row updates | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| cursor table size (cursor only) | 0 kB | 0 kB | 0 kB | 49 kB | 66 kB | 82 kB | 0 kB | 0 kB | 0 kB | 49 kB | 49 kB | 49 kB | 0 kB | 82 kB |
| cursor row updates | 0 | 0 | 0 | 128 | 269 | 451 | 0 | 0 | 0 | 126 | 141 | 114 | 0 | 151 |
| cursor row updates that were HOT | 0 | 0 | 0 | 128 | 268 | 435 | 0 | 0 | 0 | 126 | 141 | 114 | 0 | 126 |
| cursor row dead tuples, end | 0 | 0 | 0 | 2 | 9 | 57 | 0 | 0 | 0 | 11 | 13 | 7 | 0 | 54 |

### Statements (pg_stat_statements): producer side = business insert + event write; relay side = everything else on an outbox table

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| producer: calls | 200,000 | 200,000 | 200,000 | 200,001 | 200,001 | 200,002 | 200,000 | 200,000 | 200,000 | 200,002 | 200,002 | 200,004 | 200,000 | 200,002 |
| producer: total exec time, s | 103.06 | 62.40 | 100.18 | 36.30 | 38.09 | 65.92 | 62.51 | 43.52 | 64.53 | 38.30 | 37.90 | 43.42 | 52.09 | 37.31 |
| producer: exec time per 1,000 events, ms | 1030.6 | 624.0 | 1001.8 | 363.0 | 380.9 | 659.2 | 625.1 | 435.2 | 645.3 | 383.0 | 379.0 | 434.2 | 520.9 | 373.1 |
| producer: blocks dirtied per 1,000 events | 51.3 | 10.3 | 49.8 | 56.0 | 57.1 | 58.4 | 51.4 | 10.3 | 49.9 | 56.1 | 61.0 | 55.9 | 49.5 | 55.9 |
| producer: WAL bytes per event | 603 | 416 | 805 | 548 | 549 | 549 | 603 | 416 | 805 | 549 | 550 | 549 | 805 | 548 |
| relay: calls | 132 | 0 | 0 | 465 | 1,585 | 3,213 | 118 | 0 | 0 | 432 | 771 | 1,294 | 1 | 552 |
| relay: total exec time, s | 41.44 | 0.00 | 0.00 | 0.37 | 0.74 | 1.48 | 0.87 | 0.00 | 0.00 | 0.31 | 0.39 | 0.54 | 0.87 | 0.37 |
| relay: exec time per 1,000 events, ms | 414.4 | 0.0 | 0.0 | 3.7 | 7.4 | 14.8 | 8.7 | 0.0 | 0.0 | 3.1 | 3.9 | 5.4 | 8.7 | 3.7 |
| relay: blocks hit per 1,000 events | 9,240 | 0 | 0 | 1,074 | 898 | 985 | 8,913 | 0 | 0 | 995 | 299 | 1,012 | 912 | 259 |
| relay: blocks dirtied per 1,000 events | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| relay: WAL bytes per event | 292 | 0 | 0 | 0 | 1 | 2 | 111 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

### Top statements per run (pg_stat_statements, concurrent scenario unless noted)

The log relays' replication stream is not a statement and does not appear; their database work is in the WAL and buffer figures above. Mean time is per call.

**table / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO outbox_items (target, payload) VALUES ($1, $2)` | 100,000 | 63,756 | 0.638 | 591,407 | 4,103 | 44,712,450 |
| relay | `WITH claimed AS MATERIALIZED ( SELECT id FROM outbox_items WHERE target = $1 ORDER BY id LIMIT $2 FOR UPDAT...` | 132 | 41,438 | 313.921 | 924,017 | 0 | 29,183,993 |
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 39,308 | 0.393 | 404,972 | 1,027 | 15,630,940 |

**wal / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 60,018 | 0.600 | 406,288 | 1,027 | 15,629,837 |
| producer | `SELECT pg_logical_emit_message($3, $1, $2::text)` | 100,000 | 2,382 | 0.024 | 0 | 0 | 25,966,529 |

**journal / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO outbox_journal (target, payload, lsn) VALUES ($1, $2, pg_logical_emit_message($3, $1, $2::text))` | 100,000 | 54,050 | 0.540 | 397,481 | 3,947 | 64,867,551 |
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 46,132 | 0.461 | 404,192 | 1,031 | 15,630,637 |

**cursor w1x1 / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 25,715 | 0.257 | 405,632 | 1,032 | 15,630,972 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 10,580 | 0.106 | 440,460 | 4,568 | 39,210,597 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 161 | 344 | 2.134 | 106,592 | 0 | 0 |
| relay | `SELECT xid, id, pending, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM outbox_cursor WHERE targ...` | 161 | 23 | 0.143 | 322 | 0 | 10,006 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, updated_at = now() WHERE target = $4 AND shard = $5` | 128 | 8 | 0.059 | 384 | 0 | 14,213 |

**cursor w8x1 / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 27,262 | 0.273 | 405,619 | 1,032 | 15,630,955 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 10,831 | 0.108 | 443,601 | 4,678 | 39,225,084 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 525 | 597 | 1.137 | 86,209 | 0 | 0 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $4) = $2 AND xid = ...` | 250 | 57 | 0.228 | 1,058 | 0 | 0 |
| relay | `SELECT xid, id, pending, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM outbox_cursor WHERE targ...` | 525 | 56 | 0.107 | 1,439 | 0 | 38,260 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, updated_at = now() WHERE target = $4 AND shard = $5` | 284 | 25 | 0.090 | 1,093 | 2 | 63,025 |

**cursor w8x2 / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 42,844 | 0.428 | 406,588 | 1,036 | 15,630,889 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 23,076 | 0.231 | 453,734 | 4,798 | 39,225,380 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 852 | 981 | 1.151 | 88,688 | 0 | 0 |
| relay | `SELECT xid, id, pending, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM outbox_cursor WHERE targ...` | 1,493 | 295 | 0.198 | 5,651 | 0 | 55,416 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $4) = $2 AND xid = ...` | 417 | 112 | 0.269 | 1,996 | 0 | 0 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, updated_at = now() WHERE target = $4 AND shard = $5` | 449 | 97 | 0.215 | 2,187 | 4 | 112,557 |

**journal / slotloss**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO outbox_journal (target, payload, lsn) VALUES ($1, $2, pg_logical_emit_message($3, $1, $2::text))` | 100,000 | 28,148 | 0.281 | 393,463 | 3,938 | 64,873,906 |
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 23,942 | 0.239 | 403,561 | 1,017 | 15,631,601 |
| relay | `SELECT payload, lsn::text FROM outbox_journal WHERE target = $1 AND lsn > $2::pg_lsn AND lsn <= $3::pg_lsn ...` | 1 | 867 | 867.167 | 91,214 | 0 | 0 |

**cursor w1x1 / longtxn**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 28,208 | 0.282 | 404,673 | 1,027 | 15,630,907 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 9,102 | 0.091 | 440,599 | 4,561 | 39,207,224 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 177 | 341 | 1.928 | 24,482 | 0 | 0 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, updated_at = now() WHERE target = $4 AND shard = $5` | 156 | 16 | 0.099 | 754 | 4 | 30,896 |

Drain time in the concurrent scenarios includes the producer's own run, which is 6 to
11 seconds depending on the transport. With a 200 ms sink cost per batch, the publish
floor is 20 seconds for a single stream of 100 batches of 1,000; the cursor transport
publishes per shard, so its single stream needs about 130 batches and a 26-second floor.
Eight concurrent workers divide that floor by eight.

## Findings

**Headline comparison.** Treat producer differences under about 15 percent as noise:
across runs the same producer workload varied by that much on the same transport for
no physical reason. The relay-side and physical-state numbers are far more stable.

| | table | wal | journal | cursor, 1 stream | cursor, 8 workers | cursor, 8 × 2 replicas |
|---|---|---|---|---|---|---|
| producer throughput, events/s (concurrent) | 12,622 | 15,087 | 13,090 | 15,947 | 13,357 | 10,146 |
| producer p99 latency, ms (concurrent) | 85 | 74 | 95 | 62 | 80 | 205 |
| drain, producer start to last publish, s | 65.7 | 25.7 | 25.8 | 31.1 | 11.5 | 15.4 |
| drain of a 100,000 backlog, relay alone, s | 24.0 | 26.2 | 24.5 | 29.5 | 5.5 | 4.3 |
| publish rate on that backlog, events/s | 4,170 | 3,814 | 4,088 | 3,393 | 18,116 | 23,148 |
| dead tuples at peak (concurrent) | 91,000 | 0 | 0 | 0 | 0 | 0 |
| heap dead space at end, % (concurrent) | 32.5 | - | 0.0 | 0.0 | 0.0 | 0.0 |
| heap free space at end, % (concurrent) | 65.8 | - | 2.7 | 14.7 | 16.9 | 19.3 |
| index size at end | 6.3 MB | - | 4.1 MB | 6.1 MB | 6.1 MB | 6.1 MB |
| index leaf density, % | 91.1 | - | 91.7 | 88.3 | 88.3 | 88.3 |
| index deleted + empty pages (backlog, after vacuum) | 363 | - | 0 | 0 | 0 | 0 |
| WAL bytes (concurrent) | 90 MB | 42 MB | 81 MB | 55 MB | 53 MB | 55 MB |
| buffer accesses (concurrent) | 1,987,967 | 430,800 | 836,669 | 1,643,704 | 1,515,734 | 1,714,220 |
| relay statement time per 1,000 events, ms (concurrent) | 414.4 | - | - | 3.7 | 7.4 | 14.8 |
| relay statement time per 1,000 events, ms (backlog) | 8.7 | - | - | 3.1 | 3.9 | 5.4 |
| relay blocks hit per 1,000 events (concurrent) | 9,240 | - | 0 | 1,074 | 898 | 985 |
| relay WAL bytes per event (concurrent) | 292 | - | 0 | 0 | 1 | 2 |
| producer statement time per 1,000 events, ms (backlog, producer alone) | 625.1 | 435.2 | 645.3 | 383.0 | 379.0 | 434.2 |
| needs `wal_level=logical` | no | yes | yes | no | no | no |

1. **Dead tuples, heap bloat and relay contention are all `table`-only, and the cursor
   transport avoids them on a plain Postgres.** The delete-based outbox peaked at 91,000
   dead tuples; at the end of the concurrent run a third of its heap was dead rows and
   two thirds was free space in pages vacuum had emptied but could not return. Every
   other transport, including `cursor` on a database with the default `wal_level`,
   ended with zero dead space and 2 to 4 percent free.

2. **The table relay's claim is the single most expensive statement in the benchmark
   under load, and pg_stat_statements shows why.** Its claim-and-delete cost 414 ms per
   1,000 events while the producers were running, against 8.7 ms per 1,000 on the same
   backlog once they had stopped, a 48× difference, and it touched about 9,200 buffers
   per 1,000 events either way. It also wrote 292 bytes of WAL per event for the delete
   alone. The cursor relay's whole poll, claim, catch-up, read and advance, cost 3 to 4
   ms per 1,000 events as a single stream and touched about 1,000 to 1,100 buffers,
   with no measurable WAL. That is the difference between a relay that locks and deletes
   the producers' pages and one that reads committed rows under a snapshot.

3. **Index state tells the churn story most clearly.** After the backlog run and its
   vacuum, the table's two indexes held 363 deleted or empty pages that B-tree will
   never give back without a REINDEX, on a table that was empty again. The journal's
   index sat at 91 percent leaf density with nothing deleted. The cursor's per-shard
   leaf indexes ended at 88 percent density, zero fragmentation and nothing deleted,
   6.1 MB against the journal's 4.1 MB for the same rows. An earlier layout with one
   `(target, shard, xid, id)` index per day partition measured 52 percent density and
   8.8 MB, because 32 shards meant 32 interleaved insertion points and 50/50 page
   splits; partitioning each target by shard turned every insert back into a right-edge
   append. The cursor heaps end with 15 to 19 percent free space rather than the
   journal's 3, from 32 producer connections extending 32 small relations concurrently;
   that room is filled by later inserts and is not dead space.

4. **Replicas scale publishing and lose nothing.** The 100,000-event backlog that one
   cursor stream drains in 29.5 seconds drains in 5.5 seconds with eight workers
   and 4.3 with eight workers in each of two containers, a publish rate of
   23,148 events per second against the 5,000 a single stream can reach through the
   200 ms sink. The two replicas published 50,033 and 49,967 messages in the backlog
   run and 51,228 and 48,772 in the concurrent run, split by nothing but the cursor row
   locks. Relay statement time per event rises with workers, to 7 to 15 ms per 1,000,
   because batches are smaller and sixteen pollers plan and probe concurrently; on this
   four-core host that is where the ceiling is.

5. **A long transaction does not stall the cursor.** With a transaction holding an
   event open for 30 seconds across the whole run, the drain took 33.3 seconds
   against 31.1 without it, and the held event was published 34.2 seconds
   after the run started, a few seconds after its commit, through the catch-up path.
   The cursor row saw 151 updates in that run, 126 of them HOT, on an 8 kB heap.

6. **Producer cost for `cursor` is in the table and journal band, measured per
   statement.** With the relay stopped, the two producer statements cost 383 ms per
   1,000 events on `cursor`, 625 on `table`, 645 on `journal` and 435 on
   `wal`; routing each row through the day, target and shard partitions did not make
   the insert measurably dearer than a flat table. Under concurrent load every producer gets slower because the relay competes
   for the same four cores, and the table relay competes hardest.

7. **`wal` is still the lightest transport on every database axis and `journal` still
   recovers a lost slot**, replaying exactly 100,000 rows in 22.7 seconds. Their
   database work is invisible to pg_stat_statements because a replication stream is
   not a statement, which is itself a point: there is no query to tune. Their price is
   `wal_level=logical`, a slot per target and one stream per slot.

**What this means for the decision.** If logical decoding is available, `wal` or
`journal` is the lightest way to stop an outbox table from bloating. If it is not,
`cursor` removes the dead tuples, the index decay and the relay-versus-producer
contention with an ordinary Postgres and a pooler, keeps the table visible in SQL,
tolerates long transactions, and turns publishing rate into a replica count. Its costs
are transaction-id order instead of commit order, a partition tree to create and drop
on a schedule, and a per-poll planning cost that grows with the number of days
retained.

## Four things the harness taught us before any numbers

- **The usual table-outbox claim is fragile.** Written as `DELETE … WHERE id IN (SELECT
  … LIMIT 1000 FOR UPDATE SKIP LOCKED)`, the planner can choose to rescan the subquery
  per candidate row once statistics are stale after a bulk load. On this stack that
  turned a 1,000-row claim into minutes of a saturated core and a claim that returned
  more rows than its LIMIT. Writing the claim as `WITH claimed AS MATERIALIZED (…)
  DELETE … USING claimed` evaluates it exactly once. Production table relays should
  use that form.
- **The Pub/Sub emulator is not a benchmark target.** Its single Java process became the
  bottleneck at a few hundred messages per second with ordering keys and retained
  memory across runs, so publish throughput said nothing about the transports. The
  relays got a simulated sink with a fixed cost per batch instead, and the emulator was
  removed from the stack.
- **Concurrent workers log out of order.** The cursor relay's `total=` counter is
  monotonic but eight goroutines write the log, so the last line is not the largest
  value. The harness takes the maximum per replica and sums across replicas.
- **Stale replica logs lie.** Summing per-replica logs by glob meant a one-replica run
  that followed a two-replica run also counted the old second log, declared itself
  drained at half the events, and reported an impossible 18-second drain for one
  worker publishing 269 batches at 200 ms each. The harness now clears a run's replica
  logs before starting it, and a drain shorter than batches × sink latency is the
  smell to look for.

## Caveats

- One machine, one run per cell, shared cores. Treat producer differences under about
  15 percent as noise. Physical-state numbers (dead space, index density, deleted
  pages) and per-statement costs are stable across reruns.
- Default autovacuum settings. A production outbox table would be tuned more
  aggressively, which shortens the vacuum lag for `table` but does not change the
  amount of dead tuples it creates or the index pages it leaves behind.
- The simulated sink's 200 ms is a stand-in. Real Pub/Sub publish latency from inside
  the same region is typically lower; from a laptop it is higher. It affects all
  transports equally, and it is the reason the cursor transport's shard count shows up
  as extra batches.
- The cursor replica runs share one PgBouncer with a 64-connection pool alongside the
  32 producer connections, so sixteen pollers fit; more would queue on the pooler
  before they queued on Postgres.
- pg_stat_statements attributes work to statements only. The log relays' decoding
  happens in a walsender and appears in WAL and buffer totals, not in the statement
  tables.
