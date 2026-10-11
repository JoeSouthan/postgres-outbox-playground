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
| `cursor` | INSERT into `outbox_events` (append-only, `xid` and `shard` columns) | N workers per replica, each: lock a shard's cursor row `FOR UPDATE SKIP LOCKED`, read forward from it in `(xid, id)` order, publish, advance the cursor, commit; no deletes, no slot, through PgBouncer |

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
| throughput, events/s | 12,622 | 15,087 | 13,090 | 12,223 | 9,376 | 13,133 | 14,693 | 13,960 | 12,483 | 15,218 | 13,254 | 10,177 | 14,538 | 16,143 |
| throughput, transactions/s | 1,262 | 1,509 | 1,309 | 1,222 | 938 | 1,313 | 1,469 | 1,396 | 1,248 | 1,522 | 1,325 | 1,018 | 1,454 | 1,614 |
| transaction latency p50, ms | 20.63 | 17.44 | 19.48 | 22.17 | 20.54 | 19.81 | 18.02 | 16.34 | 18.68 | 17.62 | 17.52 | 23.86 | 17.97 | 16.83 |
| transaction latency p95, ms | 57.99 | 48.44 | 57.51 | 55.89 | 60.18 | 51.11 | 48.88 | 56.11 | 60.23 | 43.41 | 42.82 | 77.00 | 50.07 | 41.15 |
| transaction latency p99, ms | 85.42 | 73.50 | 94.90 | 82.68 | 256.88 | 103.39 | 73.37 | 133.82 | 148.80 | 82.95 | 92.19 | 176.71 | 77.03 | 63.27 |
| errors | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

### Relay

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| drain time, s (backlog/slotloss: relay start to last publish; concurrent: producer start to last publish) | 65.70 | 25.73 | 25.81 | 32.29 | 15.71 | 11.94 | 23.98 | 26.22 | 24.46 | 29.23 | 5.88 | 4.49 | 22.67 | 32.39 |
| publish batches | 101 | 100 | 100 | 130 | 311 | 381 | 100 | 100 | 100 | 126 | 126 | 126 | 0 | 131 |
| events replayed from journal | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 100,000 | 0 |
| cursor: pollers per replica | - | - | - | 1 | 8 | 8 | - | - | - | 1 | 8 | 8 | - | 1 |
| cursor: relay replicas | - | - | - | 1 | 1 | 2 | - | - | - | 1 | 1 | 2 | - | 1 |
| longtxn: seconds the transaction was held open | - | - | - | - | - | - | - | - | - | - | - | - | - | 30 |
| longtxn: s until its event was published too (from producer start) | - | - | - | - | - | - | - | - | - | - | - | - | - | 33.57 |

### Database

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| WAL bytes written | 90.27 MB | 42.14 MB | 81.08 MB | 60.09 MB | 58.18 MB | 57.49 MB | 72.65 MB | 42.12 MB | 81.09 MB | 60.41 MB | 57.05 MB | 61.01 MB | 81.25 MB | 60.16 MB |
| WAL records | 725,005 | 313,327 | 516,854 | 418,002 | 404,999 | 402,432 | 728,522 | 314,080 | 517,913 | 422,278 | 397,128 | 424,804 | 521,089 | 418,119 |
| outbox rows inserted | 100,000 | 0 | 100,000 | 100,000 | 96,630 | 94,990 | 100,000 | 0 | 100,000 | 100,000 | 94,940 | 101,520 | 100,000 | 100,001 |
| outbox rows deleted | 100,000 | 0 | 0 | 0 | 0 | 0 | 100,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox dead tuples, peak | 91,000 | 0 | 0 | 0 | 0 | 0 | 45,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox dead tuples, end | 96,430 | 0 | 0 | 0 | 0 | 0 | 51,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox table+index size, peak | 33.63 MB | 0.00 MB | 32.56 MB | 38.14 MB | 37.89 MB | 39.17 MB | 34.09 MB | 0.00 MB | 33.56 MB | 38.13 MB | 38.29 MB | 38.45 MB | 32.42 MB | 38.65 MB |
| outbox table+index size, end | 33.63 MB | 0.00 MB | 32.56 MB | 38.14 MB | 38.12 MB | 39.17 MB | 34.09 MB | 0.00 MB | 33.56 MB | 38.13 MB | 38.29 MB | 38.45 MB | 32.42 MB | 38.65 MB |
| autovacuum runs on outbox | 1 | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 1 | 1 |
| buffer accesses (blks_hit) | 1,987,967 | 430,800 | 836,669 | 1,391,468 | 1,303,354 | 1,297,366 | 1,933,645 | 435,514 | 847,932 | 1,357,422 | 1,298,461 | 1,364,930 | 941,498 | 1,394,015 |
| transactions committed | 10,372 | 10,133 | 10,133 | 10,282 | 10,059 | 9,936 | 10,272 | 10,143 | 10,147 | 10,288 | 9,705 | 10,349 | 10,180 | 10,328 |
| slot lag, peak | 0.00 MB | 29.01 MB | 55.61 MB | 0.00 MB | 0.00 MB | 0.00 MB | 0.00 MB | 42.74 MB | 82.01 MB | 0.00 MB | 0.00 MB | 0.00 MB | 81.88 MB | 0.00 MB |
| postgres container CPU, avg % | 99.7 | 86.0 | 90.9 | 81.3 | 233.7 | 229.6 | 96.7 | 72.7 | 79.0 | 70.4 | 149.8 | 201.0 | 78.0 | 78.3 |
| postgres container CPU, max % | 469.2 | 411.1 | 430.3 | 462.8 | 464.6 | 455.5 | 489.8 | 449.1 | 485.5 | 465.1 | 483.1 | 461.6 | 460.6 | 482.5 |

### Heap and indexes at the end of the run (pgstattuple, pgstatindex)

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| outbox heap size | 27.34 MB | 0.00 MB | 28.38 MB | 29.26 MB | 29.23 MB | 30.28 MB | 27.79 MB | 0.00 MB | 29.34 MB | 29.22 MB | 29.39 MB | 29.56 MB | 28.19 MB | 29.77 MB |
| outbox heap dead tuple space, % | 32.5 | - | 0.0 | 0.0 | 0.0 | 0.0 | 14.9 | - | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| outbox heap free space, % | 65.8 | - | 2.7 | 3.2 | 3.1 | 3.5 | 82.4 | - | 3.3 | 3.0 | 3.2 | 3.3 | 2.3 | 3.1 |
| outbox index size, end | 6.25 MB | 0.00 MB | 4.08 MB | 8.77 MB | 8.78 MB | 8.78 MB | 6.26 MB | 0.00 MB | 4.10 MB | 8.79 MB | 8.79 MB | 8.77 MB | 4.12 MB | 8.77 MB |
| outbox index size, peak | 6.25 MB | 0.00 MB | 4.08 MB | 8.77 MB | 8.78 MB | 8.78 MB | 6.26 MB | 0.00 MB | 4.10 MB | 8.79 MB | 8.79 MB | 8.77 MB | 4.12 MB | 8.77 MB |
| outbox index count | 2 | 0 | 10 | 10 | 10 | 10 | 2 | 0 | 10 | 10 | 10 | 10 | 10 | 10 |
| outbox index leaf density, % (page-weighted) | 91.1 | - | 91.7 | 51.7 | 51.7 | 51.7 | 90.8 | - | 91.1 | 51.6 | 51.6 | 51.7 | 90.8 | 51.7 |
| outbox index leaf fragmentation, % | 0.3 | - | 0.2 | 2.9 | 2.9 | 2.9 | 0.3 | - | 0.0 | 2.9 | 2.9 | 2.9 | 0.2 | 2.9 |
| outbox index deleted + empty pages | 26 | 0 | 0 | 0 | 0 | 0 | 363 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox row updates | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| cursor table size (cursor only) | 0 kB | 0 kB | 0 kB | 49 kB | 66 kB | 74 kB | 0 kB | 0 kB | 0 kB | 49 kB | 49 kB | 49 kB | 0 kB | 82 kB |
| cursor row updates | 0 | 0 | 0 | 130 | 309 | 369 | 0 | 0 | 0 | 126 | 138 | 116 | 0 | 153 |
| cursor row updates that were HOT | 0 | 0 | 0 | 130 | 302 | 346 | 0 | 0 | 0 | 126 | 138 | 116 | 0 | 127 |
| cursor row dead tuples, end | 0 | 0 | 0 | 4 | 33 | 77 | 0 | 0 | 0 | 11 | 23 | 24 | 0 | 48 |

### Statements (pg_stat_statements): producer side = business insert + event write; relay side = everything else on an outbox table

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| producer: calls | 200,000 | 200,000 | 200,000 | 200,001 | 200,001 | 200,002 | 200,000 | 200,000 | 200,000 | 200,002 | 200,002 | 200,004 | 200,000 | 200,002 |
| producer: total exec time, s | 103.06 | 62.40 | 100.18 | 89.65 | 87.32 | 55.42 | 62.51 | 43.52 | 64.53 | 53.02 | 43.54 | 54.81 | 52.09 | 45.24 |
| producer: exec time per 1,000 events, ms | 1030.6 | 624.0 | 1001.8 | 896.5 | 873.2 | 554.2 | 625.1 | 435.2 | 645.3 | 530.2 | 435.4 | 548.1 | 520.9 | 452.4 |
| producer: blocks dirtied per 1,000 events | 51.3 | 10.3 | 49.8 | 56.6 | 56.6 | 56.9 | 51.4 | 10.3 | 49.9 | 56.6 | 56.7 | 56.7 | 49.5 | 56.6 |
| producer: WAL bytes per event | 603 | 416 | 805 | 597 | 597 | 597 | 603 | 416 | 805 | 597 | 597 | 597 | 805 | 597 |
| relay: calls | 132 | 0 | 0 | 476 | 1,791 | 3,032 | 118 | 0 | 0 | 419 | 770 | 1,149 | 1 | 545 |
| relay: total exec time, s | 41.44 | 0.00 | 0.00 | 0.55 | 2.39 | 1.39 | 0.87 | 0.00 | 0.00 | 0.31 | 0.62 | 0.54 | 0.87 | 0.46 |
| relay: exec time per 1,000 events, ms | 414.4 | 0.0 | 0.0 | 5.5 | 23.9 | 13.9 | 8.7 | 0.0 | 0.0 | 3.1 | 6.2 | 5.4 | 8.7 | 4.6 |
| relay: blocks hit per 1,000 events | 9,240 | 0 | 0 | 1,322 | 959 | 1,007 | 8,913 | 0 | 0 | 920 | 1,272 | 1,280 | 912 | 1,261 |
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
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 71,381 | 0.714 | 406,301 | 1,024 | 15,629,596 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 18,270 | 0.183 | 498,645 | 4,638 | 44,077,014 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND shard = $2 AND created_at >= now() - ...` | 164 | 527 | 3.211 | 131,372 | 0 | 0 |
| relay | `SELECT xid, id, pending, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM outbox_cursor WHERE targ...` | 164 | 19 | 0.114 | 328 | 0 | 13,518 |

**cursor w8x1 / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 55,639 | 0.556 | 404,855 | 1,030 | 15,630,939 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 31,675 | 0.317 | 499,181 | 4,634 | 44,084,763 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND shard = $2 AND created_at >= now() - ...` | 598 | 1,403 | 2.346 | 91,256 | 0 | 0 |
| relay | `SELECT xid, id, pending, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM outbox_cursor WHERE targ...` | 598 | 508 | 0.850 | 1,577 | 0 | 42,160 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, updated_at = now() WHERE target = $4 AND shard = $5` | 317 | 252 | 0.795 | 1,172 | 2 | 71,184 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND shard = $2 AND created_at >= now() - ...` | 278 | 226 | 0.814 | 1,895 | 0 | 0 |

**cursor w8x2 / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 35,523 | 0.355 | 404,247 | 1,036 | 15,631,004 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 19,894 | 0.199 | 500,311 | 4,653 | 44,081,110 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND shard = $2 AND created_at >= now() - ...` | 948 | 832 | 0.878 | 92,343 | 0 | 0 |
| relay | `SELECT xid, id, pending, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM outbox_cursor WHERE targ...` | 1,341 | 241 | 0.180 | 4,207 | 0 | 59,256 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND shard = $2 AND created_at >= now() - ...` | 355 | 239 | 0.674 | 2,473 | 0 | 0 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, updated_at = now() WHERE target = $4 AND shard = $5` | 388 | 74 | 0.190 | 1,672 | 3 | 100,628 |

**journal / slotloss**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO outbox_journal (target, payload, lsn) VALUES ($1, $2, pg_logical_emit_message($3, $1, $2::text))` | 100,000 | 28,148 | 0.281 | 393,463 | 3,938 | 64,873,906 |
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 23,942 | 0.239 | 403,561 | 1,017 | 15,631,601 |
| relay | `SELECT payload, lsn::text FROM outbox_journal WHERE target = $1 AND lsn > $2::pg_lsn AND lsn <= $3::pg_lsn ...` | 1 | 867 | 867.167 | 91,214 | 0 | 0 |

**cursor w1x1 / longtxn**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 31,196 | 0.312 | 405,544 | 1,027 | 15,631,052 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 14,042 | 0.140 | 498,332 | 4,633 | 44,074,161 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND shard = $2 AND created_at >= now() - ...` | 176 | 435 | 2.471 | 124,710 | 0 | 0 |
| relay | `SELECT xid, id, pending, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM outbox_cursor WHERE targ...` | 176 | 16 | 0.091 | 524 | 0 | 9,998 |

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
| producer throughput, events/s (concurrent) | 12,622 | 15,087 | 13,090 | 12,223 | 9,376 | 13,133 |
| producer p99 latency, ms (concurrent) | 85 | 74 | 95 | 83 | 257 | 103 |
| drain, producer start to last publish, s | 65.7 | 25.7 | 25.8 | 32.3 | 15.7 | 11.9 |
| drain of a 100,000 backlog, relay alone, s | 24.0 | 26.2 | 24.5 | 29.2 | 5.9 | 4.5 |
| publish rate on that backlog, events/s | 4,170 | 3,814 | 4,088 | 3,421 | 17,007 | 22,272 |
| dead tuples at peak (concurrent) | 91,000 | 0 | 0 | 0 | 0 | 0 |
| heap dead space at end, % (concurrent) | 32.5 | - | 0.0 | 0.0 | 0.0 | 0.0 |
| heap free space at end, % (concurrent) | 65.8 | - | 2.7 | 3.2 | 3.1 | 3.5 |
| index size at end | 6.3 MB | - | 4.1 MB | 8.8 MB | 8.8 MB | 8.8 MB |
| index leaf density, % | 91.1 | - | 91.7 | 51.7 | 51.7 | 51.7 |
| index deleted + empty pages (backlog, after vacuum) | 363 | - | 0 | 0 | 0 | 0 |
| WAL bytes (concurrent) | 90 MB | 42 MB | 81 MB | 60 MB | 58 MB | 57 MB |
| buffer accesses (concurrent) | 1,987,967 | 430,800 | 836,669 | 1,391,468 | 1,303,354 | 1,297,366 |
| relay statement time per 1,000 events, ms (concurrent) | 414.4 | - | - | 5.5 | 23.9 | 13.9 |
| relay statement time per 1,000 events, ms (backlog) | 8.7 | - | - | 3.1 | 6.2 | 5.4 |
| relay blocks hit per 1,000 events (concurrent) | 9,240 | - | 0 | 1,322 | 959 | 1,007 |
| relay WAL bytes per event (concurrent) | 292 | - | 0 | 0 | 1 | 2 |
| producer statement time per 1,000 events, ms (backlog, producer alone) | 625.1 | 435.2 | 645.3 | 530.2 | 435.4 | 548.1 |
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
   alone. The cursor relay's whole poll, claim, catch-up, read and advance, cost 3 to 6
   ms per 1,000 events as a single stream and touched about 900 to 1,300 buffers, with
   no measurable WAL. That is the difference between a relay that locks and deletes
   the producers' pages and one that reads committed rows under a snapshot.

3. **Index state tells the churn story most clearly.** After the backlog run and its
   vacuum, the table's two indexes held 363 deleted or empty pages that B-tree will
   never give back without a REINDEX, on a table that was empty again. The journal's
   index sat at 91 percent leaf density with nothing deleted. The cursor index also had
   nothing deleted and 3 percent fragmentation, but at 52 percent leaf density it is
   about twice the journal's size per row, 8.8 MB against 4.1 MB. That is by
   construction: 32 shards mean 32 interleaved insertion points and 50/50 page splits
   instead of right-edge appends. It is a fill cost that does not grow over time, not
   bloat; hash-subpartitioning each day by shard would restore right-edge appends per
   shard if the index size ever mattered.

4. **Replicas scale publishing and lose nothing.** The 100,000-event backlog that one
   cursor stream drains in 29.2 seconds drains in 5.9 seconds with eight workers
   and 4.5 with eight workers in each of two containers, a publish rate of
   22,272 events per second against the 5,000 a single stream can reach through the
   200 ms sink. The two replicas published 50,228 and 49,772 messages in the backlog
   run and 49,988 and 50,012 in the concurrent run, split by nothing but the cursor row
   locks. Relay statement time per event rises with workers, to 14 to 24 ms per 1,000,
   because batches are smaller and sixteen pollers plan and probe concurrently; on this
   four-core host that is where the ceiling is.

5. **A long transaction does not stall the cursor.** With a transaction holding an
   event open for 30 seconds across the whole run, the drain took 32.4 seconds
   against 32.3 without it, and the held event was published 33.6 seconds
   after the run started, shortly after its commit, through the catch-up path. The
   cursor row saw 153 updates in that run, 127 of them HOT, on an 8 kB heap.

6. **Producer cost for `cursor` is in the table and journal band, measured per
   statement.** With the relay stopped, the two producer statements cost 530 ms per
   1,000 events on `cursor`, 625 on `table`, 645 on `journal` and 435 on
   `wal`. Under concurrent load every producer gets slower because the relay competes
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
are transaction-id order instead of commit order, a half-full index by design, and a
per-poll planning overhead that only shows when it is run as a single stream.

## Three things the harness taught us before any numbers

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
