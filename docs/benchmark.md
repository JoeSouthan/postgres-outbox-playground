# Benchmark: table outbox vs log messages vs append-only table with a cursor

The same producer workload driven through four transports, measuring the effect on the
producer, the relay and the database. Numbers are from one laptop with every component
in one virtual machine; ratios between transports matter more than absolute figures,
and the caveats at the end say which figures are stable.

## Environment

| | |
|---|---|
| Host | MacBook Pro, Intel Core i5-1038NG7 at 2.0 GHz, 4 cores / 8 threads, 16 GB RAM, NVMe SSD, macOS 26.1 |
| Docker | Docker Desktop, engine 29.4.2, Apple Virtualization framework with VirtioFS, linux/amd64 |
| Docker VM | 8 vCPUs, 8 GB RAM, 1 GB swap, 60 GB disk, kernel 6.12 linuxkit, overlay2 |
| Postgres | 18.6, one instance per transport: `table` and `cursor` run `wal_level=replica` (the default), `wal` and `journal` run `wal_level=logical` with `max_slot_wal_keep_size=2GB`; all with `shared_buffers=512MB`, `max_connections=300`, `pg_stat_statements` loaded, default autovacuum settings |
| PgBouncer | edoburu/pgbouncer, transaction mode, pool of 64 server connections, one per Postgres |
| Sink | simulated: every publish batch costs 200 ms and sends nothing, standing in for a Pub/Sub round trip |

Everything shares the VM's four physical cores. One transport's stack is busy at a
time.

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
| throughput, events/s | 12,622 | 15,087 | 13,090 | 9,800 | 15,548 | 5,441 | 14,693 | 13,960 | 12,483 | 15,712 | 17,313 | 8,416 | 14,538 | 16,887 |
| throughput, transactions/s | 1,262 | 1,509 | 1,309 | 980 | 1,555 | 544 | 1,469 | 1,396 | 1,248 | 1,571 | 1,731 | 842 | 1,454 | 1,689 |
| transaction latency p50, ms | 20.63 | 17.44 | 19.48 | 23.68 | 17.44 | 34.98 | 18.02 | 16.34 | 18.68 | 17.07 | 16.07 | 24.88 | 17.97 | 16.39 |
| transaction latency p95, ms | 57.99 | 48.44 | 57.51 | 73.21 | 42.85 | 189.15 | 48.88 | 56.11 | 60.23 | 44.03 | 37.03 | 108.35 | 50.07 | 38.57 |
| transaction latency p99, ms | 85.42 | 73.50 | 94.90 | 205.68 | 71.18 | 325.57 | 73.37 | 133.82 | 148.80 | 72.66 | 60.88 | 225.29 | 77.03 | 59.38 |
| errors | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

### Relay

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| drain time, s (backlog/slotloss: relay start to last publish; concurrent: producer start to last publish) | 65.70 | 25.73 | 25.81 | 35.40 | 10.39 | 24.52 | 23.98 | 26.22 | 24.46 | 29.47 | 6.67 | 4.72 | 22.67 | 32.75 |
| publish batches | 101 | 100 | 100 | 132 | 246 | 632 | 100 | 100 | 100 | 126 | 126 | 126 | 0 | 132 |
| events replayed from journal | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 100,000 | 0 |
| cursor: pollers per replica | - | - | - | 1 | 8 | 8 | - | - | - | 1 | 8 | 8 | - | 1 |
| cursor: relay replicas | - | - | - | 1 | 1 | 2 | - | - | - | 1 | 1 | 2 | - | 1 |
| longtxn: seconds the transaction was held open | - | - | - | - | - | - | - | - | - | - | - | - | - | 30 |
| longtxn: s until its event was published too (from producer start) | - | - | - | - | - | - | - | - | - | - | - | - | - | 34.22 |

### Database

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| WAL bytes written | 90.27 MB | 42.14 MB | 81.08 MB | 55.40 MB | 53.67 MB | 56.61 MB | 72.65 MB | 42.12 MB | 81.09 MB | 55.85 MB | 52.14 MB | 54.08 MB | 81.25 MB | 56.07 MB |
| WAL records | 725,005 | 313,327 | 516,854 | 417,474 | 405,436 | 413,596 | 728,522 | 314,080 | 517,913 | 418,436 | 395,051 | 409,402 | 521,089 | 418,572 |
| outbox rows inserted | 100,000 | 0 | 100,000 | 100,000 | 96,970 | 98,520 | 100,000 | 0 | 100,000 | 100,000 | 94,560 | 97,960 | 100,000 | 100,001 |
| outbox rows deleted | 100,000 | 0 | 0 | 0 | 0 | 0 | 100,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox dead tuples, peak | 91,000 | 0 | 0 | 0 | 0 | 0 | 45,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox dead tuples, end | 96,430 | 0 | 0 | 0 | 0 | 0 | 51,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox table+index size, peak | 33.63 MB | 0.00 MB | 32.56 MB | 43.34 MB | 44.11 MB | 44.49 MB | 34.09 MB | 0.00 MB | 33.56 MB | 46.32 MB | 43.30 MB | 43.47 MB | 32.42 MB | 43.72 MB |
| outbox table+index size, end | 33.63 MB | 0.00 MB | 32.56 MB | 43.34 MB | 44.11 MB | 45.33 MB | 34.09 MB | 0.00 MB | 33.56 MB | 46.32 MB | 43.30 MB | 43.47 MB | 32.42 MB | 43.72 MB |
| autovacuum runs on outbox | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 | 2 | 0 | 0 | 1 | 32 |
| buffer accesses (blks_hit) | 1,987,967 | 430,800 | 836,669 | 1,624,170 | 1,560,111 | 1,838,830 | 1,933,645 | 435,514 | 847,932 | 1,724,525 | 1,489,127 | 1,583,678 | 941,498 | 1,647,996 |
| transactions committed | 10,372 | 10,133 | 10,133 | 10,321 | 10,067 | 10,630 | 10,272 | 10,143 | 10,147 | 10,388 | 9,704 | 10,027 | 10,180 | 10,477 |
| slot lag, peak | 0.00 MB | 29.01 MB | 55.61 MB | 0.00 MB | 0.00 MB | 0.00 MB | 0.00 MB | 42.74 MB | 82.01 MB | 0.00 MB | 0.00 MB | 0.00 MB | 81.88 MB | 0.00 MB |
| postgres container CPU, avg % | 99.7 | 86.0 | 90.9 | 86.8 | 251.2 | 321.5 | 96.7 | 72.7 | 79.0 | 68.5 | 202.8 | 170.4 | 78.0 | 85.8 |
| postgres container CPU, max % | 469.2 | 411.1 | 430.3 | 498.1 | 488.1 | 547.5 | 489.8 | 449.1 | 485.5 | 471.6 | 480.9 | 453.4 | 460.6 | 470.0 |

### Heap and indexes at the end of the run (pgstattuple, pgstatindex)

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| outbox heap size | 27.34 MB | 0.00 MB | 28.38 MB | 33.19 MB | 33.95 MB | 35.17 MB | 27.79 MB | 0.00 MB | 29.34 MB | 36.14 MB | 33.14 MB | 33.32 MB | 28.19 MB | 33.30 MB |
| outbox heap dead tuple space, % | 32.5 | - | 0.0 | 0.0 | 0.0 | 0.0 | 14.9 | - | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| outbox heap free space, % | 65.8 | - | 2.7 | 14.6 | 16.5 | 19.4 | 82.4 | - | 3.3 | 21.6 | 14.5 | 14.9 | 2.3 | 14.9 |
| outbox index size, end | 6.25 MB | 0.00 MB | 4.08 MB | 6.41 MB | 6.41 MB | 6.41 MB | 6.26 MB | 0.00 MB | 4.10 MB | 6.41 MB | 6.41 MB | 6.41 MB | 4.12 MB | 6.41 MB |
| outbox index size, peak | 6.25 MB | 0.00 MB | 4.08 MB | 6.41 MB | 6.41 MB | 6.41 MB | 6.26 MB | 0.00 MB | 4.10 MB | 6.41 MB | 6.41 MB | 6.41 MB | 4.12 MB | 6.41 MB |
| outbox index count | 2 | 0 | 10 | 361 | 361 | 361 | 2 | 0 | 10 | 361 | 361 | 361 | 10 | 361 |
| outbox index leaf density, % (page-weighted) | 91.1 | - | 91.7 | 88.3 | 88.3 | 88.3 | 90.8 | - | 91.1 | 88.3 | 88.3 | 88.3 | 90.8 | 88.3 |
| outbox index leaf fragmentation, % | 0.3 | - | 0.2 | 0.0 | 0.0 | 0.0 | 0.3 | - | 0.0 | 0.0 | 0.0 | 0.0 | 0.2 | 0.0 |
| outbox index deleted + empty pages | 26 | 0 | 0 | 0 | 0 | 0 | 363 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| outbox row updates | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| cursor table size (cursor only) | 0 kB | 0 kB | 0 kB | 49 kB | 82 kB | 74 kB | 0 kB | 0 kB | 0 kB | 49 kB | 49 kB | 49 kB | 0 kB | 90 kB |
| cursor row updates | 0 | 0 | 0 | 132 | 241 | 634 | 0 | 0 | 0 | 126 | 136 | 125 | 0 | 151 |
| cursor row updates that were HOT | 0 | 0 | 0 | 132 | 236 | 619 | 0 | 0 | 0 | 126 | 136 | 125 | 0 | 125 |
| cursor row dead tuples, end | 0 | 0 | 0 | 3 | 24 | 43 | 0 | 0 | 0 | 6 | 8 | 1 | 0 | 62 |

### Statements (pg_stat_statements): producer side = business insert + event write; relay side = everything else on an outbox table

| metric | table / concurrent | wal / concurrent | journal / concurrent | cursor w1x1 / concurrent | cursor w8x1 / concurrent | cursor w8x2 / concurrent | table / backlog | wal / backlog | journal / backlog | cursor w1x1 / backlog | cursor w8x1 / backlog | cursor w8x2 / backlog | journal / slotloss | cursor w1x1 / longtxn |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| producer: calls | 200,000 | 200,000 | 200,000 | 200,001 | 200,001 | 200,002 | 200,000 | 200,000 | 200,000 | 200,002 | 200,002 | 200,004 | 200,000 | 200,002 |
| producer: total exec time, s | 103.06 | 62.40 | 100.18 | 94.16 | 44.40 | 83.73 | 62.51 | 43.52 | 64.53 | 52.49 | 35.76 | 62.20 | 52.09 | 40.45 |
| producer: exec time per 1,000 events, ms | 1030.6 | 624.0 | 1001.8 | 941.6 | 444.0 | 837.3 | 625.1 | 435.2 | 645.3 | 524.9 | 357.6 | 622.0 | 520.9 | 404.5 |
| producer: blocks dirtied per 1,000 events | 51.3 | 10.3 | 49.8 | 56.0 | 56.9 | 58.4 | 51.4 | 10.3 | 49.9 | 59.7 | 55.7 | 56.0 | 49.5 | 56.0 |
| producer: WAL bytes per event | 603 | 416 | 805 | 550 | 549 | 582 | 603 | 416 | 805 | 549 | 547 | 548 | 805 | 548 |
| relay: calls | 132 | 0 | 0 | 494 | 1,475 | 4,320 | 118 | 0 | 0 | 429 | 869 | 1,290 | 1 | 571 |
| relay: total exec time, s | 41.44 | 0.00 | 0.00 | 0.47 | 0.77 | 3.97 | 0.87 | 0.00 | 0.00 | 0.31 | 0.49 | 0.55 | 0.87 | 0.37 |
| relay: exec time per 1,000 events, ms | 414.4 | 0.0 | 0.0 | 4.7 | 7.7 | 39.7 | 8.7 | 0.0 | 0.0 | 3.1 | 4.9 | 5.5 | 8.7 | 3.7 |
| relay: blocks hit per 1,000 events | 9,240 | 0 | 0 | 117 | 892 | 1,053 | 8,913 | 0 | 0 | 396 | 1,005 | 1,012 | 912 | 185 |
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
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 76,254 | 0.763 | 406,985 | 1,029 | 15,629,545 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 17,910 | 0.179 | 445,687 | 4,575 | 39,323,744 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 170 | 383 | 2.250 | 10,875 | 0 | 0 |
| relay | `SELECT xid, id, pending, catchup_xid, catchup_id, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM...` | 170 | 62 | 0.364 | 340 | 0 | 15,180 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, catchup_xid = $4, catchup_id = $5, updated_at = n...` | 132 | 20 | 0.150 | 396 | 0 | 18,179 |

**cursor w8x1 / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 33,161 | 0.332 | 406,325 | 1,030 | 15,630,955 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 11,242 | 0.112 | 446,752 | 4,658 | 39,241,310 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 502 | 622 | 1.238 | 84,826 | 0 | 0 |
| relay | `SELECT xid, id, pending, catchup_xid, catchup_id, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM...` | 502 | 64 | 0.127 | 2,044 | 0 | 35,146 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $7) = $2 AND xid = ...` | 217 | 44 | 0.205 | 1,033 | 0 | 0 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, catchup_xid = $4, catchup_id = $5, updated_at = n...` | 251 | 36 | 0.144 | 1,319 | 4 | 61,948 |

**cursor w8x2 / concurrent**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 59,058 | 0.591 | 405,243 | 1,035 | 15,804,114 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 24,671 | 0.247 | 452,331 | 4,801 | 42,375,551 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 1,141 | 2,228 | 1.953 | 92,523 | 0 | 0 |
| relay | `SELECT xid, id, pending, catchup_xid, catchup_id, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM...` | 1,930 | 1,082 | 0.560 | 7,104 | 0 | 78,444 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, catchup_xid = $4, catchup_id = $5, updated_at = n...` | 640 | 411 | 0.642 | 3,162 | 3 | 165,327 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $7) = $2 AND xid = ...` | 599 | 250 | 0.418 | 2,497 | 0 | 0 |

**journal / slotloss**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO outbox_journal (target, payload, lsn) VALUES ($1, $2, pg_logical_emit_message($3, $1, $2::text))` | 100,000 | 28,148 | 0.281 | 393,463 | 3,938 | 64,873,906 |
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 23,942 | 0.239 | 403,561 | 1,017 | 15,631,601 |
| relay | `SELECT payload, lsn::text FROM outbox_journal WHERE target = $1 AND lsn > $2::pg_lsn AND lsn <= $3::pg_lsn ...` | 1 | 867 | 867.167 | 91,214 | 0 | 0 |

**cursor w1x1 / longtxn**

| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |
|---|---|---|---|---|---|---|---|
| producer | `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)` | 100,000 | 31,870 | 0.319 | 405,954 | 1,023 | 15,631,052 |
| producer | `INSERT INTO outbox_events (target, key, payload) VALUES ($1, $2, $3)` | 100,000 | 8,575 | 0.086 | 438,244 | 4,570 | 39,191,417 |
| relay | `SELECT id, xid, key, payload FROM outbox_events WHERE target = $1 AND outbox_shard(key, $6) = $2 AND (xid, ...` | 181 | 337 | 1.864 | 16,879 | 0 | 0 |
| relay | `SELECT xid, id, pending, catchup_xid, catchup_id, array(SELECT pg_snapshot_xip(pg_current_snapshot())) FROM...` | 181 | 17 | 0.093 | 629 | 0 | 10,224 |
| relay | `UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, catchup_xid = $4, catchup_id = $5, updated_at = n...` | 160 | 11 | 0.067 | 837 | 4 | 33,828 |

Drain time in the concurrent scenarios includes the producer's own run, which is 6 to
11 seconds depending on the transport. With a 200 ms sink cost per batch, the publish
floor is 20 seconds for a single stream of 100 batches of 1,000; the cursor transport
publishes per shard, so its single stream needs about 130 batches and a 26-second floor.
Eight concurrent workers divide that floor by eight.

## Findings

**Headline comparison.** Physical-state and backlog figures are stable across reruns;
concurrent-scenario figures are indicative only (see Caveats).

| | table | wal | journal | cursor, 1 stream | cursor, 8 workers | cursor, 8 × 2 replicas |
|---|---|---|---|---|---|---|
| producer throughput, events/s (concurrent) | 12,622 | 15,087 | 13,090 | 9,800 | 15,548 | 5,441 |
| producer p99 latency, ms (concurrent) | 85 | 74 | 95 | 206 | 71 | 326 |
| drain, producer start to last publish, s | 65.7 | 25.7 | 25.8 | 35.4 | 10.4 | 24.5 |
| drain of a 100,000 backlog, relay alone, s | 24.0 | 26.2 | 24.5 | 29.5 | 6.7 | 4.7 |
| publish rate on that backlog, events/s | 4,170 | 3,814 | 4,088 | 3,393 | 14,993 | 21,186 |
| dead tuples at peak (concurrent) | 91,000 | 0 | 0 | 0 | 0 | 0 |
| heap dead space at end, % (concurrent) | 32.5 | - | 0.0 | 0.0 | 0.0 | 0.0 |
| heap free space at end, % (concurrent) | 65.8 | - | 2.7 | 14.6 | 16.5 | 19.4 |
| index size at end | 6.3 MB | - | 4.1 MB | 6.4 MB | 6.4 MB | 6.4 MB |
| index leaf density, % | 91.1 | - | 91.7 | 88.3 | 88.3 | 88.3 |
| index deleted + empty pages (backlog, after vacuum) | 363 | - | 0 | 0 | 0 | 0 |
| WAL bytes (concurrent) | 90 MB | 42 MB | 81 MB | 55 MB | 54 MB | 57 MB |
| buffer accesses (concurrent) | 1,987,967 | 430,800 | 836,669 | 1,624,170 | 1,560,111 | 1,838,830 |
| relay statement time per 1,000 events, ms (concurrent) | 414.4 | - | - | 4.7 | 7.7 | 39.7 |
| relay statement time per 1,000 events, ms (backlog) | 8.7 | - | - | 3.1 | 4.9 | 5.5 |
| relay blocks hit per 1,000 events (concurrent) | 9,240 | - | 0 | 117 | 892 | 1,053 |
| relay WAL bytes per event (concurrent) | 292 | - | 0 | 0 | 1 | 2 |
| producer statement time per 1,000 events, ms (backlog, producer alone) | 625.1 | 435.2 | 645.3 | 524.9 | 357.6 | 622.0 |
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
   alone. The cursor relay's whole poll, claim, catch-up, read and advance, cost 3 to 5
   ms per 1,000 events as a single stream and touched about 1,000 buffers on the
   backlog, with no measurable WAL. That is the difference between a relay that locks and deletes
   the producers' pages and one that reads committed rows under a snapshot.

3. **Index state tells the churn story most clearly.** After the backlog run and its
   vacuum, the table's two indexes held 363 deleted or empty pages that B-tree will
   never give back without a REINDEX, on a table that was empty again. The journal's
   index sat at 91 percent leaf density with nothing deleted. The cursor's per-shard
   leaf indexes ended at 88 percent density, zero fragmentation and nothing deleted,
   6.4 MB against the journal's 4.1 MB for the same rows. With one `(target, shard,
   xid, id)` index per day instead of per-shard leaves, the same rows measured 52
   percent density and 8.8 MB, from 32 interleaved insertion points. The cursor heaps
   end with 15 to 19 percent free space, from 32 producer connections extending 32
   small relations concurrently; later inserts fill it.

4. **Replicas scale publishing and lose nothing.** The 100,000-event backlog that one
   cursor stream drains in 29.5 seconds drains in 6.7 seconds with eight workers
   and 4.7 with eight workers in each of two containers, a publish rate of
   21,186 events per second against the 5,000 a single stream can reach through the
   200 ms sink. The two replicas published 50,033 and 49,967 messages in the backlog
   run and 49,318 and 50,682 in the concurrent run, split by nothing but the cursor row
   locks. Relay statement time per event rises with workers and with producer
   contention, to 8 ms per 1,000 for eight workers and 40 for sixteen while the
   producers run, because batches are smaller and the pollers plan and probe
   concurrently with 32 producer connections on four cores; on the backlog, with the
   producers idle, sixteen pollers cost 5.5 ms per 1,000.

5. **A long transaction does not stall the cursor.** With a transaction holding an
   event open for 30 seconds across the whole run, the drain took 32.8 seconds
   against 35.4 without it, which is noise, and the held event was published 34.2
   seconds after the run started, a few seconds after its commit, through the
   catch-up path. The cursor row saw 151 updates in that run, 125 of them HOT, on an
   8 kB heap.

6. **Producer cost for `cursor` is in the table and journal band, measured per
   statement.** With the relay stopped, the two producer statements cost 358 to 622
   ms per 1,000 events on `cursor` across three identical-code runs, 625 on `table`,
   645 on `journal` and 435 on `wal`. The spread on `cursor` is host noise; partition
   routing does not put it above the table. Under concurrent load every producer slows
   because the relay competes for the same cores, and the table relay competes hardest.

7. **`wal` is the lightest transport on every database axis and `journal` recovers a
   lost slot**, replaying exactly 100,000 rows in 22.7 seconds. Their database work
   does not appear in pg_stat_statements because a replication stream is not a
   statement. Their price is `wal_level=logical`, a slot per target and one stream per
   slot.

**What this means for the decision.** If logical decoding is available, `wal` or
`journal` is the lightest way to stop an outbox table from bloating. If it is not,
`cursor` removes the dead tuples, the index decay and the relay-versus-producer
contention with an ordinary Postgres and a pooler, keeps the table visible in SQL,
tolerates long transactions, and turns publishing rate into a replica count. Its costs
are transaction-id order instead of commit order, a partition tree to create and drop
on a schedule, and a per-poll planning cost that grows with the number of days
retained.

## Harness notes

- **Table claim form.** `DELETE … WHERE id IN (SELECT … LIMIT 1000 FOR UPDATE SKIP
  LOCKED)` can be rescanned per candidate row when statistics are stale, which turned
  a 1,000-row claim into minutes of CPU here. The harness and the table page use
  `WITH claimed AS MATERIALIZED (…) DELETE … USING claimed`.
- **No Pub/Sub emulator.** Its single Java process capped at a few hundred messages per
  second with ordering keys. The relays use a simulated sink with a fixed cost per
  batch.
- **Out-of-order logs.** The cursor relay's `total=` counter is monotonic but workers log
  concurrently; the harness takes the maximum per replica and sums replicas.
- **Stale replica logs.** The harness clears a run's per-replica logs before starting
  it. A one-replica run that read a stale second log once reported a drain shorter than
  batches × sink latency, which is the check for this.

## Caveats

- One machine, one run per cell, shared cores. Two reruns of identical cursor code
  moved concurrent-scenario producer throughput by up to 45 percent and relay
  statement time under contention by 2 to 3×, so treat every concurrent-scenario
  figure as indicative only. Backlog drains, where the relay runs alone, moved by
  under 20 percent. Physical-state numbers (dead space, index density, deleted pages)
  did not move at all.
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
