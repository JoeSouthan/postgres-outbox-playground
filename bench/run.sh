#!/usr/bin/env bash
# Benchmark harness. Runs the same producer workload through each outbox transport and
# records producer, relay and database effects. Everything runs in containers; this
# script only drives docker compose from the host.
#
#   bench/run.sh [events] [per_txn] [workers]       defaults 100000 10 32
#   BENCH_MODES="table wal journal cursor" BENCH_SCENARIOS="concurrent backlog" to narrow
#   BENCH_CURSOR_WORKERS=8 BENCH_CURSOR_REPLICAS=1   cursor relay: pollers per replica,
#                                                   and how many relay containers to run
#
# Scenarios:
#   concurrent  relay running while the producer runs (steady state)
#   backlog     producer runs with the relay stopped, then the relay drains (burst)
#   slotloss    (journal only) producer runs with the relay stopped, the slot is dropped
#               to simulate a failover, the relay restarts and recovers from the journal
#   longtxn     (cursor only) as concurrent, but a transaction that wrote one event is
#               held open for BENCH_LONGTXN_S (default 30) seconds across the whole run;
#               drain must not wait for it, and its event must arrive after it commits
#
# Each transport has its own stack: postgres-<mode>, pgbouncer-<mode>, outbox-<mode>
# (the publisher) and producer-<mode>. Requires `docker compose up -d` and
# `docker compose --profile bench build`.
# One JSON line per run is appended to bench/results.jsonl; logs go to bench/logs/.
set -euo pipefail
cd "$(dirname "$0")/.."

EVENTS=${1:-100000}
PER_TXN=${2:-10}
WORKERS=${3:-32}
MODES=${BENCH_MODES:-"table wal journal cursor"}
SCENARIOS=${BENCH_SCENARIOS:-"concurrent backlog slotloss longtxn"}
CURSOR_WORKERS=${BENCH_CURSOR_WORKERS:-8}
CURSOR_REPLICAS=${BENCH_CURSOR_REPLICAS:-1}
LONGTXN_S=${BENCH_LONGTXN_S:-30}
LOGDIR=${BENCH_LOGDIR:-bench/logs}
# simulated: each publish batch costs BENCH_SINK_LATENCY (default 200ms) and nothing is
# sent, standing in for a Pub/Sub round trip. pubsub: publish to a real project.
SINK=${BENCH_SINK:-simulated}
SINK_LATENCY=${BENCH_SINK_LATENCY:-200ms}
RELAY_CONTAINER=outbox-bench-relay
TARGET=target_1
SLOT="outbox_relay_$TARGET"
mkdir -p "$LOGDIR" state/wal state/journal

RELAY_PID=""; RELAY_PIDS=(); SAMPLER_PID=""
MODE=""   # the transport of the current run; selects the stack every helper talks to
sql() { docker compose exec -T "postgres-$MODE" psql -U outbox -d outbox -Atq -c "$1"; }

outbox_table() { case "$1" in table) echo outbox_items;; journal) echo outbox_journal;; cursor) echo outbox_events;; *) echo outbox_items;; esac; }

# Size of a table including partitions and indexes (pg_total_relation_size of a
# partitioned parent is 0), and of its indexes alone.
size_sql() { echo "SELECT coalesce(sum(pg_total_relation_size(oid)),0)::bigint FROM pg_class WHERE relkind='r' AND relname LIKE '$1%'"; }
index_size_sql() { echo "SELECT coalesce(sum(pg_relation_size(i.indexrelid)),0)::bigint FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid WHERE c.relkind='r' AND c.relname LIKE '$1%'"; }

reset_db() { # resets the current mode's database
  case "$MODE" in
    table)   sql "TRUNCATE outbox_items, payments" >/dev/null; sql "VACUUM ANALYZE outbox_items" >/dev/null ;;
    wal)     sql "TRUNCATE payments" >/dev/null ;;
    journal) sql "TRUNCATE outbox_journal, payments" >/dev/null; sql "VACUUM ANALYZE outbox_journal" >/dev/null ;;
    cursor)  sql "TRUNCATE outbox_events, payments" >/dev/null; sql "DELETE FROM outbox_cursor" >/dev/null
             sql "VACUUM ANALYZE outbox_events" >/dev/null; sql "VACUUM ANALYZE outbox_cursor" >/dev/null ;;
  esac
  sql "VACUUM ANALYZE payments" >/dev/null
  sql "SELECT pg_stat_reset()" >/dev/null
  sql "SELECT pg_stat_reset_shared('wal')" >/dev/null
  sql "SELECT pg_stat_statements_reset()" >/dev/null
  if [ "$MODE" = wal ] || [ "$MODE" = journal ]; then
    # Every run starts from a fresh slot and no relay state, so "ensure slot exists"
    # really creates one (and writes the state file the slot-loss scenario relies on).
    sql "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active" >/dev/null
    rm -f "state/$MODE"/relay_*.lsn
  fi
}

start_relay() { # mode logfile  (cursor: one container per replica, logfile.rN.log)
  docker rm -f "$RELAY_CONTAINER" >/dev/null 2>&1 || true
  local se=(-e RELAY_SINK="$SINK" -e RELAY_SINK_LATENCY="$SINK_LATENCY")
  RELAY_PIDS=()
  if [ "$1" = cursor ]; then
    for i in $(seq 1 "$CURSOR_REPLICAS"); do
      docker rm -f "$RELAY_CONTAINER-$i" >/dev/null 2>&1 || true
      docker compose run --rm -T --name "$RELAY_CONTAINER-$i" "${se[@]}" -e RELAY_WORKERS="$CURSOR_WORKERS" \
        outbox-cursor > "${2%.log}.r$i.log" 2>&1 &
      RELAY_PIDS+=($!)
    done
    for i in $(seq 1 "$CURSOR_REPLICAS"); do
      for _ in $(seq 1 100); do grep -q -a 'relaying' "${2%.log}.r$i.log" 2>/dev/null && break; sleep 0.2; done
    done
    RELAY_PID=${RELAY_PIDS[0]}
    return
  fi
  docker compose run --rm -T --name "$RELAY_CONTAINER" "${se[@]}" "outbox-$1" > "$2" 2>&1 &
  RELAY_PID=$!
  for _ in $(seq 1 100); do grep -q -a -E 'streaming|relaying' "$2" 2>/dev/null && break; sleep 0.2; done
}

stop_relay() {
  if [ -n "${RELAY_PID:-}" ]; then
    docker stop -t 5 "$RELAY_CONTAINER" >/dev/null 2>&1 || true
    for i in $(seq 1 "$CURSOR_REPLICAS"); do docker stop -t 5 "$RELAY_CONTAINER-$i" >/dev/null 2>&1 || true; done
    for p in "${RELAY_PIDS[@]:-$RELAY_PID}"; do kill "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; done
    RELAY_PID=""; RELAY_PIDS=()
  fi
}

# relay_logs <logfile>: the log files for the current relay (one per cursor replica).
relay_logs() { if ls "${1%.log}".r*.log >/dev/null 2>&1; then ls "${1%.log}".r*.log; else echo "$1"; fi; }

# relay_total <logfile>: messages published so far, summed over replicas. The counter
# is monotonic but concurrent workers log out of order, so take the maximum, not the
# last line.
relay_total() {
  local sum=0 t
  for f in $(relay_logs "$1"); do
    t=$(grep -a -o 'total=[0-9]*' "$f" 2>/dev/null | cut -d= -f2 | sort -n | tail -1)
    sum=$((sum + ${t:-0}))
  done
  echo "$sum"
}

run_producer() { # mode tag -> the producer's JSON line; stderr kept in a log
  docker compose run --rm -T "producer-$1" -events "$EVENTS" -per-txn "$PER_TXN" -workers "$WORKERS" -json 2> "$LOGDIR/producer_$2.log" | tail -1
}

# Samples dead tuples, outbox size, slot lag, postgres container CPU and index size
# every 0.5s.
start_sampler() { # outfile table
  (
    while true; do
      d=$(sql "SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname LIKE '$2%'" 2>/dev/null || echo 0)
      s=$(sql "$(size_sql "$2")" 2>/dev/null || echo 0)
      l=$(sql "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)),0)::bigint FROM pg_replication_slots" 2>/dev/null || echo 0)
      c=$(docker stats --no-stream --format '{{.CPUPerc}}' "outbox-postgres-$MODE-1" 2>/dev/null | tr -d '%' || echo 0)
      i=$(sql "$(index_size_sql "$2")" 2>/dev/null || echo 0)
      echo "$d $s $l ${c:-0} ${i:-0}"
      sleep 0.5
    done
  ) > "$1" 2>/dev/null &
  SAMPLER_PID=$!
}
stop_sampler() { kill "${SAMPLER_PID:-}" 2>/dev/null || true; wait "${SAMPLER_PID:-}" 2>/dev/null || true; SAMPLER_PID=""; }

# Never leave a sampler or a relay behind: a stray sampler keeps writing into the next
# run's sample file.
trap 'stop_sampler; stop_relay' EXIT

wait_drained() { # logfile expected t0 -> seconds until the relay reports total >= expected
  local log=$1 expected=$2 t0=$3
  while true; do
    total=$(relay_total "$log")
    if [ "${total:-0}" -ge "$expected" ]; then break; fi
    if [ $(( $(date +%s) - t0 )) -gt 900 ]; then echo "timeout waiting for relay" >&2; break; fi
    sleep 0.2
  done
  python3 -c "import time; print(round(time.time()-$t0, 2))"
}

db_stats() { # table
  sql "SELECT json_build_object(
        'wal_records', w.wal_records, 'wal_fpi', w.wal_fpi, 'wal_bytes', w.wal_bytes,
        'outbox_tup_ins', (SELECT coalesce(sum(n_tup_ins),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_tup_del', (SELECT coalesce(sum(n_tup_del),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_dead_end', (SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_autovacuum', (SELECT coalesce(sum(autovacuum_count),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_bytes_end', ($(size_sql "$1")),
        'outbox_tup_upd', (SELECT coalesce(sum(n_tup_upd),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_tup_hot_upd', (SELECT coalesce(sum(n_tup_hot_upd),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_heap_bytes', (SELECT coalesce(sum(pg_relation_size(oid)),0) FROM pg_class WHERE relkind='r' AND relname LIKE '$1%'),
        'outbox_heap_dead_pct', (SELECT round(100.0*sum(t.dead_tuple_len)/nullif(sum(t.table_len),0),1) FROM pg_class c, LATERAL pgstattuple(c.oid) t WHERE c.relkind='r' AND c.relname LIKE '$1%'),
        'outbox_heap_free_pct', (SELECT round(100.0*sum(t.free_space)/nullif(sum(t.table_len),0),1) FROM pg_class c, LATERAL pgstattuple(c.oid) t WHERE c.relkind='r' AND c.relname LIKE '$1%'),
        'outbox_index_bytes', ($(index_size_sql "$1")),
        'outbox_index_count', (SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid WHERE c.relkind='r' AND c.relname LIKE '$1%'),
        'outbox_index_leaf_pages', (SELECT coalesce(sum(x.leaf_pages),0) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid, LATERAL pgstatindex(i.indexrelid) x WHERE c.relkind='r' AND c.relname LIKE '$1%'),
        'outbox_index_deleted_pages', (SELECT coalesce(sum(x.deleted_pages+x.empty_pages),0) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid, LATERAL pgstatindex(i.indexrelid) x WHERE c.relkind='r' AND c.relname LIKE '$1%'),
        'outbox_index_leaf_density_pct', (SELECT round((sum(x.avg_leaf_density*x.leaf_pages)/nullif(sum(x.leaf_pages),0))::numeric,1) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid, LATERAL pgstatindex(i.indexrelid) x WHERE c.relkind='r' AND c.relname LIKE '$1%' AND x.leaf_pages > 0),
        'outbox_index_fragmentation_pct', (SELECT round((sum(x.leaf_fragmentation*x.leaf_pages)/nullif(sum(x.leaf_pages),0))::numeric,1) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid, LATERAL pgstatindex(i.indexrelid) x WHERE c.relkind='r' AND c.relname LIKE '$1%' AND x.leaf_pages > 0),
        'cursor_bytes', coalesce(pg_total_relation_size(to_regclass('outbox_cursor')),0),
        'cursor_tup_upd', (SELECT coalesce(sum(n_tup_upd),0) FROM pg_stat_user_tables WHERE relname='outbox_cursor'),
        'cursor_tup_hot_upd', (SELECT coalesce(sum(n_tup_hot_upd),0) FROM pg_stat_user_tables WHERE relname='outbox_cursor'),
        'cursor_dead_tup', (SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname='outbox_cursor'),
        'payments_tup_ins', coalesce(p.n_tup_ins,0),
        'db_blks_read', d.blks_read, 'db_blks_hit', d.blks_hit,
        'db_tup_returned', d.tup_returned, 'db_tup_fetched', d.tup_fetched,
        'db_xact_commit', d.xact_commit, 'db_xact_rollback', d.xact_rollback)
      FROM pg_stat_wal w, pg_stat_database d
      LEFT JOIN pg_stat_user_tables p ON p.relname='payments'
      WHERE d.datname='outbox'"
}

# Per-statement costs since the run's reset, most expensive first. The harness's own
# monitoring queries are excluded. The log relays' replication stream is not a
# statement, so it does not appear; only their journal replay does.
statements() {
  sql "SELECT coalesce(json_agg(x), '[]') FROM (
        SELECT left(regexp_replace(query, '\s+', ' ', 'g'), 200) AS query, calls, rows,
               round(total_exec_time::numeric, 1) AS ms, round(mean_exec_time::numeric, 3) AS mean_ms,
               shared_blks_hit AS blks_hit, shared_blks_read AS blks_read,
               shared_blks_dirtied AS blks_dirtied, shared_blks_written AS blks_written,
               wal_records, wal_bytes
        FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
        WHERE d.datname = 'outbox'
          AND query NOT ILIKE '%pg_stat%' AND query NOT ILIKE '%pgstat%' AND query NOT ILIKE '%pg_class%'
          AND query NOT ILIKE '%pg_replication%' AND query NOT ILIKE '%pg_wal_lsn%'
          AND query NOT ILIKE 'TRUNCATE%' AND query NOT ILIKE 'VACUUM%' AND query NOT ILIKE 'DELETE FROM outbox_cursor%'
        ORDER BY total_exec_time DESC LIMIT 25) x"
}

run_one() { # mode scenario
  local mode=$1 scenario=$2 tag="${1}_${2}" tbl
  MODE=$mode
  tbl=$(outbox_table "$mode")
  local rlog="$LOGDIR/relay_$tag.log" slog="$LOGDIR/samples_$tag.txt"
  local backlog_dead="" backlog_bytes="" backlog_lag="" t1="" longtxn_end="" longtxn_pid=""
  echo "== $tag events=$EVENTS per_txn=$PER_TXN workers=$WORKERS sink=$SINK($SINK_LATENCY)" >&2
  reset_db
  start_sampler "$slog" "$tbl"
  t0=$(date +%s)
  case "$scenario" in
    concurrent)
      start_relay "$mode" "$rlog"
      producer=$(run_producer "$mode" "$tag")
      drain=$(wait_drained "$rlog" "$EVENTS" "$t0") ;;
    longtxn)
      start_relay "$mode" "$rlog"
      # One transaction writes an event and stays open for the whole run.
      sql "BEGIN; INSERT INTO outbox_events (target, key, payload) VALUES ('$TARGET', 'LONG-1', '{\"key\":\"LONG-1\"}'); SELECT pg_sleep($LONGTXN_S); COMMIT;" >/dev/null 2>&1 &
      longtxn_pid=$!
      sleep 1
      producer=$(run_producer "$mode" "$tag")
      drain=$(wait_drained "$rlog" "$EVENTS" "$t0")                 # must not wait for the open txn
      wait "$longtxn_pid" 2>/dev/null || true
      longtxn_end=$(wait_drained "$rlog" $((EVENTS + 1)) "$t0")      # its event arrives after commit
      ;;
    backlog|slotloss)
      if [ "$mode" != table ]; then start_relay "$mode" "$LOGDIR/relay_${tag}_slot.log"; stop_relay; fi   # slot + state exist
      producer=$(run_producer "$mode" "$tag")
      backlog_dead=$(sql "SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname LIKE '$tbl%'")
      backlog_bytes=$(sql "$(size_sql "$tbl")")
      backlog_lag=$(sql "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)),0)::bigint FROM pg_replication_slots")
      if [ "$scenario" = slotloss ]; then sql "SELECT pg_drop_replication_slot('$SLOT')" >/dev/null; fi
      t1=$(date +%s)
      start_relay "$mode" "$rlog"
      drain=$(wait_drained "$rlog" "$EVENTS" "$t1") ;;
  esac
  sleep 1
  stats=$(db_stats "$tbl")
  stmts=$(statements)
  stop_relay
  stop_sampler
  BENCH_CURSOR_WORKERS="$CURSOR_WORKERS" BENCH_CURSOR_REPLICAS="$CURSOR_REPLICAS" BENCH_LONGTXN_S="$LONGTXN_S" BENCH_LONGTXN_END="$longtxn_end" BENCH_STATEMENTS="$stmts" \
  python3 - "$mode" "$scenario" "$producer" "$drain" "$stats" "$slog" "$backlog_dead" "$backlog_bytes" "$backlog_lag" "$rlog" "$tbl" <<'PY'
import json, sys, glob
mode, scenario, producer, drain, stats, slog, bdead, bbytes, blag, rlog, tbl = sys.argv[1:]
import os
r = {"mode": mode, "scenario": scenario, "sink": os.environ.get("BENCH_SINK", "simulated"),
     "sink_latency": os.environ.get("BENCH_SINK_LATENCY", "200ms"), "outbox_table": tbl if mode != "wal" else None}
r["label"] = mode
if mode == "cursor":
    r["relay_workers"] = int(os.environ["BENCH_CURSOR_WORKERS"]); r["relay_replicas"] = int(os.environ["BENCH_CURSOR_REPLICAS"])
    r["label"] = f"cursor w{r['relay_workers']}x{r['relay_replicas']}"
if scenario == "longtxn":
    r["longtxn_hold_s"] = int(os.environ["BENCH_LONGTXN_S"]); r["longtxn_all_published_s"] = float(os.environ["BENCH_LONGTXN_END"])
r.update(json.loads(producer))
r["drain_s"] = float(drain)
r.update(json.loads(stats))
rows = [l.split() for l in open(slog, errors="replace") if l.strip()]
rows = [x for x in rows if all(f.replace(".", "", 1).isdigit() for f in x)]
if rows:
    r["outbox_dead_max"] = max(int(x[0]) for x in rows)
    r["outbox_bytes_max"] = max(int(x[1]) for x in rows)
    r["slot_lag_max"] = max(int(x[2]) for x in rows)
    cpus = [float(x[3]) for x in rows if len(x) > 3]
    r["pg_cpu_pct_max"] = max(cpus) if cpus else None
    r["pg_cpu_pct_avg"] = round(sum(cpus)/len(cpus), 1) if cpus else None
    idx = [int(x[4]) for x in rows if len(x) > 4]
    r["outbox_index_bytes_max"] = max(idx) if idx else None
# pg_stat_statements: keep the top statements and attribute totals to the producer
# (business insert + event write) or the relay (everything else touching an outbox).
stmts = json.loads(os.environ.get("BENCH_STATEMENTS") or "[]")
def side(q):
    ql = q.lower().lstrip()
    if ql.startswith("insert into payments") or ql.startswith("insert into outbox_") or "pg_logical_emit_message" in ql:
        return "producer"
    if "outbox_" in ql:
        return "relay"
    return "other"
for st in stmts:
    st["side"] = side(st["query"])
r["statements"] = stmts[:12]
for s_ in ("producer", "relay"):
    sel = [st for st in stmts if st["side"] == s_]
    for k in ("calls", "rows", "ms", "blks_hit", "blks_read", "blks_dirtied", "blks_written", "wal_records", "wal_bytes"):
        r[f"stmt_{s_}_{k}"] = round(sum(float(st[k]) for st in sel), 1)
if bdead:
    r["backlog_dead_tup"] = int(bdead); r["backlog_outbox_bytes"] = int(bbytes); r["backlog_slot_lag"] = int(blag)
logs = sorted(glob.glob(rlog[:-4] + ".r*.log")) or [rlog]
log = "".join(open(f, errors="replace").read() for f in logs)
r["relay_batches"] = log.count("published ")
import re
m = re.search(r"replayed (\d+) from outbox_journal", log)
r["relay_replayed"] = int(m.group(1)) if m else 0
print(json.dumps(r))
PY
}

for mode in $MODES; do
  for scenario in $SCENARIOS; do
    if [ "$scenario" = slotloss ] && [ "$mode" != journal ]; then continue; fi
    if [ "$scenario" = longtxn ] && [ "$mode" != cursor ]; then continue; fi
    run_one "$mode" "$scenario" | tee -a bench/results.jsonl
  done
done
echo "done; results in bench/results.jsonl" >&2
