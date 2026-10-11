#!/usr/bin/env python3
"""Render bench/results.jsonl (and bench/producer_only.jsonl if present) as markdown.

Runs with the same (mode, scenario) are averaged; the count of runs is shown.
Usage: bench/report.py > tables.md
"""
import json
import os
import statistics
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name):
    path = os.path.join(HERE, name)
    if not os.path.exists(path):
        return []
    return [json.loads(l) for l in open(path) if l.strip()]


def mb(b):
    return f"{b / 1_000_000:.2f} MB"


def avg(rows, key):
    if callable(key):
        vals = [v for v in (key(r) for r in rows) if v is not None]
    else:
        vals = [r[key] for r in rows if r.get(key) is not None]
    return statistics.mean(vals) if vals else None


def fmt(v, kind="n"):
    if v is None:
        return "-"
    if kind == "mb":
        return mb(v)
    if kind == "kb":
        return f"{v / 1_000:.0f} kB"
    if kind == "f1":
        return f"{v:.1f}"
    if kind == "f2":
        return f"{v:.2f}"
    if kind == "f3":
        return f"{v:.3f}"
    return f"{v:,.0f}"


def per_event(key, scale=1.0):
    """A derived column: key divided by events in the run, times scale."""
    def f(r):
        v = r.get(key)
        return None if v is None else v * scale / r["events"]
    return f


def group(rows):
    """Group by (label, scenario). label is the mode, or for the cursor transport the
    mode plus its worker and replica counts, e.g. "cursor w8x2"."""
    g = defaultdict(list)
    for r in rows:
        g[(r.get("label", r["mode"]), r["scenario"])].append(r)
    return g


def table(title, cols, groups, order):
    print(f"\n### {title}\n")
    print("| metric | " + " | ".join(f"{m} / {s}" for m, s in order) + " |")
    print("|---|" + "---|" * len(order))
    for label, key, kind in cols:
        cells = [fmt(avg(groups.get(k, []), key), kind) for k in order]
        print(f"| {label} | " + " | ".join(cells) + " |")


def main():
    rows = load("results.jsonl")
    if rows:
        g = group(rows)
        cursor_labels = sorted({k[0] for k in g if k[0].startswith("cursor")},
                               key=lambda l: tuple(int(x) for x in l[len("cursor w"):].split("x")))
        wanted = [("table", "concurrent"), ("wal", "concurrent"), ("journal", "concurrent")]
        wanted += [(c, "concurrent") for c in cursor_labels]
        wanted += [("table", "backlog"), ("wal", "backlog"), ("journal", "backlog")]
        wanted += [(c, "backlog") for c in cursor_labels]
        wanted += [("journal", "slotloss")] + [(c, "longtxn") for c in cursor_labels]
        order = [k for k in wanted if k in g]
        n = {k: len(v) for k, v in g.items()}
        ev = rows[0]["events"]
        print(f"Events per run: {ev:,} in transactions of {rows[0]['per_txn']} with {rows[0]['workers']} producer workers; "
              "runs averaged per column: " + ", ".join(f"{m}/{s}={n[(m, s)]}" for m, s in order) + ".")
        table("Producer (Go, via PgBouncer)", [
            ("throughput, events/s", "events_per_s", "n"),
            ("throughput, transactions/s", "txns_per_s", "n"),
            ("transaction latency p50, ms", "txn_ms_p50", "f2"),
            ("transaction latency p95, ms", "txn_ms_p95", "f2"),
            ("transaction latency p99, ms", "txn_ms_p99", "f2"),
            ("errors", "errors", "n"),
        ], g, order)
        table("Relay", [
            ("drain time, s (backlog/slotloss: relay start to last publish; concurrent: producer start to last publish)", "drain_s", "f2"),
            ("publish batches", "relay_batches", "n"),
            ("events replayed from journal", "relay_replayed", "n"),
            ("cursor: pollers per replica", "relay_workers", "n"),
            ("cursor: relay replicas", "relay_replicas", "n"),
            ("longtxn: seconds the transaction was held open", "longtxn_hold_s", "n"),
            ("longtxn: s until its event was published too (from producer start)", "longtxn_all_published_s", "f2"),
        ], g, order)
        table("Database", [
            ("WAL bytes written", "wal_bytes", "mb"),
            ("WAL records", "wal_records", "n"),
            ("outbox rows inserted", "outbox_tup_ins", "n"),
            ("outbox rows deleted", "outbox_tup_del", "n"),
            ("outbox dead tuples, peak", "outbox_dead_max", "n"),
            ("outbox dead tuples, end", "outbox_dead_end", "n"),
            ("outbox table+index size, peak", "outbox_bytes_max", "mb"),
            ("outbox table+index size, end", "outbox_bytes_end", "mb"),
            ("autovacuum runs on outbox", "outbox_autovacuum", "n"),
            ("buffer accesses (blks_hit)", "db_blks_hit", "n"),
            ("transactions committed", "db_xact_commit", "n"),
            ("slot lag, peak", "slot_lag_max", "mb"),
            ("postgres container CPU, avg %", "pg_cpu_pct_avg", "f1"),
            ("postgres container CPU, max %", "pg_cpu_pct_max", "f1"),
        ], g, order)
        table("Heap and indexes at the end of the run (pgstattuple, pgstatindex)", [
            ("outbox heap size", "outbox_heap_bytes", "mb"),
            ("outbox heap dead tuple space, %", "outbox_heap_dead_pct", "f1"),
            ("outbox heap free space, %", "outbox_heap_free_pct", "f1"),
            ("outbox index size, end", "outbox_index_bytes", "mb"),
            ("outbox index size, peak", "outbox_index_bytes_max", "mb"),
            ("outbox index count", "outbox_index_count", "n"),
            ("outbox index leaf density, % (page-weighted)", "outbox_index_leaf_density_pct", "f1"),
            ("outbox index leaf fragmentation, %", "outbox_index_fragmentation_pct", "f1"),
            ("outbox index deleted + empty pages", "outbox_index_deleted_pages", "n"),
            ("outbox row updates", "outbox_tup_upd", "n"),
            ("cursor table size (cursor only)", "cursor_bytes", "kb"),
            ("cursor row updates", "cursor_tup_upd", "n"),
            ("cursor row updates that were HOT", "cursor_tup_hot_upd", "n"),
            ("cursor row dead tuples, end", "cursor_dead_tup", "n"),
        ], g, order)
        table("Statements (pg_stat_statements): producer side = business insert + event write; relay side = everything else on an outbox table", [
            ("producer: calls", "stmt_producer_calls", "n"),
            ("producer: total exec time, s", lambda r: None if r.get("stmt_producer_ms") is None else r["stmt_producer_ms"] / 1000, "f2"),
            ("producer: exec time per 1,000 events, ms", per_event("stmt_producer_ms", 1000), "f1"),
            ("producer: blocks dirtied per 1,000 events", per_event("stmt_producer_blks_dirtied", 1000), "f1"),
            ("producer: WAL bytes per event", per_event("stmt_producer_wal_bytes"), "n"),
            ("relay: calls", "stmt_relay_calls", "n"),
            ("relay: total exec time, s", lambda r: None if r.get("stmt_relay_ms") is None else r["stmt_relay_ms"] / 1000, "f2"),
            ("relay: exec time per 1,000 events, ms", per_event("stmt_relay_ms", 1000), "f1"),
            ("relay: blocks hit per 1,000 events", per_event("stmt_relay_blks_hit", 1000), "n"),
            ("relay: blocks dirtied per 1,000 events", per_event("stmt_relay_blks_dirtied", 1000), "f1"),
            ("relay: WAL bytes per event", per_event("stmt_relay_wal_bytes"), "n"),
        ], g, order)
        statements_appendix(g, order)


def statements_appendix(g, order):
    """Top statements by total execution time, one small table per column of the
    concurrent and longtxn scenarios, and slotloss for the journal."""
    print("\n### Top statements per run (pg_stat_statements, concurrent scenario unless noted)\n")
    print("The log relays' replication stream is not a statement and does not appear; their "
          "database work is in the WAL and buffer figures above. Mean time is per call.\n")
    for label, scenario in order:
        if scenario not in ("concurrent", "longtxn", "slotloss"):
            continue
        rows = g[(label, scenario)]
        if not rows or not rows[-1].get("statements"):
            continue
        print(f"**{label} / {scenario}**\n")
        print("| side | statement | calls | total ms | mean ms | blks hit | blks dirtied | WAL bytes |")
        print("|---|---|---|---|---|---|---|---|")
        for st in rows[-1]["statements"][:6]:
            if st["side"] == "other":
                continue
            q = st["query"].replace("|", "\\|")
            if len(q) > 110:
                q = q[:107] + "..."
            print(f"| {st['side']} | `{q}` | {st['calls']:,} | {float(st['ms']):,.0f} | {float(st['mean_ms']):.3f} | "
                  f"{st['blks_hit']:,} | {st['blks_dirtied']:,} | {st['wal_bytes']:,} |")
        print()



if __name__ == "__main__":
    main()
