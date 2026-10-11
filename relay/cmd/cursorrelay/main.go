// cursorrelay publishes from an append-only outbox table without deleting from it and
// without logical decoding. Each (target, shard) has a cursor row holding the frontier,
// the (xid, id) of the last row published, and a pending set of transaction ids that
// were still in flight when the frontier passed them. A poll is one transaction:
//
//  1. lock the shard's cursor row FOR UPDATE SKIP LOCKED and read the snapshot's
//     in-flight transaction list;
//  2. catch up: read rows of pending transactions that have since resolved;
//  3. read forward from the frontier, in (xid, id) order, up to the batch size;
//  4. publish both sets, wait for every ack, then advance the cursor and commit.
//
// Every row read is committed by construction (it is visible under the snapshot), and
// the pending set is what makes the frontier safe to move past a long-running
// transaction: its rows are published on the first poll after it commits. Workers in
// one or many replicas race over the shard cursors; the row lock is held only for the
// batch, so shards flow to whichever worker is free and a dead replica loses nothing.
// Delivery is at-least-once: a crash after publish and before commit republishes the
// batch, and consumers dedupe on the outbox id.
//
// Everything is transaction-scoped, so the relay runs through PgBouncer in transaction
// mode like the producers do.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JoeSouthan/postgres-outbox-playground/relay/internal/sink"
)

type config struct {
	dsn         string
	target      string
	project     string
	topic       string
	sink        string
	sinkLatency time.Duration
	batch       int
	workers     int
	poll        time.Duration
	pollMax     time.Duration
	maxFull     int
	refresh     time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.dsn, "dsn", env("RELAY_DSN", "postgres://outbox:outbox@pgbouncer:5432/outbox"), "database DSN; PgBouncer in transaction mode is fine")
	flag.StringVar(&cfg.target, "target", env("RELAY_TARGET", "target_1"), "outbox_events.target this relay serves")
	flag.StringVar(&cfg.project, "project", env("PUBSUB_PROJECT_ID", "outbox-local"), "Pub/Sub project")
	flag.StringVar(&cfg.topic, "topic", env("PUBSUB_TOPIC", ""), "Pub/Sub topic (default outbox-<target>)")
	flag.StringVar(&cfg.sink, "sink", env("RELAY_SINK", "simulated"), "where to publish: simulated | pubsub")
	flag.DurationVar(&cfg.sinkLatency, "sink-latency", envDuration("RELAY_SINK_LATENCY", 200*time.Millisecond), "simulated sink: cost of one publish batch")
	flag.IntVar(&cfg.batch, "batch", envInt("RELAY_BATCH", 1000), "rows per poll per shard")
	flag.IntVar(&cfg.workers, "workers", envInt("RELAY_WORKERS", 8), "concurrent pollers in this replica; each holds one connection during a batch")
	flag.DurationVar(&cfg.poll, "poll", envDuration("RELAY_POLL", 100*time.Millisecond), "sleep after a poll that found nothing")
	flag.DurationVar(&cfg.pollMax, "poll-max", envDuration("RELAY_POLL_MAX", time.Second), "the sleep stretches towards this while every shard stays empty")
	flag.IntVar(&cfg.maxFull, "max-full", envInt("RELAY_MAX_FULL", 10), "consecutive full batches a worker takes from one shard before moving on")
	flag.DurationVar(&cfg.refresh, "refresh", envDuration("RELAY_REFRESH", 5*time.Second), "how often to re-read the target's shard count from outbox_targets")
	flag.Parse()
	if cfg.topic == "" {
		cfg.topic = "outbox-" + cfg.target
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("cursorrelay: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	snk, err := sink.New(ctx, sink.Config{Kind: cfg.sink, Latency: cfg.sinkLatency, Project: cfg.project, Topic: cfg.topic, BatchSize: cfg.batch})
	if err != nil {
		return err
	}
	defer snk.Close()

	pc, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return err
	}
	// Describe once, then execute via the unnamed statement: no server-side prepared
	// statements, so this is safe behind PgBouncer in transaction mode.
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	pc.MaxConns = int32(cfg.workers)
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return err
	}
	defer pool.Close()

	r := &relay{cfg: cfg, pool: pool, sink: snk}
	if _, err := r.refreshShards(ctx); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // a grow via outbox_reshard is picked up here; no restart needed
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.refresh):
				if _, err := r.refreshShards(ctx); err != nil {
					log.Printf("refresh shards: %v (will retry)", err)
				}
			}
		}
	}()
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r.worker(ctx, w)
		}(w)
	}
	wg.Wait()
	return ctx.Err()
}

type relay struct {
	cfg   config
	pool  *pgxpool.Pool
	sink  sink.Sink
	plan  atomic.Pointer[shardPlan] // swapped whole when the shard count changes
	next  atomic.Int64              // round-robin shard dispenser shared by the workers
	total atomic.Int64
}

// shardPlan is everything that depends on the target's shard count: the count itself
// and the poll queries with the modulus inlined as a literal so the planner prunes to
// one leaf per day partition.
type shardPlan struct {
	shards      int
	previous    int // modulus older partitions may still carry; 0 when none
	catchupSQL  string
	frontierSQL string
}

// refreshShards reads the target's shard count, the modulus its older partitions may
// still carry, and whether it has its own partitions, from outbox_targets (the default
// count for targets that share the default child). On a change it creates any missing
// cursor rows, which outbox_reshard has already positioned, and swaps the plan. The
// predicate names both moduli while previous_shards is set, so polls prune to one leaf
// on partitions built with either; a grow is a multiple of the old count, so a key in
// shard s is in old shard s % old.
func (r *relay) refreshShards(ctx context.Context) (changed bool, err error) {
	var shards, previous int
	var listed bool
	err = r.pool.QueryRow(ctx, `SELECT coalesce(t.shards, outbox_default_shards()), coalesce(t.previous_shards, 0), t.shards IS NOT NULL
		FROM (SELECT 1) x LEFT JOIN outbox_targets t ON t.target = $1`, r.cfg.target).Scan(&shards, &previous, &listed)
	if err != nil {
		return false, fmt.Errorf("shard count: %w", err)
	}
	if previous == shards {
		previous = 0
	}
	if cur := r.plan.Load(); cur != nil && cur.shards == shards && cur.previous == previous {
		return false, nil
	}
	// Cursor rows start at frontier (0, 0). Idempotent, so every replica can run it.
	if _, err := r.pool.Exec(ctx, `INSERT INTO outbox_cursor (target, shard) SELECT $1, generate_series(0, $2 - 1) ON CONFLICT DO NOTHING`, r.cfg.target, shards); err != nil {
		return false, fmt.Errorf("init cursors: %w", err)
	}
	where := fmt.Sprintf("target = $1 AND outbox_shard(key, %d) = $2", shards)
	if previous > 0 {
		where = fmt.Sprintf("target = $1 AND outbox_shard(key, %d) = $2 %% %d AND outbox_shard(key, %d) = $2", previous, previous, shards)
	}
	r.plan.Store(&shardPlan{shards: shards, previous: previous,
		catchupSQL: fmt.Sprintf(catchupSQLTmpl, where), frontierSQL: fmt.Sprintf(frontierSQLTmpl, where)})
	log.Printf("relaying outbox_events target=%s topic=%s shards=%d previous=%d (own partitions: %v) workers=%d batch=%d", r.cfg.target, r.cfg.topic, shards, previous, listed, r.cfg.workers, r.cfg.batch)
	return true, nil
}

// worker polls shards until stopped. A full batch means the shard has a backlog, so
// the worker stays on it, up to -max-full batches so one hot shard cannot starve the
// rest on a small deployment; a partial batch moves on to the next shard immediately;
// an empty poll sleeps first. The sleep starts at -poll and doubles for every full
// sweep of the shards that found nothing, up to -poll-max, so an idle deployment costs
// workers/poll-max tiny transactions per second and new work is still noticed within
// about poll-max.
func (r *relay) worker(ctx context.Context, w int) {
	idle := 0 // consecutive empty polls
	for ctx.Err() == nil {
		plan := r.plan.Load()
		shard := int(r.next.Add(1)-1) % plan.shards
		fulls := 0
		for ctx.Err() == nil {
			n, full, err := r.pollOnce(ctx, plan, shard)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("worker %d shard %d: %v (will retry)", w, shard, err)
				sleep(ctx, r.cfg.poll)
				break
			}
			if n > 0 {
				idle = 0
			}
			if full {
				fulls++
				if fulls < r.cfg.maxFull {
					continue // keep draining this shard
				}
				break // hot shard: give the others a turn, come straight back
			}
			if n == 0 {
				idle++
				sleep(ctx, r.backoff(idle, plan.shards))
			}
			break
		}
	}
}

// backoff is the sleep after the idle-th consecutive empty poll: -poll until a whole
// sweep of the shards has been empty, then doubling per further sweep up to -poll-max.
func (r *relay) backoff(idle, shards int) time.Duration {
	d := r.cfg.poll
	for sweeps := idle / max(shards, 1); sweeps > 0 && d < r.cfg.pollMax; sweeps-- {
		d *= 2
	}
	return min(d, r.cfg.pollMax)
}

const (
	// One statement claims the cursor and captures the in-flight transaction list from
	// the same snapshot the rest of the poll will read under (repeatable read).
	claimSQL = `
		SELECT xid, id, pending, catchup_xid, catchup_id, array(SELECT pg_snapshot_xip(pg_current_snapshot()))
		FROM outbox_cursor
		WHERE target = $1 AND shard = $2
		FOR UPDATE SKIP LOCKED`

	// Rows of transactions that were pending and have since resolved, in (xid, id)
	// order and bounded by the batch size, resuming after the catch-up watermark for the
	// one transaction a previous poll left half-read. Aborted transactions are simply
	// not visible, so nothing special is needed for them. The WHERE clause (%s) comes
	// from the shard plan with the modulus inlined, so target + shard prune to one leaf
	// per day partition. There is no time bound: a
	// row is never skipped because it is old; partitions are dropped by
	// outbox_drop_partitions only once every cursor is past them.
	catchupSQLTmpl = `
		SELECT id, xid, key, payload
		FROM outbox_events
		WHERE %s
		  AND xid = ANY($3::xid8[])
		  AND (xid <> $4::xid8 OR id > $5::bigint)
		ORDER BY xid, id
		LIMIT $6`

	// Forward from the frontier. Only committed rows are visible; a row from an
	// in-flight transaction with a lower xid will be picked up by catch-up later.
	frontierSQLTmpl = `
		SELECT id, xid, key, payload
		FROM outbox_events
		WHERE %s
		  AND (xid, id) > ($3::xid8, $4::bigint)
		ORDER BY xid, id
		LIMIT $5`

	advanceSQL = `
		UPDATE outbox_cursor SET xid = $1, id = $2, pending = $3, catchup_xid = $4, catchup_id = $5, updated_at = now()
		WHERE target = $6 AND shard = $7`
)

type row struct {
	id      int64
	xid     uint64
	key     *string
	payload string
}

// pollOnce runs one poll transaction for a shard. It returns the number of messages
// published and whether the frontier read filled the batch, meaning there is more.
func (r *relay) pollOnce(ctx context.Context, plan *shardPlan, shard int) (published int, full bool, err error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(context.Background())

	var (
		curXid, cuXid uint64
		curID, cuID   int64
		pending       []uint64
		xip           []uint64
	)
	err = tx.QueryRow(ctx, claimSQL, r.cfg.target, shard).Scan(&curXid, &curID, &pending, &cuXid, &cuID, &xip)
	if errors.Is(err, pgx.ErrNoRows) || isSerializationFailure(err) {
		return 0, false, nil // another worker holds this shard
	}
	if err != nil {
		return 0, false, fmt.Errorf("claim: %w", err)
	}

	inFlight := make(map[uint64]struct{}, len(xip))
	for _, x := range xip {
		inFlight[x] = struct{}{}
	}
	var resolved, still []uint64
	for _, x := range pending {
		if _, ok := inFlight[x]; ok {
			still = append(still, x)
		} else {
			resolved = append(resolved, x)
		}
	}

	var msgs []*pubsub.Message
	catchup := 0
	newXid, newID := curXid, curID
	newCuXid, newCuID := uint64(0), int64(0)
	newPending := append([]uint64{}, still...) // never nil: nil would encode as SQL NULL
	if len(resolved) > 0 {
		crows, err := readRows(ctx, tx, plan.catchupSQL, r.cfg.target, shard, resolved, cuXid, cuID, r.cfg.batch)
		if err != nil {
			return 0, false, fmt.Errorf("catch-up: %w", err)
		}
		catchup = len(crows)
		for _, rw := range crows {
			msgs = append(msgs, r.toMessage(rw, shard, true))
		}
		if catchup == r.cfg.batch {
			// The batch may have cut a transaction short. Rows come in (xid, id) order,
			// so every resolved transaction below the last row's xid is exhausted; the
			// last one stays pending behind a watermark, and the ones above it wait.
			last := crows[len(crows)-1]
			newCuXid, newCuID = last.xid, last.id
			for _, x := range resolved {
				if x >= last.xid {
					newPending = append(newPending, x)
				}
			}
			full = true
		}
	}
	var rows []row
	if !full { // a full catch-up batch owns this poll; the frontier waits for the next
		rows, err = readRows(ctx, tx, plan.frontierSQL, r.cfg.target, shard, curXid, curID, r.cfg.batch)
		if err != nil {
			return 0, false, fmt.Errorf("frontier: %w", err)
		}
		for _, rw := range rows {
			msgs = append(msgs, r.toMessage(rw, shard, false))
		}
	}

	if len(rows) > 0 {
		last := rows[len(rows)-1]
		newXid, newID = last.xid, last.id
		// Transactions still running below the new frontier may yet commit rows behind
		// it; remember them. Below an unchanged frontier they are already in the set.
		for _, x := range xip {
			if x < newXid && !contains(still, x) {
				newPending = append(newPending, x)
			}
		}
	}
	if len(msgs) == 0 && len(resolved) == 0 {
		return 0, false, nil // nothing to publish and nothing to record: no write
	}

	if len(msgs) > 0 {
		if err := r.sink.PublishAll(ctx, msgs); err != nil {
			return 0, false, fmt.Errorf("publish: %w", err)
		}
	}
	// Everything is in Pub/Sub; now the cursor may move.
	if _, err := tx.Exec(ctx, advanceSQL, newXid, newID, newPending, newCuXid, newCuID, r.cfg.target, shard); err != nil {
		return 0, false, fmt.Errorf("advance: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("commit: %w", err)
	}
	if len(msgs) > 0 {
		total := r.total.Add(int64(len(msgs)))
		log.Printf("published %d (shard=%d catchup=%d pending=%d) total=%d", len(msgs), shard, catchup, len(newPending), total)
	}
	return len(msgs), full || len(rows) >= r.cfg.batch, nil
}

func readRows(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]row, error) {
	rs, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []row
	for rs.Next() {
		var rw row
		if err := rs.Scan(&rw.id, &rw.xid, &rw.key, &rw.payload); err != nil {
			return nil, err
		}
		out = append(out, rw)
	}
	return out, rs.Err()
}

func (r *relay) toMessage(rw row, shard int, catchup bool) *pubsub.Message {
	attrs := map[string]string{
		"outbox_id": fmt.Sprint(rw.id),
		"target":    r.cfg.target,
		"pg_xid":    fmt.Sprint(rw.xid),
		"shard":     fmt.Sprint(shard),
	}
	if catchup {
		attrs["catchup"] = "true"
	}
	m := &pubsub.Message{Data: []byte(rw.payload), Attributes: attrs}
	if rw.key != nil {
		m.OrderingKey = *rw.key
	}
	return m
}

func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

func contains(xs []uint64, x uint64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
