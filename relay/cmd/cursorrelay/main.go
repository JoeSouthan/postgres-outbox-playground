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

	// The shard count is the target's partition modulus, from outbox_targets, or the
	// default for targets that share the default child. It is inlined into the poll
	// queries as a literal so the planner can prune to one leaf per day partition.
	var shards int
	var listed bool
	if err := pool.QueryRow(ctx, `SELECT coalesce(t.shards, outbox_default_shards()), t.shards IS NOT NULL FROM (SELECT 1) x LEFT JOIN outbox_targets t ON t.target = $1`, cfg.target).Scan(&shards, &listed); err != nil {
		return fmt.Errorf("shard count: %w", err)
	}
	// Cursor rows start at frontier (0, 0). Idempotent, so every replica can run it.
	if _, err := pool.Exec(ctx, `INSERT INTO outbox_cursor (target, shard) SELECT $1, generate_series(0, $2 - 1) ON CONFLICT DO NOTHING`, cfg.target, shards); err != nil {
		return fmt.Errorf("init cursors: %w", err)
	}
	log.Printf("relaying outbox_events target=%s topic=%s shards=%d (own partitions: %v) workers=%d batch=%d", cfg.target, cfg.topic, shards, listed, cfg.workers, cfg.batch)

	r := &relay{cfg: cfg, pool: pool, sink: snk, shards: shards,
		catchupSQL:  fmt.Sprintf(catchupSQLTmpl, shards),
		frontierSQL: fmt.Sprintf(frontierSQLTmpl, shards)}
	var wg sync.WaitGroup
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
	cfg         config
	pool        *pgxpool.Pool
	sink        sink.Sink
	shards      int
	catchupSQL  string // catchupSQLTmpl with this target's modulus inlined
	frontierSQL string
	next        atomic.Int64 // round-robin shard dispenser shared by the workers
	total       atomic.Int64
}

// worker polls shards until stopped. A full batch means the shard has a backlog, so
// the worker stays on it; a partial batch moves on to the next shard immediately; an
// empty poll sleeps for the poll interval first. At idle the deployment therefore costs
// workers/poll tiny transactions per second, and new work on any shard is noticed
// within about shards/workers poll intervals.
func (r *relay) worker(ctx context.Context, w int) {
	for ctx.Err() == nil {
		shard := int(r.next.Add(1)-1) % r.shards
		for ctx.Err() == nil {
			n, full, err := r.pollOnce(ctx, shard)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("worker %d shard %d: %v (will retry)", w, shard, err)
				sleep(ctx, r.cfg.poll)
				break
			}
			if full {
				continue // keep draining this shard
			}
			if n == 0 {
				sleep(ctx, r.cfg.poll)
			}
			break
		}
	}
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
	// not visible, so nothing special is needed for them. The modulus is a literal (%d)
	// so target + shard prune to one leaf per day partition. There is no time bound: a
	// row is never skipped because it is old; partitions are dropped by
	// outbox_drop_partitions only once every cursor is past them.
	catchupSQLTmpl = `
		SELECT id, xid, key, payload
		FROM outbox_events
		WHERE target = $1 AND outbox_shard(key, %d) = $2
		  AND xid = ANY($3::xid8[])
		  AND (xid <> $4::xid8 OR id > $5::bigint)
		ORDER BY xid, id
		LIMIT $6`

	// Forward from the frontier. Only committed rows are visible; a row from an
	// in-flight transaction with a lower xid will be picked up by catch-up later.
	frontierSQLTmpl = `
		SELECT id, xid, key, payload
		FROM outbox_events
		WHERE target = $1 AND outbox_shard(key, %d) = $2
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
func (r *relay) pollOnce(ctx context.Context, shard int) (published int, full bool, err error) {
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
		crows, err := readRows(ctx, tx, r.catchupSQL, r.cfg.target, shard, resolved, cuXid, cuID, r.cfg.batch)
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
		rows, err = readRows(ctx, tx, r.frontierSQL, r.cfg.target, shard, curXid, curID, r.cfg.batch)
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
