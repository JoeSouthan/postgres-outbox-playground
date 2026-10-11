-- Transport "cursor": an append-only outbox read forward by per-shard cursors, no
-- logical decoding and no deletes. Each row records the top-level transaction id that
-- wrote it (pg_current_xact_id). The relay keeps one cursor per (target, shard); any
-- number of relay replicas race over the shard cursors with FOR UPDATE SKIP LOCKED, so
-- a shard, and therefore a key, has one publisher at a time.
--
-- Partition tree: day -> target -> shard.
--
--   outbox_events                          RANGE (created_at)
--   └── outbox_events_20261011             LIST (target)
--       ├── outbox_events_20261011_target_1   LIST (outbox_shard(key, 32)), 32 leaves, index (xid, id)
--       ├── outbox_events_20261011_target_2   LIST (outbox_shard(key, 4)),   4 leaves
--       └── outbox_events_20261011_default    everything not listed in outbox_targets,
--                                             index (target, outbox_shard(key, 32), xid, id)
--
-- A target listed in outbox_targets gets its own subtree per day with its own shard
-- count; every other target shares the day's default child with the default shard
-- count. Each poll prunes to one leaf per day, every leaf index is appended at its
-- right edge, and retention, partition period and shard count are per target.
-- outbox_create_partitions(day) builds a day; outbox_reshard grows a target's shard
-- count online; outbox_droppable_partitions lists days every cursor has passed.

-- The shard of a key for a given modulus. Immutable, so it can be a partition key and
-- an index expression; the relay uses the same function in its predicates.
CREATE FUNCTION outbox_shard(key text, n int) RETURNS int
  IMMUTABLE PARALLEL SAFE LANGUAGE sql
  RETURN (hashtext(coalesce(key, '')) & 2147483647) % n;

-- Shard count for targets that are not listed in outbox_targets.
CREATE FUNCTION outbox_default_shards() RETURNS int IMMUTABLE LANGUAGE sql RETURN 32;

-- Targets that get their own partition subtree, and how many shards each has. The
-- relay reads its shard count from here; a target that is not listed uses the default
-- child and outbox_default_shards().
CREATE TABLE outbox_targets (
  target     text PRIMARY KEY,
  shards     int  NOT NULL CHECK (shards > 0),
  created_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO outbox_targets (target, shards) VALUES ('target_1', 32), ('target_2', 4);

CREATE TABLE outbox_events (
  id         bigint      GENERATED ALWAYS AS IDENTITY,
  xid        xid8        NOT NULL DEFAULT pg_current_xact_id(),
  target     text        NOT NULL,
  key        text,
  payload    text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

-- Deliberately no top-level DEFAULT partition: a default partition forbids
-- DETACH PARTITION ... CONCURRENTLY, which is the only way to drop an old day without
-- an ACCESS EXCLUSIVE lock on outbox_events, and that lock queues behind every open
-- producer transaction. Days are created a week ahead instead; alert when fewer than
-- a few days ahead exist.

-- Identifier-safe form of a target name for partition names.
CREATE FUNCTION outbox_ident(target text) RETURNS text IMMUTABLE LANGUAGE sql
  RETURN regexp_replace(lower(target), '[^a-z0-9_]', '_', 'g');

-- One target's subtree under one day partition: a LIST(shard) parent with one leaf per
-- shard and an (xid, id) index that every leaf inherits.
CREATE FUNCTION outbox_create_target_partition(dayrel text, p_target text, p_shards int) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE tgtrel text := format('%s_%s', dayrel, outbox_ident(p_target)); s int;
BEGIN
  IF to_regclass(tgtrel) IS NOT NULL THEN RETURN; END IF;
  EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%L) PARTITION BY LIST (outbox_shard(key, %s))',
                 tgtrel, dayrel, p_target, p_shards);
  EXECUTE format('CREATE INDEX ON %I (xid, id)', tgtrel);
  FOR s IN 0 .. p_shards - 1 LOOP
    EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s)', format('%s_%s', tgtrel, s), tgtrel, s);
  END LOOP;
END $$;

-- Build (or complete) one day: the day partition, a subtree per listed target, and the
-- default child. Idempotent: run it daily for today + N, and after adding a target run
-- it for the future days only.
--
-- A new day is built standalone and attached in one step. CREATE TABLE ... PARTITION OF
-- takes an ACCESS EXCLUSIVE lock on the parent, which waits behind every open producer
-- transaction and blocks every new one while it waits; ATTACH PARTITION takes SHARE
-- UPDATE EXCLUSIVE, which conflicts with neither. The CHECK constraint lets the attach
-- skip its validation scan. Adding a target's subtree to an existing day locks that
-- day's parent the strong way, so only do it for days that have no traffic yet.
CREATE FUNCTION outbox_create_partitions(day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE dayrel text := format('outbox_events_%s', to_char(day, 'YYYYMMDD')); t record; fresh boolean := false;
BEGIN
  IF to_regclass(dayrel) IS NULL THEN
    fresh := true;
    EXECUTE format('CREATE TABLE %I (LIKE outbox_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS) PARTITION BY LIST (target)', dayrel);
    EXECUTE format('ALTER TABLE %I ADD CONSTRAINT day_bounds CHECK (created_at >= %L AND created_at < %L)', dayrel, day, day + 1);
  END IF;
  FOR t IN SELECT target, shards FROM outbox_targets ORDER BY target LOOP
    PERFORM outbox_create_target_partition(dayrel, t.target, t.shards);
  END LOOP;
  IF to_regclass(dayrel || '_default') IS NULL THEN
    EXECUTE format('CREATE TABLE %I PARTITION OF %I DEFAULT', dayrel || '_default', dayrel);
    EXECUTE format('CREATE INDEX ON %I (target, outbox_shard(key, %s), xid, id)', dayrel || '_default', outbox_default_shards());
  END IF;
  IF fresh THEN
    EXECUTE format('ALTER TABLE outbox_events ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)', dayrel, day, day + 1);
    EXECUTE format('ALTER TABLE %I DROP CONSTRAINT day_bounds', dayrel);
  END IF;
END $$;

-- Grow a target's shard count online, to a multiple of the current count. Each new
-- shard s + k*old covers a subset of old shard s's keys, so it inherits that cursor
-- exactly: nothing is republished and nothing is missed. Future day partitions that are
-- still empty are rebuilt with the new count; today's keeps the old modulus until it
-- ages out, during which polls on it are correct but not pruned to one leaf. Restart
-- the target's publishers afterwards so they poll the new shards.
CREATE FUNCTION outbox_reshard(p_target text, p_shards int) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE old int; r record; n bigint;
BEGIN
  SELECT shards INTO old FROM outbox_targets WHERE target = p_target FOR UPDATE;
  IF old IS NULL THEN RAISE EXCEPTION 'target % is not listed in outbox_targets', p_target; END IF;
  IF p_shards < old OR p_shards % old <> 0 THEN
    RAISE EXCEPTION 'shard count can only grow to a multiple of the current one (% -> %)', old, p_shards;
  END IF;
  PERFORM 1 FROM outbox_cursor WHERE target = p_target FOR UPDATE;      -- no poll in flight
  INSERT INTO outbox_cursor (target, shard, xid, id, pending, catchup_xid, catchup_id)
    SELECT target, shard + k * old, xid, id, pending, catchup_xid, catchup_id
    FROM outbox_cursor, generate_series(1, p_shards / old - 1) k
    WHERE target = p_target
  ON CONFLICT DO NOTHING;
  UPDATE outbox_targets SET shards = p_shards WHERE target = p_target;
  FOR r IN
    SELECT c.relname, to_date(substr(c.relname, 15, 8), 'YYYYMMDD') AS day
    FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = 'outbox_events'::regclass AND c.relname ~ '^outbox_events_\d{8}$'
      AND to_date(substr(c.relname, 15, 8), 'YYYYMMDD') > current_date
  LOOP
    EXECUTE format('SELECT count(*) FROM %I', format('%s_%s', r.relname, outbox_ident(p_target))) INTO n;
    IF n = 0 THEN
      EXECUTE format('DROP TABLE %I', format('%s_%s', r.relname, outbox_ident(p_target)));
      PERFORM outbox_create_target_partition(r.relname, p_target, p_shards);
    END IF;
  END LOOP;
END $$;

-- Retention, step one: which day partitions older than `keep` are safe to drop. A day
-- is safe when no cursor frontier is below its highest xid and no pending transaction
-- id is at or below it, so nothing in it is unpublished. Step two is done by the
-- retention job, not here, because it cannot run inside a transaction:
--
--   ALTER TABLE outbox_events DETACH PARTITION outbox_events_20261001 CONCURRENTLY;
--   DROP TABLE outbox_events_20261001;
--
-- Concurrent detach holds no lock that conflicts with producers or relays; a plain
-- DROP of an attached partition needs ACCESS EXCLUSIVE on outbox_events and queues
-- behind every open producer transaction.
CREATE FUNCTION outbox_droppable_partitions(keep interval) RETURNS setof text
LANGUAGE plpgsql AS $$
DECLARE r record; hi xid8; lo xid8;
BEGIN
  SELECT least(min(xid), min(p)) INTO lo FROM outbox_cursor LEFT JOIN LATERAL unnest(pending) p ON true;
  FOR r IN
    SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = 'outbox_events'::regclass AND c.relname ~ '^outbox_events_\d{8}$'
      AND to_date(substr(c.relname, 15, 8), 'YYYYMMDD') < (current_date - keep)::date
    ORDER BY 1
  LOOP
    EXECUTE format('SELECT max(xid) FROM %I', r.relname) INTO hi;
    IF hi IS NULL OR lo IS NULL OR hi < lo THEN RETURN NEXT r.relname; END IF;
  END LOOP;
END $$;

SELECT outbox_create_partitions(d::date) FROM generate_series(current_date - 1, current_date + 7, '1 day') d;

-- One row per (target, shard). (xid, id) is the frontier: the last row published.
-- pending holds transaction ids that were still in flight when the frontier passed
-- their position; the relay comes back for their rows once they resolve, which is what
-- lets it move past a long-running transaction instead of waiting for it. Catch-up is
-- read in batches too: (catchup_xid, catchup_id) is the last catch-up row published of
-- a pending transaction whose rows did not fit in one batch, so a bulk transaction
-- that was held open drains at the batch size like everything else. The row is updated
-- once per batch; the low fillfactor keeps that update HOT.
CREATE TABLE outbox_cursor (
  target      text        NOT NULL,
  shard       smallint    NOT NULL,
  xid         xid8        NOT NULL DEFAULT '0',
  id          bigint      NOT NULL DEFAULT 0,
  pending     xid8[]      NOT NULL DEFAULT '{}',
  catchup_xid xid8        NOT NULL DEFAULT '0',
  catchup_id  bigint      NOT NULL DEFAULT 0,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (target, shard)
) WITH (fillfactor = 50);
