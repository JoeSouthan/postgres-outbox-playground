-- Transport "debezium": an append-only outbox table that nothing ever reads. Producers
-- insert; Debezium Server decodes the inserts from the write-ahead log through a
-- pgoutput publication, routes each row to its target's topic with the Outbox Event
-- Router, and publishes. No relay query, no cursor, no index, no deletes: the table is
-- a heap written once, partitioned by day, and old days are dropped once the slot has
-- moved past them.

CREATE TABLE outbox_events (
  id         bigint      GENERATED ALWAYS AS IDENTITY,
  target     text        NOT NULL,
  key        text,
  payload    text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

-- Inserts only, and published as changes to the root table so the connector sees one
-- table no matter how many partitions exist or how often they are rotated.
CREATE PUBLICATION outbox_pub FOR TABLE outbox_events
  WITH (publish = 'insert', publish_via_partition_root = true);

-- Build one day. Created standalone and attached, as in the cursor transport: CREATE
-- TABLE ... PARTITION OF takes ACCESS EXCLUSIVE on the parent and queues behind every
-- open producer transaction; ATTACH takes SHARE UPDATE EXCLUSIVE and does not. No
-- default partition, so that old days can be detached CONCURRENTLY.
CREATE FUNCTION outbox_create_partitions(day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE dayrel text := format('outbox_events_%s', to_char(day, 'YYYYMMDD'));
BEGIN
  IF to_regclass(dayrel) IS NOT NULL THEN RETURN; END IF;
  EXECUTE format('CREATE TABLE %I (LIKE outbox_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', dayrel);
  EXECUTE format('ALTER TABLE %I ADD CONSTRAINT day_bounds CHECK (created_at >= %L AND created_at < %L)', dayrel, day, day + 1);
  EXECUTE format('ALTER TABLE outbox_events ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)', dayrel, day, day + 1);
  EXECUTE format('ALTER TABLE %I DROP CONSTRAINT day_bounds', dayrel);
END $$;

-- Retention guard. The connector's position is a log position, not a row id, so a day
-- is safe to drop once the slot has confirmed past every insert into it. The partition
-- job records the log position some time after the day has ended (longer than the
-- longest producer transaction, so that every insert with a created_at in that day
-- has committed), and outbox_droppable_partitions compares it with the slot.
CREATE TABLE outbox_days (
  day       date   PRIMARY KEY,
  end_lsn   pg_lsn NOT NULL
);

CREATE FUNCTION outbox_close_day(day date) RETURNS void LANGUAGE sql AS $$
  INSERT INTO outbox_days (day, end_lsn) VALUES (day, pg_current_wal_lsn()) ON CONFLICT DO NOTHING;
$$;

-- Days older than `keep` whose recorded end position the slot has confirmed. The job
-- then runs, outside a transaction:
--   ALTER TABLE outbox_events DETACH PARTITION outbox_events_20261001 CONCURRENTLY;
--   DROP TABLE outbox_events_20261001;
CREATE FUNCTION outbox_droppable_partitions(keep interval, slot text DEFAULT 'outbox_debezium') RETURNS setof text
LANGUAGE sql AS $$
  SELECT c.relname
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN outbox_days d ON d.day = to_date(substr(c.relname, 15, 8), 'YYYYMMDD')
  JOIN pg_replication_slots s ON s.slot_name = slot
  WHERE i.inhparent = 'outbox_events'::regclass
    AND c.relname ~ '^outbox_events_\d{8}$'
    AND d.day < (current_date - keep)::date
    AND s.confirmed_flush_lsn >= d.end_lsn
  ORDER BY 1;
$$;

SELECT outbox_create_partitions(d::date) FROM generate_series(current_date - 1, current_date + 7, '1 day') d;
