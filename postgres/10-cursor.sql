-- Transport "cursor": an append-only outbox read forward by a cursor, no logical
-- decoding and no deletes. Each row records the top-level transaction id that wrote it
-- (pg_current_xact_id) and a shard derived from the ordering key. The relay keeps one
-- cursor per (target, shard); any number of relay replicas race over the shard cursors
-- with FOR UPDATE SKIP LOCKED, so a shard, and therefore a key, has one publisher at a
-- time. Partitioned by day like the journal: old partitions are dropped, never deleted.
-- The shard modulus is fixed here; changing it is a drain-and-remap, see DESIGN.md.
CREATE TABLE outbox_events (
  id         bigint      GENERATED ALWAYS AS IDENTITY,
  xid        xid8        NOT NULL DEFAULT pg_current_xact_id(),
  target     text        NOT NULL,
  key        text,
  shard      smallint    NOT NULL GENERATED ALWAYS AS ((hashtext(coalesce(key, '')) & 2147483647) % 32) STORED,
  payload    text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);
-- The only index the relay uses: every poll is one range probe per live partition.
CREATE INDEX outbox_events_target_shard_xid_id ON outbox_events (target, shard, xid, id);

DO $$
DECLARE d date;
BEGIN
  FOR d IN SELECT generate_series(current_date - 1, current_date + 7, '1 day')::date LOOP
    EXECUTE format('CREATE TABLE outbox_events_%s PARTITION OF outbox_events FOR VALUES FROM (%L) TO (%L)',
                   to_char(d, 'YYYYMMDD'), d, d + 1);
  END LOOP;
END $$;
CREATE TABLE outbox_events_default PARTITION OF outbox_events DEFAULT;

-- One row per (target, shard). (xid, id) is the frontier: the last row published.
-- pending holds transaction ids that were still in flight when the frontier passed
-- their position; the relay comes back for their rows once they resolve, which is what
-- lets it move past a long-running transaction instead of waiting for it. The row is
-- updated once per batch; the low fillfactor keeps that update HOT.
CREATE TABLE outbox_cursor (
  target     text        NOT NULL,
  shard      smallint    NOT NULL,
  xid        xid8        NOT NULL DEFAULT '0',
  id         bigint      NOT NULL DEFAULT 0,
  pending    xid8[]      NOT NULL DEFAULT '{}',
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (target, shard)
) WITH (fillfactor = 50);
