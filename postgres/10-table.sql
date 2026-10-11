-- Transport "table": the conventional outbox. A `target` column picks the destination;
-- a relay selects a batch FOR UPDATE SKIP LOCKED, publishes, deletes and commits.
CREATE TABLE outbox_items (
  id         bigserial PRIMARY KEY,
  target     text        NOT NULL,
  payload    text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_items_target_id ON outbox_items (target, id);

