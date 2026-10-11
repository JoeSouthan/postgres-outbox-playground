-- An empty publication: pgoutput requires one, but logical decoding messages are not
-- tied to any table, so we deliberately publish no tables. Only BEGIN / MESSAGE / COMMIT
-- records reach the relay.
CREATE PUBLICATION outbox_pub;

