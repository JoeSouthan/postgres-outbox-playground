-- Observability for the benchmark: per-statement costs and physical heap/index state.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- Demo business table written by the producer.
CREATE TABLE payments (
  id           bigserial PRIMARY KEY,
  reference    text        NOT NULL,
  amount_cents integer     NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

