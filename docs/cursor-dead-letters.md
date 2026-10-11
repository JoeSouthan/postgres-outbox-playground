# Cursor transport: dead letters (design)

Status: designed, not built. This is the plan for requirement R9 in the
[README](../README.md#requirements): no event, payload or error may hold up publishing,
and anything that cannot be published is dead-lettered and trivially republished. Part
of the [cursor transport](cursor.md).

## Why the cursor needs it

A delete-based relay can set a row aside and carry on. The cursor relay's only durable
state is the frontier, which can only move past a row once that row is accounted for.
Today a publish error rolls the poll back, which is right for a broker outage and wrong
for a deterministic rejection: the same batch fails on every retry and that shard
stops. The design adds a second place a row can be accounted for, a table in the same
database written in the same transaction that advances the cursor.

Invariant: **every committed row at or below the frontier is in Pub/Sub or in
`outbox_dead_letters`, unless its xid is in `pending`.**

## What Pub/Sub returns

- **Validation is per request.** A schema violation fails the whole publish request
  with `INVALID_ARGUMENT`, and the client reports that error on every message in the
  bundle. One such error does not identify the bad message.
- **Ordering keys pause on failure.** After a failed publish the client refuses further
  publishes on that key until `ResumePublish`. Siblings fail with a "paused" error that
  is a consequence, not a verdict.
- **Codes fall into three classes.** Only one is a dead letter.

| class | gRPC codes | action |
|---|---|---|
| transient | `Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, `Internal`, `Aborted`, `Canceled`, `Unknown`, paused key | roll back, back off the shard |
| message-permanent | `InvalidArgument`, `OutOfRange` | isolate the culprit, dead-letter it |
| topic-permanent | `NotFound`, `PermissionDenied`, `Unauthenticated`, `FailedPrecondition` | roll back, log, exit after a grace period so the Deployment shows it |

The classification is a fast path. Correctness does not depend on it being complete;
the escalation rule below is the safety net.

## The poll

1. Claim the shard and read the batch, as now.
2. Publish the whole batch, bundled, and collect one outcome per message.
3. Any topic-permanent outcome: roll back and count the failure. After a few minutes of
   only topic-permanent failures, exit non-zero.
4. For each message-permanent outcome: resume its ordering key and publish it alone, in
   order, through a second publisher handle with bundling off. Each gets its own
   verdict: delivered, transient or permanent.
5. Any transient outcome: roll back, as now. Delivered messages are republished next
   time; consumers dedupe on `outbox_id`.
6. Otherwise insert the permanent ones into `outbox_dead_letters`, advance the cursor,
   commit.

Every poll ends in one of two states: nothing durable changed, or every row in the
batch is in Pub/Sub or the dead-letter table and the cursor is past all of them.

## Escalation: nothing in the data can stop a shard

> After a shard has failed *N* consecutive polls (`failures`, say 8) and there is
> evidence that publishing works, every message in the batch is published alone and
> every one that still fails is dead-lettered with whatever error it returned.

Evidence that publishing works is a successful publish: any cursor row in the database
that advanced in the last minute, or a delivered sibling in this poll's isolation pass.
A topic metadata call is not used, because it proves the admin path and not the publish
path, and a publish-only outage would pass it.

Outcomes:

- A deterministic failure with a transient-looking code, or a code not in the table,
  is retried *N* times, then isolated and dead-lettered.
- During a broker outage nothing advances and nothing is delivered, so nothing is
  dead-lettered; every shard backs off and resumes by itself.
- A broken schema rejects everything with `InvalidArgument`; the fast path dead-letters
  it at the normal rate, the backlog does not grow, and replay after the fix is one
  statement.
- The one case that holds: every target fails with a code outside the table and
  nothing is delivered anywhere, which is indistinguishable from an outage. The
  cursor-age alert surfaces it.

## Other stalls and their bounds

| stall | bound |
|---|---|
| Cursor row locked by a dead connection | `idle_in_transaction_session_timeout` on the relay role, publish deadline plus margin. |
| Backed-off shard | `retry_after` capped at one minute, set and compared with the database clock. |
| Hot shard holding a worker | Built: a worker leaves a shard after `-max-full` consecutive full batches. |
| Ordering key paused in the client | `ResumePublish` on every failed key; publisher handle recreated after *N* failures. |
| Rows ageing out of a read window | None exists. Reads have no time bound and retention drops a day only when every cursor is past it. |
| A transaction that never ends | Not a stall: only its xid stays in `pending`. The same `idle_in_transaction_session_timeout` is the fix. |
| Quarantined key | Deliberate and visible; the shard still flows. The default policy, skip, has no such state. |
| Topic missing or permission denied | The process exits and the Deployment goes red. The one case that waits for a person, with nothing lost. |
| Pool exhaustion | Queuing, not a stall; size the pool for replicas × workers. |

Only an unreachable or misconfigured broker can hold a shard, and that is a global,
visible condition that clears when the broker is back.

## The table

```sql
CREATE TABLE outbox_dead_letters (
  event_id     bigint PRIMARY KEY,
  target       text NOT NULL,
  shard        smallint NOT NULL,
  key          text,
  xid          xid8 NOT NULL,
  payload      text NOT NULL,     -- copied: the event's partition will be dropped
  attributes   jsonb NOT NULL,    -- exactly what would have been sent
  error_code   text NOT NULL,
  error        text NOT NULL,
  dead_at      timestamptz NOT NULL DEFAULT now(),
  replayed_at  timestamptz,
  replayed_as  bigint,            -- the outbox_events id the replay produced
  discarded_at timestamptz
);
CREATE INDEX outbox_dead_letters_open ON outbox_dead_letters (target, key)
  WHERE replayed_at IS NULL AND discarded_at IS NULL;
```

It is a separate table rather than flag columns on `outbox_events` because event
partitions are dropped on retention and an unhandled dead letter must outlive them. It
is written in the poll's own transaction, so it costs nothing on the success path. A
Pub/Sub dead-letter topic is not used: those cover subscriber failures, and a second
publish is neither atomic with the cursor advance nor guaranteed to succeed.

## Ordering policy, per target

- **Skip** (default). Dead-letter the message, publish the rest. Per-key order has a
  gap that the dead letter's `xid` and `event_id` record.
- **Quarantine.** Once a key has an open dead letter, every later message for it is
  dead-lettered too until the operator replays or discards. Per-key order survives;
  the set grows. One extra query per poll against the partial index. For consumers
  that are ordered state machines per key.

## Replay and discard

A flag column works for a delete-based relay because it scans for unflagged rows. A
cursor never looks behind its frontier, so a replay inserts a copy in front of it:

```sql
WITH r AS (
  INSERT INTO outbox_events (target, key, payload, replay_of)
  SELECT target, key, payload, event_id
  FROM outbox_dead_letters
  WHERE target = $1 AND replayed_at IS NULL AND discarded_at IS NULL
  RETURNING id, replay_of
)
UPDATE outbox_dead_letters d SET replayed_at = now(), replayed_as = r.id
FROM r WHERE d.event_id = r.replay_of;
```

The copy flows through the normal poll with a `replay_of` attribute from a nullable
column on `outbox_events`. A second rejection becomes a new dead letter linked by
`replayed_as`. Discard is `SET discarded_at = now()`. The relay never deletes from the
table.

## Per-shard backoff

Three columns on `outbox_cursor`:

```sql
ALTER TABLE outbox_cursor
  ADD COLUMN failures    int NOT NULL DEFAULT 0,
  ADD COLUMN retry_after timestamptz NOT NULL DEFAULT '-infinity',
  ADD COLUMN last_error  text;
```

After a transient or topic-permanent failure the relay rolls back, then in a separate
short transaction increments `failures`, sets `retry_after` to now plus an exponential
delay capped at a minute, and records `last_error`. The claim adds `AND retry_after <=
now()`, so a backed-off shard looks locked to every worker on every replica. A
successful advance resets all three. `failures` is also the escalation counter, and
`SELECT shard, failures, last_error FROM outbox_cursor WHERE failures > 0` is the
operator's view.

## Alerts

- Open dead letters per target.
- Cursors with `failures` above a threshold, or with a backlog and a stale `updated_at`.
- Oldest unpublished row age per target, also the autoscaling signal.
- Relay restarts, which is how topic-permanent errors surface.

## Build plan

1. `Sink.PublishAll` returns one outcome per message; the Pub/Sub sink performs the
   isolation pass internally.
2. The simulated sink gains a reject pattern (message-permanent) and a transient
   failure rate.
3. The producer gains `-poison N`.
4. The relay partitions outcomes, writes dead letters, uses the backoff columns and
   escalates.
5. Bench scenarios: `poison` (1 percent poisoned: exactly 99,000 published, 1,000 open
   dead letters, clean replay), `misclassified` (poison rejected with a transient code:
   same result via escalation), `outage` (everything fails: zero dead letters, full
   drain when the sink recovers).
