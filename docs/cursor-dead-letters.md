# Cursor transport: dead letters and why it cannot get stuck

Part of the [cursor transport](cursor.md). This page answers requirement R9 in the
[README](../README.md#requirements): nothing a producer writes may hold up publishing,
and anything that cannot be published is dead-lettered and trivially republished. It is
a design; the prototype does not implement it yet, and the plan to do so is at the end.

The table relay could always deal with one bad row on its own: move it aside, delete
it, carry on. The cursor relay cannot. Its only durable state is the frontier, and the
frontier can only move past a row once the row is accounted for. In the prototype a publish error
of any kind rolls the poll back, which is exactly right for a broker outage and exactly
wrong for a deterministic rejection: the same batch fails on every retry, that shard
freezes, one thirty-second of the target's keys stop flowing, and after the lookback
window the stuck rows silently age out of every read. So the cursor needs a second
place a row can be accounted for. That place is a table in the same database, written
in the same transaction that advances the cursor.

The invariant becomes: *every committed row at or below the frontier is either in
Pub/Sub or in `outbox_dead_letters`, unless its transaction is in the pending set.*

## What Pub/Sub actually tells the relay

Three facts about the publish path shape the design.

- **Validation is per request, not per message.** The client gives one result per
  message, but the server validates a publish request as a whole. A schema violation
  fails the whole request with `INVALID_ARGUMENT`, and the client hands that same error
  to every message that was bundled into it. One `INVALID_ARGUMENT` on a message does
  not mean that message is bad; it means something in its bundle was. Dead-lettering
  on the first error would throw away up to a thousand good events.
- **Ordering keys pause on failure.** With ordering enabled the client bundles per key,
  and after any failure it refuses further publishes on that key until `ResumePublish`.
  Siblings of a failed message come back with a "publishing paused" error that is a
  consequence, not a verdict.
- **Codes sort into three classes**, and only one of them is a dead letter.

| Class | gRPC codes | Meaning | Action |
|---|---|---|---|
| Transient | `Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, `Internal`, `Aborted`, `Canceled`, `Unknown`, paused ordering key | Broker or network, try again | Roll the poll back, back off the shard |
| Message-permanent | `InvalidArgument`, `OutOfRange` | Schema violation, oversize payload, bad attribute | Isolate the culprit, dead-letter it |
| Topic-permanent | `NotFound`, `PermissionDenied`, `Unauthenticated`, `FailedPrecondition` | The topic or the identity is misconfigured | Roll back, log at error, exit after a grace period so the Deployment shows it |

A topic-permanent error is never a dead letter. Nothing is wrong with the events, and
quarantining a whole target because someone rotated a service account would be worse
than stalling.

The classification is a fast path, not the safety net. Correctness must not depend on
the table above being complete, because the one code it gets wrong is the one that
stalls a shard forever. The safety net is the escalation rule in *Nothing in the data
can stop a shard*, below.

## The poll with dead letters in it

1. Claim the shard and read the batch, unchanged.
2. **Pass one.** Publish the whole batch, bundled as now, and collect one outcome per
   message.
3. Any topic-permanent outcome: roll back, count the failure, and if the process has
   seen nothing but topic-permanent failures for a few minutes, exit non-zero.
4. **Isolation pass.** For every message whose outcome was message-permanent, resume its
   ordering key and publish it alone, in the original order, through a second publisher
   handle with bundling turned off. Each now gets its own verdict: delivered, transient,
   or permanent. The pass runs only on failure, so the bundled fast path is untouched.
5. Any transient outcome anywhere: roll back, as now. Delivered messages will be
   republished next time; consumers dedupe on `outbox_id`.
6. Otherwise, insert the permanent ones into `outbox_dead_letters`, update the cursor,
   commit.
7. **Escalation.** If this shard has now failed more than *N* consecutive polls and
   something has been published recently, the next poll runs the isolation pass on
   every message, and anything that fails alone is dead-lettered whatever its error
   code says. See the next section.

Every poll therefore ends in one of two states: nothing durable changed, or every row in
the batch is in Pub/Sub or in the dead-letter table and the cursor is past all of them.
There is no third state to reason about, and no in-memory retry bookkeeping that a
replica could lose.

## Nothing in the data can stop a shard

The requirement is that no event, no payload and no misclassified error can hold a
shard. That has to be true without trusting the code table above, so the rule is
mechanical and uses only state the database already holds:

> After a shard has failed *N* consecutive polls (the `failures` column, say *N* = 8,
> which with the backoff below is a few minutes), and there is evidence that publishing
> works, every message in the batch is published alone and every one that still fails
> is dead-lettered with whatever error it returned.

"Evidence that publishing works" is a successful publish, nothing weaker. The database
already holds it: any cursor row in the database that advanced in the last minute means
some relay just published through the same broker, and a delivered sibling in this
poll's own isolation pass means the same. Either one unlocks escalation. A metadata
call on the topic is deliberately *not* used as the guard. It proves the admin path,
credentials and topic existence, but not the publish permission or the publish backend,
so a publish-only outage would pass it and N polls later every shard would start
draining good events into the table.

If there is no evidence, nothing is dead-lettered. If there is, and a message still
cannot be published on its own after *N* attempts, the message is the problem by
elimination, and it goes to the table with its real error code attached so the triage
is honest about what happened.

This closes the two gaps the classification leaves:

- A deterministic failure that returns a code on the transient list, for example an
  `Internal` from a proxy that chokes on one payload, or a client-side serialisation
  error with no gRPC code at all. It is retried as transient *N* times, then isolated
  and dead-lettered.
- A code that is not in the table at all, from a client upgrade or a new server
  behaviour. Same path.

It also preserves the one behaviour that must not change: during a broker outage no
cursor advances and nothing is delivered, so nothing is dead-lettered, every shard backs
off, and publishing resumes by itself when the broker does. An outage is not a stuck
shard. It is a global condition that is visible on every cursor row and resolves
without an operator.

If the schema itself is broken so that every message on a target is rejected, the
rejections are `InvalidArgument`, the fast path dead-letters them without needing the
escalation guard, every shard drains into the table at the normal rate, the open-count
alert fires and the backlog does not grow. That is the correct outcome: the cursor
keeps moving, the events are safe, and the replay after the fix is one statement.

The one case that holds rather than dead-letters: every message on every target in the
database fails with a code outside the table, and nothing is delivered anywhere. That
is indistinguishable from a publish outage, so holding is the right call, and the
cursor-age alert is what surfaces it.

## Everything else that could stall, and the bound on each

With the data ruled out, these are the remaining ways a shard can fail to make
progress. Each has a bound that is set in configuration, not a human.

| Stall | Cause | Bound |
|---|---|---|
| Cursor row locked by a dead connection | Relay pod vanished mid-publish without closing the socket | `idle_in_transaction_session_timeout` on the relay role, set to the publish deadline plus margin; PgBouncer closes the server side when the client goes. Tens of seconds, never minutes. |
| Backed-off shard | `retry_after` set by a failing poll | Capped at one minute; compared and set with the database's `now()`, so replica clock skew cannot extend it. |
| Hot shard holding a worker | Full batch after full batch on one shard | A worker moves on after a fixed number of consecutive full batches, say ten, so one busy key range cannot starve the others on a small Deployment. |
| Ordering key paused in the client | Any failed publish on that key | `ResumePublish` on every failed key before the poll returns; after *N* failures the publisher handle is recreated. |
| Rows ageing out of a time window | A shard stalled longer than a read window | Removed. The relay's reads have no time bound, and retention drops a day only when `outbox_droppable_partitions` lists it: every cursor for every target is past its highest `xid` and no pending transaction id is at or below it. Nothing can age out unpublished. |
| Pending transaction that never ends | A session idle in transaction with no timeout | Not a stall: the frontier keeps moving and only that xid stays in the set. It is the same session that is holding back vacuum for the whole database, and the fix is the same `idle_in_transaction_session_timeout`. |
| Quarantined key | Operator chose quarantine for the target and has an open dead letter | Deliberate and loud: the shard still flows, only that key accumulates. Skip, the default, has no waiting-for-a-human state at all. |
| Topic missing or permission denied | Configuration | The process exits after a grace period and the Deployment goes red. This is the one case that waits for a person, and it waits visibly, with nothing lost and nothing dead-lettered. |
| Pool or pooler exhaustion | Replicas × workers above the PgBouncer pool | Queuing, not a stall; size the pool for the Deployment's maximum replicas. |

The guarantee that falls out: **only an unreachable or misconfigured broker can hold a
shard, and that is a global, visible condition that clears itself when the broker is
back.** Nothing a producer writes can.

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
  error_code   text NOT NULL,     -- the gRPC code
  error        text NOT NULL,
  dead_at      timestamptz NOT NULL DEFAULT now(),
  replayed_at  timestamptz,
  replayed_as  bigint,            -- the outbox_events id the replay produced
  discarded_at timestamptz
);
CREATE INDEX outbox_dead_letters_open ON outbox_dead_letters (target, key)
  WHERE replayed_at IS NULL AND discarded_at IS NULL;
```

This is a conventional outbox's dead-letter flag column moved off the row. It could be a pair
of columns on `outbox_events` instead, and the churn would not matter because dead
letters are rare; it is a separate table because `outbox_events` partitions are dropped
on retention and a dead letter nobody has looked at for eight days must not vanish with
its partition. The payload is copied for the same reason. The table is not partitioned
and carries no fillfactor tricks: it should be empty, and a non-empty open set is the
alert. The insert is one row per rejected event in the poll's own transaction, through
the same PgBouncer connection, so it costs nothing on the success path.

Why not a Pub/Sub dead-letter topic? Pub/Sub's own dead-letter topics are for
*subscriber* failures and do not apply to rejected publishes. Publishing the rejects to
a second, schema-less topic is possible, but that publish can also fail, and it is not
atomic with the cursor advance; the moment it is not atomic there is a window where a
row is neither published nor recorded. The table gives atomicity for free and gives the
operator SQL. Forwarding open dead letters to a topic for visibility can be a job that
reads the table; it should not be in the relay's commit path.

## Ordering

Dead-lettering the third message for key `K` and publishing the fourth and fifth breaks
per-key order for `K`. Two policies, chosen per target:

- **Skip** (default). Accept the gap. The dead letter records `xid` and `event_id`, so a
  replay knows where the message belonged, and the consumer's dedupe-by-id contract is
  unchanged. Right for the common case where events are self-contained facts.
- **Quarantine the key.** Once `K` has an open dead letter, every later message for `K`
  on that target is dead-lettered too, with error `quarantined`, until the operator
  replays or discards. Per-key order survives at the cost of a growing dead-letter set
  for that key. It needs one extra query per poll: the batch's keys against the partial
  index above, cheap because the open set is small. Right only when the consumer is an
  ordered state machine per key.

Start with skip. Quarantine is a per-target switch that can be added without touching
the cursor logic, because it only changes which messages go to which side of the commit.

## Replay and discard

A conventional outbox table republishes by flipping a dead-letter flag, which works
because that relay has no position: it scans for unflagged rows. A cursor relay reads forward
from a frontier, so a row behind the frontier stays invisible whatever its flags say,
and a replay has to put the event back in front of the cursor. The cheapest way is to
insert a copy, and it is still one statement:

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

The new row has a new id and xid and flows through the normal poll, with the normal
guarantees. If it is rejected again it becomes a new dead letter, and the `replayed_as`
chain shows the history. The nullable `replay_of bigint` column on `outbox_events` is
copied into a message attribute when set, so consumers can tell a replay from an
original. Add a `WHERE event_id = ANY($2)` to replay a chosen few.

Discard is `UPDATE ... SET discarded_at = now()` with the operator's reason appended to
`error`. Nothing is ever deleted from the table by the relay.

Re-insertion is chosen over "publish it from the dead-letter table" deliberately: it
keeps one publish path, one set of attributes, one place the sink is called, and it
means a replay after a schema fix behaves exactly like a fresh event, including the
dead-letter path if the fix was wrong.

## Transient failures: back off per shard, not per process

In the prototype a failed poll logs, sleeps one poll interval and moves on, and whichever worker
draws that shard next retries at once. During a broker outage that is every worker on
every replica re-reading a thousand rows and failing, continuously. Two columns on
`outbox_cursor` fix it and make shard health visible:

```sql
ALTER TABLE outbox_cursor
  ADD COLUMN failures    int NOT NULL DEFAULT 0,
  ADD COLUMN retry_after timestamptz NOT NULL DEFAULT '-infinity',
  ADD COLUMN last_error  text;
```

After a transient or topic-permanent failure the relay rolls the poll back and then, in
its own short transaction, bumps `failures`, sets `retry_after` to now plus an
exponential delay capped at a minute, and records `last_error`. The claim query adds
`AND retry_after <= now()`, so a backed-off shard looks to every worker on every replica
like a shard someone else holds. A successful advance resets all three. The backoff is
therefore shared by the whole Deployment rather than rediscovered by each replica, and
`failures` is also the counter that triggers the escalation rule above, so a shard that
keeps failing for a reason the code table does not recognise cannot do so for more than
*N* polls. The operator's view of a bad afternoon is one query:

```sql
SELECT shard, failures, last_error, now() - updated_at AS since_progress
FROM outbox_cursor WHERE target = 'payments' AND failures > 0;
```

## What to alert on

- Open dead letters per target greater than zero. A ticket for most targets, a page for
  the ones where quarantine is on.
- Any cursor row with `failures` above a small threshold, or with unpublished rows ahead
  of it and `updated_at` older than a few minutes. That is a stuck shard.
- Oldest unpublished row age per target, which is also the autoscaling signal.
- Relay process restarts, which is how topic-permanent errors surface.

## Prototype plan

The pieces needed to exercise all of this against the simulated sink, in order:

1. `Sink.PublishAll` returns one outcome per message instead of a single error, and the
   Pub/Sub sink does the isolation pass internally so the relay only ever sees
   delivered, transient or permanent.
2. The simulated sink grows two knobs: reject any message whose payload matches a
   pattern as message-permanent, and fail a configurable fraction of batches as
   transient.
3. The producer gains `-poison N` to mark every Nth payload.
4. The cursor relay partitions outcomes, writes dead letters, uses the backoff
   columns, escalates after *N* failures when another cursor has advanced recently or
   a sibling was delivered, and caps consecutive full batches per shard.
5. A `poison` bench scenario: a hundred thousand events with one percent poisoned must
   end with exactly 99,000 published, 1,000 open dead letters, no stuck shard, and a
   clean replay of the 1,000 once the reject pattern is removed.
6. A `misclassified` variant: the simulated sink rejects the poison with a code on the
   transient list. Same expected result, reached through escalation, within a bounded
   time.
7. An `outage` variant: the simulated sink fails everything. Expected result: zero
   dead letters, every shard backed off, full drain within seconds of the sink
   recovering.

