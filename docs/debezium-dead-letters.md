# Debezium transport: dead letters (design)

Status: designed, not built. This is the plan for requirement R9 in the
[README](../README.md#requirements) for the [`debezium` transport](debezium.md): no
event, payload or error may hold up publishing, and anything that cannot be published
is dead-lettered and trivially republished. It is the counterpart of the
[cursor dead-letter design](cursor-dead-letters.md) and reuses its error
classification; what differs is where the logic can live, because the publisher is a
product rather than code in this repository.

## How rejection works as built

Debezium Server has one failure path for its sink: a batch either succeeds entirely or
the engine stops.

1. The sink publishes a batch. The Pub/Sub sink waits up to
   `wait.message.delivery.timeout.ms` for every publish result; a retriable code is
   retried inside the client until `retry.total.timeout.ms`, a permanent code such as
   `InvalidArgument` fails at once. The HTTP sink retries a non-2xx response
   `retries` times.
2. Any failure makes the sink throw. The engine logs that it has failed and the process
   exits, with the offset of that batch uncommitted. The smoke test in this repository
   showed exactly this when the sink was unreachable: a batch of 168 events, five
   retries, then shutdown.
3. The container restarts, resumes at the last committed offset, and re-sends the whole
   batch. Messages the broker had already accepted are published again, the bad one
   fails again, and the process loops. Every target behind that slot stops.

Two properties look relevant and are not. `event.processing.failure.handling.mode`
(`fail`, `warn`, `skip`) applies to errors decoding an event from the log.
`errors.max.retries` applies to retriable connector errors such as a lost database
connection. Neither touches a sink failure, and Debezium Server has no dead-letter
queue. Kafka Connect's dead-letter topic exists only for sink connectors, so it does
not apply to Debezium as a source either.

With ordering keys on, a failed publish also pauses that key in the Pub/Sub client
until it is resumed, and the key's siblings in the same batch fail with a "paused"
error that is a consequence, not a verdict. A restart resets the client, which is why
the crash loop above does not wedge on the pause, only on the culprit.

Invariant to reach, as for the cursor: **every row the offset has passed is in Pub/Sub
or in `outbox_dead_letters`.**

## Options

Cheapest first. The first two apply to every strategy in this repository.

### 1. Make rejection impossible at insert

The only message-permanent rejection Pub/Sub issues is a schema violation. If the
payload is validated inside the producer's transaction against the same schema the
topic enforces, a row that could be rejected never exists:

```sql
CREATE EXTENSION pg_jsonschema;
ALTER TABLE outbox_events
  ALTER COLUMN payload TYPE jsonb USING payload::jsonb,
  ADD CONSTRAINT payload_matches CHECK (jsonb_matches_schema(outbox_schema(target), payload));
```

with `outbox_schema(target)` an immutable function returning the target's schema from
a small table. Drift between the two copies of the schema becomes a deploy-time check
rather than a runtime incident. It does not cover a topic that is deleted or a
permission that is revoked, which are topic-permanent and stop everything regardless of
strategy; those surface as process restarts and are a person's job.

### 2. Republish is an insert

The table is append-only and nothing consumes rows, so republishing any event, from a
dead-letter table or by hand, is inserting its target, key and payload again:

```sql
INSERT INTO outbox_events (target, key, payload)
SELECT target, key, payload FROM outbox_dead_letters WHERE event_id = $1;
```

The copy gets a new id and flows through the normal stream; consumers dedupe on the id
inside the payload, which is unchanged. No flag column, no cursor to move, no offset to
edit.

### 3. Skip by filter, manually

Debezium's filter transform drops records whose condition is false, so a poison row can
be bypassed with a condition on its id and a restart:

```properties
debezium.source.transforms=outbox,skip
debezium.source.transforms.skip.type=io.debezium.transforms.Filter
debezium.source.transforms.skip.language=jsr223.groovy
debezium.source.transforms.skip.condition=!(value.after.id in [1234L, 5678L])
```

It needs the scripting module and a JSR-223 engine added to the image, it is one config
change and one restart per incident, and the skipped row has to be recorded somewhere
by hand so it can be republished with option 2. Zero code, and acceptable for a
transport whose rejections option 1 has already made rare.

### 4. A custom sink with a dead-letter path

Debezium Server sinks are ordinary Java classes: a CDI bean implementing
`DebeziumEngine.ChangeConsumer`, named after a `debezium.sink.type` value, dropped into
the image's `lib` directory. One of a few hundred lines can run the cursor design's
poll inside Debezium's batch:

1. Publish the batch, bundled, and collect one outcome per message.
2. Any topic-permanent outcome: throw, so the engine stops and the process exits, as
   now. Nothing is marked processed.
3. For each message-permanent outcome: resume its ordering key and publish it alone
   through a second publisher handle with bundling off, so that each gets its own
   verdict.
4. Any transient outcome after retries: throw. The engine restarts the batch and
   consumers dedupe.
5. Otherwise insert the permanent ones into `outbox_dead_letters` over a JDBC
   connection, then mark every record processed and the batch finished, so the offset
   advances past all of them.

The classification table, the escalation rule (after *N* consecutive failures with
evidence that publishing works, dead-letter whatever fails alone regardless of code)
and the alerts are the cursor design's, unchanged. The dead-letter table is the cursor
design's without `shard` and `xid`, keyed by the event id Debezium carries in its
header.

The one thing this cannot match: the cursor writes the dead letter and advances in a
single database transaction, so the two can never disagree. Here the dead-letter insert
commits first and the offset follows; a crash between them replays the batch, publishes
the good rows again and inserts the dead letter again. The insert is therefore an
upsert on `event_id`, and the invariant holds at-least-once, which is what the rest of
the strategy already promises.

A Pub/Sub dead-letter topic in place of the table is tempting because it needs no
database connection from the sink, but a second publish can fail for the same reasons
as the first and is not where an operator looks. The table is.

### 5. Through Kafka

Debezium into Kafka, then a Kafka-to-Pub/Sub sink connector, which does have Kafka
Connect's `errors.tolerance=all` and `errors.deadletterqueue.topic.name`. Correct and
prebuilt, with ordering preserved per partition, at the cost of a Kafka cluster heavier
than everything else in this repository together. Listed for completeness.

## Comparison with the cursor design

| | cursor | debezium |
|---|---|---|
| where the logic lives | the relay in this repository | a custom sink class loaded into Debezium Server, or nowhere (options 1 to 3) |
| atomic with progress | yes, one transaction | no, dead letter then offset; upsert makes it idempotent |
| per-shard backoff | yes, other shards keep flowing | no shards; a transient failure holds the whole slot, which matches `wal` and `journal` |
| topic-permanent errors | process exits | process exits |
| republish | insert in front of the cursor | insert |
| operator's view | `outbox_dead_letters`, cursor `failures` | `outbox_dead_letters`, process restarts, slot lag |

## Build plan

1. Option 1 in the schema, with the producer's payload validated at insert and the
   benchmark's `-poison N` producer flag generating rows that fail the topic schema but
   pass the table's, to exercise the rest.
2. The custom sink, built against the simulated HTTP counter first with a reject
   pattern and a transient failure rate, then against Pub/Sub.
3. Bench scenarios as for the cursor: `poison`, `misclassified`, `outage`.
