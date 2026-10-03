# Usage

## 1. Install and create the tables

```bash
uv add cap-event-handler
```

Two tables live in a schema of their own, `cap` by default. Either let the app create them:

```python
app = CapApp(engine=engine, transport=SnsSqs(region="ap-southeast-2"), group="billing-service")
await app.initialize()          # CREATE TABLE IF NOT EXISTS; app.run() does this for you
```

…or manage them with Alembic, pointing autogenerate at the library's metadata:

```python
# alembic/env.py
from cap_event_handler.storage.schema import build_tables

cap_tables = build_tables(schema="cap", prefix="cap_")
target_metadata = [my_metadata, cap_tables.metadata]
```

Then set `Settings(auto_create_tables=False)` so the app never issues DDL of its own.

**The tables must live in the same PostgreSQL database as your business tables.** Atomicity comes
entirely from sharing one connection and one transaction; a separate database silently degrades
the whole thing to "commit, then hope".

## 2. Publishing inside a transaction

```python
async with session.begin():
    session.add(order)
    await app.publish("order.paid", OrderPaid(order_id=order.id), session=session)
# ← COMMIT: the order and the event are now both durable, or neither is
```

`publish()` awaits no network I/O. It inserts a row on the session you hand it and notes the id;
the send happens after your transaction commits. Roll back and there is no row and no event.

You do not have to commit through any wrapper. The library listens for `after_commit` on
SQLAlchemy's `Session` class, so *any* commit of a session that published will flush — including
`await session.commit()` on a raw session.

Publishing with no session opens its own short transaction:

```python
await app.publish("user.signed_up", UserSignedUp(user_id=user_id))
```

Same delivery guarantee, but nothing to be atomic with.

## 3. Subscribing

```python
@app.subscribe("order.paid", model=OrderPaid)
async def on_order_paid(ctx: Context[OrderPaid]) -> None:
    await ctx.session.execute(insert(invoices).values(order_id=ctx.payload.order_id))
```

`ctx` carries `payload`, `headers`, `msg_id`, `name`, `group`, `retries`, `sent_time`,
`correlation_id` and `session`.

**`ctx.session` belongs to the library.** Its transaction already holds the row that marks this
message handled, and it commits when your handler returns — which is what makes your writes and
that marker atomic. Do not commit or close it; the library raises `HandlerTransactionError` if you
do. If your handler raises, everything rolls back together and SQS redelivers.

`ctx.retries` is the broker's `ApproximateReceiveCount`, so `1` on first delivery.

## 4. Publishing from inside a handler

```python
@app.subscribe("order.paid", model=OrderPaid)
async def on_order_paid(ctx: Context[OrderPaid]) -> None:
    await ctx.session.execute(insert(invoices).values(order_id=ctx.payload.order_id))
    await app.publish("invoice.issued", InvoiceIssued(...), session=ctx.session)
```

Passing `ctx.session` puts the incoming message's marker, your writes and the outgoing event in
one transaction. The correlation id propagates automatically through a contextvar, so the whole
chain shares one `cap-corr-id`. Pass `correlation_id=` to override.

## 5. Operational settings that actually matter

`app.run()` inspects your queue at startup and warns about these. Read `app.warnings` or watch the
log.

**`inbox_retention` must exceed the queue's `MessageRetentionPeriod`.** Default 15 days against
SQS's 14-day maximum. The dedup marker has to outlive the longest window in which a redelivery can
still arrive. Shorten it and a message sitting in a DLQ for a week comes back and is handled twice.

**The queue's `VisibilityTimeout` must exceed your slowest handler.** Otherwise SQS redelivers
while the first invocation is still running. That is wasted work, not corruption — the second
attempt blocks on the first's uncommitted marker row and exits as a duplicate — but set
`expected_max_handler_seconds` so the warning tells you.

**Give the queue a redrive policy.** The library does not create one, because `maxReceiveCount` and
DLQ retention are your decisions. Without it a message that always fails retries forever instead of
being quarantined.

**`sweeper_lookback` must exceed `publish_timeout`.** This one is fatal at construction, not a
warning: a shorter lookback means the sweeper re-sends messages that are still legitimately in
flight.

## 6. Terraform-managed AWS

```python
app = CapApp(
    engine=engine,
    transport=SnsSqs(region="ap-southeast-2", provision=False),
    group="billing-service",
    settings=Settings(provision=False),
)
```

Nothing is created, subscribed or reconfigured. The library resolves topic ARNs and the queue URL
by name and fails at startup naming anything missing — including a handler whose topic your queue
is not subscribed to, which would otherwise be a handler that can never fire.

## 7. Running it

Inside a FastAPI lifespan:

```python
@asynccontextmanager
async def lifespan(_: FastAPI):
    async with app.run():
        yield
```

Or as its own process:

```bash
cap-event-handler worker myservice:app
```

Either way, shutdown stops polling, lets in-flight handlers finish or `shutdown_timeout` elapse,
then cancels the background tasks.

## 8. What is not guaranteed

**Non-database side effects are at-least-once.** Only PostgreSQL participates in the handler's
transaction. If your handler sends an email and then the process dies before the SQS ack, the
redelivery is deduplicated — but the email was already sent, and the dedup row was rolled back
with everything else. Where a side effect must not repeat, make it idempotent or record it in the
same transaction and check before acting.

**Ordering is not guaranteed.** Standard SQS queues are unordered, the sweeper re-sends out of
order, and `group_concurrency > 1` interleaves handlers. Design events so order does not matter, or
use a conditional update that is safe in any order.

**The outbox is a retry journal, not an audit log.** Succeeded rows are deleted after
`outbox_retention` (1 day by default).

## 9. NATS JetStream instead of SNS/SQS

Each app uses one transport. To use JetStream, install the extra and pass a
`NatsJetStreamTransport`:

```bash
uv add "cap-event-handler[nats]"
```

```python
from cap_event_handler import CapApp, NatsConfig
from cap_event_handler.transport.nats import NatsJetStreamTransport

app = CapApp(
    engine=engine,
    transport=NatsJetStreamTransport(NatsConfig(servers="nats://nats:4222")),
    group="billing-service",
)
```

Requires **NATS server 2.10 or later**, which supports consumers that filter on several subjects.

**Layout.** One stream (`stream`, default `CAP`) captures every event under
`<subject_prefix>.>` (default `cap.>`). Event `order.paid` is published on `cap.order-paid`.
Each group gets one durable pull consumer, named after the group, filtered to the events it
handles. Dead letters go to a second stream (`dlq_stream`, default `CAP_DLQ`) on
`<dlq_subject_prefix>.<group>` (default `cap-dlq.<group>`).

**Settings that matter** (`NatsConfig`):

| Setting | Default | Why it matters |
|---|---|---|
| `ack_wait` | 60s | Must exceed your slowest handler, like SQS `VisibilityTimeout`. Startup warns otherwise. |
| `max_deliver` | 5 | On this delivery a still-failing message is dead-lettered. At least 2. The JetStream consumer itself is created with unlimited deliveries, so the library, not NATS, decides when to give up. |
| `max_age` | 7 days | Stream retention. Keep it below `Settings.inbox_retention` (15 days); startup warns otherwise. Zero means unlimited. |
| `duplicate_window` | 5 min | JetStream drops a re-sent message with the same id inside this window. That's an optimisation; the inbox catches the rest. |
| `num_replicas` | 1 | Stream replicas on a clustered server. |

These limits apply to **every** event, because there is one stream. If two kinds of event need
different retention, run them in separate apps with different `stream` names.

**Provisioning.** With `provision=True` (the default), missing streams are created with file
storage, and each group's consumer is created or updated. **Existing streams are never
modified.** If your stream does not capture `<subject_prefix>.>`, startup fails rather than
publishing into the void. With `NatsJetStreamTransport(config, provision=False)` and
`Settings(provision=False)`, nothing is created or updated, consumers included. A missing main
stream fails startup; a missing dead-letter stream triggers a warning. If you create the consumer
yourself, give it `max_deliver` of `-1` or above `NatsConfig.max_deliver`. A consumer that stops
at or before it lets JetStream drop a message whose final delivery crashed, and startup warns.

**Dead letters.** JetStream has no redrive policy, so the library supplies one. When a message
fails on its `max_deliver`-th delivery, the library copies it to the dead-letter stream, then acks
it. The same happens without running the handler again when a later delivery arrives, which means
the final delivery ended without an outcome: the process crashed or was killed, or the handler
outran `ack_wait`. The copy carries these headers:

- `cap-dlq-reason`: why it failed;
- `cap-dlq-group`;
- `cap-dlq-deliveries`.

Both this and a failed dead-letter publish are logged at `ERROR`. After a failed dead-letter
publish the message is nak'd, and dead-lettering is retried on its next delivery. Inspect dead
letters with the NATS CLI:

```bash
nats stream view CAP_DLQ
nats stream get CAP_DLQ --last-for "cap-dlq.billing-service"
```

To replay one, republish its body and headers to the original subject (`cap.<event>`) without the
`cap-dlq-*` and `Nats-*` headers. The subject fans out to **every** group subscribed to the event,
not just the one that dead-lettered it. The failing group never committed, so its handler runs
again. The other groups skip it as a duplicate only while their inbox markers last
(`inbox_retention`, 15 days by default). Replay an older dead letter and their handlers run again
too. A body that was not valid UTF-8 is stored with replacement characters, so its original bytes
are lost.

Why this differs from .NET CAP's NATS transport: `docs/cap-porting-issues.md`, rows N1–N6.
