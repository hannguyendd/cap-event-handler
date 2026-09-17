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
