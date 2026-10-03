# cap-event-handler

Transactional outbox/inbox messaging for Python services on PostgreSQL and Amazon SNS/SQS, or
NATS JetStream (optional `nats` extra).

A domain state change and its outgoing event commit atomically in one PostgreSQL transaction.
An incoming event's effect commits atomically with the record that it was handled, so handlers
are **deduplicated**, not merely retried.

```python
from cap_event_handler import CapApp, Context, SnsSqs

app = CapApp(engine=engine, transport=SnsSqs(region="ap-southeast-2"), group="billing-service")

class OrderPaid(BaseModel):
    order_id: UUID
    amount: Decimal

# publish — the outbox row joins your transaction
async with session.begin():
    session.add(order)
    await app.publish("order.paid", OrderPaid(order_id=order.id, amount=order.total),
                      session=session)

# consume — your writes and the "handled" marker commit together
@app.subscribe("order.paid", model=OrderPaid)
async def on_order_paid(ctx: Context[OrderPaid]) -> None:
    await ctx.session.execute(insert(invoices).values(order_id=ctx.payload.order_id))
```

Run it inside your app's lifespan, or as its own process:

```python
async with app.run():   # provisions AWS resources, creates tables, starts everything
    ...
```
```bash
cap-event-handler worker myservice:app
```

## What you get

| Boundary | Guarantee |
|---|---|
| Your business write ↔ the outgoing event | **Atomic** — one transaction |
| The outbox row ↔ SNS delivery | At-least-once, never lost — a sweeper recovers anything the sender missed |
| An inbound message ↔ your handler's writes | **Atomic** — dedup marker and effect commit together |
| Your handler's writes ↔ the SQS ack | A crash in between costs a redelivery, which deduplication absorbs |

Non-database side effects inside a handler (emails, third-party calls) remain at-least-once —
only the database takes part in the transaction.

Every row of that table is an executable test in
[`tests/test_guarantees.py`](tests/test_guarantees.py).

## Design lineage

Informed by [CAP](https://github.com/dotnetcore/CAP) (`DotNetCore.CAP`), but not a port and not
wire-compatible with it. Three deliberate departures, each argued in the
[design spec](docs/superpowers/specs/2026-09-17-cap-event-handler-design.md):

- **SQS owns redelivery.** No consume-side retry table or sweeper — visibility timeouts and the
  redrive policy already provide redelivery, backoff and poison-message quarantine (§5.1).
- **The inbox deduplicates.** Keyed `(msg_id, group_name)` and written inside the handler's own
  transaction, closing the gap CAP documents and declines to fix (§3.1).
- **The flush hooks `after_commit`.** No transaction wrapper to remember, so CAP's "committed the
  raw transaction and the send never fired" footgun cannot happen (§4.2).

Background: [`docs/reference/cap-atomicity-sqs-postgres.md`](docs/reference/cap-atomicity-sqs-postgres.md).

## Docs

- [Usage guide](docs/usage.md) — setup, migrations, operational settings, Terraform, what is *not* guaranteed
- [Design spec](docs/superpowers/specs/2026-09-17-cap-event-handler-design.md)

## Development

```bash
uv sync
uv run pytest                 # 162 tests, no Docker needed
uv run pytest -m integration  # 9 more, needs Docker (PostgreSQL + LocalStack)
./check.sh                    # format, lint, mypy --strict, tests
```

Unit and pipeline tests run against in-memory SQLite and a broker simulator, so the whole default
suite finishes in a couple of seconds. The integration suite covers what those cannot: real
`FOR UPDATE SKIP LOCKED`, real row-lock contention on the dedup insert, and real SNS→SQS delivery.
