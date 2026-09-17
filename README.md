# cap-event-handler

Transactional outbox/inbox messaging for Python services on PostgreSQL and Amazon SNS/SQS.

A domain state change and its outgoing event commit atomically in one PostgreSQL transaction.
An incoming event's effect commits atomically with the record that it was handled, so handlers
are deduplicated rather than merely retried.

> **Status: design approved, not yet implemented.** The design lives in
> [`docs/superpowers/specs/2026-09-17-cap-event-handler-design.md`](docs/superpowers/specs/2026-09-17-cap-event-handler-design.md).

## What it looks like

```python
from cap_event_handler import CapApp, Context, SnsSqs

app = CapApp(engine=engine, transport=SnsSqs(region="ap-southeast-2"), group="billing-service")

# publish — the outbox row joins your transaction
async with session.begin():
    session.add(order)
    await app.publish("order.paid", OrderPaid(order_id=order.id), session=session)

# consume — your writes and the "handled" marker commit together
@app.subscribe("order.paid", model=OrderPaid)
async def on_order_paid(ctx: Context[OrderPaid]) -> None:
    ctx.session.add(Invoice(order_id=ctx.payload.order_id))
```

## Design lineage

The design is informed by [CAP](https://github.com/dotnetcore/CAP) (`DotNetCore.CAP`) but is not
a port and does not interoperate with it. It departs from CAP in three places, each argued in the
spec:

- **SQS owns redelivery.** No consume-side retry table or sweeper — visibility timeouts and the
  redrive policy already provide redelivery, backoff and poison-message quarantine (§5.1).
- **The inbox deduplicates.** Keyed `(msg_id, group_name)` and written inside the handler's own
  transaction, closing the gap CAP documents and declines to fix (§3.1).
- **Flush hooks `after_commit`.** No transaction wrapper to remember, so CAP's "committed the raw
  transaction and the send never fired" footgun cannot happen (§4.2).

Background reading: [`docs/reference/cap-atomicity-sqs-postgres.md`](docs/reference/cap-atomicity-sqs-postgres.md).

## Development

```bash
uv sync                      # install, including dev group
uv run pytest                # unit + pipeline tests, no infrastructure needed
uv run pytest -m integration # requires Docker (PostgreSQL + LocalStack)
uv run ruff check .
uv run ruff format .
uv run mypy src
```
