# cap-event-handler — Transactional Outbox/Inbox for Python, PostgreSQL and Amazon SNS/SQS

**Status:** Approved design, pending implementation plan
**Date:** 2026-09-17
**Distribution:** `cap-event-handler`   **Import:** `cap_event_handler`

## 1. Purpose

A Python library providing reliable messaging between services via the transactional outbox
pattern: a domain state change and its outgoing event commit atomically in one PostgreSQL
transaction, and an incoming event's effect commits atomically with the record that it was
handled.

The design is informed by [CAP](https://github.com/dotnetcore/CAP) (`DotNetCore.CAP`), whose
PostgreSQL + Amazon SQS behaviour is analysed in `docs/reference/cap-atomicity-sqs-postgres.md`,
copied into this repository from the CAP checkout it was written against. It is **not** a port. The library is standalone — it does not interoperate
with .NET CAP services on the wire or in the database — which frees it to close two gaps CAP
documents and declines to fix:

- CAP's consume side is at-least-once with no deduplication
  (`docs/content/user-guide/en/cap/idempotence.md`, and §3.2 of the atomicity doc). Its inbox
  assigns a fresh snowflake id per delivery, so a redelivered message re-invokes the handler.
- CAP's handler write and its "this message is done" marker commit on separate connections
  (§3, "The status update is not in your transaction"), leaving an unprotected gap.

### 1.1 Scope

In scope for v1: the publish pipeline (outbox), the consume pipeline (inbox), the background
processors that make the publish side durable, PostgreSQL storage via SQLAlchemy 2.x async +
asyncpg, and SNS fan-out → SQS-per-consumer-group transport.

Out of scope for v1: everything in §11.

### 1.2 Guarantees

| Boundary | Guarantee | Mechanism |
|---|---|---|
| Business write ↔ outbox row | **Atomic** | INSERT runs on the caller's `AsyncSession` |
| Outbox row ↔ SNS delivery | Eventual, at-least-once | Post-commit flush + `OutboxSweeper` |
| Inbound message ↔ handler's DB writes | **Atomic** | Dedup INSERT and handler writes share one transaction |
| Handler's DB writes ↔ ack | Eventual | `DeleteMessage` after COMMIT; crash in between ⇒ redelivery, absorbed by dedup |

In one line: **atomic on both sides for database work; at-least-once for non-database side
effects inside a handler.** A handler that sends an email or calls a third-party API can still
observe a duplicate invocation of that side effect, because only the database participates in
the transaction.

## 2. Wire envelope

The SNS `Message` field carries the JSON-serialised payload. `MessageAttributes` carry headers.

| Header | Type | Meaning |
|---|---|---|
| `cap-msg-id` | UUIDv7 string | Assigned once by the publisher. Stable across every redelivery. The deduplication key. |
| `cap-msg-name` | string | Topic / event name. Used for handler lookup on the consume side. |
| `cap-msg-type` | string | Fully-qualified payload model name. Diagnostics only; never used for dispatch. |
| `cap-sent-time` | RFC3339 | Timestamp the publisher created the envelope (not the SNS send time). |
| `cap-corr-id` | UUID string | Correlation id. Propagated from the inbound message when one is in scope, otherwise a fresh id. |

Header names are constants in `cap_event_handler/envelope.py`. Nothing outside that module hard-codes the
string form.

**Topic naming.** SNS topic names permit only `[A-Za-z0-9_-]`, so event names are normalised
the way CAP's `TopicNormalizer` does: `.` → `-` and `:` → `_`. Normalisation is applied when
resolving a topic ARN and when subscribing; the un-normalised name is what travels in
`cap-msg-name` and what handlers register against.

**Unknown headers are preserved.** A consumer copies every received `MessageAttribute` into
`Context.headers`, not just the known ones, so intermediaries can add their own.

## 3. Schema

```sql
CREATE TABLE cap_outbox (
    id          UUID        PRIMARY KEY,          -- UUIDv7: time-ordered, index-friendly
    name        TEXT        NOT NULL,
    headers     JSONB       NOT NULL,
    payload     JSONB       NOT NULL,
    status      TEXT        NOT NULL,             -- 'pending' | 'sent' | 'failed'
    retries     INT         NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ
);

CREATE INDEX cap_outbox_sweep_idx     ON cap_outbox (created_at) WHERE status <> 'sent';
CREATE INDEX cap_outbox_collector_idx ON cap_outbox (expires_at) WHERE status  = 'sent';

CREATE TABLE cap_inbox (
    msg_id       UUID        NOT NULL,
    group_name   TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (msg_id, group_name)
);

CREATE INDEX cap_inbox_collector_idx ON cap_inbox (expires_at);
```

Both tables live in a configurable schema, default `cap`. Table name prefix is configurable.

### 3.1 Decisions

**The inbox has no status column and no retry count.** A failed handler rolls back its
transaction, which includes the inbox INSERT. A row's existence therefore *is* the statement
"this message was processed successfully by this group." There is no partial state to
reconcile and no sweeper needed over this table. Retry bookkeeping lives in SQS
(`ApproximateReceiveCount`), not here.

**The inbox key is `(msg_id, group_name)`, not `msg_id`.** Two consumer groups subscribed to
the same topic must each process the message once; keying on `msg_id` alone would let
whichever group arrived first suppress the other.

**Inbox retention defaults to 15 days.** The deduplication ledger must outlive the longest
window in which a redelivery can arrive, which is the SQS queue's message retention period —
default 4 days, maximum 14. CAP's equivalent default (`SucceedMessageExpiredAfter`, 24 hours)
is far shorter than that window; adopting it would silently reopen the deduplication hole for
any message that sits in a queue or DLQ for more than a day. The library validates at startup
that configured inbox retention exceeds the queue's `MessageRetentionPeriod` and logs a
warning if it does not.

**There is no `Version` column.** CAP carries one in every recovery query's predicate to
partition rows across application versions, and documents the resulting footgun: changing
`CapOptions.Version` orphans in-flight rows. The same isolation is available by pointing a
deployment at a different schema, without the footgun.

**UUIDv7 rather than snowflake ids.** Time-ordered like a snowflake, so index locality is
preserved, but requires no worker-id coordination and needs no custom generator.

**Payload and headers are `JSONB`, not `TEXT`.** Enables querying and indexing during
incident triage without an application round-trip.

### 3.2 Migrations

The table definitions are exposed as a SQLAlchemy `MetaData` in `cap_event_handler/storage/schema.py`, so
applications using Alembic can autogenerate migrations against them. For applications without
a migration tool, `CapApp.initialize()` issues idempotent `CREATE TABLE IF NOT EXISTS` DDL,
which `app.run()` calls at startup unless `auto_create_tables=False`.

## 4. Publish pipeline

### 4.1 The happy path

```python
async with session.begin():
    session.add(order)
    await app.publish("order.paid", OrderPaid(order_id=order.id, amount=order.total),
                      session=session)
# ────────────────────────────────────────────────────── COMMIT — the atomic point
# after_commit listener → sender queue → SNS Publish → UPDATE status='sent'
```

`publish()` performs exactly two actions and awaits no network I/O:

1. Builds the envelope (UUIDv7 id, headers, serialised payload) and INSERTs a `cap_outbox` row
   with `status='pending'` **using the session it was handed**. It shares that session's
   connection and transaction, so the row commits with the business write or not at all.
2. Appends the row id to a list in `session.info["_cap_pending"]`.

### 4.2 Flush after commit

`CapApp` registers one `after_commit` listener on SQLAlchemy's `Session` class at construction
— `event.listen(Session, "after_commit", ...)`, which covers every session in the process,
including those created by the application's own sessionmaker. The listener returns immediately
unless `session.info` carries the `_cap_pending` key, so sessions that never published are
untouched and the application need not register anything or hand the library its sessionmaker.

When the key is present, the listener drains it and offers each id to a bounded in-process
`asyncio.Queue` via `put_nowait`.

This differs deliberately from CAP, which requires the application to commit through a
`CapTransaction` wrapper and — as §2 of the atomicity doc records — silently skips the flush if
the underlying transaction is committed directly. Hooking `after_commit` means any commit of
that session triggers the flush, so the footgun does not exist.

`after_commit` is a synchronous event and cannot await. `put_nowait` is therefore the only
operation performed inside it.

**When the queue is full, the notification is dropped.** This is not a failure path. The row is
already committed as `pending`, and `OutboxSweeper` will find it within one sweep interval. The
consequence is that the in-memory fast path is a pure latency optimisation: it needs no
backpressure protocol, no error propagation into the caller's commit, and no ordering
guarantee. Correctness rests entirely on the committed row.

Rollback needs no handling at all — `after_commit` does not fire, and the staged row was rolled
back with the rest of the transaction.

### 4.3 The sender task

One task per `CapApp` consumes the queue:

- Load the row. If `status='sent'`, skip (the sweeper may have raced it).
- `sns.publish(TopicArn=..., Message=payload_json, MessageAttributes=headers)`.
- Success → `UPDATE cap_outbox SET status='sent', sent_at=now(), expires_at=now()+outbox_retention`.
- Failure → `UPDATE cap_outbox SET status='failed', retries=retries+1, last_error=...`.

Both updates open their own session; they are not part of any application transaction.

A crash between a successful `sns.publish` and the status update leaves the row `pending` or
`failed`, so the sweeper re-sends it and the consumer sees a duplicate. This is the irreducible
at-least-once window; eliminating it requires two-phase commit between PostgreSQL and SNS,
which this library does not attempt. The consume side's deduplication absorbs it.

### 4.4 Publishing without a session

`app.publish(name, payload)` with no `session` opens its own short transaction to INSERT the
row and returns once committed. Useful for events with no accompanying state change. It offers
the same delivery guarantee but no atomicity with anything, since there is nothing to be atomic
with.

## 5. Consume pipeline

```
long poll (WaitTimeSeconds=20, MaxNumberOfMessages=10)
  → unwrap SNS notification JSON, read MessageAttributes into headers
  → resolve handler by (cap-msg-name, group)          ── miss    → reject → DLQ
  → validate Message body into the handler's model    ── invalid → reject → DLQ
  BEGIN
    INSERT INTO cap_inbox (msg_id, group_name, name, expires_at) ON CONFLICT DO NOTHING
    if rowcount == 0:                                 ── duplicate
        COMMIT; DeleteMessage; return
    await handler(ctx)                                ── ctx.session is this transaction
  COMMIT                                              ── effect + marker, atomically
  DeleteMessage                                       ── ack
```

`ctx.session` comes from an `async_sessionmaker` the library builds from the `AsyncEngine` it
was constructed with. The consumer opens one session per message, begins its transaction before
the dedup INSERT, and closes it after the commit. The application does not supply sessions to
the consume side and must not commit or close `ctx.session` itself — doing so breaks the
atomicity the pipeline exists to provide, and the library raises if it detects a handler that
left the transaction closed.

### 5.1 Why there is no consume-side retry processor

CAP persists the inbox row *before* acking the broker and re-dispatches rows stuck in
`Scheduled` from a background processor, because transports such as Kafka cannot redeliver an
individual message — an offset commit covers everything before it.

SQS can. A message that is not deleted becomes visible again when its visibility timeout
expires, and a redrive policy moves it to a dead-letter queue after `maxReceiveCount`
deliveries. Redelivery, backoff and poison-message quarantine are therefore already provided by
the broker, and reimplementing them over a database table would add a second, weaker copy.

The consequences for v1 are significant: no `Retries` column on the inbox, no status
transitions, no consume-side sweeper, no storage lock, and no `FallbackWindowLookbackSeconds`
tuning trap (CAP's §5: set it below the slowest handler's runtime and a still-executing message
gets re-dispatched).

### 5.2 The cost, and how it is contained

A handler must complete within the queue's visibility timeout. Exceed it and SQS redelivers
while the first invocation is still running.

That is a *wasted work* problem, not a *correctness* problem: the second invocation blocks on
the first's uncommitted `cap_inbox` row (same primary key, so `INSERT ... ON CONFLICT` waits on
the row lock), and once the first commits, the second's insert affects zero rows and it exits
as a duplicate. Progress is preserved; only effort is lost.

`app.run()` reads each queue's `VisibilityTimeout` at startup and logs a warning if it is below
a configured `expected_max_handler_seconds`. A visibility heartbeat that extends the lease for
long-running handlers is deferred (§11).

### 5.3 Failure dispositions

| Situation | Disposition |
|---|---|
| No handler registered for `(name, group)` | Log error, **reject** (no ack) → DLQ after `maxReceiveCount` |
| Payload fails model validation | Log error with the raw body, **reject** → DLQ |
| Handler raises | Transaction rolls back, **reject** → redelivered, then DLQ |
| Duplicate (`msg_id`, `group`) already in inbox | Commit no-op, **ack**, handler never invoked |
| Crash after COMMIT, before `DeleteMessage` | Redelivered → caught by the dedup insert → acked |
| Crash mid-handler | Rolled back, not acked → redelivered → handler re-runs cleanly |

Rejecting rather than acking an unroutable message keeps it for inspection in the DLQ instead of
discarding it. Because the queue is subscribed only to topics the group registered handlers
for, an unroutable message means a misconfiguration worth surfacing loudly. A
`on_unroutable="ack"` setting is available for deployments that prefer to drop them.

Rejection is implemented as `ChangeMessageVisibility(handle, backoff_seconds)`, default 5, so a
transient failure retries promptly rather than waiting out the full visibility timeout.

### 5.4 Polling parameters

`WaitTimeSeconds=20` (the SQS maximum) and `MaxNumberOfMessages=10`, against CAP's 5 and 1.
Same delivery latency, roughly a tenth the `ReceiveMessage` API calls. A batch is processed
sequentially unless `group_concurrency > 1`, in which case a semaphore bounds the in-flight
handlers.

`consumer_tasks` (default 1) controls how many independent polling loops run per group,
equivalent to CAP's `ConsumerThreadCount`.

## 6. Background processors

Both are publish-side only, run as asyncio tasks under `app.run()`, and use jittered intervals
so multiple instances do not synchronise.

**`OutboxSweeper`** — default every 30s:

```sql
SELECT id, name, headers, payload FROM cap_outbox
 WHERE status IN ('pending', 'failed')
   AND retries < :max_retries
   AND created_at < now() - :lookback
 ORDER BY created_at
 FOR UPDATE SKIP LOCKED
 LIMIT 200;
```

`FOR UPDATE SKIP LOCKED` is how multiple application instances divide the work without
coordination. CAP instead offers an optional `cap.lock` table with TTL-based leases, which its
own documentation describes as coarse and best-effort; row-level `SKIP LOCKED` is finer-grained,
needs no extra table, and cannot leave a stale lease behind.

The `lookback` (default 60s) keeps the sweeper from racing a send that is legitimately still in
flight. It must exceed the SNS publish timeout; the library validates this at startup.

A row reaching `max_retries` (default 50) stops being selected and remains `failed` with its
`last_error`, awaiting operator attention.

**`Collector`** — default every 5 minutes: deletes `cap_outbox` rows where
`status='sent' AND expires_at < now()`, and `cap_inbox` rows where `expires_at < now()`, both in
bounded batches (default 1000 per pass) so a long-neglected table does not produce one enormous
transaction.

## 7. Public API

```python
from cap_event_handler import CapApp, Context, SnsSqs, Settings

app = CapApp(
    engine=engine,                                   # sqlalchemy AsyncEngine
    transport=SnsSqs(region="ap-southeast-2"),
    group="billing-service",
    settings=Settings(
        group_concurrency=1,
        inbox_retention=timedelta(days=15),
        outbox_retention=timedelta(days=1),
    ),
)

class OrderPaid(BaseModel):
    order_id: UUID
    amount: Decimal

@app.subscribe("order.paid", model=OrderPaid)
async def on_order_paid(ctx: Context[OrderPaid]) -> None:
    ctx.session.add(Invoice(order_id=ctx.payload.order_id, amount=ctx.payload.amount))
```

`Context[T]` exposes: `payload: T`, `headers: Mapping[str, str]`, `msg_id: UUID`,
`name: str`, `group: str`, `retries: int` (from `ApproximateReceiveCount`), `sent_time: datetime`,
`correlation_id: UUID`, and `session: AsyncSession`.

Publishing from inside a handler uses `ctx.session`, so the outgoing event is atomic with the
incoming message's effect and its marker — the full chain in one transaction:

```python
@app.subscribe("order.paid", model=OrderPaid)
async def on_order_paid(ctx: Context[OrderPaid]) -> None:
    ctx.session.add(Invoice(order_id=ctx.payload.order_id))
    await app.publish("invoice.issued", InvoiceIssued(...), session=ctx.session)
```

The correlation id propagates through a `contextvar` that the consumer sets before invoking a
handler and resets afterwards. Any `publish()` executed while that variable is set inherits the
inbound `cap-corr-id`; a publish outside a handler generates a fresh one. Passing
`correlation_id=` explicitly overrides both.

### 7.1 Lifecycle

```python
@asynccontextmanager
async def lifespan(_: FastAPI):
    async with app.run():
        yield
```

`app.run()` on entry: provisions AWS resources (skipped when `provision=False`), creates tables
(skipped when `auto_create_tables=False`), validates configuration (§9), then starts the sender
task, the consumer tasks and the two processors. On exit it stops polling, waits for in-flight
handlers to finish or `shutdown_timeout` to elapse, and cancels the processors.

A `cap-event-handler worker` CLI wraps the same lifecycle for standalone worker processes, importing the
app from a `module:attribute` path.

### 7.2 Provisioning

Following CAP's model, the library creates what it needs at startup: an SNS topic per event
name it publishes or subscribes to, one SQS queue per consumer group, the SNS→SQS subscriptions,
and the queue access policy permitting those topics to deliver. All operations are idempotent.

`provision=False` disables every create/subscribe/set-attributes call for Terraform-managed
environments; the library then resolves ARNs and queue URLs from configuration and fails at
startup with a clear error if any are missing.

Redrive policies and DLQs are **not** provisioned in v1 even when `provision=True`, because
choosing `maxReceiveCount` and the DLQ's retention is an operational decision. `app.run()` logs
a warning when a queue it consumes has no redrive policy, since §5.3's dispositions rely on one.

## 8. Module layout

```
cap_event_handler/
  __init__.py       public exports
  app.py            CapApp — wiring, lifecycle, handler registry
  context.py        Context[T]
  envelope.py       header constants, UUIDv7 generation, encode/decode, topic normalisation
  publisher.py      publish(), after_commit staging, sender task
  consumer.py       polling loop, dispatch, dedup, ack/reject
  processors.py     OutboxSweeper, Collector
  config.py         Settings and transport configuration dataclasses
  errors.py         exception hierarchy
  storage/
    base.py         Storage Protocol
    postgres.py     SQLAlchemy Core implementation
    memory.py       in-memory fake for tests
    schema.py       MetaData / DDL
  transport/
    base.py         Transport and ConsumerClient Protocols
    snssqs.py       SNS + SQS via aiobotocore
    memory.py       in-memory fake for tests
```

`Storage` and `Transport` are `typing.Protocol` seams with exactly one production implementation
each. They exist to make the test doubles in §10 possible, not to anticipate providers nobody
has asked for; the library ships as one installable package rather than CAP's family of provider
packages. A second implementation is a packaging decision to revisit when a second backend is
actually needed.

### 8.1 Dependencies

Runtime: `sqlalchemy>=2.0`, `asyncpg`, `pydantic>=2`, `aiobotocore`. Python 3.11+.

UUIDv7 comes from `uuid.uuid7()` on Python 3.14+, falling back to the `uuid6` package below
that — isolated behind a single function in `envelope.py` so the fallback disappears when the
floor rises.

`aiobotocore` rather than `boto3` in a thread pool: the consumer's long poll holds a connection
for 20 seconds at a time, which would pin a thread per polling task.

Development: `pytest`, `pytest-asyncio`, `hypothesis`, `testcontainers[postgres]`,
`localstack`, `ruff`, `mypy` in strict mode.

## 9. Startup validation

`app.run()` fails fast or warns on configurations that silently degrade a guarantee:

| Check | Disposition |
|---|---|
| `inbox_retention` ≤ queue `MessageRetentionPeriod` | Warn — deduplication window is shorter than the redelivery window |
| Queue `VisibilityTimeout` < `expected_max_handler_seconds` | Warn — concurrent redelivery likely |
| Queue has no redrive policy | Warn — poison messages will retry indefinitely |
| `sweeper_lookback` ≤ SNS publish timeout | Fail — the sweeper will race in-flight sends |
| `provision=False` and an ARN or queue URL is unresolved | Fail — names the missing resource |
| `provision=False` and a handler's topic is not among the queue's subscriptions | Fail — dead handler. Cannot occur under `provision=True`, which subscribes the queue to every registered topic. |

## 10. Testing

Test-driven throughout, per the project's TDD practice.

Unit and full-pipeline tests run entirely in-process against `storage/memory.py` and
`transport/memory.py`, requiring no infrastructure — the property that makes
`DotNetCore.CAP.Test` fast, and the main justification for the Protocol seams. Stack: pytest,
pytest-asyncio, hypothesis for envelope round-tripping.

Integration tests run against testcontainers PostgreSQL and LocalStack, marked
`@pytest.mark.integration` so the default run skips them.

Crash-window cases get explicit tests against the fakes, each asserting the guarantee in §1.2
rather than an implementation detail:

- Crash after COMMIT, before SNS publish → sweeper re-sends; message delivered exactly once at
  the consumer despite two publish attempts.
- Crash after SNS publish, before the status update → duplicate published; consumer's dedup
  absorbs it; handler runs once.
- Rollback of the caller's transaction → no row, no send, `after_commit` never fires.
- Crash mid-handler → inbox row rolled back with the handler's writes; redelivery re-runs it
  with no residue.
- Redelivery after a successful commit but before `DeleteMessage` → handler not re-invoked.
- Two consumer groups on one topic → both handlers run exactly once.
- Concurrent redelivery while the first invocation is in flight → the second blocks, then exits
  as a duplicate; the effect appears once.
- Sweeper with two instances → `SKIP LOCKED` prevents double-sending the same row.

## 11. Deferred

Recorded for later consideration; explicitly not in v1.

- **Delayed / scheduled publish** (`publish(..., delay=...)`) — a `delayed` status plus a
  scheduler poller, as CAP does, or SQS `DelaySeconds` for short delays.
- **Visibility heartbeat** — extend a message's lease while a long handler runs, removing §5.2's
  constraint entirely.
- **DLQ inspection and replay API** — list, inspect and requeue dead-lettered messages.
- **Monitoring API and dashboard** — counts by status, message lookup, manual retry of a failed
  outbox row.
- **OpenTelemetry spans** — trace context propagated through the envelope, spanning publish and
  consume. CAP's equivalent is its `DiagnosticListener`.
- **Batch publish** — `sns.publish_batch` for the sweeper's 200-row pages.
- **FIFO queues** — CAP cannot use them at all; ordering guarantees would need design work.
- **Additional storages** (psycopg3, Django ORM) **and transports** (RabbitMQ, Kafka) behind the
  existing Protocols.
- **CloudEvents envelope** as an alternative wire format.
- **Outbox partitioning** by `created_at` for very high-volume deployments.

## 12. Open questions

None. All design decisions above are settled; items in §11 are deliberately deferred, not
unresolved.
