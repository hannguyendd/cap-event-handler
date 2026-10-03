# CAP issues and how this library handles them

Issues in the .NET CAP library (`DotNetCore.CAP`, PostgreSQL + Amazon SQS) recorded in
`docs/reference/cap-atomicity-sqs-postgres.md`, and how `cap-event-handler` responds to each.
The authoritative design is `docs/superpowers/specs/2026-09-17-cap-event-handler-design.md`.

This library is **not a port**. It learned from CAP but is standalone: it does not interoperate
with .NET CAP services on the wire or in the database. That is what lets it fix gaps CAP
documents and declines to fix.

## Issues in CAP, and how they are handled here

| # | Issue in CAP | Reference | Handling here |
|---|---|---|---|
| 1 | **The consume side does not deduplicate.** `cap.received` assigns a fresh snowflake id per delivery, so a redelivered message re-invokes the handler. Intentional per CAP's idempotence docs. | §3.2 | **Fixed.** Inbox primary key is `(msg_id, group_name)`, inserted with `ON CONFLICT DO NOTHING`. Zero rows inserted means a duplicate: ack, skip the handler. |
| 2 | **Handler write and "done" marker commit separately.** The status update opens its own connection, so a crash between the two commits re-runs the handler. | §3 | **Fixed.** One transaction holds the dedup insert and the handler's writes. SQS is acked only after it commits. |
| 3 | **Committing the raw transaction skips the flush.** Bypassing the `CapTransaction` wrapper delays delivery by roughly 300s (lookback + retry interval). | §2 | **Fixed.** A process-wide SQLAlchemy `after_commit` listener fires on any commit of the session, so there is no wrapper to bypass. |
| 4 | **Changing `Version` orphans in-flight rows**, because every recovery query filters on it. | §5 | **Removed.** No `Version` column. Isolate deployments with a separate schema instead. |
| 5 | **24h `SucceedMessageExpiredAfter` is shorter than SQS retention** (up to 14 days), which would reopen the dedup hole. | §4, §5 | Inbox retention defaults to 15 days. Startup warns if it does not exceed the queue's `MessageRetentionPeriod`. |
| 6 | **Retry-sweep lookback trap.** A lookback shorter than the slowest handler re-dispatches a message that is still running. | §5 | No consume-side retry sweep: redelivery comes from the SQS visibility timeout and redrive policy. The outbox sweeper requires `sweeper_lookback > publish_timeout` and refuses to start otherwise. |
| 7 | **Multiple instances can claim the same retry rows.** `UseStorageLock` is off by default and documented as coarse and best-effort. | §3.1, §5 | Outbox rows are claimed with `FOR UPDATE SKIP LOCKED`: no lock table, no stale leases. |
| 8 | **`EnableSubscriberParallelExecute` acks SQS early**, widening the duplicate window. | §3.1 | Ack happens only after commit, even with `group_concurrency > 1`. |
| 9 | **Visibility timeout must exceed the slowest handler.** | §5 | Same constraint. Startup warns if `VisibilityTimeout < expected_max_handler_seconds`, and if the queue has no redrive policy. |

Checks for 5, 6 and 9 are implemented in `src/cap_event_handler/validation.py`.

## Accepted limitations

Documented trade-offs, not oversights.

- **Publish is still at-least-once.** A crash after SNS accepts a message but before the row is
  marked `sent` causes a re-send. Preventing it would need two-phase commit between PostgreSQL
  and SNS. Consume-side dedup absorbs the duplicate. (Spec §4.3.)
- **Non-database side effects repeat.** Only PostgreSQL participates in the handler's
  transaction; emails, HTTP calls and similar are at-least-once. (`docs/usage.md` §8.)
- **No ordering guarantee.** Standard SQS is unordered, and the sweeper re-sends out of order.
  (`docs/usage.md` §8.)

## Deferred from v1

From spec §11.

- **Delayed / scheduled publish.** CAP supports it; this library does not yet.
- **Visibility heartbeat.** Neither CAP nor this library extends a long handler's lease. Until
  it exists, the visibility timeout must exceed the slowest handler.
- **FIFO queues.** CAP cannot use them either; ordering guarantees need design work.

## NATS JetStream transport (in design)

CAP ships a NATS transport (`src/DotNetCore.CAP.NATS` in the CAP checkout). This library is
adding a JetStream `Transport` as an alternative production transport to SNS/SQS: each `CapApp`
uses one or the other. Decisions are recorded here as the design settles, and the spec will
be the authoritative record.

| # | Concern | CAP | Here |
|---|---|---|---|
| N1 | **When the broker is acked** | Immediately after the message is stored in CAP's `received` table (`IConsumerRegister.Default.cs`). Naks only if that store fails. | Only after the handler's transaction (dedup insert + handler writes) commits, the same as on SQS. |
| N2 | **Who retries a failing handler** | CAP, from its database, up to `FailedRetryCount`. NATS has already been acked, so `max_deliver` never comes into play. | JetStream. Each failure naks with a `reject_backoff_seconds` delay, and the delivery count comes from the message metadata. There is no consume-side retry table. |
| N3 | **Messages that always fail** | Left in `received` with status `Failed`, plus `FailedThresholdCallback`. The dead-letter store is the database. | JetStream has no redrive policy, so the library supplies one. On the last allowed delivery (`receive_count >= max_deliver`) the consumer publishes the message, with its failure reason in a header, to a dead-letter subject in its own file-backed stream, then acks the original. If that publish fails it naks as usual, and `max_deliver` lets JetStream drop the message with an advisory, so it is never retried forever. |
| N4 | **Awareness of failures** | Logged via `OnLogCallback`. Ack/nak failures are only logged. | Every failed delivery is logged with the exception, as on SQS today. Dead-lettering is logged at `ERROR` with message id, name, group and delivery count. A failed dead-letter publish is logged at `ERROR` too, because that message will be dropped once `max_deliver` is reached. |
| N5 | **Stream layout** | One stream per first token of the subject (`NormalizeStreamName`: `orders.created` goes to stream `orders`). A consumer reads one stream, so CAP creates one durable consumer per group *and subject*, and each pushes into the same callback. | One shared stream, `CAP` by default, captures every event subject under one wildcard. Each group has **one** durable pull consumer filtered to its subjects (needs NATS server 2.10+ for multiple filter subjects), which fits the `receive()` loop the consumer already runs against SQS. Dead-lettered messages go to a separate stream whose subjects do not overlap the main one, because a subject cannot belong to two streams. |
| N6 | **Stream storage** | `StorageType.Memory` by default, so a NATS server restart loses every unconsumed message, even though the outbox already recorded it as sent. | File storage by default, so the outbox's "sent" means the broker durably holds the message. |

### Why this library does not copy CAP's NATS consume model

CAP acks NATS as soon as the message is stored, then runs and retries the handler from its
`received` table. Copying that would undo what this library exists to fix:

- **The handler's writes and the "done" marker would commit separately again (issue 2).** Once
  the broker is acked, the database row is the only record of the message. Its status has to
  move from `Scheduled` to `Succeeded` after the handler runs, in a transaction separate from
  the one that stored it. A crash between the two re-runs the handler. Here, the dedup insert
  and the handler's writes are one transaction, and the broker is acked only after it commits.
- **The inbox would become a work queue instead of a dedup marker.** It would need a `Retries`
  column, status transitions and a consume-side retry processor. Spec §5.1 rejects all of these.
- **That retry processor brings back issues 6 and 7.** A lookback window shorter than the slowest
  handler re-dispatches a message that is still running. Several instances can claim the same
  retry rows unless a storage lock is added.
- **CAP's reason for retrying from its database doesn't apply to JetStream.** CAP does it because
  some transports, such as Kafka, cannot redeliver one message: an offset commit covers
  everything before it. JetStream can. It acks and naks each message, naks can carry a
  redelivery delay, an unacked message is redelivered after `ack_wait`, and `max_deliver` caps
  the attempts. Like SQS, the broker already does redelivery and backoff, so a database copy
  would be a second, weaker mechanism (spec §5.1's argument, unchanged).
- **CAP compatibility is not a goal.** This library does not interoperate with .NET CAP on the
  wire or in the database, so matching CAP's behaviour gains nothing.

The one SQS feature JetStream lacks is a redrive policy. Row N3 fills only that gap. It does not
copy CAP's whole retry mechanism.

**What this costs, compared with CAP:**

- **`ack_wait` must exceed the slowest handler**, the same constraint as the SQS visibility
  timeout (spec §5.2). A slow handler is redelivered while still running. That wastes work but
  is not a correctness problem: the second attempt finds the inbox row and exits as a duplicate.
  Startup validation warns when `ack_wait < expected_max_handler_seconds`.
- **One shared stream means one set of stream limits.** Retention (`max_age`), size caps,
  replicas and storage apply to every event alike. CAP's per-prefix streams let `orders` keep
  messages longer than `audit`, for example. Here, set those limits for the most demanding
  event, or run separate apps against separate stream names. Startup validation compares
  `inbox_retention` with the stream's `max_age`, just as it compares it with the SQS
  `MessageRetentionPeriod`.
- **NATS server 2.10 or later is required**, for consumers with multiple filter subjects.
  CAP's one-consumer-per-subject layout works on older servers.
- **Failed messages are kept in a NATS stream, not a database table.** You inspect them with NATS
  tools (`nats stream view`), not SQL or a dashboard. A dead-letter inspection and replay API is
  deferred (spec §11).

## Open follow-ups

- Confirm `tests/test_guarantees.py` covers each row of the table above.
