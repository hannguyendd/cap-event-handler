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

## Open follow-ups

- Confirm `tests/test_guarantees.py` covers each row of the table above.
