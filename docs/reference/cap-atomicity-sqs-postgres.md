# Atomicity in CAP (Amazon SQS + PostgreSQL)

How CAP guarantees that a domain state change and its outgoing message happen together — what is
actually atomic, what is not, and what you have to do yourself.

All code references are to this repository at the current checkout.

---

## TL;DR

| Boundary | Guarantee | Mechanism |
|---|---|---|
| Your DB write ↔ outgoing message row | **Atomic** | The `cap.published` INSERT runs inside *your* `NpgsqlTransaction` |
| Outgoing message row ↔ SNS/SQS delivery | Eventual, at-least-once | Deferred flush after commit + `MessageNeedToRetryProcessor` sweep |
| Incoming SQS message ↔ `cap.received` row | At-least-once | Row persisted before ack; SQS `DeleteMessage` only after dispatch |
| Your handler's DB write ↔ `cap.received` status | **Not atomic** | Status update uses a separate connection |

In one line: **CAP is atomic on the publish side and at-least-once on the consume side.** An event is
never published for a rolled-back transaction and never lost for a committed one, but your subscriber
can observe the same message more than once.

---

## 1. Publish side — the transactional outbox

### 1.1 The branch

`CapPublisher.PublishInternalAsync` (`src/DotNetCore.CAP/Internal/ICapPublisher.Default.cs:153`)
chooses between two behaviours based on whether an ambient CAP transaction is set:

```csharp
if (Transaction?.DbTransaction == null)          // non-transactional
{
    var mediumMessage = await _storage.StoreMessageAsync(name, message);
    await _dispatcher.EnqueueToPublish(mediumMessage);   // sent immediately
}
else                                             // transactional (outbox)
{
    var transaction = (CapTransactionBase)Transaction;
    var mediumMessage = await _storage.StoreMessageAsync(name, message, transaction.DbTransaction);
    transaction.AddToSent(mediumMessage);        // buffered in memory, NOT sent
    if (transaction.AutoCommit) await transaction.CommitAsync(cancellationToken);
}
```

`ICapPublisher.Transaction` is backed by an `AsyncLocal<CapTransactionHolder>`, so the transaction
flows through the async call chain of the current scope without being passed explicitly.

### 1.2 The three pieces that make it atomic

**(a) The INSERT joins your transaction.**
`PostgreSqlDataStorage.StoreMessageAsync` (`src/DotNetCore.CAP.PostgreSql/IDataStorage.PostgreSql.cs:113`)
unwraps whatever transaction object it is handed and executes on that transaction's connection:

```csharp
var dbTrans = transaction as DbTransaction;
if (dbTrans == null && transaction is IDbContextTransaction dbContextTrans)
    dbTrans = dbContextTrans.GetDbTransaction();

var conn = dbTrans?.Connection!;
await conn.ExecuteNonQueryAsync(sql, dbTrans, sqlParams);
```

The row is inserted with `StatusName = 'Scheduled'`. Your `UPDATE orders ...` and the outbox INSERT
are therefore a single PostgreSQL transaction — both commit, or neither does.

**(b) The broker call is deferred until after commit.**
`CapTransactionBase.AddToSent` (`src/DotNetCore.CAP/ICapTransaction.Base.cs:97`) only enqueues onto a
`ConcurrentQueue<MediumMessage>`. Nothing touches AWS yet.

`PostgreSqlCapTransaction.CommitAsync`
(`src/DotNetCore.CAP.PostgreSql/ICapTransaction.PostgreSql.cs:40`) commits the database transaction
**first**, then calls `FlushAsync()`, which drains the buffer into
`IDispatcher.EnqueueToPublish` / `EnqueueToScheduler` for delayed messages.

`Rollback` / `RollbackAsync` deliberately do **not** call `Flush` — buffered messages are discarded,
and their rows were rolled back with the rest of the transaction.

**(c) A crash between commit and send is recoverable.**
This is the window that breaks a naive "commit, then publish". The row is committed as `Scheduled`;
if the process dies before SNS accepts it, `MessageNeedToRetryProcessor.ProcessPublishedAsync`
(`src/DotNetCore.CAP/Processor/IProcessor.NeedRetry.cs:70`) picks it up, because the recovery query
(`IDataStorage.PostgreSql.cs:353`) selects exactly that state:

```sql
SELECT "Id","Content","Retries","Added" FROM cap.published
WHERE "Retries" < @Retries
  AND "Version" = @Version
  AND "Added"  < @Added                          -- now - FallbackWindowLookbackSeconds
  AND "StatusName" IN ('Failed','Scheduled')
LIMIT 200;
```

Only once the transport reports success does `MessageSender.SetSuccessfulState`
(`src/DotNetCore.CAP/Internal/IMessageSender.Default.cs:81`) flip the row to `Succeeded` and set
`ExpiresAt = now + SucceedMessageExpiredAfter`, after which the collector deletes it.

### 1.3 Sequence

```
BEGIN                                   ← your transaction
  UPDATE orders SET status='paid'
  INSERT INTO cap.published (Scheduled) ← same transaction
COMMIT                                  ← atomic point
  ──────────────────────────────────────
  Flush() → SNS Publish                 ← after commit, may fail
  UPDATE cap.published SET StatusName='Succeeded'
  ──────────────────────────────────────
  (crash anywhere after COMMIT)
  → row stays 'Scheduled'
  → MessageNeedToRetryProcessor re-sends it
```

### 1.4 Failure matrix

| Crash point | Outcome |
|---|---|
| Before `COMMIT` | Nothing happened. No state change, no message. |
| During `COMMIT` | Postgres decides atomically. Both or neither. |
| After `COMMIT`, before SNS publish | State changed; row is `Scheduled`; retry processor re-sends. **Delayed, not lost.** |
| SNS publish succeeded, before status update | Row stays `Scheduled` → re-sent → **duplicate delivery**. |
| SNS returns an error | `SetFailedState` → `Retries++`, `StatusName='Failed'` → retried up to `FailedRetryCount`. |

The fourth row is the reason the system is at-least-once end to end even though the database side is
atomic. There is no way to make the broker ack and the database update atomic without 2PC, which CAP
does not use.

---

## 2. Requirements and footguns (publish side)

**The CAP tables must live in the same PostgreSQL database as your business tables.**
Atomicity comes entirely from sharing one `NpgsqlConnection` and one transaction. A separate CAP
database silently degrades to "commit, then hope". The schema defaults to `cap`
(`EFOptions.DefaultSchema`), so `cap.published` / `cap.received` / `cap.lock`.

**Commit through the CAP wrapper, not the raw transaction.**
`Flush()` is what pushes buffered messages to the transport. If you take the underlying
`NpgsqlTransaction` (or `DbContext.Database.CurrentTransaction`) and commit *that* directly, the
flush never runs. The message is not lost — the retry processor will find the `Scheduled` row — but
delivery is delayed by roughly `FallbackWindowLookbackSeconds` + `FailedRetryInterval` (240s + 60s by
default) instead of going out immediately.

**`autoCommit: true` commits your whole transaction on the first publish.**
Use it only when the publish is genuinely the last statement.

**Delayed messages take a different path.** `FlushAsync` routes messages carrying a `DelayTime`
header to `EnqueueToScheduler`, which sets `Queued` or `Delayed` and relies on
`ScheduleMessagesOfDelayedAsync` (`FOR UPDATE SKIP LOCKED`) to pick them up.

### 2.1 Raw Npgsql

```csharp
await using var conn = new NpgsqlConnection(connectionString);
using var tx = conn.BeginTransaction(_capPublisher, autoCommit: false);

await conn.ExecuteAsync(
    "UPDATE orders SET status='paid' WHERE id=@id",
    new { id },
    (IDbTransaction)tx.DbTransaction);

await _capPublisher.PublishAsync("order.paid", new OrderPaid(id));

await tx.CommitAsync();   // Postgres commit, then flush to SNS
```

### 2.2 EF Core

```csharp
using var tx = _dbContext.Database.BeginTransaction(_capPublisher, autoCommit: false);

_dbContext.Orders.Update(order);
await _dbContext.SaveChangesAsync();

await _capPublisher.PublishAsync("order.paid", new OrderPaid(order.Id));

await tx.CommitAsync();
```

`BeginTransaction(DatabaseFacade, ICapPublisher, ...)` returns a `CapEFDbTransaction` wrapper, so
committing the returned object is what triggers the flush.

Do **not** inject `ICapPublisher` into your `DbContext` — `ConfigurePostgreSqlOptions.Configure`
throws on that to prevent a circular reference.

---

## 3. Consume side — where atomicity stops

The receive pipeline is in `ConsumerRegister.RegisterMessageProcessor`
(`src/DotNetCore.CAP/Internal/IConsumerRegister.Default.cs:165`):

```
SQS ReceiveMessage (long poll, MaxNumberOfMessages = 1)
  → INSERT INTO cap.received (StatusName='Scheduled')   ← persisted before ack
  → _dispatcher.EnqueueToExecute(...)                   ← your [CapSubscribe] handler
  → client.CommitAsync(sender) → SQS DeleteMessage      ← ack
```

Persisting before acking is what makes the inbox row, not the SQS message, the unit of recovery: a
`Scheduled` row older than `FallbackWindowLookbackSeconds` is re-dispatched by
`MessageNeedToRetryProcessor.ProcessReceivedAsync`.

On an exception, `AmazonSQSConsumerClient.RejectAsync`
(`src/DotNetCore.CAP.AmazonSQS/AmazonSQSConsumerClient.cs:143`) calls
`ChangeMessageVisibility(handle, 3)`, releasing the lease so SQS redelivers in ~3 seconds.

**The status update is not in your transaction.** `ChangeReceiveStateAsync` →
`ChangeMessageStateAsync` is invoked with `transaction = null` and opens its own connection
(`IDataStorage.PostgreSql.cs:308`). There is no overload that accepts your handler's transaction and
nothing plumbs one through. So your business write and the "this message is done" marker commit
separately, and the gap between them is unprotected.

### 3.1 Duplicate windows

| Situation | Result |
|---|---|
| Crash after your handler commits, before the `Succeeded` update | Row stays `Scheduled` → handler re-runs |
| Crash after the `Succeeded` update, before SQS `DeleteMessage` | Visibility timeout expires → SQS redelivers → **a new `cap.received` row** → handler re-runs |
| `EnableSubscriberParallelExecute = true` | The SQS delete happens as soon as the message is queued into the in-memory channel, widening the window |
| Handler throws after partial work | `Failed`, `Retries++`, re-run up to `FailedRetryCount` |
| SQS standard queue | AWS itself is at-least-once |
| Multiple app instances | Without `UseStorageLock`, two retry processors can select the same rows |

### 3.2 The inbox does not deduplicate

`StoreReceivedMessageAsync` (`IDataStorage.PostgreSql.cs:177`) assigns a **fresh snowflake id**:

```csharp
DbId = _snowflakeId.NextId().ToString(),
```

It is not the `cap-msg-id` header. There is no unique index on the message id, no lookup before
insert, no `ON CONFLICT DO NOTHING`. A redelivered message produces a second row with a different
primary key and the handler is invoked again. `cap.received` is a durability and retry journal, not a
dedup table.

This is intentional and documented in `docs/content/user-guide/en/cap/idempotence.md`, under the
heading *"Why We Don't Provide (Achieve) Idempotency"*: succeeded rows are deleted after
`SucceedMessageExpiredAfter`, so history cannot be consulted, and only the application knows what
"already done" means for a given operation.

---

## 4. Closing the consume-side gap yourself

Use the same tool CAP uses on the publish side: one PostgreSQL transaction covering both the effect
and the record of the effect.

```sql
CREATE TABLE processed_messages (
    msg_id       TEXT        NOT NULL,
    handler      TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (msg_id, handler)
);
```

```csharp
[CapSubscribe("order.paid")]
public async Task Handle(OrderPaid msg, [FromCap] CapHeader header)
{
    var msgId = header[DotNetCore.CAP.Messages.Headers.MessageId];   // "cap-msg-id"

    await using var tx = await _conn.BeginTransactionAsync();

    var rows = await _conn.ExecuteAsync(
        """
        INSERT INTO processed_messages (msg_id, handler)
        VALUES (@msgId, @handler)
        ON CONFLICT DO NOTHING
        """,
        new { msgId, handler = nameof(Handle) }, tx);

    if (rows == 0) return;      // duplicate — already handled

    await DoTheWork(msg, tx);

    await tx.CommitAsync();
}
```

Rules that make this correct:

- **Key on `(msg_id, handler)`**, not `msg_id` alone, if several subscribers consume the same topic.
- **One transaction** for the marker and the work. Splitting them reintroduces the same gap CAP has.
- **Retain longer** than `SucceedMessageExpiredAfter` (24h default) plus your SQS message retention.
- `cap-msg-id` is stable across redeliveries of the same publish; it is assigned once by the
  publisher in `PublishInternalAsync` and travels in the headers.

Where the operation is **naturally idempotent** — a conditional update like
`UPDATE orders SET status='paid' WHERE id=@id AND status='pending'`, or an upsert — skip the marker
table. It is cheaper and cannot drift out of sync.

Note also that CAP's publish side is at-least-once, so a duplicate can originate upstream, not only
from your own consumer's retries. Dedup on the message id covers both.

---

## 5. Settings that affect these guarantees

Defaults from `src/DotNetCore.CAP/CAP.Options.cs`.

| Option | Default | Why it matters here |
|---|---|---|
| `FallbackWindowLookbackSeconds` | `240` | Rows younger than this are skipped by the retry sweep. **Must exceed your slowest handler's runtime**, or a still-executing message gets re-dispatched. A startup warning fires below 30s (`IProcessor.NeedRetry.cs:118`). |
| `FailedRetryInterval` | `60` | How often the retry processor runs. |
| `FailedRetryCount` | `50` | Retry ceiling for both publish and consume. |
| `SucceedMessageExpiredAfter` | `86400` | How long succeeded rows survive before the collector deletes them — the horizon of any dedup you build on CAP's own tables. |
| `FailedMessageExpiredAfter` | `1296000` | 15 days for failed rows. |
| `UseStorageLock` | `false` | Creates `cap.lock` and serialises the retry sweep across instances. Coarse, TTL-based, best-effort — not a per-message lock. Recommended for multi-instance deployments. |
| `EnableSubscriberParallelExecute` | `false` | Turning it on acks SQS earlier and widens the duplicate window. |
| `ConsumerThreadCount` | `1` | Number of SQS polling loops per consumer group. |
| `Version` | `"v1"` | Part of every recovery query's predicate; changing it orphans in-flight rows. |

### SQS/SNS specifics

- Topics are SNS topics; each consumer group gets one SQS queue, created by
  `CreateQueueAsync(_groupId.NormalizeForAws())`. No FIFO attributes are set, so **FIFO queues with
  content-based deduplication are not available** through this transport — standard queues only.
- `.` and `:` in topic and group names are rewritten to `-` and `_` (`TopicNormalizer`).
- Polling is `WaitTimeSeconds = 5`, `MaxNumberOfMessages = 1`.
- The queue's **visibility timeout** is the lease on an in-flight message. If it is shorter than your
  handler's runtime, SQS redelivers while you are still processing. Set it above your worst-case
  handler duration, and keep `FallbackWindowLookbackSeconds` above it too.

---

## 6. Source map

| Concern | File |
|---|---|
| Publish branch, message id assignment | `src/DotNetCore.CAP/Internal/ICapPublisher.Default.cs:153` |
| Buffer, flush, rollback semantics | `src/DotNetCore.CAP/ICapTransaction.Base.cs:97` |
| Commit-then-flush for Postgres, `BeginTransaction` helpers | `src/DotNetCore.CAP.PostgreSql/ICapTransaction.PostgreSql.cs` |
| Outbox INSERT enlisting your transaction | `src/DotNetCore.CAP.PostgreSql/IDataStorage.PostgreSql.cs:113` |
| Inbox INSERT (fresh snowflake id) | `src/DotNetCore.CAP.PostgreSql/IDataStorage.PostgreSql.cs:177` |
| State transitions (own connection) | `src/DotNetCore.CAP.PostgreSql/IDataStorage.PostgreSql.cs:308` |
| Recovery query | `src/DotNetCore.CAP.PostgreSql/IDataStorage.PostgreSql.cs:353` |
| Table DDL | `src/DotNetCore.CAP.PostgreSql/IStorageInitializer.PostgreSql.cs:67` |
| Send + success/failure state | `src/DotNetCore.CAP/Internal/IMessageSender.Default.cs` |
| Receive pipeline, ack/reject | `src/DotNetCore.CAP/Internal/IConsumerRegister.Default.cs:165` |
| Handler invocation, retry state | `src/DotNetCore.CAP/Internal/ISubscribeExector.Default.cs` |
| Retry sweep, storage lock | `src/DotNetCore.CAP/Processor/IProcessor.NeedRetry.cs` |
| SQS polling, delete, visibility | `src/DotNetCore.CAP.AmazonSQS/AmazonSQSConsumerClient.cs` |
| Project's own position on idempotency | `docs/content/user-guide/en/cap/idempotence.md` |
