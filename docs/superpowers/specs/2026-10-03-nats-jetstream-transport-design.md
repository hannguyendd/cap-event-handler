# NATS JetStream transport — design

Status: approved in brainstorming on 2026-10-03; awaiting written-spec review.

This spec extends `2026-09-17-cap-event-handler-design.md` (the "base spec"). Everything there
still holds unless this document says otherwise. How this transport differs from .NET CAP's
NATS transport, and why, is recorded in `docs/cap-porting-issues.md` (rows N1–N6 and the
section "Why this library does not copy CAP's NATS consume model").

## 1. Purpose and scope

Add NATS JetStream as an **alternative production transport** to SNS/SQS. Each `CapApp` uses
exactly one transport, chosen by the object passed as `transport=`. Running both in one app is
out of scope.

The guarantee table (base spec §1.2) is unchanged. JetStream takes SNS/SQS's place in the rows
that mention it:

| Boundary | Guarantee | Mechanism on JetStream |
|---|---|---|
| Business write ↔ outbox row | **Atomic** | Unchanged |
| Outbox row ↔ broker delivery | Eventual, at-least-once | Post-commit flush + `OutboxSweeper`; JetStream additionally drops re-sends inside its duplicate window |
| Inbound message ↔ handler's DB writes | **Atomic** | Unchanged: the message is acked only after the transaction commits |
| Poison messages | Quarantined, not lost | Library-implemented dead-letter stream (§5), because JetStream has no redrive policy |

Requires **NATS server 2.10 or later** (consumers with multiple `filter_subjects`) and
**`nats-py` 2.16 or later**.

Base spec §1.1 and §11 are updated to say this transport is in scope.

## 2. Packaging

- `nats-py>=2.16` is an **optional extra**: `cap-event-handler[nats]`. SNS/SQS users do not
  install it.
- `src/cap_event_handler/transport/nats.py` imports `nats` at module level. Nothing else in the
  package imports `transport/nats.py`, so the core import never requires the extra.
- `nats-py` ships inline type hints; no stub package is added. mypy strict covers the module.

## 3. Configuration

New frozen dataclass in `config.py`, next to `SnsSqsConfig`:

```python
@dataclass(frozen=True, slots=True)
class NatsConfig:
    servers: str | Sequence[str] = "nats://127.0.0.1:4222"
    stream: str = "CAP"
    subject_prefix: str = "cap"
    dlq_stream: str = "CAP_DLQ"
    dlq_subject_prefix: str = "cap-dlq"
    ack_wait: timedelta = timedelta(seconds=60)
    max_deliver: int = 5
    max_age: timedelta = timedelta(days=7)
    duplicate_window: timedelta = timedelta(minutes=5)
    num_replicas: int = 1
```

Validation in `__post_init__` (raises `ConfigurationError`):

- `max_deliver >= 2`. With 1 there would be no retry before dead-lettering.
- `stream != dlq_stream`. Also, neither of `subject_prefix` and `dlq_subject_prefix` may be a
  token-prefix of the other, because a subject cannot belong to two streams.
- Both prefixes and both stream names match `[A-Za-z0-9_-]+`.

Defaults explained:

- `ack_wait` 60s exceeds `Settings.expected_max_handler_seconds` (30s), so the default
  configuration passes startup validation.
- `max_age` 7 days is below `Settings.inbox_retention` (15 days), so the dedup marker outlives
  every message the stream could redeliver.
- `duplicate_window` 5 minutes covers a sweeper re-send under default settings
  (`sweeper_lookback` 60s + `sweeper_interval` 30s), so the common re-send is dropped by
  JetStream. This is an optimisation, not a guarantee: the inbox absorbs anything that gets past it.

`Settings` is unchanged. Its `max_messages <= 10` and `wait_seconds <= 20` limits are SQS limits,
and they are valid for a JetStream fetch too.

## 4. Transport mapping

`NatsJetStreamTransport(config: NatsConfig, *, provision: bool = True)` implements the
`Transport` Protocol. It owns one `nats.aio.client.Client`, connected lazily on first use and
closed by `close()`. `nats-py`'s automatic reconnect stays **on**. CAP turns it off.

Names reach the transport already normalised by `normalize_topic` to `[A-Za-z0-9_-]`. These are
valid NATS subject tokens and consumer names, so `app.py` and `publisher.py` do not change.

| Method | Behaviour |
|---|---|
| `ensure_topic(name)` | Returns `f"{subject_prefix}.{name}"`. On first call, ensures the streams (below). |
| `ensure_queue(group)` | Ensures the streams and returns `group` as the durable consumer name. It creates no consumer, because one with no filter would receive every subject in the stream. `subscribe` creates it. |
| `subscribe(queue_id, subjects)` | `add_consumer` on `stream` with `durable_name=queue_id`, `filter_subjects=sorted(subjects)`, `ack_policy=explicit`, `ack_wait`, `max_deliver`, `deliver_policy=new`. On 2.10+ this creates or updates. `deliver_policy=new` matches a freshly created SQS queue: messages published before the group existed are not delivered. |
| `publish(subject, body, headers)` | `js.publish(subject, body, headers=headers + {"Nats-Msg-Id": headers[MSG_ID]}, stream=stream)`. Raises if the publish is not acked, so the publisher treats it as a failed send. |
| `receive(queue_id, max_messages, wait_seconds)` | Pull fetch on a bound pull subscription (`pull_subscribe_bind`, cached per `queue_id`), `batch=max_messages`, `timeout=max(wait_seconds, 1)`. A fetch timeout returns `[]`. Each message becomes a `ReceivedMessage` with `headers` (minus `Nats-*` headers), `body` (UTF-8), `receipt=msg.reply` and `receive_count=msg.metadata.num_delivered`. |
| `ack(queue_id, receipt)` | Publishes `+ACK` to the receipt subject. |
| `reject(queue_id, message, *, visibility_seconds, reason)` | §5. |
| `queue_attributes(queue_id)` | From `consumer_info` and `stream_info`: `visibility_timeout = ack_wait`, `message_retention = stream max_age` (seconds; unlimited reports as a very large value), `has_redrive = max_deliver > 0 and the DLQ stream exists`, `subscribed_topics = filter_subjects`. |

**Ensuring streams.** When `provision` is set: if `stream` is missing, create it with subjects
`[f"{subject_prefix}.>"]`, file storage, `max_age`, `duplicate_window` and `num_replicas`. If
`dlq_stream` is missing, create it with subjects `[f"{dlq_subject_prefix}.>"]` and file storage,
with no `max_age`, because dead letters are kept until an operator removes them. **Existing
streams are never modified**, since operators may have tuned them. The transport only checks
that the main stream's subjects cover `f"{subject_prefix}.>"` and raises `ConfigurationError`
if they do not. When `provision` is off, a missing stream or consumer raises
`ConfigurationError` at startup, as missing SQS topics do (base spec §9).

**A missing consumer.** An app with no handlers never calls `subscribe`, so its consumer does
not exist. `receive` then returns `[]`, and `queue_attributes` reports the configured `ack_wait`
and `max_deliver` with no subscribed topics. With `provision` off, a registered handler whose
consumer is missing raises `ConfigurationError` through the existing "not subscribed" check.

The receipt is the message's reply subject (`$JS.ACK.…`), so `ack` and `reject` keep no
per-message state, the same as an SQS receipt handle.

## 5. Rejecting and dead-lettering

### 5.1 Interface change

`Transport.reject` changes from

```python
async def reject(self, queue_id: str, receipt: str, *, visibility_seconds: int) -> None
```

to

```python
async def reject(
    self, queue_id: str, message: ReceivedMessage, *, visibility_seconds: int, reason: str
) -> None
```

This changes the internal seam and is not a public API change. `reject` is called only by
`Consumer`.

- **SNS/SQS:** uses `message.receipt` as before and ignores `reason`. SQS's redrive policy
  quarantines the message.
- **Memory transport:** records `(message, reason)` so tests can assert on it.
- **NATS:** below.

`Consumer._reject(message, reason)` passes a reason from each of its four failure paths:

| Path | `reason` |
|---|---|
| Undecodable envelope | `"undecodable: <error>"` |
| No handler, `on_unroutable="reject"` | `"unroutable: no handler for <name> in group <group>"` |
| Payload validation failed | `"invalid payload: <error>"` |
| Handler raised | `"handler failed: <ExceptionType>: <message>"` |

Each reason is truncated to 1000 characters.

### 5.2 NATS reject

```
if message.receive_count < max_deliver:
    publish  -NAK {"delay": visibility_seconds * 1e9}  to message.receipt
    return
# final allowed delivery
try:
    js.publish(f"{dlq_subject_prefix}.{queue_id}", message.body,
               headers = message.headers + {
                   DLQ_REASON: reason, DLQ_GROUP: queue_id,
                   DLQ_DELIVERIES: str(message.receive_count),
                   "Nats-Msg-Id": f"{msg_id}:{queue_id}"   # only when cap-msg-id is present
               },
               stream = dlq_stream)
except Exception:
    log ERROR "dead-letter publish failed for <id> (<name>) in <group> after <n> deliveries;
               JetStream will drop it at max_deliver: <reason>"
    publish -NAK (as above)
    return
ack(message.receipt)
log ERROR "dead-lettered <id> (<name>) in <group> after <n> deliveries: <reason>"
```

- `DLQ_REASON = "cap-dlq-reason"`, `DLQ_GROUP = "cap-dlq-group"` and
  `DLQ_DELIVERIES = "cap-dlq-deliveries"` are new constants in `envelope.py`. They are pinned in
  `tests/test_envelope.py` and never hard-coded elsewhere.
- The `Nats-Msg-Id` on the dead-letter publish makes it idempotent if the final delivery is
  somehow handled twice.
- After the dead-letter publish fails and the message is nak'd, JetStream does not redeliver it,
  because `max_deliver` is reached. It emits a `MAX_DELIVERIES` advisory. That loss is why the
  failure is logged at `ERROR` with the full context.
- If acking the original fails after a successful dead-letter publish, the failure is logged at
  `WARNING`. Since this was the last allowed delivery, JetStream does not redeliver, so the
  message is in the DLQ exactly once.

### 5.3 Other failures

| Situation | Behaviour |
|---|---|
| Fetch timeout | `[]` |
| Connection lost | `nats-py` reconnects. Other errors propagate exactly as SQS errors do today. |
| Publish not acked or timed out | Raises; the publisher counts a failed send and the row stays for the sweeper. |
| `ack` / `-NAK` publish fails | Logged at `WARNING`. JetStream redelivers after `ack_wait`, and the inbox absorbs the duplicate. |

## 6. Startup validation

`validate_runtime` (base spec §9) needs no NATS-specific code. With the mapping in §4 it warns
when:

- `ack_wait < expected_max_handler_seconds`;
- `inbox_retention <= stream max_age`;
- there is no dead-letter route (`max_deliver` unlimited or the DLQ stream missing).

The existing warning text names SQS concepts (`VisibilityTimeout`, `MessageRetentionPeriod`,
"redrive policy"). It is reworded to be transport-neutral. For example, "queue visibility
timeout / ack wait".

## 7. Testing

Tests are written first.

- **`tests/test_nats_unit.py`** (no Docker):
  - building the `-NAK` delay payload;
  - header translation in both directions, including stripping `Nats-*`;
  - `NatsConfig` validation;
  - mapping consumer and stream info to `QueueAttributes`;
  - the §5.2 decision, run against a small fake JetStream client. It checks nak-before-limit,
    publish-then-ack at the limit, nak-on-DLQ-publish-failure, the headers set, and that both
    `ERROR` logs are emitted (`caplog`).
- **Memory transport and `tests/test_consumer.py`:** the new `reject` signature, and each of the
  four failure paths passing its reason.
- **`tests/test_guarantees.py`:** unchanged, and must stay green.
- **`tests/integration/test_nats.py`** (marker `integration`): a `nats:2.10` container with `-js`,
  started with testcontainers' generic `DockerContainer`, plus the existing PostgreSQL fixture. It
  covers:
  - provisioning, including the idempotent second start;
  - `provision=False` against missing streams;
  - publish → handler round trip;
  - redelivery after a nak with delay;
  - dead-lettering after `max_deliver`, with the DLQ headers checked;
  - a duplicate `Nats-Msg-Id` dropped within the window;
  - the atomic-consume guarantee over NATS: handler raises after writing, so nothing commits
    and the message is redelivered.
- `./check.sh` must end in `GATE: PASS`.

## 8. Documentation

- `docs/usage.md`: a NATS section covering:
  - installing the extra and the server 2.10+ requirement;
  - `NatsConfig` settings;
  - what provisioning does and does not touch;
  - inspecting and replaying the DLQ (`nats stream view CAP_DLQ`, republishing by hand until an
    API exists);
  - the one-stream-limits trade-off.
- Base spec: §1.1 and §11 updated to put this transport in scope.
- `docs/cap-porting-issues.md`: the "(in design)" marker is removed once implemented.
- `CLAUDE.md`: the architecture paragraph mentions `transport/nats.py`.

## 9. Not in scope

- Running SNS/SQS and NATS in one `CapApp`.
- A DLQ inspection and replay API (base spec §11 still defers it).
- Per-event stream limits (CAP's stream-per-prefix layout).
- Ack-progress heartbeats (`+WPI`) for long handlers. The base spec's visibility-heartbeat
  deferral covers this.
- NATS authentication beyond what a `nats://user:pass@host` URL or `nats-py` connect defaults
  provide. Passing extra connect options is a follow-up if needed.
