# NATS JetStream Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add NATS JetStream as an alternative production transport to SNS/SQS, with a
library-implemented dead-letter stream because JetStream has no redrive policy.

**Architecture:** A new `NatsJetStreamTransport` (`src/cap_event_handler/transport/nats.py`)
implements the existing `Transport` Protocol over `nats-py`. It uses one shared stream for every
event subject, one durable pull consumer per group, and a separate dead-letter stream.
`Transport.reject` changes to receive the whole `ReceivedMessage` and a failure `reason`, so the
NATS transport can dead-letter on the last allowed delivery. `CapApp`, the publisher and the
consumer loop are otherwise unchanged.

**Tech Stack:** Python 3.11+, `nats-py>=2.16` (optional extra `nats`), SQLAlchemy 2.x async,
pytest + pytest-asyncio, testcontainers (generic `DockerContainer` running `nats:2.10 -js`).

**Spec:** `docs/superpowers/specs/2026-10-03-nats-jetstream-transport-design.md` (extends
`docs/superpowers/specs/2026-09-17-cap-event-handler-design.md`). How and why it differs from
.NET CAP is in `docs/cap-porting-issues.md` rows N1–N6. Read the spec before starting.

## Global Constraints

- NATS server **2.10 or later**; `nats-py>=2.16`.
- `nats-py` is an **optional extra** `cap-event-handler[nats]`. Nothing outside
  `transport/nats.py` imports `nats`, and `cap_event_handler/__init__.py` must not import
  `transport/nats.py`.
- `NatsConfig` defaults: `servers="nats://127.0.0.1:4222"`, `stream="CAP"`,
  `subject_prefix="cap"`, `dlq_stream="CAP_DLQ"`, `dlq_subject_prefix="cap-dlq"`,
  `ack_wait=60s`, `max_deliver=5`, `max_age=7 days`, `duplicate_window=5 minutes`,
  `num_replicas=1`.
- Subjects: event `X` → `f"{subject_prefix}.{X}"`; dead letters → `f"{dlq_subject_prefix}.{group}"`.
- Streams use **file storage**. Existing streams are **never modified**.
- Consumers: durable pull, `ack_policy=explicit`, `deliver_policy=new`, `filter_subjects`.
- Message header names live in `envelope.py` as constants. New: `HEADER_DLQ_REASON =
  "cap-dlq-reason"`, `HEADER_DLQ_GROUP = "cap-dlq-group"`, `HEADER_DLQ_DELIVERIES =
  "cap-dlq-deliveries"`. Never hard-code `cap-*` strings elsewhere in `src/`.
- Dead-lettering and a failed dead-letter publish both log at `ERROR`. A failed ack/nak logs at
  `WARNING`.
- Failure reasons are truncated to 1000 characters.
- `uv` manages dependencies. Never run `pip` and never edit `uv.lock` by hand.
- Every commit passes `./check.sh` and its last line reads `GATE: PASS`. Don't pipe it through
  `tail`/`head`.
- Commit messages follow `docs/commit-convention.md`. The hook in `.githooks/commit-msg` is
  enabled; never use `--no-verify`. End every commit message with
  `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Tests are written first. `tests/test_guarantees.py` must stay green and unedited.

## Review Focus

1. **A multi-line failure reason.** Pydantic's `ValidationError` text spans several lines, and
   NATS headers cannot contain CR/LF. Dead-lettering must still succeed, with line breaks
   replaced by spaces. Test in Task 4.
2. **A message body that is not valid UTF-8.** It must not crash the consumer loop. It decodes
   with replacement characters, fails envelope decoding, is rejected and eventually
   dead-lettered. Test in Task 3.
3. **An operator-created consumer whose `max_deliver` is below `NatsConfig.max_deliver`.**
   JetStream would drop the message before the library dead-letters it. `queue_attributes` must
   report `has_redrive=False` so startup warns. Test in Task 3.
4. **An app with no handlers, so the consumer was never created.** `receive` must return `[]`
   after waiting, not raise or busy-spin. Test in Task 3.
5. **`duplicate_window` longer than `max_age`.** NATS rejects that stream with an opaque
   server error. `NatsConfig` must reject it at construction with a clear message. Test in
   Task 2.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `src/cap_event_handler/transport/base.py` | Modify | `Transport.reject` takes `message: ReceivedMessage` and `reason: str` |
| `src/cap_event_handler/transport/memory.py` | Modify | New `reject` signature; records `(message, reason)` in `self.rejected` |
| `src/cap_event_handler/transport/snssqs.py` | Modify | New `reject` signature; ignores `reason` |
| `src/cap_event_handler/consumer.py` | Modify | Each failure path passes a reason; `_reject` truncates it |
| `src/cap_event_handler/config.py` | Modify | `NatsConfig` |
| `src/cap_event_handler/__init__.py` | Modify | Export `NatsConfig` |
| `src/cap_event_handler/validation.py` | Modify | Transport-neutral warning text |
| `src/cap_event_handler/envelope.py` | Modify | `HEADER_DLQ_*` constants |
| `src/cap_event_handler/transport/nats.py` | Create | `NatsJetStreamTransport` |
| `pyproject.toml`, `uv.lock` | Modify (via `uv add`) | `nats` extra and dev dependency |
| `tests/test_transport_memory.py`, `tests/test_consumer.py` | Modify | New `reject` signature, reasons |
| `tests/test_config.py`, `tests/test_validation.py`, `tests/test_envelope.py` | Modify | `NatsConfig`, warning text, header pins |
| `tests/test_nats_unit.py` | Create | Transport behaviour against a fake `nats-py` client |
| `tests/integration/conftest.py`, `tests/integration/test_nats.py` | Modify / Create | Real `nats-server` |
| `docs/usage.md`, base spec, `docs/cap-porting-issues.md`, `CLAUDE.md`, `tests/integration/CLAUDE.md`, `README.md` | Modify | Documentation |

---

### Task 1: `reject` receives the message and a reason

**Files:**
- Modify: `src/cap_event_handler/transport/base.py` (the `reject` method in `Transport`)
- Modify: `src/cap_event_handler/transport/memory.py` (`__init__`, `reject`)
- Modify: `src/cap_event_handler/transport/snssqs.py` (`reject`)
- Modify: `src/cap_event_handler/consumer.py` (`handle`, `_reject`)
- Test: `tests/test_transport_memory.py`, `tests/test_consumer.py`

**Interfaces:**
- Produces:
  `Transport.reject(self, queue_id: str, message: ReceivedMessage, *, visibility_seconds: int, reason: str) -> None`.
  `MemoryTransport.rejected: list[tuple[ReceivedMessage, str]]`.
  `cap_event_handler.consumer.MAX_REASON_LENGTH = 1000`.

- [ ] **Step 1: Update the memory-transport tests to the new signature**

In `tests/test_transport_memory.py`, change the two existing `reject` calls and add one test:

```python
async def test_reject_makes_the_message_visible_after_the_backoff(transport, clock) -> None:
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    msg = (await transport.receive(queue_id, max_messages=10, wait_seconds=0))[0]
    await transport.reject(queue_id, msg, visibility_seconds=5, reason="boom")
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []
    clock.advance(timedelta(seconds=6))
    assert len(await transport.receive(queue_id, max_messages=10, wait_seconds=0)) == 1
```

In `test_redrive_moves_a_poison_message_to_the_dead_letter_list`, change the call to:

```python
        await transport.reject(queue_id, msgs[0], visibility_seconds=0, reason="boom")
```

Add:

```python
async def test_reject_records_the_reason(transport: MemoryTransport) -> None:
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    msg = (await transport.receive(queue_id, max_messages=10, wait_seconds=0))[0]
    await transport.reject(queue_id, msg, visibility_seconds=0, reason="handler failed: X")
    assert transport.rejected == [(msg, "handler failed: X")]
```

- [ ] **Step 2: Add reason assertions to the consumer tests**

In `tests/test_consumer.py`, append one assertion to each existing reject test, right after its
`drain_once()` call:

`test_a_raising_handler_rolls_back_and_rejects`:
```python
    assert transport.rejected[-1][1] == "handler failed: RuntimeError: handler exploded"
```

`test_unroutable_message_is_rejected_by_default`:
```python
    assert transport.rejected[-1][1] == (
        "unroutable: no handler for 'nobody.listens' in group 'g'"
    )
```

`test_invalid_payload_is_rejected_and_the_handler_never_runs`:
```python
    assert transport.rejected[-1][1].startswith("invalid payload: ")
```

`test_undecodable_message_is_rejected` (after `await consumer.drain_once()`):
```python
    assert transport.rejected[-1][1].startswith("undecodable: ")
```

Add a truncation test at the end of the file:

```python
async def test_a_long_failure_reason_is_truncated(storage, transport, settings) -> None:
    from cap_event_handler.consumer import MAX_REASON_LENGTH

    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        raise RuntimeError("x" * 5000)

    registry.register("order.paid", Payload, handler)
    await deliver(transport)
    await make_consumer(storage, transport, registry, settings).drain_once()
    reason = transport.rejected[-1][1]
    assert len(reason) == MAX_REASON_LENGTH
    assert reason.startswith("handler failed: RuntimeError: xxx")
```

- [ ] **Step 3: Run the tests and watch them fail**

Run: `uv run pytest tests/test_transport_memory.py tests/test_consumer.py -q`
Expected: FAIL. Errors include `TypeError: MemoryTransport.reject() got an unexpected keyword argument 'reason'` and `AttributeError: 'MemoryTransport' object has no attribute 'rejected'`.

- [ ] **Step 4: Change the Protocol**

In `src/cap_event_handler/transport/base.py`, replace `reject` with:

```python
    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        """Release the lease so the broker redelivers after `visibility_seconds`.

        `reason` says why the message failed. A transport whose broker has no
        redrive policy uses it to dead-letter the message on its last delivery;
        the others ignore it.
        """
        ...
```

- [ ] **Step 5: Update the memory transport**

In `src/cap_event_handler/transport/memory.py`, add to `__init__` after `self.dead_letter`:

```python
        self.rejected: list[tuple[ReceivedMessage, str]] = []
```

Replace `reject` with:

```python
    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        self.rejected.append((message, reason))
        now = self._clock()
        queue = self._queues[queue_id]
        for msg in queue.messages:
            if msg.receipt != message.receipt:
                continue
            msg.visible_at = now + timedelta(seconds=visibility_seconds)
            if self.max_receive_count is not None and msg.receive_count >= self.max_receive_count:
                queue.messages.remove(msg)
                self.dead_letter.append(
                    ReceivedMessage(
                        headers=dict(msg.headers),
                        body=msg.body,
                        receipt=msg.receipt,
                        receive_count=msg.receive_count,
                    )
                )
            return
```

- [ ] **Step 6: Update the SNS/SQS transport**

In `src/cap_event_handler/transport/snssqs.py`, replace `reject` with:

```python
    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        del reason  # SQS's redrive policy does the dead-lettering
        _, sqs = await self._clients()
        await sqs.change_message_visibility(
            QueueUrl=queue_id,
            ReceiptHandle=message.receipt,
            VisibilityTimeout=visibility_seconds,
        )
```

- [ ] **Step 7: Pass reasons from the consumer**

In `src/cap_event_handler/consumer.py`, add below `logger = logging.getLogger(__name__)`:

```python
MAX_REASON_LENGTH = 1000
"""Failure reasons are cut to this length before they reach the transport."""
```

In `handle`, change the four `_reject` calls:

```python
        except CapError as exc:
            self.last_error = exc
            logger.error("undecodable message on %s: %r", self._queue_id, exc)
            await self._reject(message, f"undecodable: {exc}")
            return
```

```python
            if self._settings.on_unroutable == "ack":
                await self._transport.ack(self._queue_id, message.receipt)
            else:
                await self._reject(
                    message,
                    f"unroutable: no handler for {envelope.name!r} in group {self._group!r}",
                )
            return
```

```python
            await self._reject(message, f"invalid payload: {exc}")
            return
```

```python
        except Exception as exc:
            self.last_error = exc
            logger.exception("handler for %s (%s) failed", envelope.id, envelope.name)
            await self._reject(message, f"handler failed: {type(exc).__name__}: {exc}")
            return
```

Replace `_reject` with:

```python
    async def _reject(self, message: ReceivedMessage, reason: str) -> None:
        await self._transport.reject(
            self._queue_id,
            message,
            visibility_seconds=self._settings.reject_backoff_seconds,
            reason=reason[:MAX_REASON_LENGTH],
        )
```

- [ ] **Step 8: Run the tests and watch them pass**

Run: `uv run pytest tests/test_transport_memory.py tests/test_consumer.py tests/test_guarantees.py -q`
Expected: PASS

- [ ] **Step 9: Run the gate**

Run: `./check.sh`
Expected: last line `GATE: PASS`

- [ ] **Step 10: Commit**

```bash
git add src/cap_event_handler/transport/base.py src/cap_event_handler/transport/memory.py \
  src/cap_event_handler/transport/snssqs.py src/cap_event_handler/consumer.py \
  tests/test_transport_memory.py tests/test_consumer.py
git commit -F - <<'EOF'
refactor(transport): pass the message and a failure reason to reject

A broker without a redrive policy (JetStream) must dead-letter the
message itself on its last delivery, which needs the message body,
headers, delivery count and why it failed. SQS ignores the reason.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 2: `NatsConfig` and transport-neutral startup warnings

**Files:**
- Modify: `src/cap_event_handler/config.py`
- Modify: `src/cap_event_handler/__init__.py`
- Modify: `src/cap_event_handler/validation.py` (three warning strings in `validate_runtime`)
- Test: `tests/test_config.py`, `tests/test_validation.py`

**Interfaces:**
- Produces: `cap_event_handler.config.NatsConfig`, a frozen dataclass with fields
  `servers: str | tuple[str, ...]`, `stream: str`, `subject_prefix: str`, `dlq_stream: str`,
  `dlq_subject_prefix: str`, `ack_wait: timedelta`, `max_deliver: int`, `max_age: timedelta`,
  `duplicate_window: timedelta`, `num_replicas: int`. It is exported from the package root.
  `servers` is a tuple rather than the spec's `Sequence` so the frozen dataclass stays hashable.

- [ ] **Step 1: Write the failing config tests**

Append to `tests/test_config.py`:

```python
from datetime import timedelta as _td

import pytest as _pytest

from cap_event_handler import NatsConfig
from cap_event_handler.errors import ConfigurationError as _ConfigurationError


def test_nats_config_defaults_match_the_spec() -> None:
    c = NatsConfig()
    assert c.servers == "nats://127.0.0.1:4222"
    assert (c.stream, c.subject_prefix) == ("CAP", "cap")
    assert (c.dlq_stream, c.dlq_subject_prefix) == ("CAP_DLQ", "cap-dlq")
    assert c.ack_wait == _td(seconds=60)
    assert c.max_deliver == 5
    assert c.max_age == _td(days=7)
    assert c.duplicate_window == _td(minutes=5)
    assert c.num_replicas == 1


@_pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_deliver": 1}, "max_deliver"),
        ({"stream": "X", "dlq_stream": "X"}, "dlq_stream"),
        ({"subject_prefix": "x", "dlq_subject_prefix": "x"}, "dlq_subject_prefix"),
        ({"subject_prefix": "a.b"}, "subject_prefix"),
        ({"stream": "has space"}, "stream"),
        ({"ack_wait": _td(0)}, "ack_wait"),
        ({"num_replicas": 0}, "num_replicas"),
        ({"max_age": _td(minutes=1), "duplicate_window": _td(minutes=5)}, "duplicate_window"),
    ],
)
def test_nats_config_rejects_unusable_values(kwargs, match) -> None:
    with _pytest.raises(_ConfigurationError, match=match):
        NatsConfig(**kwargs)


def test_nats_config_allows_unlimited_retention() -> None:
    assert NatsConfig(max_age=_td(0)).max_age == _td(0)
```

- [ ] **Step 2: Write the failing validation-wording test**

Append to `tests/test_validation.py`:

```python
async def test_warnings_name_the_nats_equivalents(clock) -> None:
    transport = MemoryTransport(
        clock=clock, visibility_timeout=10, message_retention=1_209_600, max_receive_count=None
    )
    queue_id, topic_ids = await _wire(transport)
    warnings = await validate_runtime(
        settings=Settings(
            schema_name=None, inbox_retention=timedelta(days=1), expected_max_handler_seconds=60
        ),
        transport=transport,
        queue_id=queue_id,
        registry=_registry(),
        topic_ids=topic_ids,
    )
    text = " ".join(warnings)
    assert "ack_wait" in text
    assert "max_age" in text
    assert "dead-letter stream" in text
```

- [ ] **Step 3: Run the tests and watch them fail**

Run: `uv run pytest tests/test_config.py tests/test_validation.py -q`
Expected: FAIL with `ImportError: cannot import name 'NatsConfig'` (the whole of `test_config.py`
errors), and `test_warnings_name_the_nats_equivalents` failing on `"ack_wait" in text`.

- [ ] **Step 4: Implement `NatsConfig`**

In `src/cap_event_handler/config.py`, add `import re` to the imports and append:

```python
_NATS_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class NatsConfig:
    """Where to reach NATS, and the shape of the JetStream streams and consumers.

    One stream (`stream`) captures every event subject under `subject_prefix`;
    dead letters go to `dlq_stream` under `dlq_subject_prefix`. Defaults pass
    startup validation against the default `Settings`.
    """

    servers: str | tuple[str, ...] = "nats://127.0.0.1:4222"
    stream: str = "CAP"
    subject_prefix: str = "cap"
    dlq_stream: str = "CAP_DLQ"
    dlq_subject_prefix: str = "cap-dlq"
    ack_wait: timedelta = timedelta(seconds=60)
    max_deliver: int = 5
    """Deliveries before the library dead-letters a failing message. At least 2."""
    max_age: timedelta = timedelta(days=7)
    """Stream retention. Zero means unlimited."""
    duplicate_window: timedelta = timedelta(minutes=5)
    num_replicas: int = 1

    def __post_init__(self) -> None:
        for name in ("stream", "subject_prefix", "dlq_stream", "dlq_subject_prefix"):
            value = getattr(self, name)
            if not _NATS_NAME_RE.match(value):
                raise ConfigurationError(
                    f"{name} must match {_NATS_NAME_RE.pattern} (one subject token), "
                    f"got {value!r}"
                )
        if self.dlq_stream == self.stream:
            raise ConfigurationError("dlq_stream must differ from stream")
        if self.dlq_subject_prefix == self.subject_prefix:
            raise ConfigurationError(
                "dlq_subject_prefix must differ from subject_prefix: "
                "a subject cannot belong to two streams"
            )
        if self.max_deliver < 2:
            raise ConfigurationError(
                f"max_deliver must be at least 2 so a failure is retried before it is "
                f"dead-lettered, got {self.max_deliver}"
            )
        if self.ack_wait <= timedelta(0):
            raise ConfigurationError("ack_wait must be positive")
        if self.duplicate_window <= timedelta(0):
            raise ConfigurationError("duplicate_window must be positive")
        if self.max_age < timedelta(0):
            raise ConfigurationError("max_age must be zero (unlimited) or positive")
        if self.max_age and self.duplicate_window > self.max_age:
            raise ConfigurationError(
                f"duplicate_window ({self.duplicate_window}) must not exceed max_age "
                f"({self.max_age}); NATS refuses such a stream"
            )
        if not 1 <= self.num_replicas <= 5:
            raise ConfigurationError(
                f"num_replicas must be between 1 and 5, got {self.num_replicas}"
            )
```

- [ ] **Step 5: Export it**

In `src/cap_event_handler/__init__.py`, change the config import to
`from cap_event_handler.config import NatsConfig, Settings, SnsSqsConfig` and add `"NatsConfig"`
to `__all__`, keeping it sorted (between `"HandlerTransactionError"` and
`"PayloadValidationError"`).

- [ ] **Step 6: Reword the warnings**

In `src/cap_event_handler/validation.py`, `validate_runtime`, replace the three warning strings:

```python
        warnings.append(
            f"inbox_retention ({inbox_seconds:.0f}s) does not exceed the queue's message "
            f"retention ({attrs.message_retention}s; SQS MessageRetentionPeriod, NATS stream "
            "max_age): a message redelivered after its deduplication marker expires will be "
            "handled twice"
        )
```

```python
        warnings.append(
            f"queue VisibilityTimeout ({attrs.visibility_timeout}s; ack_wait on NATS) is below "
            f"expected_max_handler_seconds ({settings.expected_max_handler_seconds}s): "
            "slow handlers will be redelivered while still running"
        )
```

```python
        warnings.append(
            "queue has no redrive policy (on NATS: no dead-letter stream, or a consumer "
            "max_deliver below NatsConfig.max_deliver): a message that always fails will be "
            "retried forever or dropped instead of being dead-lettered"
        )
```

The existing tests match on `inbox_retention`, `VisibilityTimeout` and `redrive`, and all three
are still present.

- [ ] **Step 7: Run the tests and watch them pass**

Run: `uv run pytest tests/test_config.py tests/test_validation.py -q`
Expected: PASS

- [ ] **Step 8: Run the gate**

Run: `./check.sh`
Expected: last line `GATE: PASS`

- [ ] **Step 9: Commit**

```bash
git add src/cap_event_handler/config.py src/cap_event_handler/__init__.py \
  src/cap_event_handler/validation.py tests/test_config.py tests/test_validation.py
git commit -F - <<'EOF'
feat(config): add NatsConfig and name NATS settings in warnings

NatsConfig describes the shared JetStream stream, the dead-letter
stream and the per-group consumer. It rejects values NATS would refuse
(duplicate_window above max_age) or that defeat dead-lettering
(max_deliver below 2). servers is a tuple, not the spec's Sequence,
so the frozen dataclass stays hashable.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 3: `NatsJetStreamTransport`: provisioning, publish, receive, ack, attributes

**Files:**
- Modify: `pyproject.toml`, `uv.lock` (through `uv add` only)
- Create: `src/cap_event_handler/transport/nats.py`
- Create: `tests/test_nats_unit.py`

**Interfaces:**
- Consumes: `NatsConfig` (Task 2); `Transport.reject` signature (Task 1);
  `HEADER_MSG_ID` from `cap_event_handler.envelope`.
- Produces, in `cap_event_handler.transport.nats`:
  - `NatsJetStreamTransport(config: NatsConfig, *, provision: bool = True, connect: Callable[..., Awaitable[Client]] = nats.connect)`.
    `connect` exists so unit tests can inject a fake client.
  - `NATS_MSG_ID = "Nats-Msg-Id"`
  - `UNLIMITED_RETENTION = 2**31 - 1`
  - `nak_payload(delay_seconds: int) -> bytes`
  - `strip_nats_headers(headers: Mapping[str, str] | None) -> dict[str, str]`
  - private `_reply(receipt: str, payload: bytes, what: str) -> None` and `_ACK = b"+ACK"`,
    used by Task 4.
  - `reject` here only naks. Task 4 adds dead-lettering.

- [ ] **Step 1: Add the dependency**

```bash
uv add --optional nats "nats-py>=2.16"
uv add --dev "nats-py>=2.16"
```

Check that `pyproject.toml` now has `[project.optional-dependencies]` with
`nats = ["nats-py>=2.16"]`, and `nats-py>=2.16` in the `dev` group. The dev entry is what lets
mypy and the unit tests import it.

- [ ] **Step 2: Write the failing unit tests**

Create `tests/test_nats_unit.py`:

```python
"""NatsJetStreamTransport against a fake nats-py client.

What needs a real nats-server (stream creation semantics, redelivery timing,
duplicate windows) is in tests/integration/test_nats.py.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from cap_event_handler.config import NatsConfig
from cap_event_handler.envelope import HEADER_MSG_ID
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.transport.base import ReceivedMessage
from cap_event_handler.transport.nats import (
    NATS_MSG_ID,
    UNLIMITED_RETENTION,
    NatsJetStreamTransport,
    nak_payload,
    strip_nats_headers,
)


class FakeSub:
    def __init__(self, js: FakeJS) -> None:
        self.js = js

    async def fetch(self, batch: int = 1, timeout: float | None = 5):
        self.js.fetch_calls.append((batch, timeout))
        if isinstance(self.js.fetch_result, Exception):
            raise self.js.fetch_result
        return self.js.fetch_result[:batch]


class FakeJS:
    def __init__(self) -> None:
        self.streams: dict[str, SimpleNamespace] = {}
        self.consumers: dict[tuple[str, str], SimpleNamespace] = {}
        self.added_streams: list[StreamConfig] = []
        self.added_consumers: list[tuple[str, ConsumerConfig]] = []
        self.published: list[tuple[str, bytes, dict[str, str], str | None]] = []
        self.fail_publish: Exception | None = None
        self.fetch_result: list[Msg] | Exception = []
        self.fetch_calls: list[tuple[int, float | None]] = []

    async def stream_info(self, name: str):
        if name not in self.streams:
            raise NotFoundError()
        return self.streams[name]

    async def add_stream(self, config: StreamConfig):
        self.added_streams.append(config)
        self.streams[config.name] = SimpleNamespace(config=config)

    async def consumer_info(self, stream: str, consumer: str, timeout=None):
        if (stream, consumer) not in self.consumers:
            raise NotFoundError()
        return self.consumers[(stream, consumer)]

    async def add_consumer(self, stream: str, config: ConsumerConfig):
        self.added_consumers.append((stream, config))
        self.consumers[(stream, config.durable_name)] = SimpleNamespace(config=config)

    async def publish(self, subject, payload=b"", timeout=None, stream=None, headers=None):
        if self.fail_publish is not None:
            raise self.fail_publish
        self.published.append((subject, payload, dict(headers or {}), stream))

    async def pull_subscribe_bind(self, durable=None, stream=None, **kwargs):
        return FakeSub(self)


class FakeClient:
    def __init__(self) -> None:
        self.js = FakeJS()
        self.replies: list[tuple[str, bytes]] = []
        self.fail_replies: Exception | None = None
        self.closed = False

    def jetstream(self) -> FakeJS:
        return self.js

    async def publish(self, subject: str, payload: bytes = b"", **kwargs) -> None:
        if self.fail_replies is not None:
            raise self.fail_replies
        self.replies.append((subject, payload))

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def client() -> FakeClient:
    return FakeClient()


def make(client: FakeClient, *, provision: bool = True, **config) -> NatsJetStreamTransport:
    async def connect(**kwargs):
        return client

    return NatsJetStreamTransport(NatsConfig(**config), provision=provision, connect=connect)


def jetstream_msg(*, data: bytes = b"{}", headers=None, delivered: int = 1) -> Msg:
    reply = f"$JS.ACK.CAP.g.{delivered}.10.10.1700000000000000000.0"
    return Msg(_client=None, subject="cap.order-paid", reply=reply, data=data, headers=headers)


def received(*, headers=None, delivered: int = 1) -> ReceivedMessage:
    return ReceivedMessage(
        headers=headers if headers is not None else {HEADER_MSG_ID: "m1"},
        body="{}",
        receipt="$JS.ACK.CAP.g.1.10.10.1700000000000000000.0",
        receive_count=delivered,
    )


# ------------------------------------------------------------------ helpers


def test_nak_payload_without_delay() -> None:
    assert nak_payload(0) == b"-NAK"


def test_nak_payload_carries_the_delay_in_nanoseconds() -> None:
    assert nak_payload(5) == b'-NAK {"delay": 5000000000}'


def test_strip_nats_headers_drops_only_jetstream_headers() -> None:
    assert strip_nats_headers({"cap-msg-id": "1", "Nats-Msg-Id": "1", "x": "y"}) == {
        "cap-msg-id": "1",
        "x": "y",
    }
    assert strip_nats_headers(None) == {}


# ------------------------------------------------------------- provisioning


async def test_ensure_topic_returns_the_prefixed_subject(client) -> None:
    assert await make(client).ensure_topic("order-paid") == "cap.order-paid"


async def test_provisioning_creates_both_file_backed_streams_once(client) -> None:
    transport = make(client)
    await transport.ensure_topic("a")
    await transport.ensure_topic("b")
    await transport.ensure_queue("g")
    main, dlq = client.js.added_streams
    assert (main.name, main.subjects, main.storage) == ("CAP", ["cap.>"], StorageType.FILE)
    assert main.max_age == timedelta(days=7).total_seconds()
    assert main.duplicate_window == timedelta(minutes=5).total_seconds()
    assert main.num_replicas == 1
    assert (dlq.name, dlq.subjects, dlq.storage) == ("CAP_DLQ", ["cap-dlq.>"], StorageType.FILE)
    assert not dlq.max_age


async def test_existing_streams_are_not_modified(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["cap.>"]))
    client.js.streams["CAP_DLQ"] = SimpleNamespace(
        config=StreamConfig(name="CAP_DLQ", subjects=["cap-dlq.>"])
    )
    await make(client).ensure_topic("a")
    assert client.js.added_streams == []


async def test_an_existing_stream_that_misses_our_subjects_is_fatal(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["x.>"]))
    with pytest.raises(ConfigurationError, match=r"cap\.>"):
        await make(client).ensure_topic("a")


async def test_provision_false_with_no_stream_is_fatal(client) -> None:
    with pytest.raises(ConfigurationError, match="provision=False"):
        await make(client, provision=False).ensure_queue("g")
    assert client.js.added_streams == []


async def test_provision_false_tolerates_a_missing_dlq_stream(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["cap.>"]))
    assert await make(client, provision=False).ensure_queue("g") == "g"
    assert client.js.added_streams == []


async def test_ensure_queue_creates_no_consumer(client) -> None:
    await make(client).ensure_queue("g")
    assert client.js.added_consumers == []


async def test_subscribe_creates_one_filtered_durable_pull_consumer(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    await transport.subscribe("g", ["cap.b", "cap.a"])
    [(stream, config)] = client.js.added_consumers
    assert stream == "CAP"
    assert config.durable_name == "g"
    assert config.filter_subjects == ["cap.a", "cap.b"]
    assert config.ack_policy == AckPolicy.EXPLICIT
    assert config.deliver_policy == DeliverPolicy.NEW
    assert config.ack_wait == 60.0
    assert config.max_deliver == 5


# ------------------------------------------------------------------ publish


async def test_publish_sets_the_jetstream_dedup_id(client) -> None:
    await make(client).publish("cap.a", '{"v":1}', {HEADER_MSG_ID: "m1", "x": "y"})
    [(subject, payload, headers, stream)] = client.js.published
    assert (subject, payload, stream) == ("cap.a", b'{"v":1}', "CAP")
    assert headers == {HEADER_MSG_ID: "m1", "x": "y", NATS_MSG_ID: "m1"}


async def test_publish_failure_propagates(client) -> None:
    client.js.fail_publish = RuntimeError("no ack")
    with pytest.raises(RuntimeError, match="no ack"):
        await make(client).publish("cap.a", "{}", {HEADER_MSG_ID: "m1"})


# ------------------------------------------------------------------ receive


async def _subscribed(client) -> NatsJetStreamTransport:
    transport = make(client)
    await transport.ensure_queue("g")
    await transport.subscribe("g", ["cap.order-paid"])
    return transport


async def test_receive_maps_jetstream_messages(client) -> None:
    transport = await _subscribed(client)
    client.js.fetch_result = [
        jetstream_msg(data=b'{"v":1}', headers={HEADER_MSG_ID: "m1", NATS_MSG_ID: "m1"}, delivered=3)
    ]
    [msg] = await transport.receive("g", max_messages=10, wait_seconds=20)
    assert msg.body == '{"v":1}'
    assert msg.headers == {HEADER_MSG_ID: "m1"}
    assert msg.receive_count == 3
    assert msg.receipt.startswith("$JS.ACK.")
    assert client.js.fetch_calls == [(10, 20)]


async def test_receive_returns_nothing_on_fetch_timeout(client) -> None:
    transport = await _subscribed(client)
    client.js.fetch_result = NatsTimeoutError()
    assert await transport.receive("g", max_messages=10, wait_seconds=0) == []
    assert client.js.fetch_calls == [(10, 1)]  # a zero wait becomes the 1s minimum


async def test_receive_without_a_consumer_waits_and_returns_nothing(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    assert await transport.receive("g", max_messages=10, wait_seconds=0) == []
    assert client.js.fetch_calls == []


async def test_a_body_that_is_not_utf8_does_not_break_receive(client) -> None:
    transport = await _subscribed(client)
    client.js.fetch_result = [jetstream_msg(data=b"\xff\xfe{", headers={HEADER_MSG_ID: "m1"})]
    [msg] = await transport.receive("g", max_messages=10, wait_seconds=0)
    assert "�" in msg.body


# -------------------------------------------------------------- ack and nak


async def test_ack_publishes_plus_ack_to_the_receipt(client) -> None:
    await make(client).ack("g", "$JS.ACK.x")
    assert client.replies == [("$JS.ACK.x", b"+ACK")]


async def test_a_failed_ack_is_logged_not_raised(client, caplog) -> None:
    client.fail_replies = RuntimeError("disconnected")
    await make(client).ack("g", "$JS.ACK.x")
    assert any(r.levelname == "WARNING" and "ack" in r.message for r in caplog.records)


async def test_reject_before_the_limit_naks_with_the_backoff(client) -> None:
    await make(client).reject("g", received(delivered=1), visibility_seconds=5, reason="r")
    assert client.replies == [(received().receipt, b'-NAK {"delay": 5000000000}')]
    assert client.js.published == []


# --------------------------------------------------------- queue attributes


async def test_queue_attributes_map_the_consumer_and_streams(client) -> None:
    transport = await _subscribed(client)
    attrs = await transport.queue_attributes("g")
    assert attrs.visibility_timeout == 60
    assert attrs.message_retention == int(timedelta(days=7).total_seconds())
    assert attrs.has_redrive is True
    assert attrs.subscribed_topics == frozenset({"cap.order-paid"})


async def test_queue_attributes_without_a_consumer_use_the_config(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    attrs = await transport.queue_attributes("g")
    assert attrs.visibility_timeout == 60
    assert attrs.has_redrive is True
    assert attrs.subscribed_topics == frozenset()


async def test_unlimited_stream_retention_reports_a_large_value(client) -> None:
    transport = make(client, max_age=timedelta(0))
    await transport.ensure_queue("g")
    assert (await transport.queue_attributes("g")).message_retention == UNLIMITED_RETENTION


async def test_no_dlq_stream_means_no_redrive(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["cap.>"]))
    transport = make(client, provision=False)
    await transport.ensure_queue("g")
    assert (await transport.queue_attributes("g")).has_redrive is False


async def test_a_consumer_that_gives_up_before_us_means_no_redrive(client) -> None:
    transport = await _subscribed(client)
    client.js.consumers[("CAP", "g")].config.max_deliver = 3  # operator-tuned below our 5
    assert (await transport.queue_attributes("g")).has_redrive is False


async def test_an_unlimited_consumer_still_has_redrive(client) -> None:
    transport = await _subscribed(client)
    client.js.consumers[("CAP", "g")].config.max_deliver = -1
    assert (await transport.queue_attributes("g")).has_redrive is True


async def test_close_closes_the_connection(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    await transport.close()
    assert client.closed is True
```

- [ ] **Step 3: Run the tests and watch them fail**

Run: `uv run pytest tests/test_nats_unit.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'cap_event_handler.transport.nats'`

- [ ] **Step 4: Implement the transport**

Create `src/cap_event_handler/transport/nats.py`:

```python
"""NATS JetStream: one shared stream, one durable pull consumer per group.

Every event subject lives in one stream so that a group's single consumer can
filter on all of its subjects; a JetStream consumer cannot span streams.
JetStream has no redrive policy, so `reject` supplies one (spec §5). See
docs/superpowers/specs/2026-10-03-nats-jetstream-transport-design.md.

Requires the `nats` extra: `pip install cap-event-handler[nats]`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from cap_event_handler.envelope import HEADER_MSG_ID
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.transport.base import QueueAttributes, ReceivedMessage

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from nats.aio.client import Client
    from nats.js.client import JetStreamContext

    from cap_event_handler.config import NatsConfig

logger = logging.getLogger(__name__)

NATS_MSG_ID = "Nats-Msg-Id"
"""JetStream's own deduplication header, set from `cap-msg-id`."""

UNLIMITED_RETENTION = 2**31 - 1
"""What `queue_attributes` reports for a stream with no `max_age`."""

_ACK = b"+ACK"


def nak_payload(delay_seconds: int) -> bytes:
    """`-NAK`, carrying a redelivery delay when one is asked for."""
    if delay_seconds <= 0:
        return b"-NAK"
    return b"-NAK " + json.dumps({"delay": delay_seconds * 1_000_000_000}).encode()


def strip_nats_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    """The message's own headers, without the `Nats-*` ones JetStream adds."""
    return {k: v for k, v in (headers or {}).items() if not k.startswith("Nats-")}


class NatsJetStreamTransport:
    """A `Transport` backed by NATS JetStream."""

    def __init__(
        self,
        config: NatsConfig,
        *,
        provision: bool = True,
        connect: Callable[..., Awaitable[Client]] = nats.connect,
    ) -> None:
        self.config = config
        self.provision = provision
        self._connect = connect
        self._nc: Client | None = None
        self._js: JetStreamContext | None = None
        self._streams_ready = False
        self._subs: dict[str, JetStreamContext.PullSubscription] = {}

    async def _client(self) -> tuple[Client, JetStreamContext]:
        if self._nc is None or self._js is None:
            servers = self.config.servers
            self._nc = await self._connect(
                servers=servers if isinstance(servers, str) else list(servers)
            )
            self._js = self._nc.jetstream()
        return self._nc, self._js

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.close()
        self._nc = None
        self._js = None
        self._subs.clear()
        self._streams_ready = False

    # ------------------------------------------------------------ provisioning

    async def _ensure_streams(self) -> None:
        """Create missing streams when provisioning; never modify existing ones."""
        if self._streams_ready:
            return
        _, js = await self._client()
        cfg = self.config
        wanted = f"{cfg.subject_prefix}.>"
        try:
            info = await js.stream_info(cfg.stream)
        except NotFoundError:
            if not self.provision:
                raise ConfigurationError(
                    f"provision=False and JetStream stream {cfg.stream!r} does not exist"
                ) from None
            await js.add_stream(
                StreamConfig(
                    name=cfg.stream,
                    subjects=[wanted],
                    storage=StorageType.FILE,
                    max_age=cfg.max_age.total_seconds(),
                    duplicate_window=cfg.duplicate_window.total_seconds(),
                    num_replicas=cfg.num_replicas,
                )
            )
        else:
            subjects = info.config.subjects or []
            if wanted not in subjects:
                raise ConfigurationError(
                    f"JetStream stream {cfg.stream!r} does not capture {wanted!r} "
                    f"(its subjects are {subjects}); events published there would be lost"
                )
        try:
            await js.stream_info(cfg.dlq_stream)
        except NotFoundError:
            # Without provisioning a missing DLQ stream is a startup warning
            # (no redrive), not an error: see queue_attributes.
            if self.provision:
                await js.add_stream(
                    StreamConfig(
                        name=cfg.dlq_stream,
                        subjects=[f"{cfg.dlq_subject_prefix}.>"],
                        storage=StorageType.FILE,
                        num_replicas=cfg.num_replicas,
                    )
                )
        self._streams_ready = True

    async def ensure_topic(self, topic: str) -> str:
        await self._ensure_streams()
        return f"{self.config.subject_prefix}.{topic}"

    async def ensure_queue(self, name: str) -> str:
        # No consumer yet: one with no filter would receive every subject.
        await self._ensure_streams()
        return name

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None:
        _, js = await self._client()
        cfg = self.config
        await js.add_consumer(
            cfg.stream,
            ConsumerConfig(
                durable_name=queue_id,
                filter_subjects=sorted(topic_ids),
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=cfg.ack_wait.total_seconds(),
                max_deliver=cfg.max_deliver,
                deliver_policy=DeliverPolicy.NEW,
            ),
        )
        self._subs.pop(queue_id, None)

    # ----------------------------------------------------------------- publish

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None:
        _, js = await self._client()
        out = dict(headers)
        if HEADER_MSG_ID in headers:
            out[NATS_MSG_ID] = headers[HEADER_MSG_ID]
        await js.publish(topic_id, body.encode(), headers=out, stream=self.config.stream)

    # ----------------------------------------------------------------- consume

    async def _pull_subscription(self, queue_id: str) -> JetStreamContext.PullSubscription | None:
        sub = self._subs.get(queue_id)
        if sub is not None:
            return sub
        _, js = await self._client()
        try:
            await js.consumer_info(self.config.stream, queue_id)
        except NotFoundError:
            return None
        sub = await js.pull_subscribe_bind(durable=queue_id, stream=self.config.stream)
        self._subs[queue_id] = sub
        return sub

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]:
        sub = await self._pull_subscription(queue_id)
        if sub is None:
            # No handlers, so no consumer: behave like an empty long poll.
            await asyncio.sleep(wait_seconds)
            return []
        try:
            msgs = await sub.fetch(batch=max_messages, timeout=max(wait_seconds, 1))
        except NatsTimeoutError:
            return []
        return [
            ReceivedMessage(
                headers=strip_nats_headers(msg.headers),
                # Replacement, not an exception: an undecodable body must reach the
                # consumer, fail envelope decoding and be rejected like any other.
                body=msg.data.decode("utf-8", errors="replace"),
                receipt=msg.reply,
                receive_count=msg.metadata.num_delivered,
            )
            for msg in msgs
        ]

    async def _reply(self, receipt: str, payload: bytes, what: str) -> None:
        """Ack or nak by publishing to the message's reply subject.

        A lost ack or nak is not fatal: JetStream redelivers after ack_wait and
        the inbox absorbs the duplicate.
        """
        nc, _ = await self._client()
        try:
            await nc.publish(receipt, payload)
        except Exception as exc:
            logger.warning(
                "JetStream %s to %s failed; it will be redelivered after ack_wait: %r",
                what,
                receipt,
                exc,
            )

    async def ack(self, queue_id: str, receipt: str) -> None:
        await self._reply(receipt, _ACK, "ack")

    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        await self._reply(message.receipt, nak_payload(visibility_seconds), "nak")

    # -------------------------------------------------------------- validation

    async def queue_attributes(self, queue_id: str) -> QueueAttributes:
        _, js = await self._client()
        cfg = self.config
        stream = await js.stream_info(cfg.stream)
        max_age = stream.config.max_age or 0
        retention = int(max_age) if max_age > 0 else UNLIMITED_RETENTION
        try:
            await js.stream_info(cfg.dlq_stream)
            dlq_exists = True
        except NotFoundError:
            dlq_exists = False
        try:
            info = await js.consumer_info(cfg.stream, queue_id)
        except NotFoundError:
            ack_wait = cfg.ack_wait.total_seconds()
            consumer_max_deliver = cfg.max_deliver
            subjects: list[str] = []
        else:
            c = info.config
            ack_wait = c.ack_wait or 30.0
            consumer_max_deliver = c.max_deliver if c.max_deliver is not None else -1
            subjects = list(c.filter_subjects or ([c.filter_subject] if c.filter_subject else []))
        # The library dead-letters on delivery `cfg.max_deliver`; a consumer that
        # gives up earlier lets JetStream drop the message first.
        reaches_our_limit = consumer_max_deliver <= 0 or consumer_max_deliver >= cfg.max_deliver
        return QueueAttributes(
            visibility_timeout=int(ack_wait),
            message_retention=retention,
            has_redrive=dlq_exists and reaches_our_limit,
            subscribed_topics=frozenset(subjects),
        )
```

- [ ] **Step 5: Run the tests and watch them pass**

Run: `uv run pytest tests/test_nats_unit.py -q`
Expected: PASS

- [ ] **Step 6: Run the gate**

Run: `./check.sh`
Expected: last line `GATE: PASS`. If mypy complains about `nats-py` types (for example
`JetStreamContext.PullSubscription`, or `connect`'s return type `NATS` against `Client`), fix it
with the narrowest annotation that is still true. Never use a blanket `# type: ignore`. Each
ignore needs an error code and a one-line reason.

- [ ] **Step 7: Confirm the core import does not need nats**

Run: `uv run python -c "import sys, cap_event_handler; assert 'nats' not in sys.modules; print('ok')"`
Expected: `ok`

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock src/cap_event_handler/transport/nats.py tests/test_nats_unit.py
git commit -F - <<'EOF'
feat(transport): add a NATS JetStream transport

One file-backed stream captures every event subject; each group gets
one durable pull consumer filtered to its subjects. Publishing sets
Nats-Msg-Id so JetStream drops a sweeper re-send inside its duplicate
window. Existing streams are never modified. nats-py is the optional
extra cap-event-handler[nats]. Rejection only naks for now.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 4: Dead-letter on the final delivery

**Files:**
- Modify: `src/cap_event_handler/envelope.py` (header constants)
- Modify: `src/cap_event_handler/transport/nats.py` (`reject`, new `_dead_letter`, new `header_safe`)
- Test: `tests/test_envelope.py`, `tests/test_nats_unit.py`

**Interfaces:**
- Consumes: `NatsJetStreamTransport._reply`, `_ACK`, `nak_payload`, and the `FakeClient`,
  `make` and `received` helpers in `tests/test_nats_unit.py` (Task 3).
- Produces: `envelope.HEADER_DLQ_REASON`, `HEADER_DLQ_GROUP`, `HEADER_DLQ_DELIVERIES`;
  `transport.nats.header_safe(value: str) -> str`.

- [ ] **Step 1: Pin the header names**

In `tests/test_envelope.py`, add `HEADER_DLQ_DELIVERIES, HEADER_DLQ_GROUP, HEADER_DLQ_REASON`
to the import from `cap_event_handler.envelope`, and add:

```python
def test_dead_letter_header_names_are_pinned() -> None:
    assert HEADER_DLQ_REASON == "cap-dlq-reason"
    assert HEADER_DLQ_GROUP == "cap-dlq-group"
    assert HEADER_DLQ_DELIVERIES == "cap-dlq-deliveries"
```

- [ ] **Step 2: Write the failing dead-letter tests**

In `tests/test_nats_unit.py`, extend the imports:

```python
from cap_event_handler.envelope import (
    HEADER_DLQ_DELIVERIES,
    HEADER_DLQ_GROUP,
    HEADER_DLQ_REASON,
    HEADER_MSG_ID,
    HEADER_MSG_NAME,
)
from cap_event_handler.transport.nats import header_safe
```

Append:

```python
# ------------------------------------------------------------ dead-lettering


async def test_final_delivery_is_dead_lettered_then_acked(client, caplog) -> None:
    msg = received(headers={HEADER_MSG_ID: "m1", HEADER_MSG_NAME: "order.paid"}, delivered=5)
    await make(client).reject("g", msg, visibility_seconds=5, reason="handler failed: E: boom")

    [(subject, payload, headers, stream)] = client.js.published
    assert (subject, payload, stream) == ("cap-dlq.g", b"{}", "CAP_DLQ")
    assert headers == {
        HEADER_MSG_ID: "m1",
        HEADER_MSG_NAME: "order.paid",
        HEADER_DLQ_REASON: "handler failed: E: boom",
        HEADER_DLQ_GROUP: "g",
        HEADER_DLQ_DELIVERIES: "5",
        NATS_MSG_ID: "m1:g",
    }
    assert client.replies == [(msg.receipt, b"+ACK")]
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "dead-lettered" in errors[0].message
    assert "m1" in errors[0].message
    assert "order.paid" in errors[0].message


async def test_a_failed_dead_letter_publish_naks_and_logs(client, caplog) -> None:
    client.js.fail_publish = RuntimeError("dlq down")
    msg = received(delivered=5)
    await make(client).reject("g", msg, visibility_seconds=5, reason="r")
    assert client.replies == [(msg.receipt, b'-NAK {"delay": 5000000000}')]
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "will drop it" in errors[0].message


async def test_a_message_without_an_id_is_dead_lettered_without_a_dedup_id(client) -> None:
    await make(client).reject("g", received(headers={}, delivered=5), visibility_seconds=0, reason="undecodable: x")
    [(_, _, headers, _)] = client.js.published
    assert NATS_MSG_ID not in headers
    assert headers[HEADER_DLQ_REASON] == "undecodable: x"


async def test_a_multi_line_reason_becomes_one_header_line(client) -> None:
    reason = "invalid payload: 1 validation error for P\nvalue\r\n  Field required"
    await make(client).reject("g", received(delivered=5), visibility_seconds=0, reason=reason)
    [(_, _, headers, _)] = client.js.published
    assert headers[HEADER_DLQ_REASON] == (
        "invalid payload: 1 validation error for P value   Field required"
    )
    assert client.replies[-1][1] == b"+ACK"


def test_header_safe_replaces_line_breaks() -> None:
    assert header_safe("a\r\nb\nc\rd") == "a b c d"


async def test_a_later_delivery_than_the_limit_is_still_dead_lettered(client) -> None:
    await make(client).reject("g", received(delivered=7), visibility_seconds=0, reason="r")
    assert len(client.js.published) == 1
```

`"a\r\nb"` becomes `"a b"`: CR+LF together turns into one space, and a lone CR or LF turns into
one space. That's why the reason above ends `"value   Field required"`: the `\r\n` becomes one
space, followed by the two spaces of indentation.

- [ ] **Step 3: Run the tests and watch them fail**

Run: `uv run pytest tests/test_nats_unit.py tests/test_envelope.py -q`
Expected: FAIL with `ImportError: cannot import name 'HEADER_DLQ_DELIVERIES'`

- [ ] **Step 4: Add the constants**

In `src/cap_event_handler/envelope.py`, below `HEADER_CORR_ID`:

```python
# Set on dead-lettered messages by transports that dead-letter in the library.
HEADER_DLQ_REASON = "cap-dlq-reason"
HEADER_DLQ_GROUP = "cap-dlq-group"
HEADER_DLQ_DELIVERIES = "cap-dlq-deliveries"
```

- [ ] **Step 5: Implement dead-lettering**

In `src/cap_event_handler/transport/nats.py`, add `import re`. Change the envelope import to:

```python
from cap_event_handler.envelope import (
    HEADER_DLQ_DELIVERIES,
    HEADER_DLQ_GROUP,
    HEADER_DLQ_REASON,
    HEADER_MSG_ID,
    HEADER_MSG_NAME,
)
```

Add next to `strip_nats_headers`:

```python
_LINE_BREAK_RE = re.compile(r"\r\n|\r|\n")


def header_safe(value: str) -> str:
    """A header value cannot span lines; validation errors and tracebacks do."""
    return _LINE_BREAK_RE.sub(" ", value)
```

Replace `reject` with:

```python
    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        """Nak, or on the last allowed delivery dead-letter and ack (spec §5.2)."""
        nak = nak_payload(visibility_seconds)
        if message.receive_count < self.config.max_deliver:
            await self._reply(message.receipt, nak, "nak")
            return
        msg_id = message.headers.get(HEADER_MSG_ID, "<no id>")
        name = message.headers.get(HEADER_MSG_NAME, "<no name>")
        try:
            await self._dead_letter(queue_id, message, reason)
        except Exception:
            logger.exception(
                "dead-letter publish failed for message %s (%s) in group %r after %d "
                "deliveries; JetStream will drop it at max_deliver: %s",
                msg_id,
                name,
                queue_id,
                message.receive_count,
                reason,
            )
            await self._reply(message.receipt, nak, "nak")
            return
        await self._reply(message.receipt, _ACK, "ack")
        logger.error(
            "dead-lettered message %s (%s) in group %r after %d deliveries: %s",
            msg_id,
            name,
            queue_id,
            message.receive_count,
            reason,
        )

    async def _dead_letter(self, queue_id: str, message: ReceivedMessage, reason: str) -> None:
        _, js = await self._client()
        headers = dict(message.headers)
        headers[HEADER_DLQ_REASON] = header_safe(reason)
        headers[HEADER_DLQ_GROUP] = queue_id
        headers[HEADER_DLQ_DELIVERIES] = str(message.receive_count)
        if HEADER_MSG_ID in message.headers:
            # Idempotent if the final delivery is somehow handled twice.
            headers[NATS_MSG_ID] = f"{message.headers[HEADER_MSG_ID]}:{queue_id}"
        await js.publish(
            f"{self.config.dlq_subject_prefix}.{queue_id}",
            message.body.encode(),
            headers=headers,
            stream=self.config.dlq_stream,
        )
```

Also update the module docstring's last paragraph so it no longer says rejection only naks. The
Task 3 docstring already points to spec §5, so nothing else needs to change.

- [ ] **Step 6: Run the tests and watch them pass**

Run: `uv run pytest tests/test_nats_unit.py tests/test_envelope.py -q`
Expected: PASS. `test_reject_before_the_limit_naks_with_the_backoff` from Task 3 still passes
because `delivered=1 < 5`.

- [ ] **Step 7: Check that no `cap-*` header is hard-coded in `src/`**

Run: `grep -rn '"cap-' src/cap_event_handler | grep -v envelope.py`
Expected: no output.

- [ ] **Step 8: Run the gate**

Run: `./check.sh`
Expected: last line `GATE: PASS`

- [ ] **Step 9: Commit**

```bash
git add src/cap_event_handler/envelope.py src/cap_event_handler/transport/nats.py \
  tests/test_envelope.py tests/test_nats_unit.py
git commit -F - <<'EOF'
feat(transport): dead-letter failing JetStream messages

JetStream has no redrive policy and drops a message at max_deliver.
On the last allowed delivery the transport now publishes the message
to <dlq_subject_prefix>.<group> in the dead-letter stream with its
failure reason, group and delivery count, then acks it. If that
publish fails it naks and logs at ERROR, since JetStream will drop it.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 5: Integration tests against a real nats-server

**Files:**
- Modify: `tests/integration/conftest.py` (new `nats_url` fixture)
- Create: `tests/integration/test_nats.py`
- Modify: `tests/integration/CLAUDE.md` (scope list)

**Interfaces:**
- Consumes: `NatsJetStreamTransport`, `NatsConfig`, `pg_engine`, `HEADER_DLQ_*`.

These need Docker. If Docker is not available, say so in your report and do not mark the task
done. Don't weaken the tests to make them pass without Docker.

- [ ] **Step 1: Add the fixture**

Append to `tests/integration/conftest.py`:

```python
@pytest.fixture(scope="session")
def nats_url() -> Iterator[str]:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = DockerContainer("nats:2.10").with_command("-js").with_exposed_ports(4222)
    with container:
        wait_for_logs(container, "Server is ready", timeout=30)
        host = container.get_container_host_ip()
        yield f"nats://{host}:{container.get_exposed_port(4222)}"
```

- [ ] **Step 2: Write the tests**

Create `tests/integration/test_nats.py`:

```python
"""What the fake client cannot prove: real JetStream streams, consumers and redelivery."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import nats
import pytest
import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler import CapApp, Context, NatsConfig, Settings
from cap_event_handler.envelope import (
    HEADER_DLQ_DELIVERIES,
    HEADER_DLQ_GROUP,
    HEADER_DLQ_REASON,
    HEADER_MSG_ID,
)
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.transport.nats import NatsJetStreamTransport

pytestmark = pytest.mark.integration


class Payload(BaseModel):
    value: str


def isolated_config(nats_url: str, **overrides) -> NatsConfig:
    """Fresh stream names and subject prefixes, so tests sharing the server never collide."""
    tag = uuid.uuid4().hex[:8]
    return NatsConfig(
        servers=nats_url,
        stream=f"CAP_{tag}",
        subject_prefix=f"cap{tag}",
        dlq_stream=f"CAP_DLQ_{tag}",
        dlq_subject_prefix=f"capdlq{tag}",
        **overrides,
    )


def build_app(pg_engine, config: NatsConfig, **settings) -> CapApp:
    return CapApp(
        engine=pg_engine,
        transport=NatsJetStreamTransport(config),
        group=f"itest-{uuid.uuid4().hex[:8]}",
        settings=Settings(schema_name=f"cap_{uuid.uuid4().hex[:8]}", **settings),
    )


async def _wait_for(predicate, seconds: float = 30.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError("condition never became true")


async def test_publish_and_consume_through_jetstream(pg_engine, nats_url) -> None:
    seen: list[Context[Payload]] = []
    app = build_app(pg_engine, isolated_config(nats_url), wait_seconds=1)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        seen.append(ctx)

    async with app.run():
        msg_id = await app.publish("order.paid", Payload(value="real"))
        await _wait_for(lambda: bool(seen))
    assert [c.payload.value for c in seen] == ["real"]
    assert seen[0].msg_id == msg_id
    assert app.warnings == []


async def test_a_failed_attempt_rolls_back_and_is_redelivered(pg_engine, nats_url) -> None:
    """The atomic-consume guarantee over JetStream: no partial write survives a nak."""
    effects = sa.Table("nats_effects", sa.MetaData(), sa.Column("v", sa.Text, primary_key=True))
    async with pg_engine.begin() as conn:
        await conn.run_sync(effects.metadata.drop_all)
        await conn.run_sync(effects.metadata.create_all)
    attempts: list[int] = []
    app = build_app(
        pg_engine, isolated_config(nats_url), wait_seconds=1, reject_backoff_seconds=1
    )

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        attempts.append(ctx.retries)
        await ctx.session.execute(sa.insert(effects).values(v=f"attempt-{ctx.retries}"))
        if len(attempts) == 1:
            raise RuntimeError("transient")

    async with app.run():
        await app.publish("order.paid", Payload(value="v"))
        await _wait_for(lambda: len(attempts) == 2)
    assert attempts == [1, 2]
    async with pg_engine.connect() as conn:
        rows = (await conn.execute(sa.select(effects.c.v))).scalars().all()
    assert rows == ["attempt-2"]


async def test_a_message_that_always_fails_is_dead_lettered(pg_engine, nats_url) -> None:
    config = isolated_config(nats_url, max_deliver=2)
    attempts: list[int] = []
    app = build_app(pg_engine, config, wait_seconds=1, reject_backoff_seconds=0)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        attempts.append(ctx.retries)
        raise RuntimeError("always")

    async with app.run():
        msg_id = await app.publish("order.paid", Payload(value="v"))
        await _wait_for(lambda: len(attempts) == 2)
        await asyncio.sleep(1)  # let reject's dead-letter publish land

    nc = await nats.connect(nats_url)
    try:
        js = nc.jetstream()
        dead = await js.get_last_msg(config.dlq_stream, f"{config.dlq_subject_prefix}.{app.group}")
        assert dead.headers[HEADER_MSG_ID] == str(msg_id)
        assert dead.headers[HEADER_DLQ_GROUP] == app.group
        assert dead.headers[HEADER_DLQ_DELIVERIES] == "2"
        assert dead.headers[HEADER_DLQ_REASON] == "handler failed: RuntimeError: always"
        info = await js.consumer_info(config.stream, app.group)
        assert info.num_ack_pending == 0
        assert info.num_pending == 0
    finally:
        await nc.close()
    assert attempts == [1, 2]


async def test_a_duplicate_publish_is_dropped_by_jetstream(nats_url) -> None:
    transport = NatsJetStreamTransport(isolated_config(nats_url))
    try:
        subject = await transport.ensure_topic("order-paid")
        queue_id = await transport.ensure_queue(f"g{uuid.uuid4().hex[:8]}")
        await transport.subscribe(queue_id, [subject])
        headers = {HEADER_MSG_ID: str(uuid.uuid4())}
        await transport.publish(subject, "{}", headers)
        await transport.publish(subject, "{}", headers)
        first = await transport.receive(queue_id, max_messages=10, wait_seconds=2)
        assert len(first) == 1
        await transport.ack(queue_id, first[0].receipt)
        assert await transport.receive(queue_id, max_messages=10, wait_seconds=1) == []
    finally:
        await transport.close()


async def test_provisioning_is_idempotent_and_reports_attributes(nats_url) -> None:
    config = isolated_config(nats_url, ack_wait=timedelta(seconds=45))
    for _ in range(2):
        transport = NatsJetStreamTransport(config)
        try:
            subject = await transport.ensure_topic("order-paid")
            await transport.ensure_queue("g")
            await transport.subscribe("g", [subject])
            attrs = await transport.queue_attributes("g")
        finally:
            await transport.close()
    assert attrs.visibility_timeout == 45
    assert attrs.message_retention == int(timedelta(days=7).total_seconds())
    assert attrs.has_redrive is True
    assert attrs.subscribed_topics == frozenset({subject})


async def test_provision_false_fails_loudly_on_a_missing_stream(nats_url) -> None:
    transport = NatsJetStreamTransport(isolated_config(nats_url), provision=False)
    try:
        with pytest.raises(ConfigurationError, match="provision=False"):
            await transport.ensure_queue("g")
    finally:
        await transport.close()
```

- [ ] **Step 3: Run them**

Run: `uv run pytest -m integration tests/integration/test_nats.py -v`
Expected: 6 passed. If a test fails because the plan's assumptions about `nats-py` or server
behaviour are wrong (not because of a bug), fix the transport, add a unit test in
`tests/test_nats_unit.py` that pins the corrected behaviour, and record the deviation in your
report.

- [ ] **Step 4: Update the integration guide**

In `tests/integration/CLAUDE.md`, add to the "Scope" list:

```markdown
- `test_nats.py`: real JetStream stream and consumer provisioning (idempotent, and
  `provision=False` failures), the publish→handler round trip, nak redelivery with rollback,
  dead-lettering after `max_deliver`, and `Nats-Msg-Id` duplicate dropping.
```

In the "Fixtures and isolation" list, add:

```markdown
- `nats_url` (session scope) starts `nats:2.10` with `-js` through a generic
  `DockerContainer`. NATS tests isolate with `isolated_config()`: a random stream name, DLQ
  stream name and subject prefixes per test.
```

- [ ] **Step 5: Run the gate**

Run: `./check.sh`
Expected: last line `GATE: PASS` (the default run deselects these tests).

- [ ] **Step 6: Commit**

```bash
git add tests/integration/conftest.py tests/integration/test_nats.py tests/integration/CLAUDE.md
git commit -F - <<'EOF'
test: cover the JetStream transport against a real nats-server

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 6: Documentation

**Files:**
- Modify: `docs/usage.md` (new section 9)
- Modify: `docs/superpowers/specs/2026-09-17-cap-event-handler-design.md` (§1.1, §11)
- Modify: `docs/cap-porting-issues.md` (section heading)
- Modify: `CLAUDE.md` (architecture paragraph)
- Modify: `README.md` (one line, if it lists transports)

- [ ] **Step 1: Add the usage section**

Append to `docs/usage.md`:

````markdown
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
| `max_deliver` | 5 | On this delivery a still-failing message is dead-lettered. At least 2. |
| `max_age` | 7 days | Stream retention. Keep it below `Settings.inbox_retention` (15 days); startup warns otherwise. Zero means unlimited. |
| `duplicate_window` | 5 min | JetStream drops a re-sent message with the same id inside this window. That's an optimisation; the inbox catches the rest. |
| `num_replicas` | 1 | Stream replicas on a clustered server. |

These limits apply to **every** event, because there is one stream. If two kinds of event need
different retention, run them in separate apps with different `stream` names.

**Provisioning.** With `provision=True` (the default), missing streams are created with file
storage, and each group's consumer is created or updated. **Existing streams are never
modified.** If your stream does not capture `<subject_prefix>.>`, startup fails rather than
publishing into the void. With `NatsJetStreamTransport(config, provision=False)` and
`Settings(provision=False)`, nothing is created. A missing main stream fails startup; a missing
dead-letter stream triggers a warning.

**Dead letters.** JetStream has no redrive policy, so the library supplies one. When a message
fails on its `max_deliver`-th delivery, the library copies it to the dead-letter stream with
these headers, then acks it:

- `cap-dlq-reason`: why it failed;
- `cap-dlq-group`;
- `cap-dlq-deliveries`.

Both this and a failed dead-letter publish are logged at `ERROR`. A failed dead-letter publish
means JetStream will drop the message. Inspect dead letters with the NATS CLI:

```bash
nats stream view CAP_DLQ
nats stream get CAP_DLQ --last-for "cap-dlq.billing-service"
```

To replay one, republish its body and headers to the original subject (`cap.<event>`) without the
`cap-dlq-*` and `Nats-*` headers. The inbox treats it as new only if the original never committed,
which is the case for every dead letter.

Why this differs from .NET CAP's NATS transport: `docs/cap-porting-issues.md`, rows N1–N6.
````

- [ ] **Step 2: Update the base spec**

In `docs/superpowers/specs/2026-09-17-cap-event-handler-design.md`, §1.1: after the "In scope for
v1" paragraph, add:

```markdown
Added after v1: NATS JetStream as an alternative transport, specified in
`2026-10-03-nats-jetstream-transport-design.md`.
```

In §11, change the "Additional storages … and transports" bullet to:

```markdown
- **Additional storages** (psycopg3, Django ORM) **and transports** (RabbitMQ, Kafka) behind the
  existing Protocols. (NATS JetStream has since been added; see
  `2026-10-03-nats-jetstream-transport-design.md`.)
```

- [ ] **Step 3: Mark the porting notes as implemented**

In `docs/cap-porting-issues.md`, rename `## NATS JetStream transport (in design)` to
`## NATS JetStream transport`. In that section's first paragraph, replace "Decisions are
recorded here as the design settles, and the spec will be the authoritative record." with
"The authoritative design is
`docs/superpowers/specs/2026-10-03-nats-jetstream-transport-design.md`."

- [ ] **Step 4: Update CLAUDE.md**

In `CLAUDE.md`, "Architecture in one paragraph": change
`a \`Transport\` (\`transport/snssqs.py\`, or \`transport/memory.py\` in tests)` to
`a \`Transport\` (\`transport/snssqs.py\` or \`transport/nats.py\` — JetStream, optional extra
\`nats\`, which dead-letters in the library because JetStream has no redrive — or
\`transport/memory.py\` in tests)`. Under "Project management", add
`uv run pytest -m integration tests/integration/test_nats.py   # needs Docker: nats-server`.

- [ ] **Step 5: README**

Run: `grep -n "SNS" README.md | head`. If the README's opening description or a feature list
names SNS/SQS as the transport, add "or NATS JetStream (optional `nats` extra)" there. If not,
leave the README unchanged.

- [ ] **Step 6: Run the gate**

Run: `./check.sh`
Expected: last line `GATE: PASS`

- [ ] **Step 7: Commit**

```bash
git add docs/usage.md docs/superpowers/specs/2026-09-17-cap-event-handler-design.md \
  docs/cap-porting-issues.md CLAUDE.md README.md
git commit -F - <<'EOF'
docs: document the NATS JetStream transport

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```
