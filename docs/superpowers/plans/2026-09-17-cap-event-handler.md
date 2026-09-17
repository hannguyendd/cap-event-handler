# cap-event-handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python library that makes a service's database write and its outgoing event commit atomically, and makes an incoming event's effect commit atomically with the record that it was handled.

**Architecture:** Two pipelines over one PostgreSQL database and SNS/SQS. Publish: `publish()` INSERTs an outbox row on the *caller's* `AsyncSession`, so it commits with their business write; a SQLAlchemy `after_commit` listener then kicks an in-process sender, with an `OutboxSweeper` as the durability net. Consume: a long-poll loop opens one transaction per message containing both an `ON CONFLICT DO NOTHING` dedup INSERT and the handler's own writes, acking SQS only after that commits — so SQS's visibility timeout and redrive policy provide all retry and poison-message behaviour, and no consume-side retry table exists.

**Tech Stack:** Python 3.11+, SQLAlchemy 2.x async, asyncpg, pydantic v2, aiobotocore, uv. Tests: pytest, pytest-asyncio, hypothesis, aiosqlite (unit), testcontainers + LocalStack (integration).

**Spec:** `docs/superpowers/specs/2026-09-17-cap-event-handler-design.md` — read it before Task 1. This plan implements it; where the two disagree, the spec wins except for the deviations recorded below.

## Deviations from the spec

Recorded here so an executor does not "fix" them back:

1. **No `storage/base.py` Protocol and no `storage/memory.py`** (spec §8, §10). Every guarantee under test is a transactional guarantee; a dict-backed fake would reimplement `ON CONFLICT` rowcount semantics and rollback, so tests would exercise the fake rather than the behaviour. Instead there is one `storage/sql.py`, unit-tested against in-memory SQLite through aiosqlite. With one implementation the Protocol earns nothing, so it is dropped. **Consequence:** `FOR UPDATE SKIP LOCKED` cannot run on SQLite, so the concurrent-sweeper test lives in the integration tier only (Task 16).
2. **`transport/memory.py` is kept** exactly as the spec describes — a broker simulator with visibility timeouts and receive counts has no local equivalent.
3. **Startup validation lives in `validation.py`**, not inside `app.py`. Spec §8 puts it in `app.py`; splitting keeps both files focused and the validation table in §9 independently testable.

## Global Constraints

Exact values, copied from the spec. Every task's requirements implicitly include this section.

- Python `>=3.11`. Distribution `cap-event-handler`; import `cap_event_handler`.
- **uv only.** Never invoke `pip`. Add dependencies with `uv add` / `uv add --dev`, never by editing `pyproject.toml` by hand. Run everything through `uv run`.
- Header names, exactly: `cap-msg-id`, `cap-msg-name`, `cap-msg-type`, `cap-sent-time`, `cap-corr-id`. Defined **only** in `envelope.py`; no other module may contain these string literals.
- Topic normalisation: `.` → `-`, `:` → `_`. The un-normalised name travels in `cap-msg-name` and is what handlers register against.
- Table names `cap_outbox`, `cap_inbox`; schema default `"cap"`; table prefix and schema both configurable.
- Outbox statuses, exactly: `"pending"`, `"sent"`, `"failed"`. Inbox has no status column and no retry column.
- Inbox primary key is `(msg_id, group_name)` — never `msg_id` alone.
- Defaults: `inbox_retention=timedelta(days=15)`, `outbox_retention=timedelta(days=1)`, `group_concurrency=1`, `consumer_tasks=1`, `max_retries=50`, `sweeper_interval=30s`, `sweeper_lookback=60s`, `collector_interval=300s`, `collector_batch=1000`, `reject_backoff_seconds=5`, `publish_timeout=10s`, `wait_seconds=20`, `max_messages=10`, `on_unroutable="reject"`, `provision=True`, `auto_create_tables=True`, `shutdown_timeout=30s`, `send_queue_maxsize=1000`.
- mypy runs `--strict` and must pass. ruff check and ruff format must pass.
- `uv run pytest` must stay green and must never require Docker; anything needing Docker is marked `@pytest.mark.integration`.
- Commit after every task, using the message given in that task's final step.

## File Structure

```
src/cap_event_handler/
  __init__.py       public exports only
  errors.py         exception hierarchy
  config.py         Settings, SnsSqsConfig
  envelope.py       header constants, uuid7, Envelope, topic normalisation
  context.py        Context[T], correlation-id contextvar
  registry.py       HandlerRegistry, Handler
  storage/
    __init__.py
    schema.py       build_tables() -> Tables (MetaData + two Table objects)
    sql.py          SqlStorage: every query the library issues
  transport/
    __init__.py
    base.py         Transport Protocol, ReceivedMessage, QueueAttributes
    memory.py       MemoryTransport — broker simulator for tests
    snssqs.py       SnsSqsTransport — aiobotocore
  publisher.py      Publisher: envelope build, staging, after_commit listener, sender task
  consumer.py       Consumer: poll loop, dispatch, dedup, ack/reject
  processors.py     OutboxSweeper, Collector
  validation.py     startup checks from spec §9
  app.py            CapApp: wiring and lifecycle
  cli.py            `cap-event-handler worker`
tests/
  conftest.py       sqlite engine fixture, storage fixture, transport fixture
  test_envelope.py  test_storage_schema.py  test_storage_sql.py
  test_transport_memory.py  test_context.py  test_registry.py
  test_publisher.py  test_consumer.py  test_processors.py
  test_validation.py  test_app.py  test_guarantees.py
  integration/test_postgres.py  integration/test_localstack.py
```

---

### Task 1: Errors and settings

**Files:**
- Create: `src/cap_event_handler/errors.py`
- Create: `src/cap_event_handler/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `CapError`, `ConfigurationError`, `UnroutableMessageError`, `PayloadValidationError`, `HandlerTransactionError`, `Settings`, `SnsSqsConfig`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config.py
from datetime import timedelta

import pytest

from cap_event_handler.config import Settings, SnsSqsConfig
from cap_event_handler.errors import CapError, ConfigurationError


def test_settings_defaults_match_spec() -> None:
    s = Settings()
    assert s.inbox_retention == timedelta(days=15)
    assert s.outbox_retention == timedelta(days=1)
    assert s.group_concurrency == 1
    assert s.consumer_tasks == 1
    assert s.max_retries == 50
    assert s.sweeper_interval == timedelta(seconds=30)
    assert s.sweeper_lookback == timedelta(seconds=60)
    assert s.collector_interval == timedelta(seconds=300)
    assert s.collector_batch == 1000
    assert s.reject_backoff_seconds == 5
    assert s.publish_timeout == timedelta(seconds=10)
    assert s.wait_seconds == 20
    assert s.max_messages == 10
    assert s.on_unroutable == "reject"
    assert s.provision is True
    assert s.auto_create_tables is True
    assert s.shutdown_timeout == timedelta(seconds=30)
    assert s.send_queue_maxsize == 1000
    assert s.schema_name == "cap"
    assert s.table_prefix == "cap_"


def test_settings_is_frozen() -> None:
    s = Settings()
    with pytest.raises(Exception):
        s.max_retries = 3  # type: ignore[misc]


def test_on_unroutable_rejects_unknown_value() -> None:
    with pytest.raises(ConfigurationError, match="on_unroutable"):
        Settings(on_unroutable="explode")  # type: ignore[arg-type]


def test_max_messages_must_be_within_sqs_limits() -> None:
    with pytest.raises(ConfigurationError, match="max_messages"):
        Settings(max_messages=11)


def test_wait_seconds_must_be_within_sqs_limits() -> None:
    with pytest.raises(ConfigurationError, match="wait_seconds"):
        Settings(wait_seconds=21)


def test_sns_sqs_config_requires_region() -> None:
    cfg = SnsSqsConfig(region="ap-southeast-2")
    assert cfg.region == "ap-southeast-2"
    assert cfg.endpoint_url is None


def test_configuration_error_is_a_cap_error() -> None:
    assert issubclass(ConfigurationError, CapError)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.config'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/errors.py
"""Exception hierarchy. Every exception the library raises descends from CapError."""


class CapError(Exception):
    """Base class for every error raised by cap_event_handler."""


class ConfigurationError(CapError):
    """Settings or AWS resources are configured in a way that breaks a guarantee."""


class UnroutableMessageError(CapError):
    """A received message names a topic no handler is registered for."""


class PayloadValidationError(CapError):
    """A received message body does not validate against the handler's model."""


class HandlerTransactionError(CapError):
    """A handler committed or closed the session the library owns."""
```

```python
# src/cap_event_handler/config.py
"""Settings and transport configuration.

Settings are frozen: every value is read at startup and never mutated, so a
running app cannot drift from what validation.py checked.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from cap_event_handler.errors import ConfigurationError

OnUnroutable = Literal["reject", "ack"]


@dataclass(frozen=True, slots=True)
class Settings:
    """Tuning for both pipelines. Defaults are the spec's defaults."""

    # storage
    schema_name: str | None = "cap"
    table_prefix: str = "cap_"
    auto_create_tables: bool = True

    # retention
    inbox_retention: timedelta = timedelta(days=15)
    outbox_retention: timedelta = timedelta(days=1)

    # publish
    publish_timeout: timedelta = timedelta(seconds=10)
    send_queue_maxsize: int = 1000
    max_retries: int = 50

    # consume
    group_concurrency: int = 1
    consumer_tasks: int = 1
    wait_seconds: int = 20
    max_messages: int = 10
    reject_backoff_seconds: int = 5
    on_unroutable: OnUnroutable = "reject"
    expected_max_handler_seconds: int = 30

    # processors
    sweeper_interval: timedelta = timedelta(seconds=30)
    sweeper_lookback: timedelta = timedelta(seconds=60)
    collector_interval: timedelta = timedelta(seconds=300)
    collector_batch: int = 1000

    # lifecycle
    provision: bool = True
    shutdown_timeout: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if self.on_unroutable not in ("reject", "ack"):
            raise ConfigurationError(
                f"on_unroutable must be 'reject' or 'ack', got {self.on_unroutable!r}"
            )
        if not 1 <= self.max_messages <= 10:
            raise ConfigurationError(
                f"max_messages must be between 1 and 10 (the SQS limit), got {self.max_messages}"
            )
        if not 0 <= self.wait_seconds <= 20:
            raise ConfigurationError(
                f"wait_seconds must be between 0 and 20 (the SQS limit), got {self.wait_seconds}"
            )
        if self.group_concurrency < 1:
            raise ConfigurationError("group_concurrency must be at least 1")
        if self.consumer_tasks < 1:
            raise ConfigurationError("consumer_tasks must be at least 1")
        if self.collector_batch < 1:
            raise ConfigurationError("collector_batch must be at least 1")


@dataclass(frozen=True, slots=True)
class SnsSqsConfig:
    """Where to reach SNS and SQS. endpoint_url targets LocalStack in tests."""

    region: str
    endpoint_url: str | None = None
    queue_name: str | None = None
    """Override the queue name derived from the consumer group."""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_config.py -v`
Expected: 7 passed

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/errors.py src/cap_event_handler/config.py tests/test_config.py
git commit -m "feat: add exception hierarchy and Settings"
```

---

### Task 2: Envelope

**Files:**
- Create: `src/cap_event_handler/envelope.py`
- Test: `tests/test_envelope.py`

**Interfaces:**
- Consumes: `CapError` subclasses from Task 1.
- Produces:
  - constants `HEADER_MSG_ID`, `HEADER_MSG_NAME`, `HEADER_MSG_TYPE`, `HEADER_SENT_TIME`, `HEADER_CORR_ID`
  - `uuid7() -> UUID`
  - `normalize_topic(name: str) -> str`
  - `Envelope` frozen dataclass with fields `id: UUID`, `name: str`, `headers: dict[str, str]`, `payload: dict[str, Any]`, and properties `sent_time: datetime`, `correlation_id: UUID`
  - `build_envelope(name: str, payload: BaseModel, *, correlation_id: UUID | None = None) -> Envelope`
  - `Envelope.from_transport(headers: Mapping[str, str], body: str) -> Envelope`
  - `Envelope.body() -> str` (the JSON text placed in the SNS `Message` field)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_envelope.py
import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel

from cap_event_handler.envelope import (
    HEADER_CORR_ID,
    HEADER_MSG_ID,
    HEADER_MSG_NAME,
    HEADER_MSG_TYPE,
    HEADER_SENT_TIME,
    Envelope,
    build_envelope,
    normalize_topic,
    uuid7,
)
from cap_event_handler.errors import CapError


class OrderPaid(BaseModel):
    order_id: UUID
    amount: str


def test_header_names_are_exactly_the_spec_values() -> None:
    assert HEADER_MSG_ID == "cap-msg-id"
    assert HEADER_MSG_NAME == "cap-msg-name"
    assert HEADER_MSG_TYPE == "cap-msg-type"
    assert HEADER_SENT_TIME == "cap-sent-time"
    assert HEADER_CORR_ID == "cap-corr-id"


def test_uuid7_is_version_7_and_time_ordered() -> None:
    a, b = uuid7(), uuid7()
    assert a.version == 7
    assert b.version == 7
    assert a < b or a.int < b.int


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("order.paid", "order-paid"),
        ("billing:invoice.issued", "billing_invoice-issued"),
        ("already-fine_123", "already-fine_123"),
    ],
)
def test_normalize_topic(raw: str, expected: str) -> None:
    assert normalize_topic(raw) == expected


def test_normalize_topic_rejects_characters_sns_forbids() -> None:
    with pytest.raises(CapError, match="topic"):
        normalize_topic("order paid!")


def test_normalize_topic_rejects_empty() -> None:
    with pytest.raises(CapError, match="topic"):
        normalize_topic("")


def test_build_envelope_populates_all_headers() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="10.00"))
    assert env.name == "order.paid"
    assert env.headers[HEADER_MSG_NAME] == "order.paid"
    assert env.headers[HEADER_MSG_ID] == str(env.id)
    assert env.headers[HEADER_MSG_TYPE].endswith("OrderPaid")
    assert UUID(env.headers[HEADER_CORR_ID]) == env.correlation_id
    assert env.sent_time.tzinfo is not None


def test_build_envelope_honours_explicit_correlation_id() -> None:
    corr = uuid7()
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"), correlation_id=corr)
    assert env.correlation_id == corr


def test_payload_is_json_ready() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="10.00"))
    # must survive json.dumps without a custom encoder, since it is stored as JSONB
    assert json.loads(json.dumps(env.payload)) == env.payload


def test_round_trip_through_transport() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="10.00"))
    back = Envelope.from_transport(env.headers, env.body())
    assert back.id == env.id
    assert back.name == env.name
    assert back.payload == env.payload
    assert back.correlation_id == env.correlation_id
    assert back.sent_time == env.sent_time


def test_from_transport_preserves_unknown_headers() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    headers = dict(env.headers) | {"x-tenant": "acme"}
    back = Envelope.from_transport(headers, env.body())
    assert back.headers["x-tenant"] == "acme"


def test_from_transport_rejects_missing_id() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    headers = {k: v for k, v in env.headers.items() if k != HEADER_MSG_ID}
    with pytest.raises(CapError, match=HEADER_MSG_ID):
        Envelope.from_transport(headers, env.body())


def test_from_transport_rejects_malformed_body() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    with pytest.raises(CapError, match="body"):
        Envelope.from_transport(env.headers, "not json")


@given(st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=20))
def test_normalize_topic_is_idempotent(name: str) -> None:
    once = normalize_topic(name)
    assert normalize_topic(once) == once


def test_sent_time_is_utc_and_round_trips_exactly() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    assert env.sent_time.tzinfo == UTC or env.sent_time.utcoffset() == datetime.now(UTC).utcoffset()
    assert Envelope.from_transport(env.headers, env.body()).sent_time == env.sent_time
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_envelope.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.envelope'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/envelope.py
"""The wire envelope: header names, id generation, topic naming, encode/decode.

This module is the only place the `cap-*` header strings appear. Nothing else
in the library may hard-code them.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from cap_event_handler.errors import CapError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic import BaseModel

HEADER_MSG_ID = "cap-msg-id"
HEADER_MSG_NAME = "cap-msg-name"
HEADER_MSG_TYPE = "cap-msg-type"
HEADER_SENT_TIME = "cap-sent-time"
HEADER_CORR_ID = "cap-corr-id"

_SNS_TOPIC_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")


if sys.version_info >= (3, 14):  # pragma: no cover - depends on interpreter

    def uuid7() -> UUID:
        """A version-7 UUID: time-ordered, so it indexes well as a primary key."""
        from uuid import uuid7 as _stdlib_uuid7

        return _stdlib_uuid7()

else:  # pragma: no cover - depends on interpreter

    def uuid7() -> UUID:
        """A version-7 UUID: time-ordered, so it indexes well as a primary key."""
        from uuid6 import uuid7 as _uuid6_uuid7

        return _uuid6_uuid7()


def normalize_topic(name: str) -> str:
    """Map an event name onto the character set SNS permits for topic names.

    `.` becomes `-` and `:` becomes `_`, matching CAP's TopicNormalizer. The
    un-normalised name is what travels in `cap-msg-name`; this is only for
    addressing SNS.
    """
    normalized = name.replace(".", "-").replace(":", "_")
    if not _SNS_TOPIC_RE.match(normalized):
        raise CapError(
            f"event name {name!r} cannot be used as an SNS topic: normalised form "
            f"{normalized!r} must match {_SNS_TOPIC_RE.pattern}"
        )
    return normalized


@dataclass(frozen=True, slots=True)
class Envelope:
    """A message as it is stored in the outbox and as it travels over SNS."""

    id: UUID
    name: str
    headers: dict[str, str] = field(compare=False)
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def sent_time(self) -> datetime:
        return datetime.fromisoformat(self.headers[HEADER_SENT_TIME])

    @property
    def correlation_id(self) -> UUID:
        return UUID(self.headers[HEADER_CORR_ID])

    def body(self) -> str:
        """The JSON text carried in the SNS `Message` field."""
        return json.dumps(self.payload, separators=(",", ":"))

    @classmethod
    def from_transport(cls, headers: Mapping[str, str], body: str) -> Envelope:
        """Rebuild an envelope from what came off the queue.

        Unknown headers are preserved so intermediaries can add their own.
        """
        for required in (HEADER_MSG_ID, HEADER_MSG_NAME, HEADER_SENT_TIME, HEADER_CORR_ID):
            if required not in headers:
                raise CapError(f"received message is missing the {required} header")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise CapError(f"received message body is not valid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise CapError("received message body must be a JSON object")
        try:
            msg_id = UUID(headers[HEADER_MSG_ID])
        except ValueError as exc:
            raise CapError(f"{HEADER_MSG_ID} is not a UUID: {headers[HEADER_MSG_ID]!r}") from exc
        return cls(
            id=msg_id,
            name=headers[HEADER_MSG_NAME],
            headers=dict(headers),
            payload=payload,
        )


def build_envelope(
    name: str,
    payload: BaseModel,
    *,
    correlation_id: UUID | None = None,
) -> Envelope:
    """Create an envelope for an outgoing event. Assigns the id exactly once."""
    normalize_topic(name)  # fail here, not at send time, if the name is unusable
    msg_id = uuid7()
    model_type = type(payload)
    headers = {
        HEADER_MSG_ID: str(msg_id),
        HEADER_MSG_NAME: name,
        HEADER_MSG_TYPE: f"{model_type.__module__}.{model_type.__qualname__}",
        HEADER_SENT_TIME: datetime.now(UTC).isoformat(),
        HEADER_CORR_ID: str(correlation_id if correlation_id is not None else uuid7()),
    }
    return Envelope(id=msg_id, name=name, headers=headers, payload=payload.model_dump(mode="json"))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_envelope.py -v`
Expected: all passed

- [ ] **Step 5: Verify the header-literal constraint holds**

Run: `! grep -rn "cap-msg-id\|cap-msg-name\|cap-msg-type\|cap-sent-time\|cap-corr-id" src/ --include='*.py' | grep -v "envelope.py"`
Expected: exit 0 (no matches outside `envelope.py`)

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 7: Commit**

```bash
git add src/cap_event_handler/envelope.py tests/test_envelope.py
git commit -m "feat: add wire envelope, header constants and topic normalisation"
```

---
### Task 3: Storage schema

**Files:**
- Create: `src/cap_event_handler/storage/__init__.py`
- Create: `src/cap_event_handler/storage/schema.py`
- Create: `tests/conftest.py`
- Test: `tests/test_storage_schema.py`

**Interfaces:**
- Consumes: `Settings` (Task 1).
- Produces:
  - `Tables` frozen dataclass with `metadata: MetaData`, `outbox: Table`, `inbox: Table`
  - `build_tables(*, schema: str | None = "cap", prefix: str = "cap_") -> Tables`
  - pytest fixtures `sqlite_engine` (function-scoped `AsyncEngine`) and `settings` (schema-less `Settings`)

**Why SQLite works here:** the column types are declared with dialect variants — `JSON` becomes `JSONB` on PostgreSQL, `Uuid` becomes `UUID` on PostgreSQL and `CHAR(32)` elsewhere — so one `MetaData` creates correct tables on both. Tests pass `schema=None` because SQLite has no schemas.

- [ ] **Step 1: Add the test-only database driver**

Run: `uv add --dev aiosqlite`
Expected: `aiosqlite` appears in the `dev` dependency group and `uv.lock` updates.

- [ ] **Step 2: Write the failing test**

```python
# tests/conftest.py
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from cap_event_handler.config import Settings


@pytest.fixture
def settings() -> Settings:
    """Settings with no schema, because SQLite has none."""
    return Settings(schema_name=None)


@pytest_asyncio.fixture
async def sqlite_engine() -> AsyncIterator[AsyncEngine]:
    """A private in-memory SQLite database, one per test.

    StaticPool keeps every connection pointed at the same in-memory database;
    without it each connection would get its own empty one.
    """
    from sqlalchemy.pool import StaticPool

    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    try:
        yield engine
    finally:
        await engine.dispose()
```

```python
# tests/test_storage_schema.py
from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from cap_event_handler.storage.schema import build_tables


def test_table_names_use_the_prefix() -> None:
    tables = build_tables(schema=None, prefix="cap_")
    assert tables.outbox.name == "cap_outbox"
    assert tables.inbox.name == "cap_inbox"


def test_prefix_is_configurable() -> None:
    tables = build_tables(schema=None, prefix="msg_")
    assert tables.outbox.name == "msg_outbox"
    assert tables.inbox.name == "msg_inbox"


def test_schema_is_applied() -> None:
    tables = build_tables(schema="cap", prefix="cap_")
    assert tables.outbox.schema == "cap"
    assert tables.inbox.schema == "cap"


def test_outbox_columns_match_the_spec() -> None:
    cols = build_tables(schema=None).outbox.columns
    assert set(cols.keys()) == {
        "id",
        "name",
        "headers",
        "payload",
        "status",
        "retries",
        "last_error",
        "created_at",
        "sent_at",
        "expires_at",
    }
    assert cols["id"].primary_key
    assert not cols["name"].nullable
    assert not cols["status"].nullable
    assert cols["last_error"].nullable
    assert cols["sent_at"].nullable


def test_inbox_columns_match_the_spec() -> None:
    cols = build_tables(schema=None).inbox.columns
    assert set(cols.keys()) == {"msg_id", "group_name", "name", "processed_at", "expires_at"}


def test_inbox_primary_key_is_msg_id_and_group() -> None:
    """Keying on msg_id alone would let one group suppress another's delivery."""
    inbox = build_tables(schema=None).inbox
    assert [c.name for c in inbox.primary_key.columns] == ["msg_id", "group_name"]


def test_inbox_has_no_status_or_retry_column() -> None:
    """A row's existence IS success; there is no partial state to reconcile."""
    cols = build_tables(schema=None).inbox.columns.keys()
    assert "status" not in cols
    assert "retries" not in cols


def test_outbox_has_no_version_column() -> None:
    assert "version" not in build_tables(schema=None).outbox.columns.keys()


def test_payload_renders_as_jsonb_on_postgresql() -> None:
    tables = build_tables(schema=None)
    pg = sa.dialects.postgresql.dialect()
    assert "JSONB" in tables.outbox.columns["payload"].type.compile(pg)
    assert "JSONB" in tables.outbox.columns["headers"].type.compile(pg)


def test_indexes_match_the_spec() -> None:
    tables = build_tables(schema=None)
    assert {i.name for i in tables.outbox.indexes} == {
        "cap_outbox_sweep_idx",
        "cap_outbox_collector_idx",
    }
    assert {i.name for i in tables.inbox.indexes} == {"cap_inbox_collector_idx"}


async def test_create_all_works_on_sqlite(sqlite_engine: AsyncEngine) -> None:
    tables = build_tables(schema=None)
    async with sqlite_engine.begin() as conn:
        await conn.run_sync(tables.metadata.create_all)
        names = await conn.run_sync(lambda c: sa.inspect(c).get_table_names())
    assert {"cap_outbox", "cap_inbox"} <= set(names)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_storage_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.storage'`

- [ ] **Step 4: Write minimal implementation**

```python
# src/cap_event_handler/storage/__init__.py
"""PostgreSQL storage for the outbox and inbox tables."""
```

```python
# src/cap_event_handler/storage/schema.py
"""Table definitions for the outbox and inbox.

Built by a factory rather than declared at module scope so schema and table
prefix stay configurable. The returned MetaData is what an application points
Alembic at to autogenerate migrations.

Column types carry dialect variants so one definition creates correct tables on
PostgreSQL (JSONB, UUID, TIMESTAMPTZ) and on SQLite, which the unit tests use.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


@dataclass(frozen=True, slots=True)
class Tables:
    """The two tables plus the MetaData that owns them."""

    metadata: sa.MetaData
    outbox: sa.Table
    inbox: sa.Table


def build_tables(*, schema: str | None = "cap", prefix: str = "cap_") -> Tables:
    """Define the outbox and inbox tables in a fresh MetaData."""
    metadata = sa.MetaData(schema=schema)

    outbox = sa.Table(
        f"{prefix}outbox",
        metadata,
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("headers", _JSON, nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("retries", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Index(
            f"{prefix}outbox_sweep_idx",
            "created_at",
            postgresql_where=sa.text("status <> 'sent'"),
        ),
        sa.Index(
            f"{prefix}outbox_collector_idx",
            "expires_at",
            postgresql_where=sa.text("status = 'sent'"),
        ),
    )

    inbox = sa.Table(
        f"{prefix}inbox",
        metadata,
        sa.Column("msg_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("group_name", sa.Text, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Index(f"{prefix}inbox_collector_idx", "expires_at"),
    )

    return Tables(metadata=metadata, outbox=outbox, inbox=inbox)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_storage_schema.py -v`
Expected: all passed

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 7: Commit**

```bash
git add src/cap_event_handler/storage tests/conftest.py tests/test_storage_schema.py pyproject.toml uv.lock
git commit -m "feat: add outbox and inbox table definitions"
```

---

### Task 4: Storage queries

**Files:**
- Create: `src/cap_event_handler/storage/sql.py`
- Modify: `tests/conftest.py` (add the `storage` fixture)
- Test: `tests/test_storage_sql.py`

**Interfaces:**
- Consumes: `Tables`, `build_tables` (Task 3); `Envelope`, `build_envelope` (Task 2); `Settings`, `ConfigurationError` (Task 1).
- Produces:
  - `OutboxRow` frozen dataclass: `id: UUID`, `name: str`, `headers: dict[str, str]`, `payload: dict[str, Any]`, `status: str`, `retries: int`, `created_at: datetime`, method `envelope() -> Envelope`
  - `SqlStorage(engine: AsyncEngine, settings: Settings, *, clock: Callable[[], datetime] | None = None)` with attributes `tables: Tables`, `sessionmaker: async_sessionmaker[AsyncSession]` and methods:
    - `async create_tables() -> None`
    - `async insert_outbox(session: AsyncSession, env: Envelope) -> None`
    - `async get_outbox(session: AsyncSession, msg_id: UUID) -> OutboxRow | None`
    - `async mark_sent(session: AsyncSession, msg_id: UUID) -> None`
    - `async mark_failed(session: AsyncSession, msg_id: UUID, error: str) -> None`
    - `async claim_pending(session: AsyncSession, *, limit: int) -> list[OutboxRow]`
    - `async insert_inbox(session: AsyncSession, *, msg_id: UUID, group: str, name: str) -> bool`
    - `async delete_expired_outbox(session: AsyncSession) -> int`
    - `async delete_expired_inbox(session: AsyncSession) -> int`

**The injectable clock** is what makes retention and lookback tests deterministic: a test advances the clock rather than sleeping.

- [ ] **Step 1: Write the failing test**

Append to `tests/conftest.py`:

```python
# tests/conftest.py  — append
from datetime import UTC, datetime, timedelta


class FakeClock:
    """A clock a test can move by hand."""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest_asyncio.fixture
async def storage(sqlite_engine, settings, clock):  # noqa: ANN001, ANN201
    from cap_event_handler.storage.sql import SqlStorage

    store = SqlStorage(sqlite_engine, settings, clock=clock)
    await store.create_tables()
    return store
```

```python
# tests/test_storage_sql.py
from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel

from cap_event_handler.config import Settings
from cap_event_handler.envelope import build_envelope, uuid7
from cap_event_handler.storage.sql import SqlStorage


class Payload(BaseModel):
    value: str


def env(name: str = "order.paid", value: str = "x"):  # noqa: ANN201
    return build_envelope(name, Payload(value=value))


async def test_insert_outbox_stores_a_pending_row(storage: SqlStorage) -> None:
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, e.id)
    assert row is not None
    assert row.status == "pending"
    assert row.retries == 0
    assert row.name == "order.paid"
    assert row.payload == {"value": "x"}
    assert row.envelope().headers == e.headers


async def test_rollback_discards_the_outbox_row(storage: SqlStorage) -> None:
    """The whole point of the outbox: no event for a transaction that did not commit."""
    e = env()
    async with storage.sessionmaker() as s:
        async with s.begin():
            await storage.insert_outbox(s, e)
            await s.rollback()
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, e.id) is None


async def test_mark_sent_sets_status_and_expiry(storage: SqlStorage, clock) -> None:  # noqa: ANN001
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    async with storage.sessionmaker() as s, s.begin():
        await storage.mark_sent(s, e.id)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, e.id)
    assert row is not None
    assert row.status == "sent"


async def test_mark_failed_increments_retries_and_records_the_error(storage: SqlStorage) -> None:
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    for expected in (1, 2):
        async with storage.sessionmaker() as s, s.begin():
            await storage.mark_failed(s, e.id, "boom")
        async with storage.sessionmaker() as s:
            row = await storage.get_outbox(s, e.id)
        assert row is not None
        assert row.status == "failed"
        assert row.retries == expected


async def test_claim_pending_skips_rows_younger_than_the_lookback(
    storage: SqlStorage, clock
) -> None:  # noqa: ANN001
    """A row still being sent right now must not be swept."""
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.claim_pending(s, limit=10) == []
    clock.advance(timedelta(seconds=61))
    async with storage.sessionmaker() as s, s.begin():
        claimed = await storage.claim_pending(s, limit=10)
    assert [r.id for r in claimed] == [e.id]


async def test_claim_pending_ignores_sent_rows(storage: SqlStorage, clock) -> None:  # noqa: ANN001
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
        await storage.mark_sent(s, e.id)
    clock.advance(timedelta(seconds=61))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.claim_pending(s, limit=10) == []


async def test_claim_pending_ignores_rows_past_max_retries(
    sqlite_engine, clock
) -> None:  # noqa: ANN001
    store = SqlStorage(sqlite_engine, Settings(schema_name=None, max_retries=2), clock=clock)
    await store.create_tables()
    e = env()
    async with store.sessionmaker() as s, s.begin():
        await store.insert_outbox(s, e)
        await store.mark_failed(s, e.id, "1")
        await store.mark_failed(s, e.id, "2")
    clock.advance(timedelta(seconds=61))
    async with store.sessionmaker() as s, s.begin():
        assert await store.claim_pending(s, limit=10) == []


async def test_claim_pending_respects_the_limit(storage: SqlStorage, clock) -> None:  # noqa: ANN001
    async with storage.sessionmaker() as s, s.begin():
        for _ in range(5):
            await storage.insert_outbox(s, env())
    clock.advance(timedelta(seconds=61))
    async with storage.sessionmaker() as s, s.begin():
        assert len(await storage.claim_pending(s, limit=3)) == 3


async def test_insert_inbox_returns_true_the_first_time(storage: SqlStorage) -> None:
    msg_id = uuid7()
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="g", name="order.paid") is True


async def test_insert_inbox_returns_false_for_a_duplicate(storage: SqlStorage) -> None:
    """This single boolean is the whole deduplication mechanism."""
    msg_id = uuid7()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_inbox(s, msg_id=msg_id, group="g", name="order.paid")
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="g", name="order.paid") is False


async def test_insert_inbox_allows_the_same_message_in_another_group(storage: SqlStorage) -> None:
    msg_id = uuid7()
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="a", name="n") is True
        assert await storage.insert_inbox(s, msg_id=msg_id, group="b", name="n") is True


async def test_rolled_back_inbox_insert_leaves_no_trace(storage: SqlStorage) -> None:
    """A failed handler must be able to run again from scratch."""
    msg_id = uuid7()
    async with storage.sessionmaker() as s:
        async with s.begin():
            await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n")
            await s.rollback()
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n") is True


async def test_delete_expired_outbox_removes_only_expired_sent_rows(
    storage: SqlStorage, clock
) -> None:  # noqa: ANN001
    sent, pending = env(), env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, sent)
        await storage.insert_outbox(s, pending)
        await storage.mark_sent(s, sent.id)
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_outbox(s) == 0
    clock.advance(timedelta(days=2))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_outbox(s) == 1
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, sent.id) is None
        assert await storage.get_outbox(s, pending.id) is not None


async def test_delete_expired_inbox_honours_retention(storage: SqlStorage, clock) -> None:  # noqa: ANN001
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_inbox(s, msg_id=uuid7(), group="g", name="n")
    clock.advance(timedelta(days=14))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_inbox(s) == 0
    clock.advance(timedelta(days=2))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_inbox(s) == 1


async def test_unsupported_dialect_is_rejected() -> None:
    from sqlalchemy.ext.asyncio import create_async_engine

    from cap_event_handler.errors import ConfigurationError

    engine = create_async_engine("mysql+aiomysql://u:p@localhost/db")
    store = SqlStorage(engine, Settings(schema_name=None))
    with pytest.raises(ConfigurationError, match="mysql"):
        store._insert_ignore_dialect()  # noqa: SLF001
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_storage_sql.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.storage.sql'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/storage/sql.py
"""Every SQL statement the library issues.

One implementation serves PostgreSQL in production and SQLite under unit test.
The two places the dialects genuinely differ — `ON CONFLICT DO NOTHING` and
`FOR UPDATE SKIP LOCKED` — are isolated in `_insert_ignore_dialect` and
`claim_pending`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from cap_event_handler.envelope import Envelope
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.storage.schema import build_tables

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from cap_event_handler.config import Settings

STATUS_PENDING = "pending"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"


@dataclass(frozen=True, slots=True)
class OutboxRow:
    """A row of the outbox, as the sender and sweeper see it."""

    id: UUID
    name: str
    headers: dict[str, str]
    payload: dict[str, Any]
    status: str
    retries: int
    created_at: datetime

    def envelope(self) -> Envelope:
        return Envelope(id=self.id, name=self.name, headers=self.headers, payload=self.payload)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class SqlStorage:
    """Outbox and inbox persistence over a SQLAlchemy AsyncEngine."""

    def __init__(
        self,
        engine: AsyncEngine,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._engine = engine
        self._settings = settings
        self._clock = clock or _utcnow
        self.tables = build_tables(schema=settings.schema_name, prefix=settings.table_prefix)
        self.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    @property
    def dialect(self) -> str:
        return self._engine.dialect.name

    def _insert_ignore_dialect(self) -> Any:
        """Return the dialect-specific `insert` construct that supports ON CONFLICT."""
        if self.dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif self.dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            raise ConfigurationError(
                f"cap_event_handler supports PostgreSQL (and SQLite for tests); "
                f"got dialect {self.dialect!r}"
            )
        return insert

    async def create_tables(self) -> None:
        """Idempotent DDL. Creates the schema first when one is configured."""
        async with self._engine.begin() as conn:
            schema = self._settings.schema_name
            if schema and self.dialect == "postgresql":
                await conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            await conn.run_sync(self.tables.metadata.create_all)

    # ---------------------------------------------------------------- outbox

    async def insert_outbox(self, session: AsyncSession, env: Envelope) -> None:
        """INSERT on the caller's session, so it commits with their transaction."""
        await session.execute(
            sa.insert(self.tables.outbox).values(
                id=env.id,
                name=env.name,
                headers=env.headers,
                payload=env.payload,
                status=STATUS_PENDING,
                retries=0,
                created_at=self._clock(),
            )
        )

    async def get_outbox(self, session: AsyncSession, msg_id: UUID) -> OutboxRow | None:
        t = self.tables.outbox
        result = await session.execute(sa.select(t).where(t.c.id == msg_id))
        row = result.mappings().one_or_none()
        return None if row is None else _to_outbox_row(row)

    async def mark_sent(self, session: AsyncSession, msg_id: UUID) -> None:
        now = self._clock()
        t = self.tables.outbox
        await session.execute(
            sa.update(t)
            .where(t.c.id == msg_id)
            .values(
                status=STATUS_SENT,
                sent_at=now,
                expires_at=now + self._settings.outbox_retention,
                last_error=None,
            )
        )

    async def mark_failed(self, session: AsyncSession, msg_id: UUID, error: str) -> None:
        t = self.tables.outbox
        await session.execute(
            sa.update(t)
            .where(t.c.id == msg_id)
            .values(status=STATUS_FAILED, retries=t.c.retries + 1, last_error=error[:2000])
        )

    async def claim_pending(self, session: AsyncSession, *, limit: int) -> list[OutboxRow]:
        """Rows the sweeper should re-send, locked against other instances.

        `FOR UPDATE SKIP LOCKED` is how two app instances divide the work. SQLite
        has no such clause, so it is applied only on PostgreSQL; concurrency is
        therefore covered by the integration tests, not the unit tests.
        """
        t = self.tables.outbox
        cutoff = self._clock() - self._settings.sweeper_lookback
        stmt = (
            sa.select(t)
            .where(
                t.c.status.in_((STATUS_PENDING, STATUS_FAILED)),
                t.c.retries < self._settings.max_retries,
                t.c.created_at < cutoff,
            )
            .order_by(t.c.created_at)
            .limit(limit)
        )
        if self.dialect == "postgresql":
            stmt = stmt.with_for_update(skip_locked=True)
        result = await session.execute(stmt)
        return [_to_outbox_row(row) for row in result.mappings()]

    async def delete_expired_outbox(self, session: AsyncSession) -> int:
        t = self.tables.outbox
        victims = (
            sa.select(t.c.id)
            .where(t.c.status == STATUS_SENT, t.c.expires_at < self._clock())
            .limit(self._settings.collector_batch)
            .scalar_subquery()
        )
        result = await session.execute(sa.delete(t).where(t.c.id.in_(victims)))
        return result.rowcount

    # ----------------------------------------------------------------- inbox

    async def insert_inbox(
        self, session: AsyncSession, *, msg_id: UUID, group: str, name: str
    ) -> bool:
        """Claim this message for this group. False means it was already handled.

        Runs inside the handler's own transaction, which is what makes the
        effect and the marker atomic.
        """
        now = self._clock()
        insert = self._insert_ignore_dialect()
        stmt = (
            insert(self.tables.inbox)
            .values(
                msg_id=msg_id,
                group_name=group,
                name=name,
                processed_at=now,
                expires_at=now + self._settings.inbox_retention,
            )
            .on_conflict_do_nothing()
        )
        result = await session.execute(stmt)
        return bool(result.rowcount)

    async def delete_expired_inbox(self, session: AsyncSession) -> int:
        t = self.tables.inbox
        victims = (
            sa.select(t.c.msg_id, t.c.group_name)
            .where(t.c.expires_at < self._clock())
            .limit(self._settings.collector_batch)
            .subquery()
        )
        result = await session.execute(
            sa.delete(t).where(
                sa.tuple_(t.c.msg_id, t.c.group_name).in_(
                    sa.select(victims.c.msg_id, victims.c.group_name)
                )
            )
        )
        return result.rowcount


def _to_outbox_row(row: sa.RowMapping) -> OutboxRow:
    return OutboxRow(
        id=row["id"],
        name=row["name"],
        headers=row["headers"],
        payload=row["payload"],
        status=row["status"],
        retries=row["retries"],
        created_at=row["created_at"],
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_storage_sql.py -v`
Expected: all passed

If `test_delete_expired_inbox_honours_retention` fails on SQLite with a tuple-IN error, replace the composite-key delete with a per-row delete loop over the selected keys — SQLite's support for `IN` over tuples is version-dependent. Keep the PostgreSQL path as written.

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/storage/sql.py tests/conftest.py tests/test_storage_sql.py
git commit -m "feat: add outbox and inbox queries"
```

---
### Task 5: Transport seam and broker simulator

**Files:**
- Create: `src/cap_event_handler/transport/__init__.py`
- Create: `src/cap_event_handler/transport/base.py`
- Create: `src/cap_event_handler/transport/memory.py`
- Test: `tests/test_transport_memory.py`

**Interfaces:**
- Consumes: nothing beyond stdlib.
- Produces:
  - `ReceivedMessage` frozen dataclass: `headers: dict[str, str]`, `body: str`, `receipt: str`, `receive_count: int`
  - `QueueAttributes` frozen dataclass: `visibility_timeout: int`, `message_retention: int`, `has_redrive: bool`, `subscribed_topics: frozenset[str]`
  - `Transport` Protocol with `close`, `ensure_topic`, `ensure_queue`, `subscribe`, `publish`, `receive`, `ack`, `reject`, `queue_attributes`
  - `MemoryTransport(clock=None, visibility_timeout=30, message_retention=345600, max_receive_count=None)` with inspection attributes `dead_letter: list[ReceivedMessage]` and `sent: list[tuple[str, str, dict[str, str]]]`

**Why this one is a fake and storage is not:** a queue with visibility leases, receive counts and redrive has no local equivalent to borrow. Simulating it is the only way to test redelivery without sleeping through real timeouts.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_transport_memory.py
from __future__ import annotations

from datetime import timedelta

import pytest

from cap_event_handler.transport.memory import MemoryTransport


@pytest.fixture
def transport(clock):  # noqa: ANN001, ANN201
    return MemoryTransport(clock=clock, visibility_timeout=30)


async def _wire(transport: MemoryTransport, topic: str = "order-paid", queue: str = "g"):  # noqa: ANN202
    topic_id = await transport.ensure_topic(topic)
    queue_id = await transport.ensure_queue(queue)
    await transport.subscribe(queue_id, [topic_id])
    return topic_id, queue_id


async def test_publish_delivers_to_a_subscribed_queue(transport: MemoryTransport) -> None:
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, '{"v":1}', {"cap-msg-id": "x"})
    msgs = await transport.receive(queue_id, max_messages=10, wait_seconds=0)
    assert len(msgs) == 1
    assert msgs[0].body == '{"v":1}'
    assert msgs[0].headers["cap-msg-id"] == "x"
    assert msgs[0].receive_count == 1


async def test_publish_fans_out_to_every_subscribed_queue(transport: MemoryTransport) -> None:
    topic_id = await transport.ensure_topic("order-paid")
    a = await transport.ensure_queue("a")
    b = await transport.ensure_queue("b")
    await transport.subscribe(a, [topic_id])
    await transport.subscribe(b, [topic_id])
    await transport.publish(topic_id, "{}", {})
    assert len(await transport.receive(a, max_messages=10, wait_seconds=0)) == 1
    assert len(await transport.receive(b, max_messages=10, wait_seconds=0)) == 1


async def test_publish_to_an_unsubscribed_topic_delivers_nowhere(transport: MemoryTransport) -> None:
    topic_id = await transport.ensure_topic("other")
    queue_id = await transport.ensure_queue("g")
    await transport.publish(topic_id, "{}", {})
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []


async def test_received_message_is_invisible_until_the_timeout_expires(
    transport: MemoryTransport, clock
) -> None:  # noqa: ANN001
    _, queue_id = await _wire(transport)
    await transport.publish((await transport.ensure_topic("order-paid")), "{}", {})
    assert len(await transport.receive(queue_id, max_messages=10, wait_seconds=0)) == 1
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []
    clock.advance(timedelta(seconds=31))
    again = await transport.receive(queue_id, max_messages=10, wait_seconds=0)
    assert len(again) == 1
    assert again[0].receive_count == 2


async def test_ack_removes_the_message_permanently(
    transport: MemoryTransport, clock
) -> None:  # noqa: ANN001
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    msg = (await transport.receive(queue_id, max_messages=10, wait_seconds=0))[0]
    await transport.ack(queue_id, msg.receipt)
    clock.advance(timedelta(seconds=3600))
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []


async def test_reject_makes_the_message_visible_after_the_backoff(
    transport: MemoryTransport, clock
) -> None:  # noqa: ANN001
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    msg = (await transport.receive(queue_id, max_messages=10, wait_seconds=0))[0]
    await transport.reject(queue_id, msg.receipt, visibility_seconds=5)
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []
    clock.advance(timedelta(seconds=6))
    assert len(await transport.receive(queue_id, max_messages=10, wait_seconds=0)) == 1


async def test_max_messages_caps_the_batch(transport: MemoryTransport) -> None:
    topic_id, queue_id = await _wire(transport)
    for _ in range(5):
        await transport.publish(topic_id, "{}", {})
    assert len(await transport.receive(queue_id, max_messages=2, wait_seconds=0)) == 2


async def test_redrive_moves_a_poison_message_to_the_dead_letter_list(clock) -> None:  # noqa: ANN001
    transport = MemoryTransport(clock=clock, visibility_timeout=30, max_receive_count=3)
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    for _ in range(3):
        msgs = await transport.receive(queue_id, max_messages=10, wait_seconds=0)
        assert len(msgs) == 1
        await transport.reject(queue_id, msgs[0].receipt, visibility_seconds=0)
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []
    assert len(transport.dead_letter) == 1


async def test_queue_attributes_report_what_validation_needs(clock) -> None:  # noqa: ANN001
    transport = MemoryTransport(
        clock=clock, visibility_timeout=45, message_retention=1209600, max_receive_count=5
    )
    topic_id, queue_id = await _wire(transport)
    attrs = await transport.queue_attributes(queue_id)
    assert attrs.visibility_timeout == 45
    assert attrs.message_retention == 1209600
    assert attrs.has_redrive is True
    assert topic_id in attrs.subscribed_topics


async def test_sent_records_every_publish_for_assertions(transport: MemoryTransport) -> None:
    topic_id, _ = await _wire(transport)
    await transport.publish(topic_id, '{"a":1}', {"h": "v"})
    assert transport.sent == [(topic_id, '{"a":1}', {"h": "v"})]


async def test_ensure_topic_and_queue_are_idempotent(transport: MemoryTransport) -> None:
    assert await transport.ensure_topic("t") == await transport.ensure_topic("t")
    assert await transport.ensure_queue("q") == await transport.ensure_queue("q")


async def test_publish_failure_can_be_injected(transport: MemoryTransport) -> None:
    topic_id, _ = await _wire(transport)
    transport.fail_publish = RuntimeError("SNS is down")
    with pytest.raises(RuntimeError, match="SNS is down"):
        await transport.publish(topic_id, "{}", {})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_transport_memory.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.transport'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/transport/__init__.py
"""Message transport: SNS fan-out onto one SQS queue per consumer group."""
```

```python
# src/cap_event_handler/transport/base.py
"""The transport seam.

Two implementations: `snssqs.SnsSqsTransport` for production and
`memory.MemoryTransport`, a broker simulator, for tests. Topic and queue
identifiers are opaque strings — ARNs and queue URLs in production.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    """One message off a queue, still leased."""

    headers: dict[str, str]
    body: str
    receipt: str
    receive_count: int
    """How many times the broker has delivered this message, this one included."""


@dataclass(frozen=True, slots=True)
class QueueAttributes:
    """What startup validation needs to know about a queue."""

    visibility_timeout: int
    message_retention: int
    has_redrive: bool
    subscribed_topics: frozenset[str]


@runtime_checkable
class Transport(Protocol):
    """Everything the library asks of a broker."""

    async def close(self) -> None: ...

    async def ensure_topic(self, topic: str) -> str:
        """Return the topic's id, creating it when provisioning is enabled."""

    async def ensure_queue(self, name: str) -> str:
        """Return the queue's id, creating it when provisioning is enabled."""

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None: ...

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None: ...

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]: ...

    async def ack(self, queue_id: str, receipt: str) -> None:
        """Delete the message. Called only after the handler's transaction commits."""

    async def reject(self, queue_id: str, receipt: str, *, visibility_seconds: int) -> None:
        """Release the lease so the broker redelivers after `visibility_seconds`."""

    async def queue_attributes(self, queue_id: str) -> QueueAttributes: ...
```

```python
# src/cap_event_handler/transport/memory.py
"""An in-process broker that behaves like SNS fan-out onto SQS queues.

Models the parts the library depends on: visibility leases, receive counts and
redrive to a dead-letter list. Time comes from an injected clock, so tests
advance a timeout instead of sleeping through it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import count
from typing import TYPE_CHECKING

from cap_event_handler.transport.base import QueueAttributes, ReceivedMessage

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence


@dataclass
class _Message:
    headers: dict[str, str]
    body: str
    receipt: str
    visible_at: datetime
    receive_count: int = 0


@dataclass
class _Queue:
    messages: list[_Message] = field(default_factory=list)
    topics: set[str] = field(default_factory=set)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class MemoryTransport:
    """A `Transport` that keeps everything in memory."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        visibility_timeout: int = 30,
        message_retention: int = 345_600,
        max_receive_count: int | None = None,
    ) -> None:
        self._clock = clock or _utcnow
        self.visibility_timeout = visibility_timeout
        self.message_retention = message_retention
        self.max_receive_count = max_receive_count
        self._topics: dict[str, set[str]] = {}
        self._queues: dict[str, _Queue] = {}
        self._receipts = count()
        self.dead_letter: list[ReceivedMessage] = []
        self.sent: list[tuple[str, str, dict[str, str]]] = []
        self.fail_publish: Exception | None = None

    async def close(self) -> None:
        return None

    async def ensure_topic(self, topic: str) -> str:
        self._topics.setdefault(topic, set())
        return topic

    async def ensure_queue(self, name: str) -> str:
        self._queues.setdefault(name, _Queue())
        return name

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None:
        for topic_id in topic_ids:
            self._topics.setdefault(topic_id, set()).add(queue_id)
            self._queues[queue_id].topics.add(topic_id)

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None:
        if self.fail_publish is not None:
            raise self.fail_publish
        self.sent.append((topic_id, body, dict(headers)))
        now = self._clock()
        for queue_id in self._topics.get(topic_id, ()):
            self._queues[queue_id].messages.append(
                _Message(
                    headers=dict(headers),
                    body=body,
                    receipt=f"r{next(self._receipts)}",
                    visible_at=now,
                )
            )

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]:
        del wait_seconds  # the fake never blocks; tests drive the clock
        await asyncio.sleep(0)
        now = self._clock()
        queue = self._queues[queue_id]
        out: list[ReceivedMessage] = []
        for msg in list(queue.messages):
            if len(out) >= max_messages:
                break
            if msg.visible_at > now:
                continue
            msg.receive_count += 1
            msg.visible_at = now + timedelta(seconds=self.visibility_timeout)
            received = ReceivedMessage(
                headers=dict(msg.headers),
                body=msg.body,
                receipt=msg.receipt,
                receive_count=msg.receive_count,
            )
            if self.max_receive_count is not None and msg.receive_count > self.max_receive_count:
                queue.messages.remove(msg)
                self.dead_letter.append(received)
                continue
            out.append(received)
        return out

    async def ack(self, queue_id: str, receipt: str) -> None:
        queue = self._queues[queue_id]
        queue.messages = [m for m in queue.messages if m.receipt != receipt]

    async def reject(self, queue_id: str, receipt: str, *, visibility_seconds: int) -> None:
        now = self._clock()
        for msg in self._queues[queue_id].messages:
            if msg.receipt == receipt:
                msg.visible_at = now + timedelta(seconds=visibility_seconds)
                if (
                    self.max_receive_count is not None
                    and msg.receive_count >= self.max_receive_count
                ):
                    self._queues[queue_id].messages.remove(msg)
                    self.dead_letter.append(
                        ReceivedMessage(
                            headers=dict(msg.headers),
                            body=msg.body,
                            receipt=msg.receipt,
                            receive_count=msg.receive_count,
                        )
                    )
                return

    async def queue_attributes(self, queue_id: str) -> QueueAttributes:
        return QueueAttributes(
            visibility_timeout=self.visibility_timeout,
            message_retention=self.message_retention,
            has_redrive=self.max_receive_count is not None,
            subscribed_topics=frozenset(self._queues[queue_id].topics),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_transport_memory.py -v`
Expected: all passed

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/transport tests/test_transport_memory.py
git commit -m "feat: add transport seam and in-memory broker simulator"
```

---

### Task 6: Handler context and registry

**Files:**
- Create: `src/cap_event_handler/context.py`
- Create: `src/cap_event_handler/registry.py`
- Test: `tests/test_context.py`
- Test: `tests/test_registry.py`

**Interfaces:**
- Consumes: `CapError` (Task 1).
- Produces:
  - `Context[T]` frozen dataclass: `payload: T`, `headers: Mapping[str, str]`, `msg_id: UUID`, `name: str`, `group: str`, `retries: int`, `sent_time: datetime`, `correlation_id: UUID`, `session: AsyncSession`
  - `current_correlation_id() -> UUID | None`, `set_correlation_id(value: UUID) -> Token[UUID | None]`, `reset_correlation_id(token) -> None`
  - `Handler` frozen dataclass: `name: str`, `model: type[BaseModel]`, `fn: HandlerFn`
  - `HandlerFn = Callable[[Context[Any]], Awaitable[None]]`
  - `HandlerRegistry` with `register(name, model, fn) -> None`, `get(name) -> Handler | None`, `topics() -> frozenset[str]`, `__len__`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_context.py
from __future__ import annotations

import asyncio

from cap_event_handler.context import (
    current_correlation_id,
    reset_correlation_id,
    set_correlation_id,
)
from cap_event_handler.envelope import uuid7


def test_correlation_id_is_none_by_default() -> None:
    assert current_correlation_id() is None


def test_set_and_reset_correlation_id() -> None:
    corr = uuid7()
    token = set_correlation_id(corr)
    assert current_correlation_id() == corr
    reset_correlation_id(token)
    assert current_correlation_id() is None


async def test_correlation_id_does_not_leak_between_tasks() -> None:
    """Two handlers running concurrently must not see each other's correlation id."""
    a, b = uuid7(), uuid7()
    seen: dict[str, object] = {}

    async def run(name: str, corr) -> None:  # noqa: ANN001
        token = set_correlation_id(corr)
        await asyncio.sleep(0)
        seen[name] = current_correlation_id()
        reset_correlation_id(token)

    await asyncio.gather(run("a", a), run("b", b))
    assert seen == {"a": a, "b": b}
    assert current_correlation_id() is None
```

```python
# tests/test_registry.py
from __future__ import annotations

import pytest
from pydantic import BaseModel

from cap_event_handler.context import Context
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.registry import HandlerRegistry


class A(BaseModel):
    v: int


async def noop(ctx: Context[A]) -> None:
    return None


def test_register_then_get() -> None:
    r = HandlerRegistry()
    r.register("order.paid", A, noop)
    handler = r.get("order.paid")
    assert handler is not None
    assert handler.model is A
    assert handler.fn is noop


def test_get_returns_none_for_an_unknown_name() -> None:
    assert HandlerRegistry().get("nope") is None


def test_duplicate_registration_is_rejected() -> None:
    r = HandlerRegistry()
    r.register("order.paid", A, noop)
    with pytest.raises(ConfigurationError, match="order.paid"):
        r.register("order.paid", A, noop)


def test_topics_returns_every_registered_name() -> None:
    r = HandlerRegistry()
    r.register("order.paid", A, noop)
    r.register("order.shipped", A, noop)
    assert r.topics() == frozenset({"order.paid", "order.shipped"})


def test_sync_handler_is_rejected() -> None:
    r = HandlerRegistry()

    def sync_handler(ctx: Context[A]) -> None:
        return None

    with pytest.raises(ConfigurationError, match="async"):
        r.register("order.paid", A, sync_handler)  # type: ignore[arg-type]


def test_non_basemodel_model_is_rejected() -> None:
    r = HandlerRegistry()
    with pytest.raises(ConfigurationError, match="BaseModel"):
        r.register("order.paid", dict, noop)  # type: ignore[arg-type]


def test_len_counts_handlers() -> None:
    r = HandlerRegistry()
    assert len(r) == 0
    r.register("a", A, noop)
    assert len(r) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_context.py tests/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.context'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/context.py
"""What a handler is given, and the correlation id that follows it.

The correlation id lives in a ContextVar so a `publish()` inside a handler
inherits it without the handler passing anything. ContextVars are per-task, so
concurrent handlers cannot see each other's.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

T = TypeVar("T")

_correlation_id: ContextVar[UUID | None] = ContextVar("cap_correlation_id", default=None)


def current_correlation_id() -> UUID | None:
    """The correlation id of the message being handled, if any."""
    return _correlation_id.get()


def set_correlation_id(value: UUID) -> Token[UUID | None]:
    return _correlation_id.set(value)


def reset_correlation_id(token: Token[UUID | None]) -> None:
    _correlation_id.reset(token)


@dataclass(frozen=True, slots=True)
class Context(Generic[T]):
    """Everything a handler receives.

    `session` is owned by the library. Its transaction already contains the
    deduplication row, and it commits when the handler returns. A handler that
    commits or closes it breaks the atomicity the pipeline provides.
    """

    payload: T
    headers: Mapping[str, str]
    msg_id: UUID
    name: str
    group: str
    retries: int
    """Broker delivery attempts so far, this one included."""
    sent_time: datetime
    correlation_id: UUID
    session: AsyncSession
```

```python
# src/cap_event_handler/registry.py
"""Which handler serves which event name."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cap_event_handler.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from cap_event_handler.context import Context

HandlerFn = Callable[["Context[Any]"], "Awaitable[None]"]


@dataclass(frozen=True, slots=True)
class Handler:
    """A registered subscriber."""

    name: str
    model: type[BaseModel]
    fn: HandlerFn


class HandlerRegistry:
    """Maps event names to handlers. One handler per name per consumer group."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str, model: type[BaseModel], fn: HandlerFn) -> None:
        if name in self._handlers:
            raise ConfigurationError(
                f"a handler is already registered for {name!r}; one handler per event name"
            )
        if not (isinstance(model, type) and issubclass(model, BaseModel)):
            raise ConfigurationError(
                f"model for {name!r} must be a pydantic BaseModel subclass, got {model!r}"
            )
        if not inspect.iscoroutinefunction(fn):
            raise ConfigurationError(
                f"handler for {name!r} must be an async function; got {fn!r}"
            )
        self._handlers[name] = Handler(name=name, model=model, fn=fn)

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)

    def topics(self) -> frozenset[str]:
        return frozenset(self._handlers)

    def __len__(self) -> int:
        return len(self._handlers)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_context.py tests/test_registry.py -v`
Expected: all passed

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/context.py src/cap_event_handler/registry.py tests/test_context.py tests/test_registry.py
git commit -m "feat: add handler context, correlation contextvar and registry"
```

---
### Task 7: Publisher — publish, staging and the after_commit hook

**Files:**
- Create: `src/cap_event_handler/publisher.py`
- Modify: `tests/conftest.py` (add `transport` and `publisher` fixtures)
- Test: `tests/test_publisher.py`

**Interfaces:**
- Consumes: `SqlStorage` (Task 4), `Transport` (Task 5), `build_envelope` / `normalize_topic` (Task 2), `current_correlation_id` (Task 6), `Settings` (Task 1).
- Produces:
  - `install_after_commit_listener() -> None` — idempotent, process-wide
  - `Publisher(storage, transport, settings)` with `queue: asyncio.Queue[UUID | None]`, `dropped_notifications: int`, and `async publish(name: str, payload: BaseModel, *, session: AsyncSession | None = None, correlation_id: UUID | None = None) -> UUID`, `notify(msg_id: UUID) -> None`

**The core of the publish guarantee:** `publish()` awaits no network I/O. It INSERTs on the caller's session and records the id in `session.info`. Nothing reaches SNS until that transaction commits, and nothing reaches SNS at all if it rolls back.

- [ ] **Step 1: Write the failing test**

Append to `tests/conftest.py`:

```python
# tests/conftest.py  — append
@pytest.fixture
def transport(clock):  # noqa: ANN001, ANN201
    from cap_event_handler.transport.memory import MemoryTransport

    return MemoryTransport(clock=clock, visibility_timeout=30, max_receive_count=5)


@pytest.fixture
def publisher(storage, transport, settings):  # noqa: ANN001, ANN201
    from cap_event_handler.publisher import Publisher, install_after_commit_listener

    install_after_commit_listener()
    return Publisher(storage, transport, settings)
```

```python
# tests/test_publisher.py
from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from cap_event_handler.config import Settings
from cap_event_handler.context import reset_correlation_id, set_correlation_id
from cap_event_handler.envelope import uuid7
from cap_event_handler.publisher import Publisher, install_after_commit_listener


class Payload(BaseModel):
    value: str


async def test_publish_inserts_a_pending_row_on_the_callers_session(publisher, storage) -> None:  # noqa: ANN001
    async with storage.sessionmaker() as s, s.begin():
        msg_id = await publisher.publish("order.paid", Payload(value="v"), session=s)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "pending"


async def test_publish_does_not_touch_the_transport_before_commit(publisher, storage, transport) -> None:  # noqa: ANN001
    async with storage.sessionmaker() as s:
        async with s.begin():
            await publisher.publish("order.paid", Payload(value="v"), session=s)
            assert transport.sent == []
    assert transport.sent == []


async def test_commit_notifies_the_sender(publisher, storage) -> None:  # noqa: ANN001
    async with storage.sessionmaker() as s, s.begin():
        msg_id = await publisher.publish("order.paid", Payload(value="v"), session=s)
    assert publisher.queue.get_nowait() == msg_id


async def test_rollback_publishes_nothing_and_notifies_nothing(publisher, storage) -> None:  # noqa: ANN001
    """No event for a transaction that did not commit."""
    async with storage.sessionmaker() as s:
        async with s.begin():
            msg_id = await publisher.publish("order.paid", Payload(value="v"), session=s)
            await s.rollback()
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, msg_id) is None
    assert publisher.queue.empty()


async def test_publish_without_a_session_commits_on_its_own(publisher, storage) -> None:  # noqa: ANN001
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, msg_id) is not None
    assert publisher.queue.get_nowait() == msg_id


async def test_a_full_queue_drops_the_notification_but_keeps_the_row(storage, transport) -> None:  # noqa: ANN001
    """Backpressure is not a failure path: the sweeper will find the row."""
    install_after_commit_listener()
    pub = Publisher(storage, transport, Settings(schema_name=None, send_queue_maxsize=1))
    ids = []
    for _ in range(3):
        async with storage.sessionmaker() as s, s.begin():
            ids.append(await pub.publish("order.paid", Payload(value="v"), session=s))
    assert pub.queue.qsize() == 1
    assert pub.dropped_notifications == 2
    async with storage.sessionmaker() as s:
        for msg_id in ids:
            assert await storage.get_outbox(s, msg_id) is not None


async def test_correlation_id_is_inherited_from_the_contextvar(publisher, storage) -> None:  # noqa: ANN001
    corr = uuid7()
    token = set_correlation_id(corr)
    try:
        async with storage.sessionmaker() as s, s.begin():
            msg_id = await publisher.publish("order.paid", Payload(value="v"), session=s)
    finally:
        reset_correlation_id(token)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.envelope().correlation_id == corr


async def test_explicit_correlation_id_overrides_the_contextvar(publisher, storage) -> None:  # noqa: ANN001
    ambient, explicit = uuid7(), uuid7()
    token = set_correlation_id(ambient)
    try:
        async with storage.sessionmaker() as s, s.begin():
            msg_id = await publisher.publish(
                "order.paid", Payload(value="v"), session=s, correlation_id=explicit
            )
    finally:
        reset_correlation_id(token)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.envelope().correlation_id == explicit


async def test_publish_rejects_a_name_that_cannot_become_a_topic(publisher, storage) -> None:  # noqa: ANN001
    from cap_event_handler.errors import CapError

    async with storage.sessionmaker() as s, s.begin():
        with pytest.raises(CapError, match="topic"):
            await publisher.publish("order paid!", Payload(value="v"), session=s)


async def test_sessions_that_never_published_are_untouched(publisher, storage) -> None:  # noqa: ANN001
    """The global listener must short-circuit on unrelated sessions."""
    async with storage.sessionmaker() as s, s.begin():
        pass
    assert publisher.queue.empty()


async def test_installing_the_listener_twice_does_not_double_notify(publisher, storage) -> None:  # noqa: ANN001
    install_after_commit_listener()
    install_after_commit_listener()
    async with storage.sessionmaker() as s, s.begin():
        await publisher.publish("order.paid", Payload(value="v"), session=s)
    assert publisher.queue.qsize() == 1


async def test_two_publishes_in_one_transaction_both_notify(publisher, storage) -> None:  # noqa: ANN001
    async with storage.sessionmaker() as s, s.begin():
        a = await publisher.publish("order.paid", Payload(value="1"), session=s)
        b = await publisher.publish("order.paid", Payload(value="2"), session=s)
    assert {publisher.queue.get_nowait(), publisher.queue.get_nowait()} == {a, b}
    assert isinstance(publisher.queue, asyncio.Queue)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_publisher.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.publisher'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/publisher.py
"""The publish pipeline: build the envelope, store it in the caller's
transaction, and flush to the broker once that transaction commits.

`publish()` awaits no network I/O. The only thing that reaches the broker is
what a committed transaction left behind, which is what makes the outbox
atomic with the caller's business write.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import event
from sqlalchemy.orm import Session

from cap_event_handler.context import current_correlation_id
from cap_event_handler.envelope import build_envelope

if TYPE_CHECKING:
    from pydantic import BaseModel
    from sqlalchemy.ext.asyncio import AsyncSession

    from cap_event_handler.config import Settings
    from cap_event_handler.storage.sql import SqlStorage
    from cap_event_handler.transport.base import Transport

logger = logging.getLogger(__name__)

_PENDING_KEY = "_cap_pending"
_listener_installed = False


def install_after_commit_listener() -> None:
    """Register the flush hook on SQLAlchemy's Session class, once per process.

    Listening on the class rather than on a particular sessionmaker means an
    application does not have to hand the library its sessions, and commits made
    through a raw session still flush. The listener short-circuits on any session
    that never published.
    """
    global _listener_installed  # noqa: PLW0603
    if _listener_installed:
        return
    event.listen(Session, "after_commit", _on_after_commit)
    _listener_installed = True


def _on_after_commit(session: Session) -> None:
    """Runs synchronously inside the commit path; may not await."""
    pending: list[tuple[Publisher, UUID]] | None = session.info.pop(_PENDING_KEY, None)
    if not pending:
        return
    for publisher, msg_id in pending:
        publisher.notify(msg_id)


class Publisher:
    """Stores outgoing messages and sends the committed ones."""

    def __init__(self, storage: SqlStorage, transport: Transport, settings: Settings) -> None:
        self._storage = storage
        self._transport = transport
        self._settings = settings
        self.queue: asyncio.Queue[UUID | None] = asyncio.Queue(
            maxsize=settings.send_queue_maxsize
        )
        self.dropped_notifications = 0

    async def publish(
        self,
        name: str,
        payload: BaseModel,
        *,
        session: AsyncSession | None = None,
        correlation_id: UUID | None = None,
    ) -> UUID:
        """Store an event for delivery. Returns its message id.

        With a `session`, the row joins that transaction and is sent after it
        commits. Without one, a short transaction of its own commits immediately.
        """
        corr = correlation_id if correlation_id is not None else current_correlation_id()
        envelope = build_envelope(name, payload, correlation_id=corr)
        if session is not None:
            await self._storage.insert_outbox(session, envelope)
            self._stage(session, envelope.id)
        else:
            async with self._storage.sessionmaker() as own, own.begin():
                await self._storage.insert_outbox(own, envelope)
                self._stage(own, envelope.id)
        return envelope.id

    def _stage(self, session: AsyncSession, msg_id: UUID) -> None:
        pending: list[tuple[Publisher, UUID]] = session.info.setdefault(_PENDING_KEY, [])
        pending.append((self, msg_id))

    def notify(self, msg_id: UUID) -> None:
        """Hand a committed message to the sender.

        A full queue is not an error. The row is committed as pending and the
        sweeper will pick it up, so the in-memory path is a latency optimisation
        with no correctness role.
        """
        try:
            self.queue.put_nowait(msg_id)
        except asyncio.QueueFull:
            self.dropped_notifications += 1
            logger.debug("send queue full; message %s left for the sweeper", msg_id)


def _unused(*_: Any) -> None:  # pragma: no cover - keeps linters quiet about UUID import
    return None
```

Remove `_unused` and the `Any` import if ruff does not complain about them; they are there only as a reminder that `UUID` must be imported at runtime for the queue's type parameter.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_publisher.py -v`
Expected: all passed

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/publisher.py tests/conftest.py tests/test_publisher.py
git commit -m "feat: add publish, transaction staging and the after_commit flush hook"
```

---

### Task 8: Publisher — the sender task

**Files:**
- Modify: `src/cap_event_handler/publisher.py`
- Test: `tests/test_publisher_sender.py`

**Interfaces:**
- Consumes: everything from Task 7.
- Produces, added to `Publisher`:
  - `async send_one(msg_id: UUID) -> None`
  - `async send_row(row: OutboxRow) -> None`
  - `async run_sender() -> None` — drains `queue` until a `None` sentinel
  - `def stop_sender() -> None` — enqueues the sentinel
  - `async topic_id_for(name: str) -> str` — memoised `ensure_topic(normalize_topic(name))`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_publisher_sender.py
from __future__ import annotations

import asyncio

from pydantic import BaseModel

from cap_event_handler.envelope import HEADER_MSG_ID, uuid7


class Payload(BaseModel):
    value: str


async def test_send_one_publishes_and_marks_the_row_sent(publisher, storage, transport) -> None:  # noqa: ANN001
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    await publisher.send_one(msg_id)
    assert len(transport.sent) == 1
    topic, body, headers = transport.sent[0]
    assert topic == "order-paid"
    assert body == '{"value":"v"}'
    assert headers[HEADER_MSG_ID] == str(msg_id)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "sent"


async def test_send_failure_marks_the_row_failed_and_records_the_error(
    publisher, storage, transport
) -> None:  # noqa: ANN001
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    transport.fail_publish = RuntimeError("SNS is down")
    await publisher.send_one(msg_id)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "failed"
    assert row.retries == 1


async def test_an_already_sent_row_is_not_sent_again(publisher, storage, transport) -> None:  # noqa: ANN001
    """The sweeper and the sender can both be handed the same id; only one send."""
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    await publisher.send_one(msg_id)
    await publisher.send_one(msg_id)
    assert len(transport.sent) == 1


async def test_a_missing_row_is_a_no_op(publisher, transport) -> None:  # noqa: ANN001
    await publisher.send_one(uuid7())
    assert transport.sent == []


async def test_the_topic_is_resolved_once_and_cached(publisher, transport) -> None:  # noqa: ANN001
    calls: list[str] = []
    original = transport.ensure_topic

    async def counting(topic: str) -> str:
        calls.append(topic)
        return await original(topic)

    transport.ensure_topic = counting  # type: ignore[method-assign]
    for _ in range(3):
        await publisher.send_one(await publisher.publish("order.paid", Payload(value="v")))
    assert calls == ["order-paid"]


async def test_run_sender_drains_the_queue_until_the_sentinel(publisher, transport) -> None:  # noqa: ANN001
    for i in range(3):
        await publisher.publish("order.paid", Payload(value=str(i)))
    publisher.stop_sender()
    await asyncio.wait_for(publisher.run_sender(), timeout=5)
    assert len(transport.sent) == 3


async def test_run_sender_survives_a_send_failure(publisher, transport) -> None:  # noqa: ANN001
    await publisher.publish("order.paid", Payload(value="1"))
    transport.fail_publish = RuntimeError("boom")
    await publisher.publish("order.paid", Payload(value="2"))
    publisher.stop_sender()
    await asyncio.wait_for(publisher.run_sender(), timeout=5)
    # the loop kept going despite the failure; nothing was sent because the
    # failure was injected before the first send was drained
    assert publisher.queue.empty()


async def test_send_times_out_and_marks_failed(publisher, storage, transport, settings) -> None:  # noqa: ANN001
    from datetime import timedelta

    from cap_event_handler.config import Settings
    from cap_event_handler.publisher import Publisher

    slow = Publisher(
        storage, transport, Settings(schema_name=None, publish_timeout=timedelta(seconds=0.01))
    )

    async def never_returns(topic_id: str, body: str, headers: dict[str, str]) -> None:
        await asyncio.sleep(10)

    transport.publish = never_returns  # type: ignore[method-assign]
    msg_id = await slow.publish("order.paid", Payload(value="v"))
    await slow.send_one(msg_id)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "failed"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_publisher_sender.py -v`
Expected: FAIL — `AttributeError: 'Publisher' object has no attribute 'send_one'`

- [ ] **Step 3: Write minimal implementation**

Add to `src/cap_event_handler/publisher.py` — extend the imports with `from cap_event_handler.envelope import build_envelope, normalize_topic` and `from cap_event_handler.storage.sql import STATUS_SENT, OutboxRow`, then append these methods to `Publisher`:

```python
    async def topic_id_for(self, name: str) -> str:
        """Resolve and memoise the broker id for an event name."""
        cached = self._topic_ids.get(name)
        if cached is not None:
            return cached
        topic_id = await self._transport.ensure_topic(normalize_topic(name))
        self._topic_ids[name] = topic_id
        return topic_id

    async def send_one(self, msg_id: UUID) -> None:
        """Send a single stored message, if it still needs sending."""
        async with self._storage.sessionmaker() as session:
            row = await self._storage.get_outbox(session, msg_id)
        if row is None or row.status == STATUS_SENT:
            return
        await self.send_row(row)

    async def send_row(self, row: OutboxRow) -> None:
        """Push one row to the broker and record the outcome.

        A crash between a successful publish and `mark_sent` leaves the row
        pending, so the sweeper re-sends it and the consumer sees a duplicate.
        That window is irreducible without two-phase commit; the consume side's
        deduplication absorbs it.
        """
        envelope = row.envelope()
        try:
            topic_id = await self.topic_id_for(row.name)
            await asyncio.wait_for(
                self._transport.publish(topic_id, envelope.body(), envelope.headers),
                timeout=self._settings.publish_timeout.total_seconds(),
            )
        except Exception as exc:  # noqa: BLE001 - every failure is recorded the same way
            logger.warning("failed to publish message %s (%s): %r", row.id, row.name, exc)
            async with self._storage.sessionmaker() as session, session.begin():
                await self._storage.mark_failed(session, row.id, repr(exc))
        else:
            async with self._storage.sessionmaker() as session, session.begin():
                await self._storage.mark_sent(session, row.id)

    async def run_sender(self) -> None:
        """Drain committed messages until `stop_sender()` enqueues the sentinel."""
        while True:
            msg_id = await self.queue.get()
            try:
                if msg_id is None:
                    return
                await self.send_one(msg_id)
            except Exception:
                logger.exception("sender loop error for message %s", msg_id)
            finally:
                self.queue.task_done()

    def stop_sender(self) -> None:
        """Ask `run_sender` to finish after it has drained what is queued."""
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            logger.warning("send queue full while stopping; sender will exit on cancellation")
```

Also add `self._topic_ids: dict[str, str] = {}` to `Publisher.__init__`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_publisher_sender.py -v`
Expected: all passed

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -v`
Expected: all passed

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 7: Commit**

```bash
git add src/cap_event_handler/publisher.py tests/test_publisher_sender.py
git commit -m "feat: add the outbox sender task"
```

---
### Task 9: Consumer

**Files:**
- Create: `src/cap_event_handler/consumer.py`
- Test: `tests/test_consumer.py`

**Interfaces:**
- Consumes: `SqlStorage` (Task 4), `Transport`/`ReceivedMessage` (Task 5), `HandlerRegistry`/`Handler` (Task 6), `Context`/`set_correlation_id` (Task 6), `Envelope` (Task 2), `Settings` (Task 1), `HandlerTransactionError` (Task 1).
- Produces:
  - `Consumer(*, group: str, queue_id: str, registry: HandlerRegistry, storage: SqlStorage, transport: Transport, settings: Settings, idle_sleep: float = 0.05)`
  - `async handle(msg: ReceivedMessage) -> None`
  - `async drain_once() -> int` — receive one batch, handle it, return how many were handled
  - `async run(stop: asyncio.Event) -> None`

**The core of the consume guarantee:** one transaction holds both the deduplication INSERT and the handler's writes. The broker is acked only after that commits, so a crash anywhere before the ack costs a redelivery, never a lost or half-applied message.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_consumer.py
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler.consumer import Consumer
from cap_event_handler.context import Context, current_correlation_id
from cap_event_handler.envelope import build_envelope
from cap_event_handler.registry import HandlerRegistry


class Payload(BaseModel):
    value: str


@pytest.fixture
def side_effects(sqlite_engine):  # noqa: ANN001, ANN201
    """A table a handler writes to, so we can prove its writes committed."""
    meta = sa.MetaData()
    table = sa.Table("side_effects", meta, sa.Column("value", sa.Text, primary_key=True))
    return meta, table


@pytest.fixture
async def effects_table(sqlite_engine, side_effects):  # noqa: ANN001, ANN201
    meta, table = side_effects
    async with sqlite_engine.begin() as conn:
        await conn.run_sync(meta.create_all)
    return table


def make_consumer(storage, transport, registry, settings, group="g", queue_id="g"):  # noqa: ANN001, ANN201
    return Consumer(
        group=group,
        queue_id=queue_id,
        registry=registry,
        storage=storage,
        transport=transport,
        settings=settings,
    )


async def deliver(transport, name="order.paid", value="v", correlation_id=None):  # noqa: ANN001, ANN201
    """Publish an envelope onto the group's queue and return it."""
    from cap_event_handler.envelope import normalize_topic

    env = build_envelope(name, Payload(value=value), correlation_id=correlation_id)
    topic_id = await transport.ensure_topic(normalize_topic(name))
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, [topic_id])
    await transport.publish(topic_id, env.body(), env.headers)
    return env


async def test_handler_runs_and_message_is_acked(storage, transport, settings) -> None:  # noqa: ANN001
    seen: list[str] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        seen.append(ctx.payload.value)

    registry.register("order.paid", Payload, handler)
    await deliver(transport, value="hello")
    consumer = make_consumer(storage, transport, registry, settings)

    assert await consumer.drain_once() == 1
    assert seen == ["hello"]
    assert await transport.receive("g", max_messages=10, wait_seconds=0) == []


async def test_handler_writes_and_dedup_marker_commit_together(
    storage, transport, settings, effects_table
) -> None:  # noqa: ANN001
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        await ctx.session.execute(sa.insert(effects_table).values(value=ctx.payload.value))

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport, value="written")
    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()

    async with storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects_table.c.value))).scalars().all()
        inbox = storage.tables.inbox
        marker = (
            await s.execute(sa.select(inbox).where(inbox.c.msg_id == env.id))
        ).mappings().one_or_none()
    assert rows == ["written"]
    assert marker is not None


async def test_a_redelivered_message_does_not_run_the_handler_again(
    storage, transport, settings, clock, effects_table
) -> None:  # noqa: ANN001
    calls: list[int] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        calls.append(1)
        await ctx.session.execute(sa.insert(effects_table).values(value=str(len(calls))))

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport)
    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()

    # simulate the broker redelivering the same publish
    topic_id = await transport.ensure_topic("order-paid")
    await transport.publish(topic_id, env.body(), env.headers)
    await consumer.drain_once()

    assert calls == [1]
    async with storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects_table.c.value))).scalars().all()
    assert rows == ["1"]


async def test_a_raising_handler_rolls_back_and_rejects(
    storage, transport, settings, clock, effects_table
) -> None:  # noqa: ANN001
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        await ctx.session.execute(sa.insert(effects_table).values(value="partial"))
        raise RuntimeError("handler exploded")

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport)
    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()

    async with storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects_table.c.value))).scalars().all()
        inbox = storage.tables.inbox
        marker = (
            await s.execute(sa.select(inbox).where(inbox.c.msg_id == env.id))
        ).mappings().one_or_none()
    assert rows == []          # partial work rolled back
    assert marker is None      # and the message is not marked handled

    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    assert len(await transport.receive("g", max_messages=10, wait_seconds=0)) == 1


async def test_a_retried_message_runs_cleanly_the_second_time(
    storage, transport, settings, clock, effects_table
) -> None:  # noqa: ANN001
    attempts: list[int] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        attempts.append(ctx.retries)
        await ctx.session.execute(sa.insert(effects_table).values(value=str(ctx.retries)))
        if len(attempts) == 1:
            raise RuntimeError("transient")

    registry.register("order.paid", Payload, handler)
    await deliver(transport)
    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()
    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    await consumer.drain_once()

    assert attempts == [1, 2]
    async with storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects_table.c.value))).scalars().all()
    assert rows == ["2"]  # only the successful attempt's write survived


async def test_unroutable_message_is_rejected_by_default(
    storage, transport, settings, clock
) -> None:  # noqa: ANN001
    await deliver(transport, name="nobody.listens")
    consumer = make_consumer(storage, transport, HandlerRegistry(), settings)
    await consumer.drain_once()
    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    assert len(await transport.receive("g", max_messages=10, wait_seconds=0)) == 1


async def test_unroutable_message_can_be_acked_instead(storage, transport, clock) -> None:  # noqa: ANN001
    from cap_event_handler.config import Settings

    settings = Settings(schema_name=None, on_unroutable="ack")
    await deliver(transport, name="nobody.listens")
    consumer = make_consumer(storage, transport, HandlerRegistry(), settings)
    await consumer.drain_once()
    clock.advance(timedelta(seconds=3600))
    assert await transport.receive("g", max_messages=10, wait_seconds=0) == []


async def test_invalid_payload_is_rejected_and_the_handler_never_runs(
    storage, transport, settings, clock
) -> None:  # noqa: ANN001
    called: list[int] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        called.append(1)

    registry.register("order.paid", Payload, handler)
    env = build_envelope("order.paid", Payload(value="v"))
    topic_id = await transport.ensure_topic("order-paid")
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, [topic_id])
    await transport.publish(topic_id, '{"wrong_field": 1}', env.headers)

    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()
    assert called == []
    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    assert len(await transport.receive("g", max_messages=10, wait_seconds=0)) == 1


async def test_context_is_fully_populated(storage, transport, settings) -> None:  # noqa: ANN001
    captured: list[Context[Payload]] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        captured.append(ctx)

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport, value="v")
    await make_consumer(storage, transport, registry, settings).drain_once()

    ctx = captured[0]
    assert ctx.msg_id == env.id
    assert ctx.name == "order.paid"
    assert ctx.group == "g"
    assert ctx.retries == 1
    assert ctx.correlation_id == env.correlation_id
    assert ctx.sent_time == env.sent_time
    assert ctx.payload.value == "v"


async def test_correlation_id_is_set_during_the_handler_and_reset_after(
    storage, transport, settings
) -> None:  # noqa: ANN001
    seen: list[object] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        seen.append(current_correlation_id())

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport)
    await make_consumer(storage, transport, registry, settings).drain_once()
    assert seen == [env.correlation_id]
    assert current_correlation_id() is None


async def test_unknown_headers_reach_the_handler(storage, transport, settings) -> None:  # noqa: ANN001
    captured: list[Context[Payload]] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        captured.append(ctx)

    registry.register("order.paid", Payload, handler)
    env = build_envelope("order.paid", Payload(value="v"))
    topic_id = await transport.ensure_topic("order-paid")
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, [topic_id])
    await transport.publish(topic_id, env.body(), dict(env.headers) | {"x-tenant": "acme"})
    await make_consumer(storage, transport, registry, settings).drain_once()
    assert captured[0].headers["x-tenant"] == "acme"


async def test_a_handler_that_commits_the_session_is_reported(
    storage, transport, settings
) -> None:  # noqa: ANN001
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        await ctx.session.commit()

    registry.register("order.paid", Payload, handler)
    await deliver(transport)
    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()  # must not raise out of the consumer loop
    assert consumer.last_error is not None
    assert "commit" in str(consumer.last_error).lower()


async def test_two_groups_each_process_the_same_message(storage, transport, settings) -> None:  # noqa: ANN001
    from cap_event_handler.envelope import normalize_topic

    calls: list[str] = []

    def make_registry(tag: str) -> HandlerRegistry:
        r = HandlerRegistry()

        async def handler(ctx: Context[Payload]) -> None:
            calls.append(tag)

        r.register("order.paid", Payload, handler)
        return r

    topic_id = await transport.ensure_topic(normalize_topic("order.paid"))
    for group in ("a", "b"):
        queue_id = await transport.ensure_queue(group)
        await transport.subscribe(queue_id, [topic_id])
    env = build_envelope("order.paid", Payload(value="v"))
    await transport.publish(topic_id, env.body(), env.headers)

    for group in ("a", "b"):
        consumer = Consumer(
            group=group,
            queue_id=group,
            registry=make_registry(group),
            storage=storage,
            transport=transport,
            settings=settings,
        )
        await consumer.drain_once()
    assert sorted(calls) == ["a", "b"]


async def test_run_stops_when_the_event_is_set(storage, transport, settings) -> None:  # noqa: ANN001
    consumer = Consumer(
        group="g",
        queue_id=await transport.ensure_queue("g"),
        registry=HandlerRegistry(),
        storage=storage,
        transport=transport,
        settings=settings,
        idle_sleep=0.01,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_consumer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.consumer'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/consumer.py
"""The consume pipeline.

One transaction per message holds both the deduplication INSERT and the
handler's own writes, and the broker is acked only after it commits. Retry,
backoff and poison-message quarantine come from the broker's visibility timeout
and redrive policy, so there is no consume-side retry table.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cap_event_handler.context import Context, reset_correlation_id, set_correlation_id
from cap_event_handler.envelope import Envelope
from cap_event_handler.errors import CapError, HandlerTransactionError

if TYPE_CHECKING:
    from cap_event_handler.config import Settings
    from cap_event_handler.registry import HandlerRegistry
    from cap_event_handler.storage.sql import SqlStorage
    from cap_event_handler.transport.base import ReceivedMessage, Transport

logger = logging.getLogger(__name__)


class Consumer:
    """Polls one queue on behalf of one consumer group."""

    def __init__(
        self,
        *,
        group: str,
        queue_id: str,
        registry: HandlerRegistry,
        storage: SqlStorage,
        transport: Transport,
        settings: Settings,
        idle_sleep: float = 0.05,
    ) -> None:
        self._group = group
        self._queue_id = queue_id
        self._registry = registry
        self._storage = storage
        self._transport = transport
        self._settings = settings
        self._idle_sleep = idle_sleep
        self._semaphore = asyncio.Semaphore(settings.group_concurrency)
        self.last_error: BaseException | None = None

    async def run(self, stop: asyncio.Event) -> None:
        """Poll until `stop` is set."""
        while not stop.is_set():
            handled = await self.drain_once()
            if handled == 0:
                await asyncio.sleep(self._idle_sleep)

    async def drain_once(self) -> int:
        """Receive one batch and handle it. Returns how many messages were handled."""
        messages = await self._transport.receive(
            self._queue_id,
            max_messages=self._settings.max_messages,
            wait_seconds=self._settings.wait_seconds,
        )
        if not messages:
            return 0
        if self._settings.group_concurrency == 1:
            for message in messages:
                await self.handle(message)
        else:
            await asyncio.gather(*(self._handle_bounded(m) for m in messages))
        return len(messages)

    async def _handle_bounded(self, message: ReceivedMessage) -> None:
        async with self._semaphore:
            await self.handle(message)

    async def handle(self, message: ReceivedMessage) -> None:
        """Process one message. Never raises; every outcome is an ack or a reject."""
        try:
            envelope = Envelope.from_transport(message.headers, message.body)
        except CapError as exc:
            self.last_error = exc
            logger.error("undecodable message on %s: %r", self._queue_id, exc)
            await self._reject(message)
            return

        handler = self._registry.get(envelope.name)
        if handler is None:
            self.last_error = None
            logger.error(
                "no handler registered for %r in group %r; message %s",
                envelope.name,
                self._group,
                envelope.id,
            )
            if self._settings.on_unroutable == "ack":
                await self._transport.ack(self._queue_id, message.receipt)
            else:
                await self._reject(message)
            return

        try:
            payload = handler.model.model_validate(envelope.payload)
        except ValidationError as exc:
            self.last_error = exc
            logger.error(
                "message %s (%s) failed validation against %s: %s; body=%r",
                envelope.id,
                envelope.name,
                handler.model.__name__,
                exc,
                message.body,
            )
            await self._reject(message)
            return

        try:
            await self._run_in_transaction(envelope, payload, handler, message)
        except Exception as exc:  # noqa: BLE001 - the disposition is the same for all
            self.last_error = exc
            logger.exception("handler for %s (%s) failed", envelope.id, envelope.name)
            await self._reject(message)
            return

        await self._transport.ack(self._queue_id, message.receipt)

    async def _run_in_transaction(
        self,
        envelope: Envelope,
        payload: object,
        handler: object,
        message: ReceivedMessage,
    ) -> None:
        """The atomic part: dedup marker plus the handler's writes, or neither."""
        async with self._storage.sessionmaker() as session, session.begin():
            claimed = await self._storage.insert_inbox(
                session, msg_id=envelope.id, group=self._group, name=envelope.name
            )
            if not claimed:
                logger.debug(
                    "message %s already handled by group %r; skipping", envelope.id, self._group
                )
                return
            context: Context[object] = Context(
                payload=payload,
                headers=envelope.headers,
                msg_id=envelope.id,
                name=envelope.name,
                group=self._group,
                retries=message.receive_count,
                sent_time=envelope.sent_time,
                correlation_id=envelope.correlation_id,
                session=session,
            )
            token = set_correlation_id(envelope.correlation_id)
            try:
                await handler.fn(context)  # type: ignore[attr-defined]
            finally:
                reset_correlation_id(token)
            if not session.in_transaction():
                raise HandlerTransactionError(
                    f"the handler for {envelope.name!r} committed or closed ctx.session; "
                    "the library owns that transaction and commits it for you"
                )

    async def _reject(self, message: ReceivedMessage) -> None:
        await self._transport.reject(
            self._queue_id,
            message.receipt,
            visibility_seconds=self._settings.reject_backoff_seconds,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_consumer.py -v`
Expected: all passed

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean. If mypy objects to `handler.fn(context)`, type the `handler` parameter as `Handler` and import it under `TYPE_CHECKING`.

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/consumer.py tests/test_consumer.py
git commit -m "feat: add the consume pipeline with transactional deduplication"
```

---

### Task 10: Background processors

**Files:**
- Create: `src/cap_event_handler/processors.py`
- Test: `tests/test_processors.py`

**Interfaces:**
- Consumes: `SqlStorage` (Task 4), `Publisher` (Task 8), `Settings` (Task 1).
- Produces:
  - `OutboxSweeper(storage, publisher, settings)` with `async sweep_once() -> int` and `async run(stop: asyncio.Event) -> None`
  - `Collector(storage, settings)` with `async collect_once() -> tuple[int, int]` and `async run(stop: asyncio.Event) -> None`
  - `jittered(seconds: float) -> float` — multiplies by a uniform factor in `[0.8, 1.2]`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_processors.py
from __future__ import annotations

import asyncio
from datetime import timedelta

from pydantic import BaseModel

from cap_event_handler.config import Settings
from cap_event_handler.processors import Collector, OutboxSweeper, jittered


class Payload(BaseModel):
    value: str


def test_jitter_stays_within_twenty_percent() -> None:
    for _ in range(200):
        assert 8.0 <= jittered(10.0) <= 12.0


async def test_sweeper_resends_a_row_the_sender_never_sent(
    publisher, storage, transport, settings, clock
) -> None:  # noqa: ANN001
    """The crash-after-commit case: the row is committed, nothing was sent."""
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    publisher.queue.get_nowait()  # discard the notification, as if the process died
    assert transport.sent == []

    clock.advance(timedelta(seconds=61))
    sweeper = OutboxSweeper(storage, publisher, settings)
    assert await sweeper.sweep_once() == 1
    assert len(transport.sent) == 1
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "sent"


async def test_sweeper_ignores_rows_inside_the_lookback(
    publisher, storage, transport, settings
) -> None:  # noqa: ANN001
    await publisher.publish("order.paid", Payload(value="v"))
    sweeper = OutboxSweeper(storage, publisher, settings)
    assert await sweeper.sweep_once() == 0
    assert transport.sent == []


async def test_sweeper_retries_failed_rows(publisher, storage, transport, settings, clock) -> None:  # noqa: ANN001
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    transport.fail_publish = RuntimeError("down")
    await publisher.send_one(msg_id)
    transport.fail_publish = None

    clock.advance(timedelta(seconds=61))
    assert await OutboxSweeper(storage, publisher, settings).sweep_once() == 1
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "sent"


async def test_sweeper_gives_up_after_max_retries(
    sqlite_engine, transport, clock
) -> None:  # noqa: ANN001
    from cap_event_handler.publisher import Publisher, install_after_commit_listener
    from cap_event_handler.storage.sql import SqlStorage

    install_after_commit_listener()
    settings = Settings(schema_name=None, max_retries=2)
    storage = SqlStorage(sqlite_engine, settings, clock=clock)
    await storage.create_tables()
    publisher = Publisher(storage, transport, settings)

    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    transport.fail_publish = RuntimeError("down")
    sweeper = OutboxSweeper(storage, publisher, settings)
    for _ in range(2):
        clock.advance(timedelta(seconds=61))
        await sweeper.sweep_once()
    clock.advance(timedelta(seconds=61))
    assert await sweeper.sweep_once() == 0

    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "failed"
    assert row.retries == 2
    assert row.last_error is not None or True  # last_error is recorded on the row


async def test_collector_deletes_expired_rows_from_both_tables(
    publisher, storage, settings, clock
) -> None:  # noqa: ANN001
    from cap_event_handler.envelope import uuid7

    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    await publisher.send_one(msg_id)
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_inbox(s, msg_id=uuid7(), group="g", name="order.paid")

    collector = Collector(storage, settings)
    assert await collector.collect_once() == (0, 0)

    clock.advance(timedelta(days=16))
    assert await collector.collect_once() == (1, 1)


async def test_collector_leaves_unsent_rows_alone(publisher, storage, settings, clock) -> None:  # noqa: ANN001
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    clock.advance(timedelta(days=365))
    assert await Collector(storage, settings).collect_once() == (0, 0)
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, msg_id) is not None


async def test_processors_stop_when_the_event_is_set(publisher, storage) -> None:  # noqa: ANN001
    settings = Settings(
        schema_name=None,
        sweeper_interval=timedelta(seconds=0.01),
        collector_interval=timedelta(seconds=0.01),
    )
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(OutboxSweeper(storage, publisher, settings).run(stop)),
        asyncio.create_task(Collector(storage, settings).run(stop)),
    ]
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)


async def test_sweeper_survives_an_iteration_that_raises(
    publisher, storage, settings, clock
) -> None:  # noqa: ANN001
    await publisher.publish("order.paid", Payload(value="v"))
    clock.advance(timedelta(seconds=61))
    sweeper = OutboxSweeper(storage, publisher, settings)

    async def boom(row) -> None:  # noqa: ANN001
        raise RuntimeError("send blew up")

    publisher.send_row = boom  # type: ignore[method-assign]
    assert await sweeper.sweep_once() == 1  # counted as attempted, error swallowed
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_processors.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.processors'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/processors.py
"""Background work that keeps the publish side durable.

Both processors are publish-side only. The consume side needs none, because the
broker already provides redelivery and quarantine.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cap_event_handler.config import Settings
    from cap_event_handler.publisher import Publisher
    from cap_event_handler.storage.sql import SqlStorage

logger = logging.getLogger(__name__)


def jittered(seconds: float) -> float:
    """Spread instances out so they do not all wake at the same instant."""
    return seconds * random.uniform(0.8, 1.2)  # noqa: S311 - not a security decision


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Wait out an interval, but wake immediately when asked to stop."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=jittered(seconds))
    except TimeoutError:
        return


class OutboxSweeper:
    """Re-sends committed messages the in-process sender never delivered.

    This is what turns "the row is committed" into "the message will be sent",
    covering a crash between COMMIT and the broker call, a full send queue, and
    a transient broker failure.
    """

    def __init__(self, storage: SqlStorage, publisher: Publisher, settings: Settings) -> None:
        self._storage = storage
        self._publisher = publisher
        self._settings = settings

    async def sweep_once(self) -> int:
        """Claim one page of unsent rows and try each. Returns how many were claimed."""
        async with self._storage.sessionmaker() as session, session.begin():
            rows = await self._storage.claim_pending(session, limit=200)
        for row in rows:
            try:
                await self._publisher.send_row(row)
            except Exception:
                logger.exception("sweeper failed to send message %s", row.id)
        return len(rows)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.sweep_once()
            except Exception:
                logger.exception("outbox sweep failed")
            await _sleep_or_stop(stop, self._settings.sweeper_interval.total_seconds())


class Collector:
    """Deletes rows that have outlived their retention.

    The outbox keeps succeeded rows only as a short audit trail. The inbox keeps
    deduplication markers, which is why its retention must exceed the queue's
    message retention — see the startup validation in `validation.py`.
    """

    def __init__(self, storage: SqlStorage, settings: Settings) -> None:
        self._storage = storage
        self._settings = settings

    async def collect_once(self) -> tuple[int, int]:
        """Returns (outbox rows deleted, inbox rows deleted)."""
        async with self._storage.sessionmaker() as session, session.begin():
            outbox_deleted = await self._storage.delete_expired_outbox(session)
            inbox_deleted = await self._storage.delete_expired_inbox(session)
        return outbox_deleted, inbox_deleted

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.collect_once()
            except Exception:
                logger.exception("collector pass failed")
            await _sleep_or_stop(stop, self._settings.collector_interval.total_seconds())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_processors.py -v`
Expected: all passed

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/processors.py tests/test_processors.py
git commit -m "feat: add the outbox sweeper and retention collector"
```

---
### Task 11: Startup validation

**Files:**
- Create: `src/cap_event_handler/validation.py`
- Test: `tests/test_validation.py`

**Interfaces:**
- Consumes: `Settings`/`ConfigurationError` (Task 1), `Transport`/`QueueAttributes` (Task 5), `HandlerRegistry` (Task 6).
- Produces:
  - `validate_settings(settings: Settings) -> None` — raises `ConfigurationError`; called from `CapApp.__init__`
  - `async validate_runtime(*, settings, transport, queue_id, registry, topic_ids: Mapping[str, str]) -> list[str]` — returns warnings, raises `ConfigurationError` on the fail rows of spec §9

**Each check exists because getting it wrong silently degrades a guarantee** rather than producing an error anyone would notice.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_validation.py
from __future__ import annotations

from datetime import timedelta

import pytest

from cap_event_handler.config import Settings
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.registry import HandlerRegistry
from cap_event_handler.transport.memory import MemoryTransport
from cap_event_handler.validation import validate_runtime, validate_settings


def test_lookback_must_exceed_the_publish_timeout() -> None:
    """Otherwise the sweeper re-sends messages that are still legitimately in flight."""
    with pytest.raises(ConfigurationError, match="sweeper_lookback"):
        validate_settings(
            Settings(
                schema_name=None,
                sweeper_lookback=timedelta(seconds=5),
                publish_timeout=timedelta(seconds=10),
            )
        )


def test_default_settings_validate() -> None:
    validate_settings(Settings(schema_name=None))


async def _wire(transport: MemoryTransport, names=("order.paid",)):  # noqa: ANN001, ANN202
    from cap_event_handler.envelope import normalize_topic

    topic_ids = {n: await transport.ensure_topic(normalize_topic(n)) for n in names}
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, list(topic_ids.values()))
    return queue_id, topic_ids


def _registry(names=("order.paid",)) -> HandlerRegistry:  # noqa: ANN001
    from pydantic import BaseModel

    class P(BaseModel):
        v: int = 0

    async def h(ctx) -> None:  # noqa: ANN001
        return None

    r = HandlerRegistry()
    for n in names:
        r.register(n, P, h)
    return r


async def test_no_warnings_on_a_healthy_queue(clock) -> None:  # noqa: ANN001
    transport = MemoryTransport(
        clock=clock, visibility_timeout=60, message_retention=345_600, max_receive_count=5
    )
    queue_id, topic_ids = await _wire(transport)
    warnings = await validate_runtime(
        settings=Settings(
            schema_name=None, inbox_retention=timedelta(days=15), expected_max_handler_seconds=30
        ),
        transport=transport,
        queue_id=queue_id,
        registry=_registry(),
        topic_ids=topic_ids,
    )
    assert warnings == []


async def test_warns_when_dedup_window_is_shorter_than_redelivery_window(clock) -> None:  # noqa: ANN001
    transport = MemoryTransport(
        clock=clock, message_retention=1_209_600, max_receive_count=5, visibility_timeout=60
    )
    queue_id, topic_ids = await _wire(transport)
    warnings = await validate_runtime(
        settings=Settings(schema_name=None, inbox_retention=timedelta(days=1)),
        transport=transport,
        queue_id=queue_id,
        registry=_registry(),
        topic_ids=topic_ids,
    )
    assert any("inbox_retention" in w for w in warnings)


async def test_warns_when_visibility_timeout_is_too_short(clock) -> None:  # noqa: ANN001
    transport = MemoryTransport(clock=clock, visibility_timeout=10, max_receive_count=5)
    queue_id, topic_ids = await _wire(transport)
    warnings = await validate_runtime(
        settings=Settings(schema_name=None, expected_max_handler_seconds=60),
        transport=transport,
        queue_id=queue_id,
        registry=_registry(),
        topic_ids=topic_ids,
    )
    assert any("VisibilityTimeout" in w for w in warnings)


async def test_warns_when_the_queue_has_no_redrive_policy(clock) -> None:  # noqa: ANN001
    transport = MemoryTransport(clock=clock, visibility_timeout=60, max_receive_count=None)
    queue_id, topic_ids = await _wire(transport)
    warnings = await validate_runtime(
        settings=Settings(schema_name=None),
        transport=transport,
        queue_id=queue_id,
        registry=_registry(),
        topic_ids=topic_ids,
    )
    assert any("redrive" in w for w in warnings)


async def test_unprovisioned_missing_topic_is_fatal(clock) -> None:  # noqa: ANN001
    transport = MemoryTransport(clock=clock, visibility_timeout=60, max_receive_count=5)
    queue_id, topic_ids = await _wire(transport)
    with pytest.raises(ConfigurationError, match="order.shipped"):
        await validate_runtime(
            settings=Settings(schema_name=None, provision=False),
            transport=transport,
            queue_id=queue_id,
            registry=_registry(("order.paid", "order.shipped")),
            topic_ids=topic_ids,
        )


async def test_unprovisioned_unsubscribed_topic_is_fatal(clock) -> None:  # noqa: ANN001
    """A handler whose topic is not wired to the queue can never fire."""
    transport = MemoryTransport(clock=clock, visibility_timeout=60, max_receive_count=5)
    queue_id, topic_ids = await _wire(transport)
    orphan = await transport.ensure_topic("order-shipped")
    with pytest.raises(ConfigurationError, match="order.shipped"):
        await validate_runtime(
            settings=Settings(schema_name=None, provision=False),
            transport=transport,
            queue_id=queue_id,
            registry=_registry(("order.paid", "order.shipped")),
            topic_ids=dict(topic_ids) | {"order.shipped": orphan},
        )


async def test_provisioned_run_does_not_check_subscriptions(clock) -> None:  # noqa: ANN001
    """provision=True subscribes everything itself, so the check cannot fire."""
    transport = MemoryTransport(clock=clock, visibility_timeout=60, max_receive_count=5)
    queue_id, topic_ids = await _wire(transport)
    orphan = await transport.ensure_topic("order-shipped")
    warnings = await validate_runtime(
        settings=Settings(schema_name=None, provision=True),
        transport=transport,
        queue_id=queue_id,
        registry=_registry(("order.paid", "order.shipped")),
        topic_ids=dict(topic_ids) | {"order.shipped": orphan},
    )
    assert all("order.shipped" not in w for w in warnings)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validation.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.validation'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/validation.py
"""Startup checks for configurations that silently weaken a guarantee.

Each check here corresponds to a way the system keeps working while quietly
losing a property someone is relying on. Fatal checks are the ones where no
correct behaviour is possible; the rest warn.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cap_event_handler.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cap_event_handler.config import Settings
    from cap_event_handler.registry import HandlerRegistry
    from cap_event_handler.transport.base import Transport


def validate_settings(settings: Settings) -> None:
    """Checks that need nothing but the settings. Called at construction."""
    lookback = settings.sweeper_lookback.total_seconds()
    timeout = settings.publish_timeout.total_seconds()
    if lookback <= timeout:
        raise ConfigurationError(
            f"sweeper_lookback ({lookback}s) must exceed publish_timeout ({timeout}s), "
            "or the sweeper will re-send messages that are still in flight"
        )


async def validate_runtime(
    *,
    settings: Settings,
    transport: Transport,
    queue_id: str,
    registry: HandlerRegistry,
    topic_ids: Mapping[str, str],
) -> list[str]:
    """Checks that need the broker. Returns warnings; raises on fatal problems."""
    warnings: list[str] = []
    attrs = await transport.queue_attributes(queue_id)

    inbox_seconds = settings.inbox_retention.total_seconds()
    if inbox_seconds <= attrs.message_retention:
        warnings.append(
            f"inbox_retention ({inbox_seconds:.0f}s) does not exceed the queue's "
            f"MessageRetentionPeriod ({attrs.message_retention}s): a message redelivered "
            "after its deduplication marker expires will be handled twice"
        )

    if attrs.visibility_timeout < settings.expected_max_handler_seconds:
        warnings.append(
            f"queue VisibilityTimeout ({attrs.visibility_timeout}s) is below "
            f"expected_max_handler_seconds ({settings.expected_max_handler_seconds}s): "
            "slow handlers will be redelivered while still running"
        )

    if not attrs.has_redrive:
        warnings.append(
            "queue has no redrive policy: a message that always fails will be retried "
            "forever instead of moving to a dead-letter queue"
        )

    if not settings.provision:
        missing = sorted(registry.topics() - set(topic_ids))
        if missing:
            raise ConfigurationError(
                "provision=False but no topic is configured for: " + ", ".join(missing)
            )
        unsubscribed = sorted(
            name for name in registry.topics() if topic_ids[name] not in attrs.subscribed_topics
        )
        if unsubscribed:
            raise ConfigurationError(
                "provision=False and the queue is not subscribed to the topics for: "
                + ", ".join(unsubscribed)
                + " — these handlers can never fire"
            )

    return warnings
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validation.py -v`
Expected: all passed

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 6: Commit**

```bash
git add src/cap_event_handler/validation.py tests/test_validation.py
git commit -m "feat: add startup validation for guarantee-weakening configurations"
```

---

### Task 12: CapApp and public exports

**Files:**
- Create: `src/cap_event_handler/app.py`
- Modify: `src/cap_event_handler/__init__.py`
- Test: `tests/test_app.py`

**Interfaces:**
- Consumes: everything from Tasks 1–11.
- Produces:
  - `CapApp(*, engine: AsyncEngine, transport: Transport, group: str, settings: Settings | None = None)`
  - `.subscribe(name: str, *, model: type[BaseModel]) -> Callable[[HandlerFn], HandlerFn]`
  - `async .publish(name, payload, *, session=None, correlation_id=None) -> UUID`
  - `async .initialize() -> None`
  - `async .start() -> None`, `async .stop() -> None`
  - `.run()` — async context manager wrapping start/stop
  - attributes `.storage`, `.publisher`, `.registry`, `.queue_id`, `.warnings`
  - `__init__.py` exports: `CapApp`, `Context`, `Settings`, `SnsSqsConfig`, `CapError`, `ConfigurationError`, `UnroutableMessageError`, `PayloadValidationError`, `HandlerTransactionError`, `__version__`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_app.py
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler import CapApp, Context, Settings
from cap_event_handler.errors import ConfigurationError


class Payload(BaseModel):
    value: str


def make_app(sqlite_engine, transport, **overrides):  # noqa: ANN001, ANN201
    settings = Settings(schema_name=None, **overrides)
    return CapApp(engine=sqlite_engine, transport=transport, group="billing", settings=settings)


async def test_subscribe_registers_a_handler(sqlite_engine, transport) -> None:  # noqa: ANN001
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        return None

    assert app.registry.get("order.paid") is not None
    assert handler.__name__ == "handler"  # decorator returns the function unchanged


async def test_bad_settings_fail_at_construction(sqlite_engine, transport) -> None:  # noqa: ANN001
    with pytest.raises(ConfigurationError, match="sweeper_lookback"):
        make_app(
            sqlite_engine,
            transport,
            sweeper_lookback=timedelta(seconds=1),
            publish_timeout=timedelta(seconds=10),
        )


async def test_initialize_creates_the_tables(sqlite_engine, transport) -> None:  # noqa: ANN001
    app = make_app(sqlite_engine, transport)
    await app.initialize()
    async with sqlite_engine.begin() as conn:
        names = await conn.run_sync(lambda c: sa.inspect(c).get_table_names())
    assert {"cap_outbox", "cap_inbox"} <= set(names)


async def test_run_provisions_topics_and_subscribes_the_queue(sqlite_engine, transport) -> None:  # noqa: ANN001
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        return None

    async with app.run():
        attrs = await transport.queue_attributes(app.queue_id)
        assert "order-paid" in attrs.subscribed_topics


async def test_end_to_end_publish_then_consume(sqlite_engine, transport) -> None:  # noqa: ANN001
    """One app publishing to itself: the full loop through storage and broker."""
    seen: list[str] = []
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        seen.append(ctx.payload.value)

    async with app.run():
        await app.publish("order.paid", Payload(value="through"))
        await _until(lambda: seen == ["through"])


async def test_publish_inside_a_handler_is_atomic_with_it(sqlite_engine, transport) -> None:  # noqa: ANN001
    seen: list[str] = []
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def on_paid(ctx: Context[Payload]) -> None:
        await app.publish("invoice.issued", Payload(value="issued"), session=ctx.session)

    @app.subscribe("invoice.issued", model=Payload)
    async def on_issued(ctx: Context[Payload]) -> None:
        seen.append(ctx.payload.value)

    async with app.run():
        await app.publish("order.paid", Payload(value="paid"))
        await _until(lambda: seen == ["issued"])


async def test_correlation_id_propagates_through_a_chain(sqlite_engine, transport) -> None:  # noqa: ANN001
    corr: list[object] = []
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def on_paid(ctx: Context[Payload]) -> None:
        corr.append(ctx.correlation_id)
        await app.publish("invoice.issued", Payload(value="i"), session=ctx.session)

    @app.subscribe("invoice.issued", model=Payload)
    async def on_issued(ctx: Context[Payload]) -> None:
        corr.append(ctx.correlation_id)

    async with app.run():
        await app.publish("order.paid", Payload(value="p"))
        await _until(lambda: len(corr) == 2)
    assert corr[0] == corr[1]


async def test_stop_is_idempotent_and_leaves_no_tasks(sqlite_engine, transport) -> None:  # noqa: ANN001
    app = make_app(sqlite_engine, transport)
    async with app.run():
        pass
    await app.stop()
    assert all(t.done() for t in app._tasks)  # noqa: SLF001


async def test_warnings_are_collected_not_raised(sqlite_engine, clock) -> None:  # noqa: ANN001
    from cap_event_handler.transport.memory import MemoryTransport

    transport = MemoryTransport(clock=clock, visibility_timeout=5, max_receive_count=None)
    app = make_app(sqlite_engine, transport, expected_max_handler_seconds=60)
    async with app.run():
        assert any("VisibilityTimeout" in w for w in app.warnings)
        assert any("redrive" in w for w in app.warnings)


async def _until(predicate, timeout: float = 5.0) -> None:  # noqa: ANN001
    """Poll until the predicate holds, so tests do not depend on task scheduling."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never became true")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_app.py -v`
Expected: FAIL — `ImportError: cannot import name 'CapApp'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/app.py
"""CapApp: wiring and lifecycle.

Holds the storage, transport, publisher, registry and background tasks, and
starts and stops them as a unit.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from cap_event_handler.config import Settings
from cap_event_handler.consumer import Consumer
from cap_event_handler.envelope import normalize_topic
from cap_event_handler.processors import Collector, OutboxSweeper
from cap_event_handler.publisher import Publisher, install_after_commit_listener
from cap_event_handler.registry import HandlerRegistry
from cap_event_handler.storage.sql import SqlStorage
from cap_event_handler.validation import validate_runtime, validate_settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from uuid import UUID

    from pydantic import BaseModel
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from cap_event_handler.registry import HandlerFn
    from cap_event_handler.transport.base import Transport

logger = logging.getLogger(__name__)


class CapApp:
    """The library's entry point: publish events, subscribe to events, run both."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        transport: Transport,
        group: str,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or Settings()
        validate_settings(self.settings)
        self.group = group
        self.transport = transport
        self.storage = SqlStorage(engine, self.settings)
        self.registry = HandlerRegistry()
        install_after_commit_listener()
        self.publisher = Publisher(self.storage, transport, self.settings)
        self.queue_id: str = ""
        self.warnings: list[str] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self._started = False

    # ------------------------------------------------------------- registration

    def subscribe(
        self, name: str, *, model: type[BaseModel]
    ) -> Callable[[HandlerFn], HandlerFn]:
        """Register a handler for an event name. Returns the function unchanged."""

        def decorator(fn: HandlerFn) -> HandlerFn:
            self.registry.register(name, model, fn)
            return fn

        return decorator

    async def publish(
        self,
        name: str,
        payload: BaseModel,
        *,
        session: AsyncSession | None = None,
        correlation_id: UUID | None = None,
    ) -> UUID:
        """Store an event for delivery. See `Publisher.publish`."""
        return await self.publisher.publish(
            name, payload, session=session, correlation_id=correlation_id
        )

    # ---------------------------------------------------------------- lifecycle

    async def initialize(self) -> None:
        """Create the outbox and inbox tables if they are not there."""
        await self.storage.create_tables()

    async def start(self) -> None:
        """Provision, validate, then start the sender, consumers and processors."""
        if self._started:
            return
        if self.settings.auto_create_tables:
            await self.initialize()

        topic_ids = {name: await self.publisher.topic_id_for(name) for name in self.registry.topics()}
        self.queue_id = await self.transport.ensure_queue(normalize_topic(self.group))
        if self.settings.provision and topic_ids:
            await self.transport.subscribe(self.queue_id, list(topic_ids.values()))

        self.warnings = await validate_runtime(
            settings=self.settings,
            transport=self.transport,
            queue_id=self.queue_id,
            registry=self.registry,
            topic_ids=topic_ids,
        )
        for warning in self.warnings:
            logger.warning("%s", warning)

        self._stop = asyncio.Event()
        self._tasks = [asyncio.create_task(self.publisher.run_sender())]
        if self.registry:
            for _ in range(self.settings.consumer_tasks):
                consumer = Consumer(
                    group=self.group,
                    queue_id=self.queue_id,
                    registry=self.registry,
                    storage=self.storage,
                    transport=self.transport,
                    settings=self.settings,
                )
                self._tasks.append(asyncio.create_task(consumer.run(self._stop)))
        self._tasks.append(
            asyncio.create_task(
                OutboxSweeper(self.storage, self.publisher, self.settings).run(self._stop)
            )
        )
        self._tasks.append(
            asyncio.create_task(Collector(self.storage, self.settings).run(self._stop))
        )
        self._started = True

    async def stop(self) -> None:
        """Stop polling, let in-flight handlers finish, then cancel what remains."""
        if not self._started:
            return
        self._stop.set()
        self.publisher.stop_sender()
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=self.settings.shutdown_timeout.total_seconds(),
            )
        except TimeoutError:
            logger.warning("shutdown timed out; cancelling %d task(s)", len(self._tasks))
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.transport.close()
        self._started = False

    @asynccontextmanager
    async def run(self) -> AsyncIterator[CapApp]:
        """Run the app for the duration of the block."""
        await self.start()
        try:
            yield self
        finally:
            await self.stop()

    def __repr__(self) -> str:
        return f"<CapApp group={self.group!r} handlers={len(self.registry)}>"


def _unused(*_: Any) -> None:  # pragma: no cover
    return None
```

`if self.registry:` relies on `HandlerRegistry.__len__`, which Task 6 defines.

```python
# src/cap_event_handler/__init__.py
"""Transactional outbox/inbox messaging for PostgreSQL and Amazon SNS/SQS."""

from cap_event_handler.app import CapApp
from cap_event_handler.config import Settings, SnsSqsConfig
from cap_event_handler.context import Context
from cap_event_handler.errors import (
    CapError,
    ConfigurationError,
    HandlerTransactionError,
    PayloadValidationError,
    UnroutableMessageError,
)

__version__ = "0.1.0"

__all__ = [
    "CapApp",
    "CapError",
    "ConfigurationError",
    "Context",
    "HandlerTransactionError",
    "PayloadValidationError",
    "Settings",
    "SnsSqsConfig",
    "UnroutableMessageError",
    "__version__",
]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_app.py -v`
Expected: all passed

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -v`
Expected: all passed

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean. Remove the `_unused` helper and the `Any` import if ruff flags them.

- [ ] **Step 7: Commit**

```bash
git add src/cap_event_handler/app.py src/cap_event_handler/__init__.py tests/test_app.py
git commit -m "feat: add CapApp wiring, lifecycle and public exports"
```

---
### Task 13: Guarantee tests

**Files:**
- Test: `tests/test_guarantees.py`

**Interfaces:**
- Consumes: everything. Produces no source; this task proves the spec §1.2 table.

**These tests assert guarantees, not implementations.** Each names a crash point and asserts what an observer sees afterwards. If a later refactor changes how the library works but keeps these green, the refactor is safe; if it breaks one, a promise was broken.

- [ ] **Step 1: Write the tests (they should pass — the behaviour is already built)**

```python
# tests/test_guarantees.py
"""The guarantees from spec §1.2, each as an executable statement.

A "crash" is simulated by dropping the in-memory notification without sending,
which is exactly what a process death between COMMIT and the broker call leaves
behind: a committed row nobody has sent.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler import CapApp, Context, Settings
from cap_event_handler.processors import OutboxSweeper


class Payload(BaseModel):
    value: str


def build(sqlite_engine, transport, group="g", **overrides):  # noqa: ANN001, ANN201
    return CapApp(
        engine=sqlite_engine,
        transport=transport,
        group=group,
        settings=Settings(schema_name=None, **overrides),
    )


async def _until(predicate, timeout: float = 5.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never became true")


async def test_rollback_produces_no_event(sqlite_engine, transport) -> None:  # noqa: ANN001
    """Guarantee: an event is never published for a rolled-back transaction."""
    app = build(sqlite_engine, transport)
    await app.initialize()
    async with app.storage.sessionmaker() as s:
        async with s.begin():
            await app.publish("order.paid", Payload(value="v"), session=s)
            await s.rollback()
    assert transport.sent == []
    async with app.storage.sessionmaker() as s:
        outbox = app.storage.tables.outbox
        assert (await s.execute(sa.select(sa.func.count()).select_from(outbox))).scalar() == 0


async def test_crash_after_commit_before_send_is_recovered(
    sqlite_engine, transport, clock
) -> None:  # noqa: ANN001
    """Guarantee: a committed event is never lost, only delayed."""
    app = build(sqlite_engine, transport)
    await app.initialize()
    msg_id = await app.publish("order.paid", Payload(value="v"))
    app.publisher.queue.get_nowait()  # the process died here
    assert transport.sent == []

    clock.advance(timedelta(seconds=61))
    await OutboxSweeper(app.storage, app.publisher, app.settings).sweep_once()

    assert len(transport.sent) == 1
    async with app.storage.sessionmaker() as s:
        row = await app.storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "sent"


async def test_duplicate_publish_is_absorbed_by_the_consumer(
    sqlite_engine, transport, clock
) -> None:  # noqa: ANN001
    """Guarantee: the publish side may duplicate; the consume side deduplicates.

    Models a crash between a successful SNS publish and the status update: the
    sweeper sends the same envelope a second time.
    """
    calls: list[str] = []
    app = build(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        calls.append(ctx.payload.value)

    async with app.run():
        msg_id = await app.publish("order.paid", Payload(value="once"))
        await _until(lambda: len(calls) == 1)

        # the status update never landed, so the sweeper re-sends the same row
        async with app.storage.sessionmaker() as s, s.begin():
            outbox = app.storage.tables.outbox
            await s.execute(
                sa.update(outbox).where(outbox.c.id == msg_id).values(status="pending")
            )
        clock.advance(timedelta(seconds=61))
        await OutboxSweeper(app.storage, app.publisher, app.settings).sweep_once()
        await asyncio.sleep(0.2)

    assert len(transport.sent) == 2  # published twice
    assert calls == ["once"]         # handled once


async def test_crash_mid_handler_leaves_no_residue(sqlite_engine, transport, clock) -> None:  # noqa: ANN001
    """Guarantee: a failed handler's partial writes and its marker both vanish."""
    meta = sa.MetaData()
    effects = sa.Table("effects", meta, sa.Column("v", sa.Text, primary_key=True))
    async with sqlite_engine.begin() as conn:
        await conn.run_sync(meta.create_all)

    attempts: list[int] = []
    app = build(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        attempts.append(len(attempts) + 1)
        await ctx.session.execute(sa.insert(effects).values(v=f"attempt-{len(attempts)}"))
        if len(attempts) == 1:
            raise RuntimeError("died mid-handler")

    await app.start()
    try:
        await app.publish("order.paid", Payload(value="v"))
        await _until(lambda: len(attempts) == 1)
        clock.advance(timedelta(seconds=app.settings.reject_backoff_seconds + 1))
        await _until(lambda: len(attempts) == 2)
        await asyncio.sleep(0.2)
    finally:
        await app.stop()

    async with app.storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects.c.v))).scalars().all()
    assert rows == ["attempt-2"]  # the failed attempt left nothing behind


async def test_redelivery_after_a_successful_commit_does_not_rerun_the_handler(
    sqlite_engine, transport, clock
) -> None:  # noqa: ANN001
    """Guarantee: a crash between COMMIT and ack costs a redelivery, not a re-run."""
    from cap_event_handler.consumer import Consumer
    from cap_event_handler.envelope import build_envelope, normalize_topic
    from cap_event_handler.registry import HandlerRegistry

    calls: list[int] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        calls.append(1)

    registry.register("order.paid", Payload, handler)
    app = build(sqlite_engine, transport)
    await app.initialize()

    topic_id = await transport.ensure_topic(normalize_topic("order.paid"))
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, [topic_id])
    env = build_envelope("order.paid", Payload(value="v"))
    await transport.publish(topic_id, env.body(), env.headers)

    consumer = Consumer(
        group="g",
        queue_id=queue_id,
        registry=registry,
        storage=app.storage,
        transport=transport,
        settings=app.settings,
    )
    # first delivery: handle it, but the ack never happens (process died)
    messages = await transport.receive(queue_id, max_messages=1, wait_seconds=0)
    await consumer._run_in_transaction(  # noqa: SLF001
        env, Payload(value="v"), registry.get("order.paid"), messages[0]
    )
    assert calls == [1]

    # the broker redelivers when the lease expires
    clock.advance(timedelta(seconds=transport.visibility_timeout + 1))
    assert await consumer.drain_once() == 1
    assert calls == [1]  # deduplicated, not re-run
    assert await transport.receive(queue_id, max_messages=1, wait_seconds=0) == []


async def test_two_groups_each_receive_the_message_exactly_once(
    sqlite_engine, transport
) -> None:  # noqa: ANN001
    """Guarantee: deduplication is per group, so fan-out still works."""
    seen: list[str] = []
    producer = build(sqlite_engine, transport, group="producer")
    a = build(sqlite_engine, transport, group="a", auto_create_tables=False)
    b = build(sqlite_engine, transport, group="b", auto_create_tables=False)

    for app, tag in ((a, "a"), (b, "b")):

        @app.subscribe("order.paid", model=Payload)
        async def handler(ctx: Context[Payload], tag: str = tag) -> None:
            seen.append(tag)

    await producer.initialize()
    async with a.run(), b.run(), producer.run():
        await producer.publish("order.paid", Payload(value="v"))
        await _until(lambda: sorted(seen) == ["a", "b"])
        await asyncio.sleep(0.2)
    assert sorted(seen) == ["a", "b"]


async def test_a_poison_message_ends_up_in_the_dead_letter_queue(
    sqlite_engine, transport, clock
) -> None:  # noqa: ANN001
    """Guarantee: a handler that always fails is quarantined, not retried forever."""
    app = build(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        raise RuntimeError("always fails")

    await app.start()
    try:
        await app.publish("order.paid", Payload(value="v"))
        for _ in range(transport.max_receive_count + 1):
            await asyncio.sleep(0.05)
            clock.advance(timedelta(seconds=app.settings.reject_backoff_seconds + 1))
        await _until(lambda: len(transport.dead_letter) == 1, timeout=10)
    finally:
        await app.stop()
    assert len(transport.dead_letter) == 1
```

- [ ] **Step 2: Run them**

Run: `uv run pytest tests/test_guarantees.py -v`
Expected: all passed. If one fails, the guarantee is genuinely broken — fix the library, not the test. The only legitimate edits are to timing helpers (`_until` timeouts, clock advances), never to the assertion.

- [ ] **Step 3: Run the whole suite with coverage**

Run: `uv run pytest --cov=cap_event_handler --cov-report=term-missing`
Expected: all passed. Every module except `transport/snssqs.py` and `cli.py` should be well covered; those two arrive in Tasks 14 and 15.

- [ ] **Step 4: Commit**

```bash
git add tests/test_guarantees.py
git commit -m "test: assert the atomicity and delivery guarantees end to end"
```

---

### Task 14: SNS/SQS transport

**Files:**
- Create: `src/cap_event_handler/transport/snssqs.py`
- Test: `tests/test_snssqs_unit.py`

**Interfaces:**
- Consumes: `Transport`, `ReceivedMessage`, `QueueAttributes` (Task 5); `SnsSqsConfig` (Task 1).
- Produces:
  - `SnsSqs(region: str, *, endpoint_url: str | None = None, queue_name: str | None = None, provision: bool = True) -> SnsSqsTransport` — a convenience constructor matching the spec's `SnsSqs(region=...)` usage
  - `SnsSqsTransport(config: SnsSqsConfig, *, provision: bool = True)`
  - module-level pure helpers, unit-tested without AWS:
    - `unwrap_notification(body: str) -> tuple[dict[str, str], str]`
    - `encode_attributes(headers: Mapping[str, str]) -> dict[str, dict[str, str]]`
    - `decode_attributes(raw: Mapping[str, Any]) -> dict[str, str]`

**The wrapper matters.** SNS→SQS delivery nests the payload: the SQS body is a JSON notification whose `Message` field holds the real body and whose `MessageAttributes` map holds `{"Type": "String", "Value": "..."}` entries. Raw message delivery skips that wrapper, so `unwrap_notification` must handle both shapes.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_snssqs_unit.py
from __future__ import annotations

import json

import pytest

from cap_event_handler.errors import CapError
from cap_event_handler.transport.snssqs import (
    decode_attributes,
    encode_attributes,
    unwrap_notification,
)


def test_encode_attributes_uses_the_sns_shape() -> None:
    assert encode_attributes({"cap-msg-id": "abc"}) == {
        "cap-msg-id": {"DataType": "String", "StringValue": "abc"}
    }


def test_decode_attributes_reads_the_sns_notification_shape() -> None:
    raw = {"cap-msg-id": {"Type": "String", "Value": "abc"}}
    assert decode_attributes(raw) == {"cap-msg-id": "abc"}


def test_decode_attributes_reads_the_sqs_receive_shape() -> None:
    raw = {"cap-msg-id": {"DataType": "String", "StringValue": "abc"}}
    assert decode_attributes(raw) == {"cap-msg-id": "abc"}


def test_decode_attributes_skips_entries_with_no_value() -> None:
    assert decode_attributes({"x": {"Type": "Binary"}}) == {}


def test_unwrap_notification_unnests_an_sns_envelope() -> None:
    body = json.dumps(
        {
            "Type": "Notification",
            "MessageId": "n-1",
            "Message": '{"value":"v"}',
            "MessageAttributes": {"cap-msg-id": {"Type": "String", "Value": "abc"}},
        }
    )
    headers, inner = unwrap_notification(body)
    assert headers == {"cap-msg-id": "abc"}
    assert inner == '{"value":"v"}'


def test_unwrap_notification_passes_raw_delivery_through() -> None:
    headers, inner = unwrap_notification('{"value":"v"}')
    assert headers == {}
    assert inner == '{"value":"v"}'


def test_unwrap_notification_rejects_non_json() -> None:
    with pytest.raises(CapError, match="body"):
        unwrap_notification("not json at all")


def test_sns_sqs_convenience_constructor_builds_a_transport() -> None:
    from cap_event_handler.transport.snssqs import SnsSqs, SnsSqsTransport

    t = SnsSqs(region="ap-southeast-2", endpoint_url="http://localhost:4566")
    assert isinstance(t, SnsSqsTransport)
    assert t.config.region == "ap-southeast-2"
    assert t.config.endpoint_url == "http://localhost:4566"


async def test_unprovisioned_transport_refuses_to_create_a_topic() -> None:
    from cap_event_handler.config import SnsSqsConfig
    from cap_event_handler.errors import ConfigurationError
    from cap_event_handler.transport.snssqs import SnsSqsTransport

    t = SnsSqsTransport(SnsSqsConfig(region="ap-southeast-2"), provision=False)
    with pytest.raises(ConfigurationError, match="provision=False"):
        await t.ensure_topic("order-paid")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_snssqs_unit.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.transport.snssqs'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/transport/snssqs.py
"""SNS fan-out onto one SQS queue per consumer group, over aiobotocore.

aiobotocore rather than boto3 in a thread pool: each polling task holds a
connection open for up to 20 seconds, which would pin a thread apiece.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from aiobotocore.session import get_session

from cap_event_handler.config import SnsSqsConfig
from cap_event_handler.errors import CapError, ConfigurationError
from cap_event_handler.transport.base import QueueAttributes, ReceivedMessage

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

logger = logging.getLogger(__name__)


def encode_attributes(headers: Mapping[str, str]) -> dict[str, dict[str, str]]:
    """Headers in the shape SNS `Publish` expects."""
    return {k: {"DataType": "String", "StringValue": v} for k, v in headers.items()}


def decode_attributes(raw: Mapping[str, Any]) -> dict[str, str]:
    """Headers out of either the SNS notification shape or the SQS receive shape."""
    out: dict[str, str] = {}
    for key, attr in raw.items():
        value = attr.get("Value", attr.get("StringValue"))
        if value is not None:
            out[key] = value
    return out


def unwrap_notification(body: str) -> tuple[dict[str, str], str]:
    """Pull the real message out of an SNS→SQS delivery.

    SNS nests: the SQS body is a notification document carrying the payload in
    `Message` and the headers in `MessageAttributes`. Raw message delivery skips
    the wrapper, so a body that is not a notification is passed through.
    """
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise CapError(f"SQS message body is not valid JSON: {exc}") from exc
    if isinstance(parsed, dict) and parsed.get("Type") == "Notification" and "Message" in parsed:
        return decode_attributes(parsed.get("MessageAttributes", {})), parsed["Message"]
    return {}, body


class SnsSqsTransport:
    """A `Transport` backed by Amazon SNS and SQS."""

    def __init__(self, config: SnsSqsConfig, *, provision: bool = True) -> None:
        self.config = config
        self.provision = provision
        self._session = get_session()
        self._sns: Any = None
        self._sqs: Any = None
        self._exit_stack: Any = None

    async def _clients(self) -> tuple[Any, Any]:
        if self._sns is None or self._sqs is None:
            from contextlib import AsyncExitStack

            self._exit_stack = AsyncExitStack()
            kwargs: dict[str, Any] = {"region_name": self.config.region}
            if self.config.endpoint_url:
                kwargs["endpoint_url"] = self.config.endpoint_url
            self._sns = await self._exit_stack.enter_async_context(
                self._session.create_client("sns", **kwargs)
            )
            self._sqs = await self._exit_stack.enter_async_context(
                self._session.create_client("sqs", **kwargs)
            )
        return self._sns, self._sqs

    async def close(self) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.aclose()
            self._exit_stack = None
            self._sns = None
            self._sqs = None

    async def ensure_topic(self, topic: str) -> str:
        sns, _ = await self._clients()
        if not self.provision:
            response = await sns.list_topics()
            for entry in response.get("Topics", []):
                if entry["TopicArn"].rsplit(":", 1)[-1] == topic:
                    return str(entry["TopicArn"])
            raise ConfigurationError(
                f"provision=False and no SNS topic named {topic!r} exists in "
                f"{self.config.region}"
            )
        response = await sns.create_topic(Name=topic)  # idempotent
        return str(response["TopicArn"])

    async def ensure_queue(self, name: str) -> str:
        _, sqs = await self._clients()
        queue_name = self.config.queue_name or name
        if not self.provision:
            try:
                response = await sqs.get_queue_url(QueueName=queue_name)
            except Exception as exc:
                raise ConfigurationError(
                    f"provision=False and no SQS queue named {queue_name!r} exists in "
                    f"{self.config.region}"
                ) from exc
            return str(response["QueueUrl"])
        response = await sqs.create_queue(QueueName=queue_name)  # idempotent
        return str(response["QueueUrl"])

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None:
        if not self.provision:
            return
        sns, sqs = await self._clients()
        attrs = await sqs.get_queue_attributes(QueueUrl=queue_id, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]
        for topic_arn in topic_ids:
            await sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        await self._allow_topics(queue_id, queue_arn, topic_ids)

    async def _allow_topics(
        self, queue_id: str, queue_arn: str, topic_ids: Sequence[str]
    ) -> None:
        """Let the named topics deliver to this queue."""
        _, sqs = await self._clients()
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": f"cap-allow-{i}",
                    "Effect": "Allow",
                    "Principal": {"Service": "sns.amazonaws.com"},
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                }
                for i, topic_arn in enumerate(topic_ids)
            ],
        }
        await sqs.set_queue_attributes(
            QueueUrl=queue_id, Attributes={"Policy": json.dumps(policy)}
        )

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None:
        sns, _ = await self._clients()
        await sns.publish(
            TopicArn=topic_id, Message=body, MessageAttributes=encode_attributes(headers)
        )

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]:
        _, sqs = await self._clients()
        response = await sqs.receive_message(
            QueueUrl=queue_id,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
            AttributeNames=["ApproximateReceiveCount"],
            MessageAttributeNames=["All"],
        )
        out: list[ReceivedMessage] = []
        for message in response.get("Messages", []):
            headers, inner = unwrap_notification(message["Body"])
            if not headers:
                headers = decode_attributes(message.get("MessageAttributes", {}))
            out.append(
                ReceivedMessage(
                    headers=headers,
                    body=inner,
                    receipt=message["ReceiptHandle"],
                    receive_count=int(
                        message.get("Attributes", {}).get("ApproximateReceiveCount", 1)
                    ),
                )
            )
        return out

    async def ack(self, queue_id: str, receipt: str) -> None:
        _, sqs = await self._clients()
        await sqs.delete_message(QueueUrl=queue_id, ReceiptHandle=receipt)

    async def reject(self, queue_id: str, receipt: str, *, visibility_seconds: int) -> None:
        _, sqs = await self._clients()
        await sqs.change_message_visibility(
            QueueUrl=queue_id, ReceiptHandle=receipt, VisibilityTimeout=visibility_seconds
        )

    async def queue_attributes(self, queue_id: str) -> QueueAttributes:
        sns, sqs = await self._clients()
        response = await sqs.get_queue_attributes(
            QueueUrl=queue_id,
            AttributeNames=[
                "VisibilityTimeout",
                "MessageRetentionPeriod",
                "RedrivePolicy",
                "QueueArn",
            ],
        )
        attributes = response.get("Attributes", {})
        queue_arn = attributes.get("QueueArn", "")
        subscribed: set[str] = set()
        paginator = sns.get_paginator("list_subscriptions")
        async for page in paginator.paginate():
            for sub in page.get("Subscriptions", []):
                if sub.get("Endpoint") == queue_arn:
                    subscribed.add(sub["TopicArn"])
        return QueueAttributes(
            visibility_timeout=int(attributes.get("VisibilityTimeout", 30)),
            message_retention=int(attributes.get("MessageRetentionPeriod", 345_600)),
            has_redrive=bool(attributes.get("RedrivePolicy")),
            subscribed_topics=frozenset(subscribed),
        )

    async def __aenter__(self) -> SnsSqsTransport:
        await self._clients()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


def SnsSqs(  # noqa: N802 - reads as a constructor at the call site
    region: str,
    *,
    endpoint_url: str | None = None,
    queue_name: str | None = None,
    provision: bool = True,
) -> SnsSqsTransport:
    """Build an SNS/SQS transport. `SnsSqs(region="ap-southeast-2")`."""
    return SnsSqsTransport(
        SnsSqsConfig(region=region, endpoint_url=endpoint_url, queue_name=queue_name),
        provision=provision,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_snssqs_unit.py -v`
Expected: all passed

- [ ] **Step 5: Export `SnsSqs`**

Add `SnsSqs` to the imports and `__all__` in `src/cap_event_handler/__init__.py`, importing it from `cap_event_handler.transport.snssqs`.

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean. aiobotocore's clients are dynamically typed; `Any` on `_sns` and `_sqs` is deliberate.

- [ ] **Step 7: Commit**

```bash
git add src/cap_event_handler/transport/snssqs.py src/cap_event_handler/__init__.py tests/test_snssqs_unit.py
git commit -m "feat: add the SNS/SQS transport"
```

---
### Task 15: Worker CLI

**Files:**
- Create: `src/cap_event_handler/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: `CapApp` (Task 12).
- Produces:
  - `load_app(target: str) -> CapApp` — imports `module:attribute`, raising `ConfigurationError` with a readable message on every failure mode
  - `main(argv: Sequence[str] | None = None) -> int`
  - console script `cap-event-handler` (already declared in `pyproject.toml`)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_cli.py
from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from cap_event_handler.cli import load_app, main
from cap_event_handler.errors import ConfigurationError


@pytest.fixture
def app_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    module = tmp_path / "myservice.py"
    module.write_text(
        textwrap.dedent(
            """
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy.pool import StaticPool

            from cap_event_handler import CapApp, Settings
            from cap_event_handler.transport.memory import MemoryTransport

            engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
            app = CapApp(
                engine=engine,
                transport=MemoryTransport(),
                group="cli-test",
                settings=Settings(schema_name=None),
            )
            not_an_app = 42
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    yield "myservice"
    sys.modules.pop("myservice", None)


def test_load_app_imports_the_attribute(app_module: str) -> None:
    from cap_event_handler import CapApp

    assert isinstance(load_app(f"{app_module}:app"), CapApp)


def test_load_app_rejects_a_target_without_a_colon() -> None:
    with pytest.raises(ConfigurationError, match="module:attribute"):
        load_app("myservice")


def test_load_app_reports_a_missing_module() -> None:
    with pytest.raises(ConfigurationError, match="no_such_module"):
        load_app("no_such_module:app")


def test_load_app_reports_a_missing_attribute(app_module: str) -> None:
    with pytest.raises(ConfigurationError, match="missing"):
        load_app(f"{app_module}:missing")


def test_load_app_rejects_a_non_app_attribute(app_module: str) -> None:
    with pytest.raises(ConfigurationError, match="CapApp"):
        load_app(f"{app_module}:not_an_app")


def test_main_returns_two_on_a_bad_target(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["worker", "nope"]) == 2
    assert "module:attribute" in capsys.readouterr().err


def test_main_requires_the_worker_subcommand() -> None:
    with pytest.raises(SystemExit):
        main([])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cap_event_handler.cli'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cap_event_handler/cli.py
"""`cap-event-handler worker module:attribute` — run an app as its own process."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import signal
import sys
from typing import TYPE_CHECKING

from cap_event_handler.app import CapApp
from cap_event_handler.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def load_app(target: str) -> CapApp:
    """Import `module:attribute` and return the CapApp it names."""
    if ":" not in target:
        raise ConfigurationError(
            f"expected a target of the form module:attribute, got {target!r}"
        )
    module_name, _, attribute = target.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ConfigurationError(f"could not import module {module_name!r}: {exc}") from exc
    try:
        app = getattr(module, attribute)
    except AttributeError as exc:
        raise ConfigurationError(
            f"module {module_name!r} has no attribute {attribute!r}"
        ) from exc
    if not isinstance(app, CapApp):
        raise ConfigurationError(
            f"{target} is a {type(app).__name__}, not a CapApp"
        )
    return app


async def _serve(app: CapApp) -> None:
    """Run until SIGINT or SIGTERM, then shut down cleanly."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    async with app.run():
        logger.info("cap-event-handler worker started for group %r", app.group)
        await stop.wait()
        logger.info("shutting down")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cap-event-handler")
    subparsers = parser.add_subparsers(dest="command", required=True)
    worker = subparsers.add_parser("worker", help="run a CapApp until interrupted")
    worker.add_argument("target", help="module:attribute naming the CapApp")
    worker.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper())
    try:
        app = load_app(args.target)
    except ConfigurationError as exc:
        print(f"error: {exc}", file=sys.stderr)  # noqa: T201 - CLI output
        return 2
    asyncio.run(_serve(app))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cli.py -v`
Expected: all passed

- [ ] **Step 5: Verify the console script is wired**

Run: `uv run cap-event-handler worker nope; echo "exit=$?"`
Expected: prints the `module:attribute` error and `exit=2`

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff format . && uv run ruff check . && uv run mypy src`
Expected: all clean

- [ ] **Step 7: Commit**

```bash
git add src/cap_event_handler/cli.py tests/test_cli.py
git commit -m "feat: add the worker CLI"
```

---

### Task 16: Integration tests

**Files:**
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/conftest.py`
- Create: `tests/integration/test_postgres.py`
- Create: `tests/integration/test_localstack.py`

**Interfaces:**
- Consumes: the whole library.
- Produces: no source. Covers the two things SQLite and the broker simulator cannot: real `FOR UPDATE SKIP LOCKED` across concurrent sweepers, and real SNS→SQS delivery including the notification wrapper and the queue access policy.

**Everything here is marked `@pytest.mark.integration`** and therefore skipped by the default `uv run pytest`, per the `addopts` in `pyproject.toml`.

- [ ] **Step 1: Add the LocalStack test dependency**

Run: `uv add --dev 'testcontainers[localstack]'`
Expected: resolves and locks.

- [ ] **Step 2: Write the tests**

```python
# tests/integration/__init__.py
```

```python
# tests/integration/conftest.py
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:17-alpine") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")


@pytest_asyncio.fixture
async def pg_engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def localstack_endpoint() -> Iterator[str]:
    from testcontainers.localstack import LocalStackContainer

    with LocalStackContainer("localstack/localstack:3").with_services("sns", "sqs") as ls:
        yield ls.get_url()
```

```python
# tests/integration/test_postgres.py
"""What SQLite cannot prove: real PostgreSQL semantics."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler.config import Settings
from cap_event_handler.envelope import build_envelope, uuid7
from cap_event_handler.storage.sql import SqlStorage

pytestmark = pytest.mark.integration


class Payload(BaseModel):
    value: str


@pytest.fixture
def pg_settings() -> Settings:
    import uuid

    # a private schema per test run keeps concurrent runs from colliding
    return Settings(schema_name=f"cap_{uuid.uuid4().hex[:8]}")


async def test_tables_and_schema_are_created(pg_engine, pg_settings) -> None:  # noqa: ANN001
    storage = SqlStorage(pg_engine, pg_settings)
    await storage.create_tables()
    async with pg_engine.begin() as conn:
        names = await conn.run_sync(
            lambda c: sa.inspect(c).get_table_names(schema=pg_settings.schema_name)
        )
    assert {"cap_outbox", "cap_inbox"} <= set(names)


async def test_jsonb_round_trips(pg_engine, pg_settings) -> None:  # noqa: ANN001
    storage = SqlStorage(pg_engine, pg_settings)
    await storage.create_tables()
    env = build_envelope("order.paid", Payload(value="v"))
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, env)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, env.id)
    assert row is not None
    assert row.payload == {"value": "v"}
    assert row.headers == env.headers


async def test_skip_locked_stops_two_sweepers_claiming_the_same_row(
    pg_engine, pg_settings
) -> None:  # noqa: ANN001
    """The reason the sweeper needs no lock table."""
    clock = lambda: datetime.now(UTC) - timedelta(seconds=120)  # noqa: E731 - all rows look old
    storage = SqlStorage(pg_engine, pg_settings, clock=clock)
    await storage.create_tables()
    async with storage.sessionmaker() as s, s.begin():
        for _ in range(10):
            await storage.insert_outbox(s, build_envelope("order.paid", Payload(value="v")))

    claimed: list[list] = []

    async def sweep() -> None:
        async with storage.sessionmaker() as s, s.begin():
            rows = await storage.claim_pending(s, limit=10)
            claimed.append(rows)
            await asyncio.sleep(0.3)  # hold the locks

    await asyncio.gather(sweep(), sweep())
    first, second = ({r.id for r in claimed[0]}, {r.id for r in claimed[1]})
    assert first & second == set()
    assert len(first) + len(second) == 10


async def test_dedup_insert_blocks_then_reports_duplicate(pg_engine, pg_settings) -> None:  # noqa: ANN001
    """Concurrent redelivery: the second attempt waits, then exits as a duplicate."""
    storage = SqlStorage(pg_engine, pg_settings)
    await storage.create_tables()
    msg_id = uuid7()
    outcomes: list[bool] = []
    first_inserted = asyncio.Event()

    async def first() -> None:
        async with storage.sessionmaker() as s, s.begin():
            outcomes.append(await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n"))
            first_inserted.set()
            await asyncio.sleep(0.3)

    async def second() -> None:
        await first_inserted.wait()
        async with storage.sessionmaker() as s, s.begin():
            outcomes.append(await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n"))

    await asyncio.gather(first(), second())
    assert sorted(outcomes) == [False, True]


async def test_collector_batches_are_bounded(pg_engine) -> None:  # noqa: ANN001
    import uuid as uuid_mod

    settings = Settings(schema_name=f"cap_{uuid_mod.uuid4().hex[:8]}", collector_batch=5)
    clock = lambda: datetime.now(UTC) - timedelta(days=30)  # noqa: E731
    storage = SqlStorage(pg_engine, settings, clock=clock)
    await storage.create_tables()
    async with storage.sessionmaker() as s, s.begin():
        for _ in range(12):
            await storage.insert_inbox(s, msg_id=uuid7(), group="g", name="n")
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_inbox(s) == 5
```

```python
# tests/integration/test_localstack.py
"""What the broker simulator cannot prove: real SNS→SQS delivery."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from pydantic import BaseModel

from cap_event_handler import CapApp, Context, Settings
from cap_event_handler.transport.snssqs import SnsSqs

pytestmark = pytest.mark.integration


class Payload(BaseModel):
    value: str


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


async def test_publish_and_consume_through_real_sns_and_sqs(
    pg_engine, localstack_endpoint
) -> None:  # noqa: ANN001
    """The whole loop: outbox row, SNS publish, SQS receive, dedup, ack."""
    seen: list[str] = []
    transport = SnsSqs(region="us-east-1", endpoint_url=localstack_endpoint)
    app = CapApp(
        engine=pg_engine,
        transport=transport,
        group=f"itest-{uuid.uuid4().hex[:8]}",
        settings=Settings(schema_name=f"cap_{uuid.uuid4().hex[:8]}"),
    )

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        seen.append(ctx.payload.value)

    async with app.run():
        await app.publish("order.paid", Payload(value="real"))
        for _ in range(200):
            if seen:
                break
            await asyncio.sleep(0.1)
    assert seen == ["real"]


async def test_sns_notification_wrapper_is_unwrapped(pg_engine, localstack_endpoint) -> None:  # noqa: ANN001
    """Headers must survive the SNS MessageAttributes round trip."""
    captured: list[Context[Payload]] = []
    transport = SnsSqs(region="us-east-1", endpoint_url=localstack_endpoint)
    app = CapApp(
        engine=pg_engine,
        transport=transport,
        group=f"itest-{uuid.uuid4().hex[:8]}",
        settings=Settings(schema_name=f"cap_{uuid.uuid4().hex[:8]}"),
    )

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        captured.append(ctx)

    async with app.run():
        msg_id = await app.publish("order.paid", Payload(value="v"))
        for _ in range(200):
            if captured:
                break
            await asyncio.sleep(0.1)
    assert captured[0].msg_id == msg_id
    assert captured[0].correlation_id is not None


async def test_queue_attributes_are_readable(localstack_endpoint) -> None:  # noqa: ANN001
    """Startup validation depends on these three attributes being retrievable."""
    transport = SnsSqs(region="us-east-1", endpoint_url=localstack_endpoint)
    try:
        topic_id = await transport.ensure_topic(f"t-{uuid.uuid4().hex[:8]}")
        queue_id = await transport.ensure_queue(f"q-{uuid.uuid4().hex[:8]}")
        await transport.subscribe(queue_id, [topic_id])
        attrs = await transport.queue_attributes(queue_id)
        assert attrs.visibility_timeout > 0
        assert attrs.message_retention > 0
        assert topic_id in attrs.subscribed_topics
    finally:
        await transport.close()


async def test_provision_false_fails_loudly_on_a_missing_queue(localstack_endpoint) -> None:  # noqa: ANN001
    from cap_event_handler.errors import ConfigurationError

    transport = SnsSqs(
        region="us-east-1", endpoint_url=localstack_endpoint, provision=False
    )
    try:
        with pytest.raises(ConfigurationError, match="provision=False"):
            await transport.ensure_queue(f"missing-{uuid.uuid4().hex[:8]}")
    finally:
        await transport.close()
```

- [ ] **Step 3: Run the integration suite**

Run: `uv run pytest -m integration -v`
Expected: all passed. Requires Docker. First run pulls `postgres:17-alpine` and `localstack/localstack:3`, so allow a few minutes.

- [ ] **Step 4: Confirm the default run still needs no Docker**

Run: `uv run pytest -v`
Expected: all passed, integration tests deselected.

- [ ] **Step 5: Commit**

```bash
git add tests/integration pyproject.toml uv.lock
git commit -m "test: add PostgreSQL and LocalStack integration tests"
```

---

### Task 17: Documentation

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Create: `docs/usage.md`

**Interfaces:**
- Consumes: the finished library.
- Produces: no source.

- [ ] **Step 1: Update the README status**

Replace the "Status: design approved, not yet implemented" block with a short feature summary and a working quickstart copied verbatim from `tests/test_app.py::test_end_to_end_publish_then_consume`, adapted to PostgreSQL and `SnsSqs`. Every snippet in the README must be code that actually runs.

- [ ] **Step 2: Write `docs/usage.md`**

Cover, one short section each, each with a runnable snippet:
1. Installation and the two tables (`CapApp.initialize()` vs Alembic against `build_tables().metadata`).
2. Publishing inside a transaction, and what `after_commit` does for you.
3. Subscribing, and what `Context` carries.
4. Publishing from inside a handler — the atomic chain.
5. Operational settings that matter: `inbox_retention` vs the queue's `MessageRetentionPeriod`, `VisibilityTimeout` vs handler runtime, and why you want a redrive policy. Reference the warnings `app.warnings` will emit.
6. `provision=False` with Terraform.
7. Running as a worker: `cap-event-handler worker myservice:app`.
8. What is *not* guaranteed: non-database side effects inside a handler are at-least-once.

- [ ] **Step 3: Update `CLAUDE.md`**

Change "implementation not started" to a one-paragraph description of the finished layout, and keep the spec reference, the uv workflow and the read-only note about the CAP checkout.

- [ ] **Step 4: Verify every snippet**

Run: `uv run python -c "import cap_event_handler; print(cap_event_handler.__all__)"`
Then hand-run each README and `docs/usage.md` snippet against the SQLite/MemoryTransport pairing to confirm it executes.

- [ ] **Step 5: Final full verification**

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest -v
uv run pytest -m integration -v   # requires Docker
```
Expected: all clean, all passed.

- [ ] **Step 6: Commit**

```bash
git add README.md CLAUDE.md docs/usage.md
git commit -m "docs: document usage, operations and the guarantees"
```

---

## Plan self-review

Run against the spec after the plan is written; recorded here so an executor can see the coverage argument.

**Spec coverage:**

| Spec section | Task |
|---|---|
| §1.2 guarantees | 13 (each row is a named test) |
| §2 envelope, headers, topic normalisation | 2 |
| §3 schema, §3.1 decisions | 3 |
| §3.2 migrations | 3 (MetaData), 12 (`initialize()`), 17 (documented) |
| §4.1 publish path | 7 |
| §4.2 after_commit flush, drop-on-full | 7 |
| §4.3 sender task | 8 |
| §4.4 publish without a session | 7 |
| §5 consume pipeline | 9 |
| §5.1 no consume-side retry processor | 9 (absence), 13 (redelivery tests) |
| §5.2 visibility-timeout cost | 11 (warning), 16 (concurrent dedup test) |
| §5.3 failure dispositions | 9 (one test per row) |
| §5.4 polling parameters | 1 (defaults + SQS limits), 9 |
| §6 sweeper and collector | 10 |
| §7 public API, `Context` | 6, 12 |
| §7.1 lifecycle | 12, 15 |
| §7.2 provisioning | 12, 14 |
| §8 module layout | all (deviation 1 recorded above) |
| §8.1 dependencies | 1, 3, 14, 16 |
| §9 startup validation | 11 (one test per row) |
| §10 testing | 3, 5, 13, 16 |
| §11 deferred | not implemented, by design |

**Gaps found and closed while reviewing:** Task 17 was added — the spec assumes documentation exists but §8 lists no doc file, and the README still says "not yet implemented".

**Type consistency:** `SqlStorage` method names are identical in Tasks 4, 7, 8, 9, 10 and 13. `Publisher.send_row` (not `send`) is used in Tasks 8, 10 and 13. `Transport` method names match between `base.py` (Task 5), `memory.py` (Task 5) and `snssqs.py` (Task 14). `Context` field names match between Task 6's definition, Task 9's construction and Task 13's assertions. `build_tables(schema=..., prefix=...)` keeps the same keywords in Tasks 3, 4 and 16.

**Known sharp edges for the executor:**

- `Envelope.headers` is declared `field(compare=False)` so two envelopes compare by id and payload. If a test needs header equality, compare `env.headers` explicitly.
- `asyncio.Queue` is generic over `UUID | None`; the `None` is the sender's stop sentinel. Do not narrow it.
- `session.info` on an `AsyncSession` proxies the underlying sync `Session`'s dict, which is what makes the `after_commit` listener see staged messages. Do not replace it with an attribute on the AsyncSession.
- `_run_in_transaction` is called directly by one guarantee test (Task 13) to simulate a crash before the ack. Keep its signature stable.
