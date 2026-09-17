from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from cap_event_handler.config import Settings
from cap_event_handler.envelope import normalize_topic
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


async def _wire(transport: MemoryTransport, names=("order.paid",)):
    topic_ids = {n: await transport.ensure_topic(normalize_topic(n)) for n in names}
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, list(topic_ids.values()))
    return queue_id, topic_ids


def _registry(names=("order.paid",)) -> HandlerRegistry:
    class P(BaseModel):
        v: int = 0

    async def h(ctx) -> None:
        return None

    r = HandlerRegistry()
    for n in names:
        r.register(n, P, h)
    return r


async def test_no_warnings_on_a_healthy_queue(clock) -> None:
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


async def test_warns_when_dedup_window_is_shorter_than_redelivery_window(clock) -> None:
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


async def test_warns_when_visibility_timeout_is_too_short(clock) -> None:
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


async def test_warns_when_the_queue_has_no_redrive_policy(clock) -> None:
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


async def test_unprovisioned_missing_topic_is_fatal(clock) -> None:
    transport = MemoryTransport(clock=clock, visibility_timeout=60, max_receive_count=5)
    queue_id, topic_ids = await _wire(transport)
    with pytest.raises(ConfigurationError, match=r"order\.shipped"):
        await validate_runtime(
            settings=Settings(schema_name=None, provision=False),
            transport=transport,
            queue_id=queue_id,
            registry=_registry(("order.paid", "order.shipped")),
            topic_ids=topic_ids,
        )


async def test_unprovisioned_unsubscribed_topic_is_fatal(clock) -> None:
    """A handler whose topic is not wired to the queue can never fire."""
    transport = MemoryTransport(clock=clock, visibility_timeout=60, max_receive_count=5)
    queue_id, topic_ids = await _wire(transport)
    orphan = await transport.ensure_topic("order-shipped")
    with pytest.raises(ConfigurationError, match=r"order\.shipped"):
        await validate_runtime(
            settings=Settings(schema_name=None, provision=False),
            transport=transport,
            queue_id=queue_id,
            registry=_registry(("order.paid", "order.shipped")),
            topic_ids=dict(topic_ids) | {"order.shipped": orphan},
        )


async def test_provisioned_run_does_not_check_subscriptions(clock) -> None:
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
