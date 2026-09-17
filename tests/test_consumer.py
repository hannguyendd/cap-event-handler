from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
import pytest_asyncio
import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler.consumer import Consumer
from cap_event_handler.context import Context, current_correlation_id
from cap_event_handler.envelope import build_envelope, normalize_topic
from cap_event_handler.registry import HandlerRegistry


class Payload(BaseModel):
    value: str


@pytest_asyncio.fixture
async def effects_table(sqlite_engine):
    """A table a handler writes to, so we can prove its writes committed."""
    meta = sa.MetaData()
    table = sa.Table("side_effects", meta, sa.Column("value", sa.Text, primary_key=True))
    async with sqlite_engine.begin() as conn:
        await conn.run_sync(meta.create_all)
    return table


def make_consumer(storage, transport, registry, settings, group="g", queue_id="g"):
    return Consumer(
        group=group,
        queue_id=queue_id,
        registry=registry,
        storage=storage,
        transport=transport,
        settings=settings,
    )


async def deliver(transport, name="order.paid", value="v"):
    """Publish an envelope onto the group's queue and return it."""
    env = build_envelope(name, Payload(value=value))
    topic_id = await transport.ensure_topic(normalize_topic(name))
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, [topic_id])
    await transport.publish(topic_id, env.body(), env.headers)
    return env


async def test_handler_runs_and_message_is_acked(storage, transport, settings) -> None:
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
) -> None:
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        await ctx.session.execute(sa.insert(effects_table).values(value=ctx.payload.value))

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport, value="written")
    await make_consumer(storage, transport, registry, settings).drain_once()

    async with storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects_table.c.value))).scalars().all()
        inbox = storage.tables.inbox
        marker = (
            (await s.execute(sa.select(inbox).where(inbox.c.msg_id == env.id)))
            .mappings()
            .one_or_none()
        )
    assert rows == ["written"]
    assert marker is not None


async def test_a_redelivered_message_does_not_run_the_handler_again(
    storage, transport, settings, effects_table
) -> None:
    calls: list[int] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        calls.append(1)
        await ctx.session.execute(sa.insert(effects_table).values(value=str(len(calls))))

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport)
    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()

    topic_id = await transport.ensure_topic("order-paid")
    await transport.publish(topic_id, env.body(), env.headers)
    await consumer.drain_once()

    assert calls == [1]
    async with storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects_table.c.value))).scalars().all()
    assert rows == ["1"]


async def test_a_raising_handler_rolls_back_and_rejects(
    storage, transport, settings, clock, effects_table
) -> None:
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        await ctx.session.execute(sa.insert(effects_table).values(value="partial"))
        raise RuntimeError("handler exploded")

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport)
    await make_consumer(storage, transport, registry, settings).drain_once()

    async with storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects_table.c.value))).scalars().all()
        inbox = storage.tables.inbox
        marker = (
            (await s.execute(sa.select(inbox).where(inbox.c.msg_id == env.id)))
            .mappings()
            .one_or_none()
        )
    assert rows == []
    assert marker is None

    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    assert len(await transport.receive("g", max_messages=10, wait_seconds=0)) == 1


async def test_a_retried_message_runs_cleanly_the_second_time(
    storage, transport, settings, clock, effects_table
) -> None:
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
    assert rows == ["2"]


async def test_unroutable_message_is_rejected_by_default(
    storage, transport, settings, clock
) -> None:
    await deliver(transport, name="nobody.listens")
    await make_consumer(storage, transport, HandlerRegistry(), settings).drain_once()
    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    assert len(await transport.receive("g", max_messages=10, wait_seconds=0)) == 1


async def test_unroutable_message_can_be_acked_instead(storage, transport, clock) -> None:
    from cap_event_handler.config import Settings

    settings = Settings(schema_name=None, on_unroutable="ack")
    await deliver(transport, name="nobody.listens")
    await make_consumer(storage, transport, HandlerRegistry(), settings).drain_once()
    clock.advance(timedelta(seconds=3600))
    assert await transport.receive("g", max_messages=10, wait_seconds=0) == []


async def test_invalid_payload_is_rejected_and_the_handler_never_runs(
    storage, transport, settings, clock
) -> None:
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

    await make_consumer(storage, transport, registry, settings).drain_once()
    assert called == []
    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    assert len(await transport.receive("g", max_messages=10, wait_seconds=0)) == 1


async def test_context_is_fully_populated(storage, transport, settings) -> None:
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
) -> None:
    seen: list[object] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        seen.append(current_correlation_id())

    registry.register("order.paid", Payload, handler)
    env = await deliver(transport)
    await make_consumer(storage, transport, registry, settings).drain_once()
    assert seen == [env.correlation_id]
    assert current_correlation_id() is None


async def test_unknown_headers_reach_the_handler(storage, transport, settings) -> None:
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


async def test_a_handler_that_commits_the_session_is_reported(storage, transport, settings) -> None:
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        await ctx.session.commit()

    registry.register("order.paid", Payload, handler)
    await deliver(transport)
    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()
    assert consumer.last_error is not None
    assert "commit" in str(consumer.last_error).lower()


async def test_two_groups_each_process_the_same_message(storage, transport, settings) -> None:
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


async def test_run_stops_when_the_event_is_set(storage, transport, settings) -> None:
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


async def test_concurrent_group_processes_a_batch(storage, transport, clock) -> None:
    from cap_event_handler.config import Settings

    seen: list[str] = []
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        await asyncio.sleep(0)
        seen.append(ctx.payload.value)

    registry.register("order.paid", Payload, handler)
    for i in range(5):
        await deliver(transport, value=str(i))
    consumer = make_consumer(
        storage, transport, registry, Settings(schema_name=None, group_concurrency=3)
    )
    assert await consumer.drain_once() == 5
    assert sorted(seen) == ["0", "1", "2", "3", "4"]


@pytest.mark.parametrize("bad_body", ["not json", "[1,2,3]"])
async def test_undecodable_message_is_rejected(
    storage, transport, settings, clock, bad_body
) -> None:
    registry = HandlerRegistry()

    async def handler(ctx: Context[Payload]) -> None:
        return None

    registry.register("order.paid", Payload, handler)
    env = build_envelope("order.paid", Payload(value="v"))
    topic_id = await transport.ensure_topic("order-paid")
    queue_id = await transport.ensure_queue("g")
    await transport.subscribe(queue_id, [topic_id])
    await transport.publish(topic_id, bad_body, env.headers)

    consumer = make_consumer(storage, transport, registry, settings)
    await consumer.drain_once()
    assert consumer.last_error is not None
    clock.advance(timedelta(seconds=settings.reject_backoff_seconds + 1))
    assert len(await transport.receive("g", max_messages=10, wait_seconds=0)) == 1
