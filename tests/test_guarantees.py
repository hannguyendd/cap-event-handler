"""The guarantees from spec section 1.2, each as an executable statement.

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

from .helpers import until


class Payload(BaseModel):
    value: str


def build(sqlite_engine, transport, clock=None, group="g", **overrides):
    return CapApp(
        engine=sqlite_engine,
        transport=transport,
        group=group,
        settings=Settings(schema_name=None, **overrides),
        clock=clock,
    )


async def test_rollback_produces_no_event(sqlite_engine, transport) -> None:
    """Guarantee: an event is never published for a rolled-back transaction."""
    app = build(sqlite_engine, transport)
    await app.initialize()
    async with app.storage.sessionmaker() as s, s.begin():
        await app.publish("order.paid", Payload(value="v"), session=s)
        await s.rollback()
    assert transport.sent == []
    async with app.storage.sessionmaker() as s:
        outbox = app.storage.tables.outbox
        assert (await s.execute(sa.select(sa.func.count()).select_from(outbox))).scalar() == 0


async def test_crash_after_commit_before_send_is_recovered(sqlite_engine, transport, clock) -> None:
    """Guarantee: a committed event is never lost, only delayed."""
    app = build(sqlite_engine, transport, clock)
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
) -> None:
    """Guarantee: the publish side may duplicate; the consume side deduplicates.

    Models a crash between a successful SNS publish and the status update: the
    sweeper sends the same envelope a second time.
    """
    calls: list[str] = []
    app = build(sqlite_engine, transport, clock)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        calls.append(ctx.payload.value)

    async with app.run():
        msg_id = await app.publish("order.paid", Payload(value="once"))
        await until(lambda: len(calls) == 1)

        async with app.storage.sessionmaker() as s, s.begin():
            outbox = app.storage.tables.outbox
            await s.execute(sa.update(outbox).where(outbox.c.id == msg_id).values(status="pending"))
        clock.advance(timedelta(seconds=61))
        await OutboxSweeper(app.storage, app.publisher, app.settings).sweep_once()
        await until(lambda: len(transport.sent) == 2)
        await asyncio.sleep(0.2)

    assert len(transport.sent) == 2  # published twice
    assert calls == ["once"]  # handled once


async def test_crash_mid_handler_leaves_no_residue(sqlite_engine, transport, clock) -> None:
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
        await until(lambda: len(attempts) == 1)
        clock.advance(timedelta(seconds=app.settings.reject_backoff_seconds + 1))
        await until(lambda: len(attempts) == 2)
        await asyncio.sleep(0.2)
    finally:
        await app.stop()

    async with app.storage.sessionmaker() as s:
        rows = (await s.execute(sa.select(effects.c.v))).scalars().all()
    assert rows == ["attempt-2"]  # the failed attempt left nothing behind


async def test_redelivery_after_a_successful_commit_does_not_rerun_the_handler(
    sqlite_engine, transport, clock
) -> None:
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
    # first delivery: handled and committed, but the ack never happens
    messages = await transport.receive(queue_id, max_messages=1, wait_seconds=0)
    handler_entry = registry.get("order.paid")
    assert handler_entry is not None
    await consumer._run_in_transaction(env, Payload(value="v"), handler_entry, messages[0])
    assert calls == [1]

    # the broker redelivers when the lease expires
    clock.advance(timedelta(seconds=transport.visibility_timeout + 1))
    assert await consumer.drain_once() == 1
    assert calls == [1]  # deduplicated, not re-run
    assert await transport.receive(queue_id, max_messages=1, wait_seconds=0) == []


async def test_two_groups_each_receive_the_message_exactly_once(sqlite_engine, transport) -> None:
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
        await until(lambda: sorted(seen) == ["a", "b"])
        await asyncio.sleep(0.2)
    assert sorted(seen) == ["a", "b"]


async def test_a_poison_message_ends_up_in_the_dead_letter_queue(
    sqlite_engine, transport, clock
) -> None:
    """Guarantee: a handler that always fails is quarantined, not retried forever."""
    app = build(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        raise RuntimeError("always fails")

    await app.start()
    try:
        await app.publish("order.paid", Payload(value="v"))
        for _ in range(transport.max_receive_count + 2):
            await asyncio.sleep(0.05)
            clock.advance(timedelta(seconds=app.settings.reject_backoff_seconds + 1))
        await until(lambda: len(transport.dead_letter) == 1, seconds=10)
    finally:
        await app.stop()
    assert len(transport.dead_letter) == 1
