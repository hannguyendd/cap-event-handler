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
) -> None:
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
) -> None:
    await publisher.publish("order.paid", Payload(value="v"))
    assert await OutboxSweeper(storage, publisher, settings).sweep_once() == 0
    assert transport.sent == []


async def test_sweeper_retries_failed_rows(publisher, storage, transport, settings, clock) -> None:
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


async def test_sweeper_gives_up_after_max_retries(sqlite_engine, transport, clock) -> None:
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


async def test_collector_deletes_expired_rows_from_both_tables(
    publisher, storage, settings, clock
) -> None:
    from cap_event_handler.envelope import uuid7

    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    await publisher.send_one(msg_id)
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_inbox(s, msg_id=uuid7(), group="g", name="order.paid")

    collector = Collector(storage, settings)
    assert await collector.collect_once() == (0, 0)

    clock.advance(timedelta(days=16))
    assert await collector.collect_once() == (1, 1)


async def test_collector_leaves_unsent_rows_alone(publisher, storage, settings, clock) -> None:
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    clock.advance(timedelta(days=365))
    assert await Collector(storage, settings).collect_once() == (0, 0)
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, msg_id) is not None


async def test_processors_stop_when_the_event_is_set(publisher, storage) -> None:
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
) -> None:
    await publisher.publish("order.paid", Payload(value="v"))
    clock.advance(timedelta(seconds=61))
    sweeper = OutboxSweeper(storage, publisher, settings)

    async def boom(row) -> None:
        raise RuntimeError("send blew up")

    publisher.send_row = boom
    assert await sweeper.sweep_once() == 1
