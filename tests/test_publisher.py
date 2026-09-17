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


async def test_publish_inserts_a_pending_row_on_the_callers_session(publisher, storage) -> None:
    async with storage.sessionmaker() as s, s.begin():
        msg_id = await publisher.publish("order.paid", Payload(value="v"), session=s)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "pending"


async def test_publish_does_not_touch_the_transport_before_commit(
    publisher, storage, transport
) -> None:
    async with storage.sessionmaker() as s, s.begin():
        await publisher.publish("order.paid", Payload(value="v"), session=s)
        assert transport.sent == []
    assert transport.sent == []


async def test_commit_notifies_the_sender(publisher, storage) -> None:
    async with storage.sessionmaker() as s, s.begin():
        msg_id = await publisher.publish("order.paid", Payload(value="v"), session=s)
    assert publisher.queue.get_nowait() == msg_id


async def test_rollback_publishes_nothing_and_notifies_nothing(publisher, storage) -> None:
    """No event for a transaction that did not commit."""
    async with storage.sessionmaker() as s, s.begin():
        msg_id = await publisher.publish("order.paid", Payload(value="v"), session=s)
        await s.rollback()
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, msg_id) is None
    assert publisher.queue.empty()


async def test_publish_without_a_session_commits_on_its_own(publisher, storage) -> None:
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, msg_id) is not None
    assert publisher.queue.get_nowait() == msg_id


async def test_a_full_queue_drops_the_notification_but_keeps_the_row(storage, transport) -> None:
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


async def test_correlation_id_is_inherited_from_the_contextvar(publisher, storage) -> None:
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


async def test_explicit_correlation_id_overrides_the_contextvar(publisher, storage) -> None:
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


async def test_publish_rejects_a_name_that_cannot_become_a_topic(publisher, storage) -> None:
    from cap_event_handler.errors import CapError

    async with storage.sessionmaker() as s, s.begin():
        with pytest.raises(CapError, match="topic"):
            await publisher.publish("order paid!", Payload(value="v"), session=s)


async def test_sessions_that_never_published_are_untouched(publisher, storage) -> None:
    """The global listener must short-circuit on unrelated sessions."""
    async with storage.sessionmaker() as s, s.begin():
        pass
    assert publisher.queue.empty()


async def test_installing_the_listener_twice_does_not_double_notify(publisher, storage) -> None:
    install_after_commit_listener()
    install_after_commit_listener()
    async with storage.sessionmaker() as s, s.begin():
        await publisher.publish("order.paid", Payload(value="v"), session=s)
    assert publisher.queue.qsize() == 1


async def test_two_publishes_in_one_transaction_both_notify(publisher, storage) -> None:
    async with storage.sessionmaker() as s, s.begin():
        a = await publisher.publish("order.paid", Payload(value="1"), session=s)
        b = await publisher.publish("order.paid", Payload(value="2"), session=s)
    assert {publisher.queue.get_nowait(), publisher.queue.get_nowait()} == {a, b}
    assert isinstance(publisher.queue, asyncio.Queue)
