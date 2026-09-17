from __future__ import annotations

import asyncio
from datetime import timedelta

from pydantic import BaseModel

from cap_event_handler.envelope import HEADER_MSG_ID, uuid7


class Payload(BaseModel):
    value: str


async def test_send_one_publishes_and_marks_the_row_sent(publisher, storage, transport) -> None:
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
) -> None:
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    transport.fail_publish = RuntimeError("SNS is down")
    await publisher.send_one(msg_id)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "failed"
    assert row.retries == 1


async def test_an_already_sent_row_is_not_sent_again(publisher, transport) -> None:
    """The sweeper and the sender can both be handed the same id; only one send."""
    msg_id = await publisher.publish("order.paid", Payload(value="v"))
    await publisher.send_one(msg_id)
    await publisher.send_one(msg_id)
    assert len(transport.sent) == 1


async def test_a_missing_row_is_a_no_op(publisher, transport) -> None:
    await publisher.send_one(uuid7())
    assert transport.sent == []


async def test_the_topic_is_resolved_once_and_cached(publisher, transport) -> None:
    calls: list[str] = []
    original = transport.ensure_topic

    async def counting(topic: str) -> str:
        calls.append(topic)
        return await original(topic)

    transport.ensure_topic = counting
    for _ in range(3):
        await publisher.send_one(await publisher.publish("order.paid", Payload(value="v")))
    assert calls == ["order-paid"]


async def test_run_sender_drains_the_queue_until_the_sentinel(publisher, transport) -> None:
    for i in range(3):
        await publisher.publish("order.paid", Payload(value=str(i)))
    publisher.stop_sender()
    await asyncio.wait_for(publisher.run_sender(), timeout=5)
    assert len(transport.sent) == 3


async def test_run_sender_survives_a_send_failure(publisher, storage, transport) -> None:
    good = await publisher.publish("order.paid", Payload(value="1"))
    bad = await publisher.publish("order.paid", Payload(value="2"))

    original = transport.publish
    calls = {"n": 0}

    async def flaky(topic_id, body, headers):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return await original(topic_id, body, headers)

    transport.publish = flaky
    publisher.stop_sender()
    await asyncio.wait_for(publisher.run_sender(), timeout=5)

    async with storage.sessionmaker() as s:
        first = await storage.get_outbox(s, good)
        second = await storage.get_outbox(s, bad)
    assert first is not None
    assert second is not None
    assert first.status == "failed"  # the loop recorded the failure
    assert second.status == "sent"  # and kept going


async def test_send_times_out_and_marks_failed(storage, transport) -> None:
    from cap_event_handler.config import Settings
    from cap_event_handler.publisher import Publisher

    slow = Publisher(
        storage, transport, Settings(schema_name=None, publish_timeout=timedelta(seconds=0.01))
    )

    async def never_returns(topic_id, body, headers):
        await asyncio.sleep(10)

    transport.publish = never_returns
    msg_id = await slow.publish("order.paid", Payload(value="v"))
    await slow.send_one(msg_id)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, msg_id)
    assert row is not None
    assert row.status == "failed"
