from __future__ import annotations

from datetime import timedelta

import pytest

from cap_event_handler.transport.memory import MemoryTransport


@pytest.fixture
def transport(clock):
    return MemoryTransport(clock=clock, visibility_timeout=30)


async def _wire(transport: MemoryTransport, topic: str = "order-paid", queue: str = "g"):
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


async def test_publish_to_an_unsubscribed_topic_delivers_nowhere(
    transport: MemoryTransport,
) -> None:
    topic_id = await transport.ensure_topic("other")
    queue_id = await transport.ensure_queue("g")
    await transport.publish(topic_id, "{}", {})
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []


async def test_received_message_is_invisible_until_the_timeout_expires(transport, clock) -> None:
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    assert len(await transport.receive(queue_id, max_messages=10, wait_seconds=0)) == 1
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []
    clock.advance(timedelta(seconds=31))
    again = await transport.receive(queue_id, max_messages=10, wait_seconds=0)
    assert len(again) == 1
    assert again[0].receive_count == 2


async def test_ack_removes_the_message_permanently(transport, clock) -> None:
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    msg = (await transport.receive(queue_id, max_messages=10, wait_seconds=0))[0]
    await transport.ack(queue_id, msg.receipt)
    clock.advance(timedelta(seconds=3600))
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []


async def test_reject_makes_the_message_visible_after_the_backoff(transport, clock) -> None:
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    msg = (await transport.receive(queue_id, max_messages=10, wait_seconds=0))[0]
    await transport.reject(queue_id, msg, visibility_seconds=5, reason="boom")
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []
    clock.advance(timedelta(seconds=6))
    assert len(await transport.receive(queue_id, max_messages=10, wait_seconds=0)) == 1


async def test_max_messages_caps_the_batch(transport: MemoryTransport) -> None:
    topic_id, queue_id = await _wire(transport)
    for _ in range(5):
        await transport.publish(topic_id, "{}", {})
    assert len(await transport.receive(queue_id, max_messages=2, wait_seconds=0)) == 2


async def test_redrive_moves_a_poison_message_to_the_dead_letter_list(clock) -> None:
    transport = MemoryTransport(clock=clock, visibility_timeout=30, max_receive_count=3)
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    for _ in range(3):
        msgs = await transport.receive(queue_id, max_messages=10, wait_seconds=0)
        assert len(msgs) == 1
        await transport.reject(queue_id, msgs[0], visibility_seconds=0, reason="boom")
    assert await transport.receive(queue_id, max_messages=10, wait_seconds=0) == []
    assert len(transport.dead_letter) == 1


async def test_queue_attributes_report_what_validation_needs(clock) -> None:
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


def test_memory_transport_satisfies_the_protocol() -> None:
    from cap_event_handler.transport.base import Transport

    assert isinstance(MemoryTransport(), Transport)


async def test_reject_records_the_reason(transport: MemoryTransport) -> None:
    topic_id, queue_id = await _wire(transport)
    await transport.publish(topic_id, "{}", {})
    msg = (await transport.receive(queue_id, max_messages=10, wait_seconds=0))[0]
    await transport.reject(queue_id, msg, visibility_seconds=0, reason="handler failed: X")
    assert transport.rejected == [(msg, "handler failed: X")]
