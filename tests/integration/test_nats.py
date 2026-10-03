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
    app = build_app(pg_engine, isolated_config(nats_url), wait_seconds=1, reject_backoff_seconds=1)

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


async def test_a_message_never_acknowledged_is_dead_lettered_not_dropped(nats_url) -> None:
    """A final delivery that crashes or times out never calls reject; it must not be lost."""
    config = isolated_config(nats_url, max_deliver=2, ack_wait=timedelta(seconds=1))
    transport = NatsJetStreamTransport(config)
    try:
        subject = await transport.ensure_topic("order-paid")
        queue_id = await transport.ensure_queue(f"g{uuid.uuid4().hex[:8]}")
        await transport.subscribe(queue_id, [subject])
        msg_id = str(uuid.uuid4())
        await transport.publish(subject, "{}", {HEADER_MSG_ID: msg_id})
        for delivery in (1, 2):  # handed over, then "crash": neither ack nor reject
            [msg] = await transport.receive(queue_id, max_messages=10, wait_seconds=3)
            assert msg.receive_count == delivery
        assert await transport.receive(queue_id, max_messages=10, wait_seconds=3) == []
    finally:
        await transport.close()

    nc = await nats.connect(nats_url)
    try:
        js = nc.jetstream()
        dead = await js.get_last_msg(config.dlq_stream, f"{config.dlq_subject_prefix}.{queue_id}")
        assert dead.headers[HEADER_MSG_ID] == msg_id
        assert dead.headers[HEADER_DLQ_DELIVERIES] == "3"
        assert "not acknowledged" in dead.headers[HEADER_DLQ_REASON]
    finally:
        await nc.close()
