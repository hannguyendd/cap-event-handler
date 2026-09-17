"""What the broker simulator cannot prove: real SNS to SQS delivery."""

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


def build_app(pg_engine, endpoint) -> CapApp:
    return CapApp(
        engine=pg_engine,
        transport=SnsSqs(region="us-east-1", endpoint_url=endpoint),
        group=f"itest-{uuid.uuid4().hex[:8]}",
        settings=Settings(schema_name=f"cap_{uuid.uuid4().hex[:8]}"),
    )


async def _wait_for(predicate, seconds: float = 30.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError("condition never became true")


async def test_publish_and_consume_through_real_sns_and_sqs(pg_engine, localstack_endpoint) -> None:
    """The whole loop: outbox row, SNS publish, SQS receive, dedup, ack."""
    seen: list[str] = []
    app = build_app(pg_engine, localstack_endpoint)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        seen.append(ctx.payload.value)

    async with app.run():
        await app.publish("order.paid", Payload(value="real"))
        await _wait_for(lambda: bool(seen))
    assert seen == ["real"]


async def test_sns_notification_wrapper_is_unwrapped(pg_engine, localstack_endpoint) -> None:
    """Headers must survive the SNS MessageAttributes round trip."""
    captured: list[Context[Payload]] = []
    app = build_app(pg_engine, localstack_endpoint)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        captured.append(ctx)

    async with app.run():
        msg_id = await app.publish("order.paid", Payload(value="v"))
        await _wait_for(lambda: bool(captured))
    assert captured[0].msg_id == msg_id
    assert captured[0].correlation_id is not None


async def test_queue_attributes_are_readable(localstack_endpoint) -> None:
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


async def test_provision_false_fails_loudly_on_a_missing_queue(localstack_endpoint) -> None:
    from cap_event_handler.errors import ConfigurationError

    transport = SnsSqs(region="us-east-1", endpoint_url=localstack_endpoint, provision=False)
    try:
        with pytest.raises(ConfigurationError, match="provision=False"):
            await transport.ensure_queue(f"missing-{uuid.uuid4().hex[:8]}")
    finally:
        await transport.close()
