"""NatsJetStreamTransport against a fake nats-py client.

What needs a real nats-server (stream creation semantics, redelivery timing,
duplicate windows) is in tests/integration/test_nats.py.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from cap_event_handler.config import NatsConfig
from cap_event_handler.envelope import HEADER_MSG_ID
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.transport.base import ReceivedMessage
from cap_event_handler.transport.nats import (
    NATS_MSG_ID,
    UNLIMITED_RETENTION,
    NatsJetStreamTransport,
    nak_payload,
    strip_nats_headers,
)


class FakeSub:
    def __init__(self, js: FakeJS) -> None:
        self.js = js

    async def fetch(self, batch: int = 1, timeout: float | None = 5):  # noqa: ASYNC109 - mirrors nats-py
        self.js.fetch_calls.append((batch, timeout))
        if isinstance(self.js.fetch_result, Exception):
            raise self.js.fetch_result
        return self.js.fetch_result[:batch]


class FakeJS:
    def __init__(self) -> None:
        self.streams: dict[str, SimpleNamespace] = {}
        self.consumers: dict[tuple[str, str], SimpleNamespace] = {}
        self.added_streams: list[StreamConfig] = []
        self.added_consumers: list[tuple[str, ConsumerConfig]] = []
        self.published: list[tuple[str, bytes, dict[str, str], str | None]] = []
        self.fail_publish: Exception | None = None
        self.fetch_result: list[Msg] | Exception = []
        self.fetch_calls: list[tuple[int, float | None]] = []

    async def stream_info(self, name: str):
        if name not in self.streams:
            raise NotFoundError()
        return self.streams[name]

    async def add_stream(self, config: StreamConfig):
        self.added_streams.append(config)
        self.streams[config.name] = SimpleNamespace(config=config)

    async def consumer_info(self, stream: str, consumer: str, timeout=None):  # noqa: ASYNC109 - mirrors nats-py
        if (stream, consumer) not in self.consumers:
            raise NotFoundError()
        return self.consumers[(stream, consumer)]

    async def add_consumer(self, stream: str, config: ConsumerConfig):
        self.added_consumers.append((stream, config))
        self.consumers[(stream, config.durable_name)] = SimpleNamespace(config=config)

    async def publish(self, subject, payload=b"", timeout=None, stream=None, headers=None):  # noqa: ASYNC109 - mirrors nats-py
        if self.fail_publish is not None:
            raise self.fail_publish
        self.published.append((subject, payload, dict(headers or {}), stream))

    async def pull_subscribe_bind(self, durable=None, stream=None, **kwargs):
        return FakeSub(self)


class FakeClient:
    def __init__(self) -> None:
        self.js = FakeJS()
        self.replies: list[tuple[str, bytes]] = []
        self.fail_replies: Exception | None = None
        self.closed = False

    def jetstream(self) -> FakeJS:
        return self.js

    async def publish(self, subject: str, payload: bytes = b"", **kwargs) -> None:
        if self.fail_replies is not None:
            raise self.fail_replies
        self.replies.append((subject, payload))

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def client() -> FakeClient:
    return FakeClient()


def make(client: FakeClient, *, provision: bool = True, **config) -> NatsJetStreamTransport:
    async def connect(**kwargs):
        return client

    return NatsJetStreamTransport(NatsConfig(**config), provision=provision, connect=connect)


def jetstream_msg(*, data: bytes = b"{}", headers=None, delivered: int = 1) -> Msg:
    reply = f"$JS.ACK.CAP.g.{delivered}.10.10.1700000000000000000.0"
    return Msg(_client=None, subject="cap.order-paid", reply=reply, data=data, headers=headers)


def received(*, headers=None, delivered: int = 1) -> ReceivedMessage:
    return ReceivedMessage(
        headers=headers if headers is not None else {HEADER_MSG_ID: "m1"},
        body="{}",
        receipt="$JS.ACK.CAP.g.1.10.10.1700000000000000000.0",
        receive_count=delivered,
    )


# ------------------------------------------------------------------ helpers


def test_nak_payload_without_delay() -> None:
    assert nak_payload(0) == b"-NAK"


def test_nak_payload_carries_the_delay_in_nanoseconds() -> None:
    assert nak_payload(5) == b'-NAK {"delay": 5000000000}'


def test_strip_nats_headers_drops_only_jetstream_headers() -> None:
    assert strip_nats_headers({"cap-msg-id": "1", "Nats-Msg-Id": "1", "x": "y"}) == {
        "cap-msg-id": "1",
        "x": "y",
    }
    assert strip_nats_headers(None) == {}


# ------------------------------------------------------------- provisioning


async def test_ensure_topic_returns_the_prefixed_subject(client) -> None:
    assert await make(client).ensure_topic("order-paid") == "cap.order-paid"


async def test_provisioning_creates_both_file_backed_streams_once(client) -> None:
    transport = make(client)
    await transport.ensure_topic("a")
    await transport.ensure_topic("b")
    await transport.ensure_queue("g")
    main, dlq = client.js.added_streams
    assert (main.name, main.subjects, main.storage) == ("CAP", ["cap.>"], StorageType.FILE)
    assert main.max_age == timedelta(days=7).total_seconds()
    assert main.duplicate_window == timedelta(minutes=5).total_seconds()
    assert main.num_replicas == 1
    assert (dlq.name, dlq.subjects, dlq.storage) == ("CAP_DLQ", ["cap-dlq.>"], StorageType.FILE)
    assert not dlq.max_age


async def test_existing_streams_are_not_modified(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["cap.>"]))
    client.js.streams["CAP_DLQ"] = SimpleNamespace(
        config=StreamConfig(name="CAP_DLQ", subjects=["cap-dlq.>"])
    )
    await make(client).ensure_topic("a")
    assert client.js.added_streams == []


async def test_an_existing_stream_that_misses_our_subjects_is_fatal(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["x.>"]))
    with pytest.raises(ConfigurationError, match=r"cap\.>"):
        await make(client).ensure_topic("a")


async def test_provision_false_with_no_stream_is_fatal(client) -> None:
    with pytest.raises(ConfigurationError, match="provision=False"):
        await make(client, provision=False).ensure_queue("g")
    assert client.js.added_streams == []


async def test_provision_false_tolerates_a_missing_dlq_stream(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["cap.>"]))
    assert await make(client, provision=False).ensure_queue("g") == "g"
    assert client.js.added_streams == []


async def test_ensure_queue_creates_no_consumer(client) -> None:
    await make(client).ensure_queue("g")
    assert client.js.added_consumers == []


async def test_subscribe_creates_one_filtered_durable_pull_consumer(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    await transport.subscribe("g", ["cap.b", "cap.a"])
    [(stream, config)] = client.js.added_consumers
    assert stream == "CAP"
    assert config.durable_name == "g"
    assert config.filter_subjects == ["cap.a", "cap.b"]
    assert config.ack_policy == AckPolicy.EXPLICIT
    assert config.deliver_policy == DeliverPolicy.NEW
    assert config.ack_wait == 60.0
    assert config.max_deliver == 5


# ------------------------------------------------------------------ publish


async def test_publish_sets_the_jetstream_dedup_id(client) -> None:
    await make(client).publish("cap.a", '{"v":1}', {HEADER_MSG_ID: "m1", "x": "y"})
    [(subject, payload, headers, stream)] = client.js.published
    assert (subject, payload, stream) == ("cap.a", b'{"v":1}', "CAP")
    assert headers == {HEADER_MSG_ID: "m1", "x": "y", NATS_MSG_ID: "m1"}


async def test_publish_failure_propagates(client) -> None:
    client.js.fail_publish = RuntimeError("no ack")
    with pytest.raises(RuntimeError, match="no ack"):
        await make(client).publish("cap.a", "{}", {HEADER_MSG_ID: "m1"})


# ------------------------------------------------------------------ receive


async def _subscribed(client) -> NatsJetStreamTransport:
    transport = make(client)
    await transport.ensure_queue("g")
    await transport.subscribe("g", ["cap.order-paid"])
    return transport


async def test_receive_maps_jetstream_messages(client) -> None:
    transport = await _subscribed(client)
    client.js.fetch_result = [
        jetstream_msg(
            data=b'{"v":1}', headers={HEADER_MSG_ID: "m1", NATS_MSG_ID: "m1"}, delivered=3
        )
    ]
    [msg] = await transport.receive("g", max_messages=10, wait_seconds=20)
    assert msg.body == '{"v":1}'
    assert msg.headers == {HEADER_MSG_ID: "m1"}
    assert msg.receive_count == 3
    assert msg.receipt.startswith("$JS.ACK.")
    assert client.js.fetch_calls == [(10, 20)]


async def test_receive_returns_nothing_on_fetch_timeout(client) -> None:
    transport = await _subscribed(client)
    client.js.fetch_result = NatsTimeoutError()
    assert await transport.receive("g", max_messages=10, wait_seconds=0) == []
    assert client.js.fetch_calls == [(10, 1)]  # a zero wait becomes the 1s minimum


async def test_receive_without_a_consumer_waits_and_returns_nothing(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    assert await transport.receive("g", max_messages=10, wait_seconds=0) == []
    assert client.js.fetch_calls == []


async def test_a_body_that_is_not_utf8_does_not_break_receive(client) -> None:
    transport = await _subscribed(client)
    client.js.fetch_result = [jetstream_msg(data=b"\xff\xfe{", headers={HEADER_MSG_ID: "m1"})]
    [msg] = await transport.receive("g", max_messages=10, wait_seconds=0)
    assert "�" in msg.body


# -------------------------------------------------------------- ack and nak


async def test_ack_publishes_plus_ack_to_the_receipt(client) -> None:
    await make(client).ack("g", "$JS.ACK.x")
    assert client.replies == [("$JS.ACK.x", b"+ACK")]


async def test_a_failed_ack_is_logged_not_raised(client, caplog) -> None:
    client.fail_replies = RuntimeError("disconnected")
    await make(client).ack("g", "$JS.ACK.x")
    assert any(r.levelname == "WARNING" and "ack" in r.message for r in caplog.records)


async def test_reject_before_the_limit_naks_with_the_backoff(client) -> None:
    await make(client).reject("g", received(delivered=1), visibility_seconds=5, reason="r")
    assert client.replies == [(received().receipt, b'-NAK {"delay": 5000000000}')]
    assert client.js.published == []


# --------------------------------------------------------- queue attributes


async def test_queue_attributes_map_the_consumer_and_streams(client) -> None:
    transport = await _subscribed(client)
    attrs = await transport.queue_attributes("g")
    assert attrs.visibility_timeout == 60
    assert attrs.message_retention == int(timedelta(days=7).total_seconds())
    assert attrs.has_redrive is True
    assert attrs.subscribed_topics == frozenset({"cap.order-paid"})


async def test_queue_attributes_without_a_consumer_use_the_config(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    attrs = await transport.queue_attributes("g")
    assert attrs.visibility_timeout == 60
    assert attrs.has_redrive is True
    assert attrs.subscribed_topics == frozenset()


async def test_unlimited_stream_retention_reports_a_large_value(client) -> None:
    transport = make(client, max_age=timedelta(0))
    await transport.ensure_queue("g")
    assert (await transport.queue_attributes("g")).message_retention == UNLIMITED_RETENTION


async def test_no_dlq_stream_means_no_redrive(client) -> None:
    client.js.streams["CAP"] = SimpleNamespace(config=StreamConfig(name="CAP", subjects=["cap.>"]))
    transport = make(client, provision=False)
    await transport.ensure_queue("g")
    assert (await transport.queue_attributes("g")).has_redrive is False


async def test_a_consumer_that_gives_up_before_us_means_no_redrive(client) -> None:
    transport = await _subscribed(client)
    client.js.consumers[("CAP", "g")].config.max_deliver = 3  # operator-tuned below our 5
    assert (await transport.queue_attributes("g")).has_redrive is False


async def test_an_unlimited_consumer_still_has_redrive(client) -> None:
    transport = await _subscribed(client)
    client.js.consumers[("CAP", "g")].config.max_deliver = -1
    assert (await transport.queue_attributes("g")).has_redrive is True


async def test_close_closes_the_connection(client) -> None:
    transport = make(client)
    await transport.ensure_queue("g")
    await transport.close()
    assert client.closed is True
