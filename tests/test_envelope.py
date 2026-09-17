import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel

from cap_event_handler.envelope import (
    HEADER_CORR_ID,
    HEADER_MSG_ID,
    HEADER_MSG_NAME,
    HEADER_MSG_TYPE,
    HEADER_SENT_TIME,
    Envelope,
    build_envelope,
    normalize_topic,
    uuid7,
)
from cap_event_handler.errors import CapError


class OrderPaid(BaseModel):
    order_id: UUID
    amount: str


def test_header_names_are_exactly_the_spec_values() -> None:
    assert HEADER_MSG_ID == "cap-msg-id"
    assert HEADER_MSG_NAME == "cap-msg-name"
    assert HEADER_MSG_TYPE == "cap-msg-type"
    assert HEADER_SENT_TIME == "cap-sent-time"
    assert HEADER_CORR_ID == "cap-corr-id"


def test_uuid7_is_version_7_and_time_ordered() -> None:
    a, b = uuid7(), uuid7()
    assert a.version == 7
    assert b.version == 7
    assert a.int < b.int


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("order.paid", "order-paid"),
        ("billing:invoice.issued", "billing_invoice-issued"),
        ("already-fine_123", "already-fine_123"),
    ],
)
def test_normalize_topic(raw: str, expected: str) -> None:
    assert normalize_topic(raw) == expected


def test_normalize_topic_rejects_characters_sns_forbids() -> None:
    with pytest.raises(CapError, match="topic"):
        normalize_topic("order paid!")


def test_normalize_topic_rejects_empty() -> None:
    with pytest.raises(CapError, match="topic"):
        normalize_topic("")


def test_build_envelope_populates_all_headers() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="10.00"))
    assert env.name == "order.paid"
    assert env.headers[HEADER_MSG_NAME] == "order.paid"
    assert env.headers[HEADER_MSG_ID] == str(env.id)
    assert env.headers[HEADER_MSG_TYPE].endswith("OrderPaid")
    assert UUID(env.headers[HEADER_CORR_ID]) == env.correlation_id
    assert env.sent_time.tzinfo is not None


def test_build_envelope_honours_explicit_correlation_id() -> None:
    corr = uuid7()
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"), correlation_id=corr)
    assert env.correlation_id == corr


def test_payload_is_json_ready() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="10.00"))
    assert json.loads(json.dumps(env.payload)) == env.payload


def test_round_trip_through_transport() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="10.00"))
    back = Envelope.from_transport(env.headers, env.body())
    assert back.id == env.id
    assert back.name == env.name
    assert back.payload == env.payload
    assert back.correlation_id == env.correlation_id
    assert back.sent_time == env.sent_time


def test_from_transport_preserves_unknown_headers() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    headers = dict(env.headers) | {"x-tenant": "acme"}
    back = Envelope.from_transport(headers, env.body())
    assert back.headers["x-tenant"] == "acme"


def test_from_transport_rejects_missing_id() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    headers = {k: v for k, v in env.headers.items() if k != HEADER_MSG_ID}
    with pytest.raises(CapError, match=HEADER_MSG_ID):
        Envelope.from_transport(headers, env.body())


def test_from_transport_rejects_malformed_body() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    with pytest.raises(CapError, match="body"):
        Envelope.from_transport(env.headers, "not json")


@given(
    st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=20)
)
def test_normalize_topic_is_idempotent(name: str) -> None:
    once = normalize_topic(name)
    assert normalize_topic(once) == once


def test_sent_time_is_utc_and_round_trips_exactly() -> None:
    env = build_envelope("order.paid", OrderPaid(order_id=uuid7(), amount="1"))
    assert env.sent_time.tzinfo == UTC or env.sent_time.utcoffset() == datetime.now(UTC).utcoffset()
    assert Envelope.from_transport(env.headers, env.body()).sent_time == env.sent_time
