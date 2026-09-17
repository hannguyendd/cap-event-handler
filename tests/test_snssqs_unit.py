from __future__ import annotations

import json

import pytest

from cap_event_handler.errors import CapError
from cap_event_handler.transport.snssqs import (
    decode_attributes,
    encode_attributes,
    unwrap_notification,
)


def test_encode_attributes_uses_the_sns_shape() -> None:
    assert encode_attributes({"cap-msg-id": "abc"}) == {
        "cap-msg-id": {"DataType": "String", "StringValue": "abc"}
    }


def test_decode_attributes_reads_the_sns_notification_shape() -> None:
    raw = {"cap-msg-id": {"Type": "String", "Value": "abc"}}
    assert decode_attributes(raw) == {"cap-msg-id": "abc"}


def test_decode_attributes_reads_the_sqs_receive_shape() -> None:
    raw = {"cap-msg-id": {"DataType": "String", "StringValue": "abc"}}
    assert decode_attributes(raw) == {"cap-msg-id": "abc"}


def test_decode_attributes_skips_entries_with_no_value() -> None:
    assert decode_attributes({"x": {"Type": "Binary"}}) == {}


def test_unwrap_notification_unnests_an_sns_envelope() -> None:
    body = json.dumps(
        {
            "Type": "Notification",
            "MessageId": "n-1",
            "Message": '{"value":"v"}',
            "MessageAttributes": {"cap-msg-id": {"Type": "String", "Value": "abc"}},
        }
    )
    headers, inner = unwrap_notification(body)
    assert headers == {"cap-msg-id": "abc"}
    assert inner == '{"value":"v"}'


def test_unwrap_notification_passes_raw_delivery_through() -> None:
    headers, inner = unwrap_notification('{"value":"v"}')
    assert headers == {}
    assert inner == '{"value":"v"}'


def test_unwrap_notification_rejects_non_json() -> None:
    with pytest.raises(CapError, match="body"):
        unwrap_notification("not json at all")


def test_sns_sqs_convenience_constructor_builds_a_transport() -> None:
    from cap_event_handler.transport.snssqs import SnsSqs, SnsSqsTransport

    t = SnsSqs(region="ap-southeast-2", endpoint_url="http://localhost:4566")
    assert isinstance(t, SnsSqsTransport)
    assert t.config.region == "ap-southeast-2"
    assert t.config.endpoint_url == "http://localhost:4566"


def test_sns_sqs_transport_satisfies_the_protocol() -> None:
    from cap_event_handler.transport.base import Transport
    from cap_event_handler.transport.snssqs import SnsSqs

    assert isinstance(SnsSqs(region="us-east-1"), Transport)


def test_sns_sqs_is_exported_from_the_package() -> None:
    import cap_event_handler as m

    assert "SnsSqs" in m.__all__
    assert hasattr(m, "SnsSqs")
