"""SNS fan-out onto one SQS queue per consumer group, over aiobotocore.

aiobotocore rather than boto3 in a thread pool: each polling task holds a
connection open for up to 20 seconds, which would pin a thread apiece.
"""

from __future__ import annotations

import json
import logging
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from aiobotocore.session import get_session

from cap_event_handler.config import SnsSqsConfig
from cap_event_handler.errors import CapError, ConfigurationError
from cap_event_handler.transport.base import QueueAttributes, ReceivedMessage

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

logger = logging.getLogger(__name__)


def encode_attributes(headers: Mapping[str, str]) -> dict[str, dict[str, str]]:
    """Headers in the shape SNS `Publish` expects."""
    return {k: {"DataType": "String", "StringValue": v} for k, v in headers.items()}


def decode_attributes(raw: Mapping[str, Any]) -> dict[str, str]:
    """Headers out of either the SNS notification shape or the SQS receive shape."""
    out: dict[str, str] = {}
    for key, attr in raw.items():
        value = attr.get("Value", attr.get("StringValue"))
        if value is not None:
            out[key] = value
    return out


def unwrap_notification(body: str) -> tuple[dict[str, str], str]:
    """Pull the real message out of an SNS-to-SQS delivery.

    SNS nests: the SQS body is a notification document carrying the payload in
    `Message` and the headers in `MessageAttributes`. Raw message delivery skips
    the wrapper, so a body that is not a notification is passed through.
    """
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise CapError(f"SQS message body is not valid JSON: {exc}") from exc
    if isinstance(parsed, dict) and parsed.get("Type") == "Notification" and "Message" in parsed:
        return decode_attributes(parsed.get("MessageAttributes", {})), parsed["Message"]
    return {}, body


class SnsSqsTransport:
    """A `Transport` backed by Amazon SNS and SQS."""

    def __init__(self, config: SnsSqsConfig, *, provision: bool = True) -> None:
        self.config = config
        self.provision = provision
        self._session = get_session()
        self._sns: Any = None
        self._sqs: Any = None
        self._exit_stack: AsyncExitStack | None = None

    async def _clients(self) -> tuple[Any, Any]:
        if self._sns is None or self._sqs is None:
            self._exit_stack = AsyncExitStack()
            kwargs: dict[str, Any] = {"region_name": self.config.region}
            if self.config.endpoint_url:
                kwargs["endpoint_url"] = self.config.endpoint_url
            self._sns = await self._exit_stack.enter_async_context(
                self._session.create_client("sns", **kwargs)
            )
            self._sqs = await self._exit_stack.enter_async_context(
                self._session.create_client("sqs", **kwargs)
            )
        return self._sns, self._sqs

    async def close(self) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.aclose()
            self._exit_stack = None
            self._sns = None
            self._sqs = None

    async def ensure_topic(self, topic: str) -> str:
        sns, _ = await self._clients()
        if not self.provision:
            response = await sns.list_topics()
            for entry in response.get("Topics", []):
                if entry["TopicArn"].rsplit(":", 1)[-1] == topic:
                    return str(entry["TopicArn"])
            raise ConfigurationError(
                f"provision=False and no SNS topic named {topic!r} exists in {self.config.region}"
            )
        response = await sns.create_topic(Name=topic)  # idempotent
        return str(response["TopicArn"])

    async def ensure_queue(self, name: str) -> str:
        _, sqs = await self._clients()
        queue_name = self.config.queue_name or name
        if not self.provision:
            try:
                response = await sqs.get_queue_url(QueueName=queue_name)
            except Exception as exc:
                raise ConfigurationError(
                    f"provision=False and no SQS queue named {queue_name!r} exists in "
                    f"{self.config.region}"
                ) from exc
            return str(response["QueueUrl"])
        response = await sqs.create_queue(QueueName=queue_name)  # idempotent
        return str(response["QueueUrl"])

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None:
        if not self.provision:
            return
        sns, sqs = await self._clients()
        attrs = await sqs.get_queue_attributes(QueueUrl=queue_id, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]
        for topic_arn in topic_ids:
            await sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        await self._allow_topics(queue_id, queue_arn, topic_ids)

    async def _allow_topics(self, queue_id: str, queue_arn: str, topic_ids: Sequence[str]) -> None:
        """Let the named topics deliver to this queue."""
        _, sqs = await self._clients()
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": f"cap-allow-{i}",
                    "Effect": "Allow",
                    "Principal": {"Service": "sns.amazonaws.com"},
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                }
                for i, topic_arn in enumerate(topic_ids)
            ],
        }
        await sqs.set_queue_attributes(QueueUrl=queue_id, Attributes={"Policy": json.dumps(policy)})

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None:
        sns, _ = await self._clients()
        await sns.publish(
            TopicArn=topic_id, Message=body, MessageAttributes=encode_attributes(headers)
        )

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]:
        _, sqs = await self._clients()
        response = await sqs.receive_message(
            QueueUrl=queue_id,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
            AttributeNames=["ApproximateReceiveCount"],
            MessageAttributeNames=["All"],
        )
        out: list[ReceivedMessage] = []
        for message in response.get("Messages", []):
            headers, inner = unwrap_notification(message["Body"])
            if not headers:
                headers = decode_attributes(message.get("MessageAttributes", {}))
            out.append(
                ReceivedMessage(
                    headers=headers,
                    body=inner,
                    receipt=message["ReceiptHandle"],
                    receive_count=int(
                        message.get("Attributes", {}).get("ApproximateReceiveCount", 1)
                    ),
                )
            )
        return out

    async def ack(self, queue_id: str, receipt: str) -> None:
        _, sqs = await self._clients()
        await sqs.delete_message(QueueUrl=queue_id, ReceiptHandle=receipt)

    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        del reason  # SQS's redrive policy does the dead-lettering
        _, sqs = await self._clients()
        await sqs.change_message_visibility(
            QueueUrl=queue_id,
            ReceiptHandle=message.receipt,
            VisibilityTimeout=visibility_seconds,
        )

    async def queue_attributes(self, queue_id: str) -> QueueAttributes:
        sns, sqs = await self._clients()
        response = await sqs.get_queue_attributes(
            QueueUrl=queue_id,
            AttributeNames=[
                "VisibilityTimeout",
                "MessageRetentionPeriod",
                "RedrivePolicy",
                "QueueArn",
            ],
        )
        attributes = response.get("Attributes", {})
        queue_arn = attributes.get("QueueArn", "")
        subscribed: set[str] = set()
        paginator = sns.get_paginator("list_subscriptions")
        async for page in paginator.paginate():
            for sub in page.get("Subscriptions", []):
                if sub.get("Endpoint") == queue_arn:
                    subscribed.add(sub["TopicArn"])
        return QueueAttributes(
            visibility_timeout=int(attributes.get("VisibilityTimeout", 30)),
            message_retention=int(attributes.get("MessageRetentionPeriod", 345_600)),
            has_redrive=bool(attributes.get("RedrivePolicy")),
            subscribed_topics=frozenset(subscribed),
        )

    async def __aenter__(self) -> SnsSqsTransport:
        await self._clients()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


def SnsSqs(  # noqa: N802 - reads as a constructor at the call site
    region: str,
    *,
    endpoint_url: str | None = None,
    queue_name: str | None = None,
    provision: bool = True,
) -> SnsSqsTransport:
    """Build an SNS/SQS transport. `SnsSqs(region="ap-southeast-2")`."""
    return SnsSqsTransport(
        SnsSqsConfig(region=region, endpoint_url=endpoint_url, queue_name=queue_name),
        provision=provision,
    )
