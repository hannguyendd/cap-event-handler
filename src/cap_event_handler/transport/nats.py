"""NATS JetStream: one shared stream, one durable pull consumer per group.

Every event subject lives in one stream so that a group's single consumer can
filter on all of its subjects; a JetStream consumer cannot span streams.
JetStream has no redrive policy, so `reject` supplies one (spec §5). See
docs/superpowers/specs/2026-10-03-nats-jetstream-transport-design.md.

Requires the `nats` extra: `uv add "cap-event-handler[nats]"`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import TYPE_CHECKING

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from cap_event_handler.envelope import (
    HEADER_DLQ_DELIVERIES,
    HEADER_DLQ_GROUP,
    HEADER_DLQ_REASON,
    HEADER_MSG_ID,
    HEADER_MSG_NAME,
)
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.transport.base import QueueAttributes, ReceivedMessage

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from nats.aio.client import Client
    from nats.js.client import JetStreamContext

    from cap_event_handler.config import NatsConfig

logger = logging.getLogger(__name__)

NATS_MSG_ID = "Nats-Msg-Id"
"""JetStream's own deduplication header, set from `cap-msg-id`."""

UNLIMITED_RETENTION = 2**31 - 1
"""What `queue_attributes` reports for a stream with no `max_age`."""

_ACK = b"+ACK"


def nak_payload(delay_seconds: int) -> bytes:
    """`-NAK`, carrying a redelivery delay when one is asked for."""
    if delay_seconds <= 0:
        return b"-NAK"
    return b"-NAK " + json.dumps({"delay": delay_seconds * 1_000_000_000}).encode()


def strip_nats_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    """The message's own headers, without the `Nats-*` ones JetStream adds."""
    return {k: v for k, v in (headers or {}).items() if not k.startswith("Nats-")}


_LINE_BREAK_RE = re.compile(r"\r\n|\r|\n")


def header_safe(value: str) -> str:
    """A header value cannot span lines; validation errors and tracebacks do."""
    return _LINE_BREAK_RE.sub(" ", value)


class NatsJetStreamTransport:
    """A `Transport` backed by NATS JetStream."""

    def __init__(
        self,
        config: NatsConfig,
        *,
        provision: bool = True,
        connect: Callable[..., Awaitable[Client]] = nats.connect,
    ) -> None:
        self.config = config
        self.provision = provision
        self._connect = connect
        self._nc: Client | None = None
        self._js: JetStreamContext | None = None
        self._streams_ready = False
        self._subs: dict[str, JetStreamContext.PullSubscription] = {}

    async def _client(self) -> tuple[Client, JetStreamContext]:
        if self._nc is not None and self._nc.is_closed:
            # nats-py closes the client for good once it stops reconnecting; start over
            # rather than handing every caller a dead connection.
            self._forget_connection()
        if self._nc is None or self._js is None:
            servers = self.config.servers
            self._nc = await self._connect(
                servers=servers if isinstance(servers, str) else list(servers),
                max_reconnect_attempts=-1,
            )
            self._js = self._nc.jetstream()
        return self._nc, self._js

    def _forget_connection(self) -> None:
        self._nc = None
        self._js = None
        self._subs.clear()
        self._streams_ready = False

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.close()
        self._forget_connection()

    # ------------------------------------------------------------ provisioning

    async def _ensure_streams(self) -> None:
        """Create missing streams when provisioning; never modify existing ones."""
        _, js = await self._client()  # first: a replaced connection resets _streams_ready
        if self._streams_ready:
            return
        cfg = self.config
        wanted = f"{cfg.subject_prefix}.>"
        try:
            info = await js.stream_info(cfg.stream)
        except NotFoundError:
            if not self.provision:
                raise ConfigurationError(
                    f"provision=False and JetStream stream {cfg.stream!r} does not exist"
                ) from None
            await js.add_stream(
                StreamConfig(
                    name=cfg.stream,
                    subjects=[wanted],
                    storage=StorageType.FILE,
                    max_age=cfg.max_age.total_seconds(),
                    duplicate_window=cfg.duplicate_window.total_seconds(),
                    num_replicas=cfg.num_replicas,
                )
            )
        else:
            subjects = info.config.subjects or []
            if wanted not in subjects:
                raise ConfigurationError(
                    f"JetStream stream {cfg.stream!r} does not capture {wanted!r} "
                    f"(its subjects are {subjects}); events published there would be lost"
                )
        try:
            await js.stream_info(cfg.dlq_stream)
        except NotFoundError:
            # Without provisioning a missing DLQ stream is a startup warning
            # (no redrive), not an error: see queue_attributes.
            if self.provision:
                await js.add_stream(
                    StreamConfig(
                        name=cfg.dlq_stream,
                        subjects=[f"{cfg.dlq_subject_prefix}.>"],
                        storage=StorageType.FILE,
                        num_replicas=cfg.num_replicas,
                    )
                )
        self._streams_ready = True

    async def ensure_topic(self, topic: str) -> str:
        await self._ensure_streams()
        return f"{self.config.subject_prefix}.{topic}"

    async def ensure_queue(self, name: str) -> str:
        # No consumer yet: one with no filter would receive every subject.
        await self._ensure_streams()
        return name

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None:
        if not self.provision:
            return  # the operator owns the consumer, as Terraform owns SQS wiring
        _, js = await self._client()
        cfg = self.config
        await js.add_consumer(
            cfg.stream,
            ConsumerConfig(
                durable_name=queue_id,
                filter_subjects=sorted(topic_ids),
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=cfg.ack_wait.total_seconds(),
                # Unlimited on purpose: JetStream counts deliveries that end in a crash or
                # an ack_wait expiry and would drop the message silently. `receive`
                # dead-letters anything past cfg.max_deliver instead.
                max_deliver=-1,
                deliver_policy=DeliverPolicy.NEW,
            ),
        )
        self._subs.pop(queue_id, None)

    # ----------------------------------------------------------------- publish

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None:
        _, js = await self._client()
        out = dict(headers)
        if HEADER_MSG_ID in headers:
            out[NATS_MSG_ID] = headers[HEADER_MSG_ID]
        await js.publish(topic_id, body.encode(), headers=out, stream=self.config.stream)

    # ----------------------------------------------------------------- consume

    async def _pull_subscription(self, queue_id: str) -> JetStreamContext.PullSubscription | None:
        sub = self._subs.get(queue_id)
        if sub is not None:
            return sub
        _, js = await self._client()
        try:
            await js.consumer_info(self.config.stream, queue_id)
        except NotFoundError:
            return None
        sub = await js.pull_subscribe_bind(durable=queue_id, stream=self.config.stream)
        self._subs[queue_id] = sub
        return sub

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]:
        sub = await self._pull_subscription(queue_id)
        if sub is None:
            # No handlers, so no consumer: behave like an empty long poll.
            await asyncio.sleep(wait_seconds)
            return []
        try:
            msgs = await sub.fetch(batch=max_messages, timeout=max(wait_seconds, 1))
        except TimeoutError:  # nats-py raises its own subclass and, rarely, the builtin
            return []
        out: list[ReceivedMessage] = []
        for msg in msgs:
            received = ReceivedMessage(
                headers=strip_nats_headers(msg.headers),
                # Replacement, not an exception: an undecodable body must reach the
                # consumer, fail envelope decoding and be rejected like any other.
                body=msg.data.decode("utf-8", errors="replace"),
                receipt=msg.reply,
                receive_count=msg.metadata.num_delivered,
            )
            if received.receive_count > self.config.max_deliver:
                # Its last allowed delivery ended without an ack or a reject: the
                # handler crashed, hung past ack_wait or was killed. Quarantine it
                # rather than run it again.
                await self.reject(
                    queue_id,
                    received,
                    visibility_seconds=int(self.config.ack_wait.total_seconds()),
                    reason=(
                        f"not acknowledged within ack_wait on delivery "
                        f"{received.receive_count - 1} (max_deliver {self.config.max_deliver})"
                    ),
                )
                continue
            out.append(received)
        return out

    async def _reply(self, receipt: str, payload: bytes, what: str) -> None:
        """Ack or nak by publishing to the message's reply subject.

        A lost ack or nak is not fatal: JetStream redelivers after ack_wait and
        the inbox absorbs the duplicate.
        """
        nc, _ = await self._client()
        try:
            await nc.publish(receipt, payload)
        except Exception as exc:
            logger.warning(
                "JetStream %s to %s failed; it will be redelivered after ack_wait: %r",
                what,
                receipt,
                exc,
            )

    async def ack(self, queue_id: str, receipt: str) -> None:
        await self._reply(receipt, _ACK, "ack")

    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        """Nak, or on the last allowed delivery dead-letter and ack (spec §5.2)."""
        nak = nak_payload(visibility_seconds)
        if message.receive_count < self.config.max_deliver:
            await self._reply(message.receipt, nak, "nak")
            return
        msg_id = message.headers.get(HEADER_MSG_ID, "<no id>")
        name = message.headers.get(HEADER_MSG_NAME, "<no name>")
        try:
            await self._dead_letter(queue_id, message, reason)
        except Exception:
            logger.exception(
                "dead-letter publish failed for message %s (%s) in group %r after %d "
                "deliveries; it will be retried on its next delivery: %s",
                msg_id,
                name,
                queue_id,
                message.receive_count,
                reason,
            )
            await self._reply(message.receipt, nak, "nak")
            return
        await self._reply(message.receipt, _ACK, "ack")
        logger.error(
            "dead-lettered message %s (%s) in group %r after %d deliveries: %s",
            msg_id,
            name,
            queue_id,
            message.receive_count,
            reason,
        )

    async def _dead_letter(self, queue_id: str, message: ReceivedMessage, reason: str) -> None:
        _, js = await self._client()
        headers = dict(message.headers)
        headers[HEADER_DLQ_REASON] = header_safe(reason)
        headers[HEADER_DLQ_GROUP] = queue_id
        headers[HEADER_DLQ_DELIVERIES] = str(message.receive_count)
        if HEADER_MSG_ID in message.headers:
            # Idempotent if the final delivery is somehow handled twice.
            headers[NATS_MSG_ID] = f"{message.headers[HEADER_MSG_ID]}:{queue_id}"
        await js.publish(
            f"{self.config.dlq_subject_prefix}.{queue_id}",
            message.body.encode(),
            headers=headers,
            stream=self.config.dlq_stream,
        )

    # -------------------------------------------------------------- validation

    async def queue_attributes(self, queue_id: str) -> QueueAttributes:
        _, js = await self._client()
        cfg = self.config
        stream = await js.stream_info(cfg.stream)
        max_age = stream.config.max_age or 0
        retention = int(max_age) if max_age > 0 else UNLIMITED_RETENTION
        try:
            await js.stream_info(cfg.dlq_stream)
            dlq_exists = True
        except NotFoundError:
            dlq_exists = False
        try:
            info = await js.consumer_info(cfg.stream, queue_id)
        except NotFoundError:
            ack_wait = cfg.ack_wait.total_seconds()
            consumer_max_deliver = -1  # what subscribe creates
            subjects: list[str] = []
        else:
            c = info.config
            ack_wait = c.ack_wait or 30.0
            consumer_max_deliver = c.max_deliver if c.max_deliver is not None else -1
            subjects = list(c.filter_subjects or ([c.filter_subject] if c.filter_subject else []))
        # The library dead-letters on delivery cfg.max_deliver, or on the next one if
        # that delivery ended in a crash or timeout. A consumer that stops at or
        # before cfg.max_deliver lets JetStream drop the message first.
        reaches_our_limit = consumer_max_deliver <= 0 or consumer_max_deliver > cfg.max_deliver
        return QueueAttributes(
            visibility_timeout=int(ack_wait),
            message_retention=retention,
            has_redrive=dlq_exists and reaches_our_limit,
            subscribed_topics=frozenset(subjects),
        )
