"""An in-process broker that behaves like SNS fan-out onto SQS queues.

Models the parts the library depends on: visibility leases, receive counts and
redrive to a dead-letter list. Time comes from an injected clock, so tests
advance a timeout instead of sleeping through it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import count
from typing import TYPE_CHECKING

from cap_event_handler.transport.base import QueueAttributes, ReceivedMessage

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence


@dataclass
class _Message:
    headers: dict[str, str]
    body: str
    receipt: str
    visible_at: datetime
    receive_count: int = 0


@dataclass
class _Queue:
    messages: list[_Message] = field(default_factory=list)
    topics: set[str] = field(default_factory=set)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class MemoryTransport:
    """A `Transport` that keeps everything in memory."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        visibility_timeout: int = 30,
        message_retention: int = 345_600,
        max_receive_count: int | None = None,
    ) -> None:
        self._clock = clock or _utcnow
        self.visibility_timeout = visibility_timeout
        self.message_retention = message_retention
        self.max_receive_count = max_receive_count
        self._topics: dict[str, set[str]] = {}
        self._queues: dict[str, _Queue] = {}
        self._receipts = count()
        self.dead_letter: list[ReceivedMessage] = []
        self.rejected: list[tuple[ReceivedMessage, str]] = []
        self.sent: list[tuple[str, str, dict[str, str]]] = []
        self.fail_publish: Exception | None = None

    async def close(self) -> None:
        return None

    async def ensure_topic(self, topic: str) -> str:
        self._topics.setdefault(topic, set())
        return topic

    async def ensure_queue(self, name: str) -> str:
        self._queues.setdefault(name, _Queue())
        return name

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None:
        for topic_id in topic_ids:
            self._topics.setdefault(topic_id, set()).add(queue_id)
            self._queues[queue_id].topics.add(topic_id)

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None:
        if self.fail_publish is not None:
            raise self.fail_publish
        self.sent.append((topic_id, body, dict(headers)))
        now = self._clock()
        for queue_id in self._topics.get(topic_id, ()):
            self._queues[queue_id].messages.append(
                _Message(
                    headers=dict(headers),
                    body=body,
                    receipt=f"r{next(self._receipts)}",
                    visible_at=now,
                )
            )

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]:
        del wait_seconds  # the fake never blocks; tests drive the clock
        await asyncio.sleep(0)
        now = self._clock()
        queue = self._queues[queue_id]
        out: list[ReceivedMessage] = []
        for msg in list(queue.messages):
            if len(out) >= max_messages:
                break
            if msg.visible_at > now:
                continue
            msg.receive_count += 1
            msg.visible_at = now + timedelta(seconds=self.visibility_timeout)
            received = ReceivedMessage(
                headers=dict(msg.headers),
                body=msg.body,
                receipt=msg.receipt,
                receive_count=msg.receive_count,
            )
            if self.max_receive_count is not None and msg.receive_count > self.max_receive_count:
                queue.messages.remove(msg)
                self.dead_letter.append(received)
                continue
            out.append(received)
        return out

    async def ack(self, queue_id: str, receipt: str) -> None:
        queue = self._queues[queue_id]
        queue.messages = [m for m in queue.messages if m.receipt != receipt]

    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        self.rejected.append((message, reason))
        now = self._clock()
        queue = self._queues[queue_id]
        for msg in queue.messages:
            if msg.receipt != message.receipt:
                continue
            msg.visible_at = now + timedelta(seconds=visibility_seconds)
            if self.max_receive_count is not None and msg.receive_count >= self.max_receive_count:
                queue.messages.remove(msg)
                self.dead_letter.append(
                    ReceivedMessage(
                        headers=dict(msg.headers),
                        body=msg.body,
                        receipt=msg.receipt,
                        receive_count=msg.receive_count,
                    )
                )
            return

    async def queue_attributes(self, queue_id: str) -> QueueAttributes:
        return QueueAttributes(
            visibility_timeout=self.visibility_timeout,
            message_retention=self.message_retention,
            has_redrive=self.max_receive_count is not None,
            subscribed_topics=frozenset(self._queues[queue_id].topics),
        )
