"""The transport seam.

Two implementations: `snssqs.SnsSqsTransport` for production and
`memory.MemoryTransport`, a broker simulator, for tests. Topic and queue
identifiers are opaque strings -- ARNs and queue URLs in production.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    """One message off a queue, still leased."""

    headers: dict[str, str]
    body: str
    receipt: str
    receive_count: int
    """How many times the broker has delivered this message, this one included."""


@dataclass(frozen=True, slots=True)
class QueueAttributes:
    """What startup validation needs to know about a queue."""

    visibility_timeout: int
    message_retention: int
    has_redrive: bool
    subscribed_topics: frozenset[str]


@runtime_checkable
class Transport(Protocol):
    """Everything the library asks of a broker."""

    async def close(self) -> None: ...

    async def ensure_topic(self, topic: str) -> str:
        """Return the topic's id, creating it when provisioning is enabled."""
        ...

    async def ensure_queue(self, name: str) -> str:
        """Return the queue's id, creating it when provisioning is enabled."""
        ...

    async def subscribe(self, queue_id: str, topic_ids: Sequence[str]) -> None: ...

    async def publish(self, topic_id: str, body: str, headers: Mapping[str, str]) -> None: ...

    async def receive(
        self, queue_id: str, *, max_messages: int, wait_seconds: int
    ) -> list[ReceivedMessage]: ...

    async def ack(self, queue_id: str, receipt: str) -> None:
        """Delete the message. Called only after the handler's transaction commits."""
        ...

    async def reject(
        self,
        queue_id: str,
        message: ReceivedMessage,
        *,
        visibility_seconds: int,
        reason: str,
    ) -> None:
        """Release the lease so the broker redelivers after `visibility_seconds`.

        `reason` says why the message failed. A transport whose broker has no
        redrive policy uses it to dead-letter the message on its last delivery;
        the others ignore it.
        """
        ...

    async def queue_attributes(self, queue_id: str) -> QueueAttributes: ...
