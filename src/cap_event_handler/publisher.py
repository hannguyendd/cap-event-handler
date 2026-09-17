"""The publish pipeline: build the envelope, store it in the caller's
transaction, and flush to the broker once that transaction commits.

`publish()` awaits no network I/O. The only thing that reaches the broker is
what a committed transaction left behind, which is what makes the outbox
atomic with the caller's business write.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import event
from sqlalchemy.orm import Session

from cap_event_handler.context import current_correlation_id
from cap_event_handler.envelope import build_envelope, normalize_topic
from cap_event_handler.storage.sql import STATUS_SENT

if TYPE_CHECKING:
    from pydantic import BaseModel
    from sqlalchemy.ext.asyncio import AsyncSession

    from cap_event_handler.config import Settings
    from cap_event_handler.storage.sql import OutboxRow, SqlStorage
    from cap_event_handler.transport.base import Transport

logger = logging.getLogger(__name__)

_PENDING_KEY = "_cap_pending"
_listener_installed = False


def install_after_commit_listener() -> None:
    """Register the flush hook on SQLAlchemy's Session class, once per process.

    Listening on the class rather than on a particular sessionmaker means an
    application does not have to hand the library its sessions, and commits made
    through a raw session still flush. The listener short-circuits on any session
    that never published.
    """
    global _listener_installed
    if _listener_installed:
        return
    event.listen(Session, "after_commit", _on_after_commit)
    _listener_installed = True


def _on_after_commit(session: Session) -> None:
    """Runs synchronously inside the commit path; may not await."""
    pending: list[tuple[Publisher, UUID]] | None = session.info.pop(_PENDING_KEY, None)
    if not pending:
        return
    for publisher, msg_id in pending:
        publisher.notify(msg_id)


class Publisher:
    """Stores outgoing messages and sends the committed ones."""

    def __init__(self, storage: SqlStorage, transport: Transport, settings: Settings) -> None:
        self._storage = storage
        self._transport = transport
        self._settings = settings
        self._topic_ids: dict[str, str] = {}
        self.queue: asyncio.Queue[UUID | None] = asyncio.Queue(maxsize=settings.send_queue_maxsize)
        self.dropped_notifications = 0

    async def publish(
        self,
        name: str,
        payload: BaseModel,
        *,
        session: AsyncSession | None = None,
        correlation_id: UUID | None = None,
    ) -> UUID:
        """Store an event for delivery. Returns its message id.

        With a `session`, the row joins that transaction and is sent after it
        commits. Without one, a short transaction of its own commits immediately.
        """
        corr = correlation_id if correlation_id is not None else current_correlation_id()
        envelope = build_envelope(name, payload, correlation_id=corr)
        if session is not None:
            await self._storage.insert_outbox(session, envelope)
            self._stage(session, envelope.id)
        else:
            async with self._storage.sessionmaker() as own, own.begin():
                await self._storage.insert_outbox(own, envelope)
                self._stage(own, envelope.id)
        return envelope.id

    def _stage(self, session: AsyncSession, msg_id: UUID) -> None:
        pending: list[tuple[Publisher, UUID]] = session.info.setdefault(_PENDING_KEY, [])
        pending.append((self, msg_id))

    def notify(self, msg_id: UUID) -> None:
        """Hand a committed message to the sender.

        A full queue is not an error. The row is committed as pending and the
        sweeper will pick it up, so the in-memory path is a latency optimisation
        with no correctness role.
        """
        try:
            self.queue.put_nowait(msg_id)
        except asyncio.QueueFull:
            self.dropped_notifications += 1
            logger.debug("send queue full; message %s left for the sweeper", msg_id)

    # ------------------------------------------------------------------ sending

    async def topic_id_for(self, name: str) -> str:
        """Resolve and memoise the broker id for an event name."""
        cached = self._topic_ids.get(name)
        if cached is not None:
            return cached
        topic_id = await self._transport.ensure_topic(normalize_topic(name))
        self._topic_ids[name] = topic_id
        return topic_id

    async def send_one(self, msg_id: UUID) -> None:
        """Send a single stored message, if it still needs sending."""
        async with self._storage.sessionmaker() as session:
            row = await self._storage.get_outbox(session, msg_id)
        if row is None or row.status == STATUS_SENT:
            return
        await self.send_row(row)

    async def send_row(self, row: OutboxRow) -> None:
        """Push one row to the broker and record the outcome.

        A crash between a successful publish and `mark_sent` leaves the row
        pending, so the sweeper re-sends it and the consumer sees a duplicate.
        That window is irreducible without two-phase commit; the consume side's
        deduplication absorbs it.
        """
        envelope = row.envelope()
        try:
            topic_id = await self.topic_id_for(row.name)
            await asyncio.wait_for(
                self._transport.publish(topic_id, envelope.body(), envelope.headers),
                timeout=self._settings.publish_timeout.total_seconds(),
            )
        except Exception as exc:
            logger.warning("failed to publish message %s (%s): %r", row.id, row.name, exc)
            async with self._storage.sessionmaker() as session, session.begin():
                await self._storage.mark_failed(session, row.id, repr(exc))
        else:
            async with self._storage.sessionmaker() as session, session.begin():
                await self._storage.mark_sent(session, row.id)

    async def run_sender(self) -> None:
        """Drain committed messages until `stop_sender()` enqueues the sentinel."""
        while True:
            msg_id = await self.queue.get()
            try:
                if msg_id is None:
                    return
                await self.send_one(msg_id)
            except Exception:
                logger.exception("sender loop error for message %s", msg_id)
            finally:
                self.queue.task_done()

    def stop_sender(self) -> None:
        """Ask `run_sender` to finish after it has drained what is queued."""
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            logger.warning("send queue full while stopping; sender will exit on cancellation")
