"""Background work that keeps the publish side durable.

Both processors are publish-side only. The consume side needs none, because the
broker already provides redelivery and quarantine.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cap_event_handler.config import Settings
    from cap_event_handler.publisher import Publisher
    from cap_event_handler.storage.sql import SqlStorage

logger = logging.getLogger(__name__)


def jittered(seconds: float) -> float:
    """Spread instances out so they do not all wake at the same instant."""
    return seconds * random.uniform(0.8, 1.2)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Wait out an interval, but wake immediately when asked to stop."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=jittered(seconds))
    except TimeoutError:
        return


class OutboxSweeper:
    """Re-sends committed messages the in-process sender never delivered.

    This is what turns "the row is committed" into "the message will be sent",
    covering a crash between COMMIT and the broker call, a full send queue, and
    a transient broker failure.
    """

    def __init__(self, storage: SqlStorage, publisher: Publisher, settings: Settings) -> None:
        self._storage = storage
        self._publisher = publisher
        self._settings = settings

    async def sweep_once(self) -> int:
        """Claim one page of unsent rows and try each. Returns how many were claimed."""
        async with self._storage.sessionmaker() as session, session.begin():
            rows = await self._storage.claim_pending(session, limit=200)
        for row in rows:
            try:
                await self._publisher.send_row(row)
            except Exception:
                logger.exception("sweeper failed to send message %s", row.id)
        return len(rows)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.sweep_once()
            except Exception:
                logger.exception("outbox sweep failed")
            await _sleep_or_stop(stop, self._settings.sweeper_interval.total_seconds())


class Collector:
    """Deletes rows that have outlived their retention.

    The outbox keeps succeeded rows only as a short audit trail. The inbox keeps
    deduplication markers, which is why its retention must exceed the queue's
    message retention -- see the startup validation in `validation.py`.
    """

    def __init__(self, storage: SqlStorage, settings: Settings) -> None:
        self._storage = storage
        self._settings = settings

    async def collect_once(self) -> tuple[int, int]:
        """Returns (outbox rows deleted, inbox rows deleted)."""
        async with self._storage.sessionmaker() as session, session.begin():
            outbox_deleted = await self._storage.delete_expired_outbox(session)
            inbox_deleted = await self._storage.delete_expired_inbox(session)
        return outbox_deleted, inbox_deleted

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.collect_once()
            except Exception:
                logger.exception("collector pass failed")
            await _sleep_or_stop(stop, self._settings.collector_interval.total_seconds())
