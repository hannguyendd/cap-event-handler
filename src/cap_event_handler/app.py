"""CapApp: wiring and lifecycle.

Holds the storage, transport, publisher, registry and background tasks, and
starts and stops them as a unit.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from cap_event_handler.config import Settings
from cap_event_handler.consumer import Consumer
from cap_event_handler.envelope import normalize_topic
from cap_event_handler.processors import Collector, OutboxSweeper
from cap_event_handler.publisher import Publisher, install_after_commit_listener
from cap_event_handler.registry import HandlerRegistry
from cap_event_handler.storage.sql import SqlStorage
from cap_event_handler.validation import validate_runtime, validate_settings

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from uuid import UUID

    from pydantic import BaseModel
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from cap_event_handler.registry import HandlerFn
    from cap_event_handler.transport.base import Transport

logger = logging.getLogger(__name__)


class CapApp:
    """The library's entry point: publish events, subscribe to events, run both."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        transport: Transport,
        group: str,
        settings: Settings | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings or Settings()
        validate_settings(self.settings)
        self.group = group
        self.transport = transport
        # `clock` exists so retention and lookback windows can be driven
        # deterministically; production leaves it at the wall clock.
        self.storage = SqlStorage(engine, self.settings, clock=clock)
        self.registry = HandlerRegistry()
        install_after_commit_listener()
        self.publisher = Publisher(self.storage, transport, self.settings)
        self.queue_id: str = ""
        self.warnings: list[str] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self._started = False

    # ------------------------------------------------------------- registration

    def subscribe(self, name: str, *, model: type[BaseModel]) -> Callable[[HandlerFn], HandlerFn]:
        """Register a handler for an event name. Returns the function unchanged."""

        def decorator(fn: HandlerFn) -> HandlerFn:
            self.registry.register(name, model, fn)
            return fn

        return decorator

    async def publish(
        self,
        name: str,
        payload: BaseModel,
        *,
        session: AsyncSession | None = None,
        correlation_id: UUID | None = None,
    ) -> UUID:
        """Store an event for delivery. See `Publisher.publish`."""
        return await self.publisher.publish(
            name, payload, session=session, correlation_id=correlation_id
        )

    # ---------------------------------------------------------------- lifecycle

    async def initialize(self) -> None:
        """Create the outbox and inbox tables if they are not there."""
        await self.storage.create_tables()

    async def start(self) -> None:
        """Provision, validate, then start the sender, consumers and processors."""
        if self._started:
            return
        if self.settings.auto_create_tables:
            await self.initialize()

        topic_ids = {
            name: await self.publisher.topic_id_for(name) for name in sorted(self.registry.topics())
        }
        self.queue_id = await self.transport.ensure_queue(normalize_topic(self.group))
        if self.settings.provision and topic_ids:
            await self.transport.subscribe(self.queue_id, list(topic_ids.values()))

        self.warnings = await validate_runtime(
            settings=self.settings,
            transport=self.transport,
            queue_id=self.queue_id,
            registry=self.registry,
            topic_ids=topic_ids,
        )
        for warning in self.warnings:
            logger.warning("%s", warning)

        self._stop = asyncio.Event()
        self._tasks = [asyncio.create_task(self.publisher.run_sender())]
        if len(self.registry):
            for _ in range(self.settings.consumer_tasks):
                consumer = Consumer(
                    group=self.group,
                    queue_id=self.queue_id,
                    registry=self.registry,
                    storage=self.storage,
                    transport=self.transport,
                    settings=self.settings,
                )
                self._tasks.append(asyncio.create_task(consumer.run(self._stop)))
        self._tasks.append(
            asyncio.create_task(
                OutboxSweeper(self.storage, self.publisher, self.settings).run(self._stop)
            )
        )
        self._tasks.append(
            asyncio.create_task(Collector(self.storage, self.settings).run(self._stop))
        )
        self._started = True

    async def stop(self) -> None:
        """Stop polling, let in-flight handlers finish, then cancel what remains."""
        if not self._started:
            return
        self._stop.set()
        self.publisher.stop_sender()
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=self.settings.shutdown_timeout.total_seconds(),
            )
        except TimeoutError:
            logger.warning("shutdown timed out; cancelling %d task(s)", len(self._tasks))
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.transport.close()
        self._started = False

    @asynccontextmanager
    async def run(self) -> AsyncGenerator[CapApp]:
        """Run the app for the duration of the block."""
        await self.start()
        try:
            yield self
        finally:
            await self.stop()

    def __repr__(self) -> str:
        return f"<CapApp group={self.group!r} handlers={len(self.registry)}>"
