"""The consume pipeline.

One transaction per message holds both the deduplication INSERT and the
handler's own writes, and the broker is acked only after it commits. Retry,
backoff and poison-message quarantine come from the broker's visibility timeout
and redrive policy, so there is no consume-side retry table.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from cap_event_handler.context import Context, reset_correlation_id, set_correlation_id
from cap_event_handler.envelope import Envelope
from cap_event_handler.errors import CapError, HandlerTransactionError

if TYPE_CHECKING:
    from cap_event_handler.config import Settings
    from cap_event_handler.registry import Handler, HandlerRegistry
    from cap_event_handler.storage.sql import SqlStorage
    from cap_event_handler.transport.base import ReceivedMessage, Transport

logger = logging.getLogger(__name__)

MAX_REASON_LENGTH = 1000
"""Failure reasons are cut to this length before they reach the transport."""


class Consumer:
    """Polls one queue on behalf of one consumer group."""

    def __init__(
        self,
        *,
        group: str,
        queue_id: str,
        registry: HandlerRegistry,
        storage: SqlStorage,
        transport: Transport,
        settings: Settings,
        idle_sleep: float = 0.05,
    ) -> None:
        self._group = group
        self._queue_id = queue_id
        self._registry = registry
        self._storage = storage
        self._transport = transport
        self._settings = settings
        self._idle_sleep = idle_sleep
        self._semaphore = asyncio.Semaphore(settings.group_concurrency)
        self.last_error: BaseException | None = None

    async def run(self, stop: asyncio.Event) -> None:
        """Poll until `stop` is set."""
        while not stop.is_set():
            handled = await self.drain_once()
            if handled == 0:
                await asyncio.sleep(self._idle_sleep)

    async def drain_once(self) -> int:
        """Receive one batch and handle it. Returns how many messages were handled."""
        messages = await self._transport.receive(
            self._queue_id,
            max_messages=self._settings.max_messages,
            wait_seconds=self._settings.wait_seconds,
        )
        if not messages:
            return 0
        if self._settings.group_concurrency == 1:
            for message in messages:
                await self.handle(message)
        else:
            await asyncio.gather(*(self._handle_bounded(m) for m in messages))
        return len(messages)

    async def _handle_bounded(self, message: ReceivedMessage) -> None:
        async with self._semaphore:
            await self.handle(message)

    async def handle(self, message: ReceivedMessage) -> None:
        """Process one message. Never raises; every outcome is an ack or a reject."""
        try:
            envelope = Envelope.from_transport(message.headers, message.body)
        except CapError as exc:
            self.last_error = exc
            logger.error("undecodable message on %s: %r", self._queue_id, exc)
            await self._reject(message, f"undecodable: {exc}")
            return

        handler = self._registry.get(envelope.name)
        if handler is None:
            logger.error(
                "no handler registered for %r in group %r; message %s",
                envelope.name,
                self._group,
                envelope.id,
            )
            if self._settings.on_unroutable == "ack":
                await self._transport.ack(self._queue_id, message.receipt)
            else:
                await self._reject(
                    message,
                    f"unroutable: no handler for {envelope.name!r} in group {self._group!r}",
                )
            return

        try:
            payload = handler.model.model_validate(envelope.payload)
        except ValidationError as exc:
            self.last_error = exc
            logger.error(
                "message %s (%s) failed validation against %s: %s; body=%r",
                envelope.id,
                envelope.name,
                handler.model.__name__,
                exc,
                message.body,
            )
            await self._reject(message, f"invalid payload: {exc}")
            return

        try:
            await self._run_in_transaction(envelope, payload, handler, message)
        except Exception as exc:
            self.last_error = exc
            logger.exception("handler for %s (%s) failed", envelope.id, envelope.name)
            await self._reject(message, f"handler failed: {type(exc).__name__}: {exc}")
            return

        await self._transport.ack(self._queue_id, message.receipt)

    async def _run_in_transaction(
        self,
        envelope: Envelope,
        payload: Any,
        handler: Handler,
        message: ReceivedMessage,
    ) -> None:
        """The atomic part: dedup marker plus the handler's writes, or neither."""
        async with self._storage.sessionmaker() as session, session.begin():
            claimed = await self._storage.insert_inbox(
                session, msg_id=envelope.id, group=self._group, name=envelope.name
            )
            if not claimed:
                logger.debug(
                    "message %s already handled by group %r; skipping", envelope.id, self._group
                )
                return
            context: Context[Any] = Context(
                payload=payload,
                headers=envelope.headers,
                msg_id=envelope.id,
                name=envelope.name,
                group=self._group,
                retries=message.receive_count,
                sent_time=envelope.sent_time,
                correlation_id=envelope.correlation_id,
                session=session,
            )
            token = set_correlation_id(envelope.correlation_id)
            try:
                await handler.fn(context)
            finally:
                reset_correlation_id(token)
            if not session.in_transaction():
                raise HandlerTransactionError(
                    f"the handler for {envelope.name!r} committed or closed ctx.session; "
                    "the library owns that transaction and commits it for you"
                )

    async def _reject(self, message: ReceivedMessage, reason: str) -> None:
        await self._transport.reject(
            self._queue_id,
            message,
            visibility_seconds=self._settings.reject_backoff_seconds,
            reason=reason[:MAX_REASON_LENGTH],
        )
