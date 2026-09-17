"""What a handler is given, and the correlation id that follows it.

The correlation id lives in a ContextVar so a `publish()` inside a handler
inherits it without the handler passing anything. ContextVars are per-task, so
concurrent handlers cannot see each other's.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

T = TypeVar("T")

_correlation_id: ContextVar[UUID | None] = ContextVar("cap_correlation_id", default=None)


def current_correlation_id() -> UUID | None:
    """The correlation id of the message being handled, if any."""
    return _correlation_id.get()


def set_correlation_id(value: UUID) -> Token[UUID | None]:
    return _correlation_id.set(value)


def reset_correlation_id(token: Token[UUID | None]) -> None:
    _correlation_id.reset(token)


@dataclass(frozen=True, slots=True)
class Context(Generic[T]):
    """Everything a handler receives.

    `session` is owned by the library. Its transaction already contains the
    deduplication row, and it commits when the handler returns. A handler that
    commits or closes it breaks the atomicity the pipeline provides.
    """

    payload: T
    headers: Mapping[str, str]
    msg_id: UUID
    name: str
    group: str
    retries: int
    """Broker delivery attempts so far, this one included."""
    sent_time: datetime
    correlation_id: UUID
    session: AsyncSession
