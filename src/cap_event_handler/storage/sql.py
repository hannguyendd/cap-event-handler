"""Every SQL statement the library issues.

One implementation serves PostgreSQL in production and SQLite under unit test.
The two places the dialects genuinely differ -- `ON CONFLICT DO NOTHING` and
`FOR UPDATE SKIP LOCKED` -- are isolated in `_insert_ignore_dialect` and
`claim_pending`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from cap_event_handler.envelope import Envelope
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.storage.schema import build_tables

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import CursorResult
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from cap_event_handler.config import Settings

STATUS_PENDING = "pending"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"


@dataclass(frozen=True, slots=True)
class OutboxRow:
    """A row of the outbox, as the sender and sweeper see it."""

    id: UUID
    name: str
    headers: dict[str, str]
    payload: dict[str, Any]
    status: str
    retries: int
    created_at: datetime

    def envelope(self) -> Envelope:
        return Envelope(id=self.id, name=self.name, headers=self.headers, payload=self.payload)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class SqlStorage:
    """Outbox and inbox persistence over a SQLAlchemy AsyncEngine."""

    def __init__(
        self,
        engine: AsyncEngine,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._engine = engine
        self._settings = settings
        self._clock = clock or _utcnow
        self.tables = build_tables(schema=settings.schema_name, prefix=settings.table_prefix)
        self.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    @property
    def dialect(self) -> str:
        return self._engine.dialect.name

    def _insert_ignore_dialect(self) -> Any:
        """Return the dialect-specific `insert` construct that supports ON CONFLICT."""
        if self.dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            return pg_insert
        if self.dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            return sqlite_insert
        raise ConfigurationError(
            f"cap_event_handler supports PostgreSQL (and SQLite for tests); "
            f"got dialect {self.dialect!r}"
        )

    async def create_tables(self) -> None:
        """Idempotent DDL. Creates the schema first when one is configured."""
        async with self._engine.begin() as conn:
            schema = self._settings.schema_name
            if schema and self.dialect == "postgresql":
                await conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            await conn.run_sync(self.tables.metadata.create_all)

    # ---------------------------------------------------------------- outbox

    async def insert_outbox(self, session: AsyncSession, env: Envelope) -> None:
        """INSERT on the caller's session, so it commits with their transaction."""
        await session.execute(
            sa.insert(self.tables.outbox).values(
                id=env.id,
                name=env.name,
                headers=env.headers,
                payload=env.payload,
                status=STATUS_PENDING,
                retries=0,
                created_at=self._clock(),
            )
        )

    async def get_outbox(self, session: AsyncSession, msg_id: UUID) -> OutboxRow | None:
        t = self.tables.outbox
        result = await session.execute(sa.select(t).where(t.c.id == msg_id))
        row = result.mappings().one_or_none()
        return None if row is None else _to_outbox_row(row)

    async def mark_sent(self, session: AsyncSession, msg_id: UUID) -> None:
        now = self._clock()
        t = self.tables.outbox
        await session.execute(
            sa.update(t)
            .where(t.c.id == msg_id)
            .values(
                status=STATUS_SENT,
                sent_at=now,
                expires_at=now + self._settings.outbox_retention,
                last_error=None,
            )
        )

    async def mark_failed(self, session: AsyncSession, msg_id: UUID, error: str) -> None:
        t = self.tables.outbox
        await session.execute(
            sa.update(t)
            .where(t.c.id == msg_id)
            .values(status=STATUS_FAILED, retries=t.c.retries + 1, last_error=error[:2000])
        )

    async def claim_pending(self, session: AsyncSession, *, limit: int) -> list[OutboxRow]:
        """Rows the sweeper should re-send, locked against other instances.

        `FOR UPDATE SKIP LOCKED` is how two app instances divide the work. SQLite
        has no such clause, so it is applied only on PostgreSQL; concurrency is
        therefore covered by the integration tests, not the unit tests.
        """
        t = self.tables.outbox
        cutoff = self._clock() - self._settings.sweeper_lookback
        stmt = (
            sa.select(t)
            .where(
                t.c.status.in_((STATUS_PENDING, STATUS_FAILED)),
                t.c.retries < self._settings.max_retries,
                t.c.created_at < cutoff,
            )
            .order_by(t.c.created_at)
            .limit(limit)
        )
        if self.dialect == "postgresql":
            stmt = stmt.with_for_update(skip_locked=True)
        result = await session.execute(stmt)
        return [_to_outbox_row(row) for row in result.mappings()]

    async def delete_expired_outbox(self, session: AsyncSession) -> int:
        t = self.tables.outbox
        victims = (
            sa.select(t.c.id)
            .where(t.c.status == STATUS_SENT, t.c.expires_at < self._clock())
            .limit(self._settings.collector_batch)
            .scalar_subquery()
        )
        result = await session.execute(sa.delete(t).where(t.c.id.in_(victims)))
        return cast("CursorResult[Any]", result).rowcount

    # ----------------------------------------------------------------- inbox

    async def insert_inbox(
        self, session: AsyncSession, *, msg_id: UUID, group: str, name: str
    ) -> bool:
        """Claim this message for this group. False means it was already handled.

        Runs inside the handler's own transaction, which is what makes the
        effect and the marker atomic.
        """
        now = self._clock()
        insert = self._insert_ignore_dialect()
        stmt = (
            insert(self.tables.inbox)
            .values(
                msg_id=msg_id,
                group_name=group,
                name=name,
                processed_at=now,
                expires_at=now + self._settings.inbox_retention,
            )
            .on_conflict_do_nothing()
        )
        result = await session.execute(stmt)
        return bool(cast("CursorResult[Any]", result).rowcount)

    async def delete_expired_inbox(self, session: AsyncSession) -> int:
        t = self.tables.inbox
        keys = (
            await session.execute(
                sa.select(t.c.msg_id, t.c.group_name)
                .where(t.c.expires_at < self._clock())
                .limit(self._settings.collector_batch)
            )
        ).all()
        if not keys:
            return 0
        result = await session.execute(
            sa.delete(t).where(
                sa.or_(
                    *(
                        sa.and_(t.c.msg_id == msg_id, t.c.group_name == group)
                        for msg_id, group in keys
                    )
                )
            )
        )
        return cast("CursorResult[Any]", result).rowcount


def _to_outbox_row(row: sa.RowMapping) -> OutboxRow:
    return OutboxRow(
        id=row["id"],
        name=row["name"],
        headers=row["headers"],
        payload=row["payload"],
        status=row["status"],
        retries=row["retries"],
        created_at=row["created_at"],
    )
