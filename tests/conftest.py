from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import StaticPool

from cap_event_handler.config import Settings


@pytest.fixture
def settings() -> Settings:
    """Settings with no schema, because SQLite has none."""
    return Settings(schema_name=None)


@pytest_asyncio.fixture
async def sqlite_engine() -> AsyncIterator[AsyncEngine]:
    """A private in-memory SQLite database, one per test.

    StaticPool keeps every connection pointed at the same in-memory database;
    without it each connection would get its own empty one.
    """
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    try:
        yield engine
    finally:
        await engine.dispose()


class FakeClock:
    """A clock a test can move by hand."""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest_asyncio.fixture
async def storage(sqlite_engine, settings, clock):
    from cap_event_handler.storage.sql import SqlStorage

    store = SqlStorage(sqlite_engine, settings, clock=clock)
    await store.create_tables()
    return store


@pytest.fixture
def transport(clock):
    from cap_event_handler.transport.memory import MemoryTransport

    return MemoryTransport(clock=clock, visibility_timeout=30, max_receive_count=5)


@pytest.fixture
def publisher(storage, transport, settings):
    from cap_event_handler.publisher import Publisher, install_after_commit_listener

    install_after_commit_listener()
    return Publisher(storage, transport, settings)
