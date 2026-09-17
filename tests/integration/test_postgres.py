"""What SQLite cannot prove: real PostgreSQL semantics."""

from __future__ import annotations

import asyncio
import uuid as uuid_mod
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler.config import Settings
from cap_event_handler.envelope import build_envelope, uuid7
from cap_event_handler.storage.sql import SqlStorage

pytestmark = pytest.mark.integration


class Payload(BaseModel):
    value: str


def fresh_settings(**overrides) -> Settings:
    """A private schema per test keeps concurrent runs from colliding."""
    return Settings(schema_name=f"cap_{uuid_mod.uuid4().hex[:8]}", **overrides)


async def test_tables_and_schema_are_created(pg_engine) -> None:
    settings = fresh_settings()
    storage = SqlStorage(pg_engine, settings)
    await storage.create_tables()
    async with pg_engine.begin() as conn:
        names = await conn.run_sync(
            lambda c: sa.inspect(c).get_table_names(schema=settings.schema_name)
        )
    assert {"cap_outbox", "cap_inbox"} <= set(names)


async def test_jsonb_round_trips(pg_engine) -> None:
    storage = SqlStorage(pg_engine, fresh_settings())
    await storage.create_tables()
    env = build_envelope("order.paid", Payload(value="v"))
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, env)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, env.id)
    assert row is not None
    assert row.payload == {"value": "v"}
    assert row.headers == env.headers


async def test_skip_locked_stops_two_sweepers_claiming_the_same_row(pg_engine) -> None:
    """The reason the sweeper needs no lock table."""

    def two_minutes_ago() -> datetime:
        return datetime.now(UTC) - timedelta(seconds=120)

    settings = fresh_settings()
    # Writer backdates created_at; reader uses the real clock, so the rows fall
    # outside the sweeper's lookback window.
    writer = SqlStorage(pg_engine, settings, clock=two_minutes_ago)
    reader = SqlStorage(pg_engine, settings)
    await writer.create_tables()
    async with writer.sessionmaker() as s, s.begin():
        for _ in range(10):
            await writer.insert_outbox(s, build_envelope("order.paid", Payload(value="v")))

    claimed: list[list] = []

    async def sweep() -> None:
        async with reader.sessionmaker() as s, s.begin():
            rows = await reader.claim_pending(s, limit=10)
            claimed.append(rows)
            await asyncio.sleep(0.3)  # hold the locks

    await asyncio.gather(sweep(), sweep())
    first, second = ({r.id for r in claimed[0]}, {r.id for r in claimed[1]})
    assert first & second == set()
    assert len(first) + len(second) == 10


async def test_dedup_insert_blocks_then_reports_duplicate(pg_engine) -> None:
    """Concurrent redelivery: the second attempt waits, then exits as a duplicate."""
    storage = SqlStorage(pg_engine, fresh_settings())
    await storage.create_tables()
    msg_id = uuid7()
    outcomes: list[bool] = []
    first_inserted = asyncio.Event()

    async def first() -> None:
        async with storage.sessionmaker() as s, s.begin():
            outcomes.append(await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n"))
            first_inserted.set()
            await asyncio.sleep(0.3)

    async def second() -> None:
        await first_inserted.wait()
        async with storage.sessionmaker() as s, s.begin():
            outcomes.append(await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n"))

    await asyncio.gather(first(), second())
    assert sorted(outcomes) == [False, True]


async def test_collector_batches_are_bounded(pg_engine) -> None:
    def thirty_days_ago() -> datetime:
        return datetime.now(UTC) - timedelta(days=30)

    settings = fresh_settings(collector_batch=5)
    writer = SqlStorage(pg_engine, settings, clock=thirty_days_ago)
    reader = SqlStorage(pg_engine, settings)
    await writer.create_tables()
    async with writer.sessionmaker() as s, s.begin():
        for _ in range(12):
            await writer.insert_inbox(s, msg_id=uuid7(), group="g", name="n")
    async with reader.sessionmaker() as s, s.begin():
        assert await reader.delete_expired_inbox(s) == 5
