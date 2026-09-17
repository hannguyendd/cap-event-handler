from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from cap_event_handler.config import Settings
from cap_event_handler.envelope import build_envelope, uuid7
from cap_event_handler.storage.sql import SqlStorage


class Payload(BaseModel):
    value: str


def env(name: str = "order.paid", value: str = "x"):
    return build_envelope(name, Payload(value=value))


async def test_insert_outbox_stores_a_pending_row(storage: SqlStorage) -> None:
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, e.id)
    assert row is not None
    assert row.status == "pending"
    assert row.retries == 0
    assert row.name == "order.paid"
    assert row.payload == {"value": "x"}
    assert row.envelope().headers == e.headers


async def test_rollback_discards_the_outbox_row(storage: SqlStorage) -> None:
    """The whole point of the outbox: no event for a transaction that did not commit."""
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
        await s.rollback()
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, e.id) is None


async def test_mark_sent_sets_status_and_expiry(storage: SqlStorage) -> None:
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    async with storage.sessionmaker() as s, s.begin():
        await storage.mark_sent(s, e.id)
    async with storage.sessionmaker() as s:
        row = await storage.get_outbox(s, e.id)
    assert row is not None
    assert row.status == "sent"


async def test_mark_failed_increments_retries_and_records_the_error(storage: SqlStorage) -> None:
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    for expected in (1, 2):
        async with storage.sessionmaker() as s, s.begin():
            await storage.mark_failed(s, e.id, "boom")
        async with storage.sessionmaker() as s:
            row = await storage.get_outbox(s, e.id)
        assert row is not None
        assert row.status == "failed"
        assert row.retries == expected


async def test_claim_pending_skips_rows_younger_than_the_lookback(storage, clock) -> None:
    """A row still being sent right now must not be swept."""
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.claim_pending(s, limit=10) == []
    clock.advance(timedelta(seconds=61))
    async with storage.sessionmaker() as s, s.begin():
        claimed = await storage.claim_pending(s, limit=10)
    assert [r.id for r in claimed] == [e.id]


async def test_claim_pending_ignores_sent_rows(storage, clock) -> None:
    e = env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, e)
        await storage.mark_sent(s, e.id)
    clock.advance(timedelta(seconds=61))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.claim_pending(s, limit=10) == []


async def test_claim_pending_ignores_rows_past_max_retries(sqlite_engine, clock) -> None:
    store = SqlStorage(sqlite_engine, Settings(schema_name=None, max_retries=2), clock=clock)
    await store.create_tables()
    e = env()
    async with store.sessionmaker() as s, s.begin():
        await store.insert_outbox(s, e)
        await store.mark_failed(s, e.id, "1")
        await store.mark_failed(s, e.id, "2")
    clock.advance(timedelta(seconds=61))
    async with store.sessionmaker() as s, s.begin():
        assert await store.claim_pending(s, limit=10) == []


async def test_claim_pending_respects_the_limit(storage, clock) -> None:
    async with storage.sessionmaker() as s, s.begin():
        for _ in range(5):
            await storage.insert_outbox(s, env())
    clock.advance(timedelta(seconds=61))
    async with storage.sessionmaker() as s, s.begin():
        assert len(await storage.claim_pending(s, limit=3)) == 3


async def test_insert_inbox_returns_true_the_first_time(storage: SqlStorage) -> None:
    msg_id = uuid7()
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="g", name="order.paid") is True


async def test_insert_inbox_returns_false_for_a_duplicate(storage: SqlStorage) -> None:
    """This single boolean is the whole deduplication mechanism."""
    msg_id = uuid7()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_inbox(s, msg_id=msg_id, group="g", name="order.paid")
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="g", name="order.paid") is False


async def test_insert_inbox_allows_the_same_message_in_another_group(storage: SqlStorage) -> None:
    msg_id = uuid7()
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="a", name="n") is True
        assert await storage.insert_inbox(s, msg_id=msg_id, group="b", name="n") is True


async def test_rolled_back_inbox_insert_leaves_no_trace(storage: SqlStorage) -> None:
    """A failed handler must be able to run again from scratch."""
    msg_id = uuid7()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n")
        await s.rollback()
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.insert_inbox(s, msg_id=msg_id, group="g", name="n") is True


async def test_delete_expired_outbox_removes_only_expired_sent_rows(storage, clock) -> None:
    sent, pending = env(), env()
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_outbox(s, sent)
        await storage.insert_outbox(s, pending)
        await storage.mark_sent(s, sent.id)
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_outbox(s) == 0
    clock.advance(timedelta(days=2))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_outbox(s) == 1
    async with storage.sessionmaker() as s:
        assert await storage.get_outbox(s, sent.id) is None
        assert await storage.get_outbox(s, pending.id) is not None


async def test_delete_expired_inbox_honours_retention(storage, clock) -> None:
    async with storage.sessionmaker() as s, s.begin():
        await storage.insert_inbox(s, msg_id=uuid7(), group="g", name="n")
    clock.advance(timedelta(days=14))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_inbox(s) == 0
    clock.advance(timedelta(days=2))
    async with storage.sessionmaker() as s, s.begin():
        assert await storage.delete_expired_inbox(s) == 1


def test_unsupported_dialect_is_rejected(sqlite_engine) -> None:
    """Only PostgreSQL and SQLite have the ON CONFLICT syntax this relies on."""
    from cap_event_handler.errors import ConfigurationError

    class MysqlStorage(SqlStorage):
        @property
        def dialect(self) -> str:
            return "mysql"

    store = MysqlStorage(sqlite_engine, Settings(schema_name=None))
    with pytest.raises(ConfigurationError, match="mysql"):
        store._insert_ignore_dialect()
