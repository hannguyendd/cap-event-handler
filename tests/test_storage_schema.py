from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from cap_event_handler.storage.schema import build_tables


def test_table_names_use_the_prefix() -> None:
    tables = build_tables(schema=None, prefix="cap_")
    assert tables.outbox.name == "cap_outbox"
    assert tables.inbox.name == "cap_inbox"


def test_prefix_is_configurable() -> None:
    tables = build_tables(schema=None, prefix="msg_")
    assert tables.outbox.name == "msg_outbox"
    assert tables.inbox.name == "msg_inbox"


def test_schema_is_applied() -> None:
    tables = build_tables(schema="cap", prefix="cap_")
    assert tables.outbox.schema == "cap"
    assert tables.inbox.schema == "cap"


def test_outbox_columns_match_the_spec() -> None:
    cols = build_tables(schema=None).outbox.columns
    assert set(cols.keys()) == {
        "id",
        "name",
        "headers",
        "payload",
        "status",
        "retries",
        "last_error",
        "created_at",
        "sent_at",
        "expires_at",
    }
    assert cols["id"].primary_key
    assert not cols["name"].nullable
    assert not cols["status"].nullable
    assert cols["last_error"].nullable
    assert cols["sent_at"].nullable


def test_inbox_columns_match_the_spec() -> None:
    cols = build_tables(schema=None).inbox.columns
    assert set(cols.keys()) == {"msg_id", "group_name", "name", "processed_at", "expires_at"}


def test_inbox_primary_key_is_msg_id_and_group() -> None:
    """Keying on msg_id alone would let one group suppress another's delivery."""
    inbox = build_tables(schema=None).inbox
    assert [c.name for c in inbox.primary_key.columns] == ["msg_id", "group_name"]


def test_inbox_has_no_status_or_retry_column() -> None:
    """A row's existence IS success; there is no partial state to reconcile."""
    cols = build_tables(schema=None).inbox.columns.keys()
    assert "status" not in cols
    assert "retries" not in cols


def test_outbox_has_no_version_column() -> None:
    assert "version" not in build_tables(schema=None).outbox.columns.keys()


def test_payload_renders_as_jsonb_on_postgresql() -> None:
    tables = build_tables(schema=None)
    pg = sa.dialects.postgresql.dialect()
    assert "JSONB" in tables.outbox.columns["payload"].type.compile(pg)
    assert "JSONB" in tables.outbox.columns["headers"].type.compile(pg)


def test_indexes_match_the_spec() -> None:
    tables = build_tables(schema=None)
    assert {i.name for i in tables.outbox.indexes} == {
        "cap_outbox_sweep_idx",
        "cap_outbox_collector_idx",
    }
    assert {i.name for i in tables.inbox.indexes} == {"cap_inbox_collector_idx"}


async def test_create_all_works_on_sqlite(sqlite_engine: AsyncEngine) -> None:
    tables = build_tables(schema=None)
    async with sqlite_engine.begin() as conn:
        await conn.run_sync(tables.metadata.create_all)
        names = await conn.run_sync(lambda c: sa.inspect(c).get_table_names())
    assert {"cap_outbox", "cap_inbox"} <= set(names)
