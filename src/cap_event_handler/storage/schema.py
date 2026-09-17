"""Table definitions for the outbox and inbox.

Built by a factory rather than declared at module scope so schema and table
prefix stay configurable. The returned MetaData is what an application points
Alembic at to autogenerate migrations.

Column types carry dialect variants so one definition creates correct tables on
PostgreSQL (JSONB, UUID, TIMESTAMPTZ) and on SQLite, which the unit tests use.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


@dataclass(frozen=True, slots=True)
class Tables:
    """The two tables plus the MetaData that owns them."""

    metadata: sa.MetaData
    outbox: sa.Table
    inbox: sa.Table


def build_tables(*, schema: str | None = "cap", prefix: str = "cap_") -> Tables:
    """Define the outbox and inbox tables in a fresh MetaData."""
    metadata = sa.MetaData(schema=schema)

    outbox = sa.Table(
        f"{prefix}outbox",
        metadata,
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("headers", _JSON, nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("retries", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Index(
            f"{prefix}outbox_sweep_idx",
            "created_at",
            postgresql_where=sa.text("status <> 'sent'"),
        ),
        sa.Index(
            f"{prefix}outbox_collector_idx",
            "expires_at",
            postgresql_where=sa.text("status = 'sent'"),
        ),
    )

    inbox = sa.Table(
        f"{prefix}inbox",
        metadata,
        sa.Column("msg_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("group_name", sa.Text, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Index(f"{prefix}inbox_collector_idx", "expires_at"),
    )

    return Tables(metadata=metadata, outbox=outbox, inbox=inbox)
