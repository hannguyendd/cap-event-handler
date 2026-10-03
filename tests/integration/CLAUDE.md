# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

These tests need Docker. They are deselected by default and run with
`uv run pytest -m integration` (or `uv run pytest -m integration tests/integration/test_postgres.py`).
Every module sets `pytestmark = pytest.mark.integration`. A new file here needs that too, or it
will run in the default suite and fail without Docker.

## Scope: only what the unit suite cannot prove

- `test_postgres.py`: real `FOR UPDATE SKIP LOCKED` (two sweepers never claim the same row),
  row-lock blocking on the inbox dedup insert, JSONB round trips, schema creation, bounded
  collector batches.
- `test_localstack.py`: real SNS→SQS delivery, the notification unwrap, readable queue
  attributes, `provision=False` failures.

Everything else belongs in the SQLite + `MemoryTransport` unit suite. Keep this suite small.

## Fixtures and isolation (`conftest.py`)

- `postgres_url` (session scope) starts `postgres:17-alpine` with testcontainers and rewrites the
  URL to `postgresql+asyncpg://`. `pg_engine` is a fresh engine for each test.
- `localstack_endpoint` (session scope) starts `localstack/localstack:3` with only SNS and SQS.
  `test_localstack.py` sets dummy AWS credentials and `us-east-1` through an autouse fixture.
- The containers are shared across the session, so **each test isolates itself with random
  names**: a fresh `schema_name` (`cap_<hex>`) per test, a random consumer `group`, and random
  topic and queue names. Follow this pattern; never reuse a fixed schema or queue name.
- To age rows past a lookback or retention window, give the writing `SqlStorage` a backdated
  `clock` and the reading one the real clock.
- Waits poll with a deadline (`_wait_for`, 30 s). Never use a bare `sleep` to wait for delivery.
