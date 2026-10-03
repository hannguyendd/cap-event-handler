# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Every SQL statement lives in `sql.py`

`SqlStorage` is the only place the library issues SQL. One implementation serves PostgreSQL in
production and SQLite in unit tests. The dialects differ in exactly two places, and each is kept
in one spot:

- `ON CONFLICT DO NOTHING`: `_insert_ignore_dialect()` returns the PostgreSQL or SQLite `insert`
  construct. Any other dialect raises `ConfigurationError`.
- `FOR UPDATE SKIP LOCKED`: added in `claim_pending()` only on PostgreSQL. SQLite has no such
  clause, so sweeper concurrency is tested only in `tests/integration/test_postgres.py`.

If you add a statement that needs a third dialect difference, isolate it the same way and add an
integration test. Don't skip the SQLite path.

## Whose session

- `insert_outbox` and `insert_inbox` run on the **caller's** session and never commit. That is
  the whole point: the outbox row commits with the business write, and the inbox marker commits
  with the handler's writes. Never open a separate transaction here or call `commit()`.
- `insert_inbox` returns `False` when the row already exists (judged by `rowcount`). The consumer
  treats `False` as "duplicate, skip the handler". On PostgreSQL a concurrent second insert
  *blocks* on the row lock until the first transaction finishes, then returns `False`.
- `sessionmaker` uses `expire_on_commit=False`, so rows stay readable after commit.

## Schema (`schema.py`)

- `build_tables(schema=..., prefix=...)` builds fresh `MetaData` each time instead of declaring
  tables at module scope, so the schema name and table prefix stay configurable. Applications
  point Alembic autogenerate at `Tables.metadata` (see `docs/usage.md`). **Any column or index
  change is therefore a migration for every user.** Treat it as a breaking change.
- Column types use dialect variants (`JSONB` on PostgreSQL, `JSON` on SQLite) so one definition
  works in both. The partial indexes (`postgresql_where=`) match the sweeper and collector
  queries. If you change a query's `WHERE`, change the index with it.
- `Settings.schema_name=None` means no schema. The unit tests use it because SQLite has no
  schemas. `create_tables()` runs `CREATE SCHEMA IF NOT EXISTS` only on PostgreSQL.

## Time

All timestamps (`created_at`, `sent_at`, `expires_at`, `processed_at`) come from the injected
`clock`, never `func.now()`. That is how the retention, lookback and collector tests stay
deterministic. Integration tests backdate rows by giving the writer and the reader different
clocks.

## Outbox status

`pending` → `sent` (via `mark_sent`, which also sets `expires_at` for the collector) or
`failed` (via `mark_failed`, which increments `retries`). The sweeper re-claims `pending` and
`failed` rows older than `sweeper_lookback` while `retries < max_retries`. The collectors delete
in bounded batches of `collector_batch`. Keep deletes bounded so that one run can never lock a
whole table.
