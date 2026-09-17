# CLAUDE.md

## What this is

`cap-event-handler` — a Python library implementing the transactional outbox/inbox pattern over
PostgreSQL (SQLAlchemy 2.x async + asyncpg) and Amazon SNS/SQS. Distribution name
`cap-event-handler`, import name `cap_event_handler`.

The authoritative design is `docs/superpowers/specs/2026-09-17-cap-event-handler-design.md`, and
the plan it was built from is `docs/superpowers/plans/2026-09-17-cap-event-handler.md` (which
records three deliberate deviations from the spec). Read the spec before changing behaviour; do not
re-litigate decisions recorded there — raise a concern and follow it.

## Project management

`uv` manages everything. Never invoke `pip`, and never edit `uv.lock` by hand.

```bash
uv sync                         # install (dev group included by default)
uv add <pkg>                    # runtime dependency
uv add --dev <pkg>              # dev dependency
./check.sh                      # THE GATE: format, lint --fix, mypy --strict, tests
uv run pytest -m integration    # needs Docker: PostgreSQL + LocalStack
```

**Always verify with `./check.sh` and read its final `GATE: PASS` / `GATE: FAIL` line.** Piping any
individual tool to `tail` or `head` masks its exit code, which has already let a lint failure
through once.

`pytest` is configured with `addopts = "-m 'not integration'"`, so the default run never needs
Docker. mypy runs strict; new code is expected to pass it. Markdown is excluded from ruff because
ruff formats fenced Python blocks and mangles the hand-aligned snippets in the spec and plan.

## Architecture in one paragraph

`CapApp` (`app.py`) wires a `SqlStorage` (`storage/sql.py`), a `Transport`
(`transport/snssqs.py`, or `transport/memory.py` in tests), a `Publisher` and a `Consumer`, and
runs them plus two processors (`processors.py`). **Publish:** `publish()` inserts an outbox row on
the *caller's* session and stages the id in `session.info`; a process-wide `after_commit` listener
on SQLAlchemy's `Session` class hands it to the sender, and `OutboxSweeper` recovers anything the
sender missed. **Consume:** one transaction per message holds an `ON CONFLICT DO NOTHING` dedup
insert *and* the handler's writes, and SQS is acked only after it commits — so redelivery, backoff
and quarantine come from the queue's visibility timeout and redrive policy, and there is no
consume-side retry table.

## Conventions

- Message header names are constants in `envelope.py` (`cap-msg-id`, `cap-msg-name`, …). Never
  hard-code the string form elsewhere; a test enforces this.
- `SqlStorage` and `CapApp` take an injectable `clock`, which is how retention and lookback windows
  are tested deterministically. Production leaves it at the wall clock.
- Unit tests run against in-memory SQLite (real transactions, real `ON CONFLICT`) plus a broker
  simulator. There is no storage fake — the guarantees under test are transactional, so a fake
  would only test itself. Anything needing real `FOR UPDATE SKIP LOCKED` or real SNS goes in
  `tests/integration/`.
- `tests/test_guarantees.py` asserts the guarantee table, not implementation details. A refactor
  that keeps it green is safe; one that breaks it broke a promise.
- Tests are written first.

## Reference material, not our code

`docs/reference/cap-atomicity-sqs-postgres.md` analyses the .NET CAP library this design learned
from. The CAP checkout at `/Users/nguyendinhhan/Programming/CHashtag/CAP` is **read-only
reference** — never modify it.
