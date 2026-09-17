# CLAUDE.md

## What this is

`cap-event-handler` — a Python library implementing the transactional outbox/inbox pattern over
PostgreSQL (SQLAlchemy 2.x async + asyncpg) and Amazon SNS/SQS. Distribution name
`cap-event-handler`, import name `cap_event_handler`.

**Status: design approved, implementation not started.** The authoritative design is
`docs/superpowers/specs/2026-09-17-cap-event-handler-design.md`. Read it before writing code —
it settles the schema, the two pipelines, the failure dispositions and what is deliberately
deferred (§11). Do not re-litigate decisions recorded there; raise a concern and follow the spec.

## Project management

`uv` manages everything. Never invoke `pip`, and never edit `uv.lock` by hand.

```bash
uv sync                         # install (dev group included by default)
uv add <pkg>                    # runtime dependency
uv add --dev <pkg>              # dev dependency
uv run pytest                   # unit + pipeline tests; no Docker needed
uv run pytest -m integration    # needs Docker: PostgreSQL + LocalStack
uv run ruff check . && uv run ruff format . && uv run mypy src
```

`pytest` is configured with `addopts = "-m 'not integration'"`, so the default run skips anything
requiring Docker. mypy runs in strict mode; new code is expected to pass it.

## Conventions

- Module layout is fixed by spec §8. `storage/` and `transport/` each define a `typing.Protocol`
  in `base.py` with one production implementation and one in-memory fake. The fakes are what let
  the pipeline tests run with no infrastructure — keep them in step with the real ones.
- Message header names are constants in `envelope.py` (`cap-msg-id`, `cap-msg-name`, …). Never
  hard-code the string form elsewhere.
- Tests are written first (see `superpowers:test-driven-development`). The crash-window cases in
  spec §10 assert the guarantees in §1.2, not implementation details.

## Reference material, not our code

`docs/reference/cap-atomicity-sqs-postgres.md` analyses the .NET CAP library this design learned
from. The CAP checkout at `/Users/nguyendinhhan/Programming/CHashtag/CAP` is **read-only
reference** — never modify it.
