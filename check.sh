#!/usr/bin/env bash
# The verification gate. Exits non-zero on the first failure, so it cannot be
# silently passed by a pipeline whose last command is `tail`.
set -euo pipefail
uv run ruff format .
uv run ruff check .
uv run mypy src
uv run pytest -q
