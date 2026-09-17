#!/usr/bin/env bash
# The verification gate. Prints a final PASS/FAIL line so that piping its
# output to `tail` cannot hide a failure.
set -uo pipefail
fail=0
run() { echo "--- $* ---"; "$@" || fail=1; }
run uv run ruff format .
run uv run ruff check .
run uv run mypy src
run uv run pytest -q
if [ "$fail" -eq 0 ]; then echo "GATE: PASS"; else echo "GATE: FAIL"; fi
exit "$fail"
