from __future__ import annotations

import asyncio

from cap_event_handler.context import (
    current_correlation_id,
    reset_correlation_id,
    set_correlation_id,
)
from cap_event_handler.envelope import uuid7


def test_correlation_id_is_none_by_default() -> None:
    assert current_correlation_id() is None


def test_set_and_reset_correlation_id() -> None:
    corr = uuid7()
    token = set_correlation_id(corr)
    assert current_correlation_id() == corr
    reset_correlation_id(token)
    assert current_correlation_id() is None


async def test_correlation_id_does_not_leak_between_tasks() -> None:
    """Two handlers running concurrently must not see each other's correlation id."""
    a, b = uuid7(), uuid7()
    seen: dict[str, object] = {}

    async def run(name: str, corr) -> None:
        token = set_correlation_id(corr)
        await asyncio.sleep(0)
        seen[name] = current_correlation_id()
        reset_correlation_id(token)

    await asyncio.gather(run("a", a), run("b", b))
    assert seen == {"a": a, "b": b}
    assert current_correlation_id() is None
