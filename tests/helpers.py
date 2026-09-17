"""Shared test helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable


async def until(predicate: Callable[[], bool], *, seconds: float = 5.0) -> None:
    """Poll until the predicate holds, so tests do not depend on task scheduling."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never became true")
