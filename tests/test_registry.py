from __future__ import annotations

import pytest
from pydantic import BaseModel

from cap_event_handler.context import Context
from cap_event_handler.errors import ConfigurationError
from cap_event_handler.registry import HandlerRegistry


class A(BaseModel):
    v: int


async def noop(ctx: Context[A]) -> None:
    return None


def test_register_then_get() -> None:
    r = HandlerRegistry()
    r.register("order.paid", A, noop)
    handler = r.get("order.paid")
    assert handler is not None
    assert handler.model is A
    assert handler.fn is noop


def test_get_returns_none_for_an_unknown_name() -> None:
    assert HandlerRegistry().get("nope") is None


def test_duplicate_registration_is_rejected() -> None:
    r = HandlerRegistry()
    r.register("order.paid", A, noop)
    with pytest.raises(ConfigurationError, match=r"order\.paid"):
        r.register("order.paid", A, noop)


def test_topics_returns_every_registered_name() -> None:
    r = HandlerRegistry()
    r.register("order.paid", A, noop)
    r.register("order.shipped", A, noop)
    assert r.topics() == frozenset({"order.paid", "order.shipped"})


def test_sync_handler_is_rejected() -> None:
    r = HandlerRegistry()

    def sync_handler(ctx: Context[A]) -> None:
        return None

    with pytest.raises(ConfigurationError, match="async"):
        r.register("order.paid", A, sync_handler)  # type: ignore[arg-type]


def test_non_basemodel_model_is_rejected() -> None:
    r = HandlerRegistry()
    with pytest.raises(ConfigurationError, match="BaseModel"):
        r.register("order.paid", dict, noop)  # type: ignore[arg-type]


def test_len_counts_handlers() -> None:
    r = HandlerRegistry()
    assert len(r) == 0
    r.register("a", A, noop)
    assert len(r) == 1
