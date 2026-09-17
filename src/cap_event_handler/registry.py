"""Which handler serves which event name."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cap_event_handler.errors import ConfigurationError

if TYPE_CHECKING:
    from cap_event_handler.context import Context

HandlerFn = Callable[["Context[Any]"], "Awaitable[None]"]


@dataclass(frozen=True, slots=True)
class Handler:
    """A registered subscriber."""

    name: str
    model: type[BaseModel]
    fn: HandlerFn


class HandlerRegistry:
    """Maps event names to handlers. One handler per name per consumer group."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str, model: type[BaseModel], fn: HandlerFn) -> None:
        if name in self._handlers:
            raise ConfigurationError(
                f"a handler is already registered for {name!r}; one handler per event name"
            )
        if not (isinstance(model, type) and issubclass(model, BaseModel)):
            raise ConfigurationError(
                f"model for {name!r} must be a pydantic BaseModel subclass, got {model!r}"
            )
        if not inspect.iscoroutinefunction(fn):
            raise ConfigurationError(f"handler for {name!r} must be an async function; got {fn!r}")
        self._handlers[name] = Handler(name=name, model=model, fn=fn)

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)

    def topics(self) -> frozenset[str]:
        return frozenset(self._handlers)

    def __len__(self) -> int:
        return len(self._handlers)
