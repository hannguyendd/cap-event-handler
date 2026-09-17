"""`cap-event-handler worker module:attribute` -- run an app as its own process."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import logging
import signal
import sys
from typing import TYPE_CHECKING

from cap_event_handler.app import CapApp
from cap_event_handler.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def load_app(target: str) -> CapApp:
    """Import `module:attribute` and return the CapApp it names."""
    if ":" not in target:
        raise ConfigurationError(f"expected a target of the form module:attribute, got {target!r}")
    module_name, _, attribute = target.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ConfigurationError(f"could not import module {module_name!r}: {exc}") from exc
    try:
        app = getattr(module, attribute)
    except AttributeError as exc:
        raise ConfigurationError(f"module {module_name!r} has no attribute {attribute!r}") from exc
    if not isinstance(app, CapApp):
        raise ConfigurationError(f"{target} is a {type(app).__name__}, not a CapApp")
    return app


async def _serve(app: CapApp) -> None:
    """Run until SIGINT or SIGTERM, then shut down cleanly."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Windows has no signal handlers on the proactor loop.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    async with app.run():
        logger.info("cap-event-handler worker started for group %r", app.group)
        await stop.wait()
        logger.info("shutting down")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cap-event-handler")
    subparsers = parser.add_subparsers(dest="command", required=True)
    worker = subparsers.add_parser("worker", help="run a CapApp until interrupted")
    worker.add_argument("target", help="module:attribute naming the CapApp")
    worker.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper())
    try:
        app = load_app(args.target)
    except ConfigurationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    asyncio.run(_serve(app))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
