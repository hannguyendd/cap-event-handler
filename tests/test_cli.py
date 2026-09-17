from __future__ import annotations

import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path

import pytest

from cap_event_handler.cli import load_app, main
from cap_event_handler.errors import ConfigurationError


@pytest.fixture
def app_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    module = tmp_path / "myservice.py"
    module.write_text(
        textwrap.dedent(
            """
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy.pool import StaticPool

            from cap_event_handler import CapApp, Settings
            from cap_event_handler.transport.memory import MemoryTransport

            engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
            app = CapApp(
                engine=engine,
                transport=MemoryTransport(),
                group="cli-test",
                settings=Settings(schema_name=None),
            )
            not_an_app = 42
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    yield "myservice"
    sys.modules.pop("myservice", None)


def test_load_app_imports_the_attribute(app_module: str) -> None:
    from cap_event_handler import CapApp

    assert isinstance(load_app(f"{app_module}:app"), CapApp)


def test_load_app_rejects_a_target_without_a_colon() -> None:
    with pytest.raises(ConfigurationError, match="module:attribute"):
        load_app("myservice")


def test_load_app_reports_a_missing_module() -> None:
    with pytest.raises(ConfigurationError, match="no_such_module"):
        load_app("no_such_module:app")


def test_load_app_reports_a_missing_attribute(app_module: str) -> None:
    with pytest.raises(ConfigurationError, match="missing"):
        load_app(f"{app_module}:missing")


def test_load_app_rejects_a_non_app_attribute(app_module: str) -> None:
    with pytest.raises(ConfigurationError, match="CapApp"):
        load_app(f"{app_module}:not_an_app")


def test_main_returns_two_on_a_bad_target(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["worker", "nope"]) == 2
    assert "module:attribute" in capsys.readouterr().err


def test_main_requires_the_worker_subcommand() -> None:
    with pytest.raises(SystemExit):
        main([])
