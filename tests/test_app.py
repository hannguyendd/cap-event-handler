from __future__ import annotations

from datetime import timedelta

import pytest
import sqlalchemy as sa
from pydantic import BaseModel

from cap_event_handler import CapApp, Context, Settings
from cap_event_handler.errors import ConfigurationError

from .helpers import until


class Payload(BaseModel):
    value: str


def make_app(sqlite_engine, transport, **overrides):
    settings = Settings(schema_name=None, **overrides)
    return CapApp(engine=sqlite_engine, transport=transport, group="billing", settings=settings)


async def test_subscribe_registers_a_handler(sqlite_engine, transport) -> None:
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        return None

    assert app.registry.get("order.paid") is not None
    assert handler.__name__ == "handler"


async def test_bad_settings_fail_at_construction(sqlite_engine, transport) -> None:
    with pytest.raises(ConfigurationError, match="sweeper_lookback"):
        make_app(
            sqlite_engine,
            transport,
            sweeper_lookback=timedelta(seconds=1),
            publish_timeout=timedelta(seconds=10),
        )


async def test_initialize_creates_the_tables(sqlite_engine, transport) -> None:
    app = make_app(sqlite_engine, transport)
    await app.initialize()
    async with sqlite_engine.begin() as conn:
        names = await conn.run_sync(lambda c: sa.inspect(c).get_table_names())
    assert {"cap_outbox", "cap_inbox"} <= set(names)


async def test_run_provisions_topics_and_subscribes_the_queue(sqlite_engine, transport) -> None:
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        return None

    async with app.run():
        attrs = await transport.queue_attributes(app.queue_id)
        assert "order-paid" in attrs.subscribed_topics


async def test_end_to_end_publish_then_consume(sqlite_engine, transport) -> None:
    """One app publishing to itself: the full loop through storage and broker."""
    seen: list[str] = []
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def handler(ctx: Context[Payload]) -> None:
        seen.append(ctx.payload.value)

    async with app.run():
        await app.publish("order.paid", Payload(value="through"))
        await until(lambda: seen == ["through"])


async def test_publish_inside_a_handler_is_atomic_with_it(sqlite_engine, transport) -> None:
    seen: list[str] = []
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def on_paid(ctx: Context[Payload]) -> None:
        await app.publish("invoice.issued", Payload(value="issued"), session=ctx.session)

    @app.subscribe("invoice.issued", model=Payload)
    async def on_issued(ctx: Context[Payload]) -> None:
        seen.append(ctx.payload.value)

    async with app.run():
        await app.publish("order.paid", Payload(value="paid"))
        await until(lambda: seen == ["issued"])


async def test_correlation_id_propagates_through_a_chain(sqlite_engine, transport) -> None:
    corr: list[object] = []
    app = make_app(sqlite_engine, transport)

    @app.subscribe("order.paid", model=Payload)
    async def on_paid(ctx: Context[Payload]) -> None:
        corr.append(ctx.correlation_id)
        await app.publish("invoice.issued", Payload(value="i"), session=ctx.session)

    @app.subscribe("invoice.issued", model=Payload)
    async def on_issued(ctx: Context[Payload]) -> None:
        corr.append(ctx.correlation_id)

    async with app.run():
        await app.publish("order.paid", Payload(value="p"))
        await until(lambda: len(corr) == 2)
    assert corr[0] == corr[1]


async def test_stop_is_idempotent_and_leaves_no_tasks(sqlite_engine, transport) -> None:
    app = make_app(sqlite_engine, transport)
    async with app.run():
        pass
    await app.stop()
    assert all(t.done() for t in app._tasks)


async def test_warnings_are_collected_not_raised(sqlite_engine, clock) -> None:
    from cap_event_handler.transport.memory import MemoryTransport

    transport = MemoryTransport(clock=clock, visibility_timeout=5, max_receive_count=None)
    app = make_app(sqlite_engine, transport, expected_max_handler_seconds=60)
    async with app.run():
        assert any("VisibilityTimeout" in w for w in app.warnings)
        assert any("redrive" in w for w in app.warnings)


def test_public_exports() -> None:
    import cap_event_handler as m

    for name in m.__all__:
        assert hasattr(m, name), name


async def test_repr_is_informative(sqlite_engine, transport) -> None:
    app = make_app(sqlite_engine, transport)
    assert "billing" in repr(app)
