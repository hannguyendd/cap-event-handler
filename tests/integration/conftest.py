from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer("postgres:17-alpine") as pg:
        url = pg.get_connection_url()
        yield url.replace("postgresql+psycopg2://", "postgresql+asyncpg://").replace(
            "postgresql+psycopg://", "postgresql+asyncpg://"
        )


@pytest_asyncio.fixture
async def pg_engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def localstack_endpoint() -> Iterator[str]:
    from testcontainers.community.localstack import LocalStackContainer

    with LocalStackContainer("localstack/localstack:3").with_services("sns", "sqs") as ls:
        yield ls.get_url()


@pytest.fixture(scope="session")
def nats_url() -> Iterator[str]:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = (
        DockerContainer("nats:2.10")
        .with_command("-js")
        .with_exposed_ports(4222)
        .waiting_for(LogMessageWaitStrategy("Server is ready"))
    )
    with container:
        host = container.get_container_host_ip()
        yield f"nats://{host}:{container.get_exposed_port(4222)}"
