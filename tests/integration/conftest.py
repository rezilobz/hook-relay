from collections.abc import AsyncGenerator, Generator
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from testcontainers.postgres import PostgresContainer

from hookrelay.api.dependencies import get_producer as get_producer_dep
from hookrelay.db.models import Base
from hookrelay.db.session import get_db
from hookrelay.main import create_app


class FakeProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def publish(self, topic: str, message: dict) -> None:
        self.published.append((topic, message))


@pytest.fixture(scope="session")
def postgres_url() -> Generator[str, None, None]:
    with PostgresContainer("postgres:16-alpine") as container:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://"
        )


@pytest.fixture()
async def async_engine(postgres_url: str) -> AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(postgres_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture()
async def db(async_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_engine, expire_on_commit=False) as session:
        yield session


@pytest.fixture()
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture()
async def client(
    async_engine: AsyncEngine, fake_producer: FakeProducer
) -> AsyncGenerator[AsyncClient, None]:
    app = create_app()

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with AsyncSession(async_engine, expire_on_commit=False) as session:
            yield session

    async def override_get_producer() -> FakeProducer:
        return fake_producer

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_producer_dep] = override_get_producer

    with patch("hookrelay.main.HookRelayProducer", new=lambda: fake_producer):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            yield ac
