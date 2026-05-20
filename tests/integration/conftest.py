from collections.abc import AsyncGenerator, Generator
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

from hookrelay.api.dependencies import get_producer as get_producer_dep
from hookrelay.db.models import Base
from hookrelay.db.session import get_db
from hookrelay.main import create_app


class FakeProducer:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.published: list[tuple[str, dict]] = []
        self.should_fail = should_fail

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def publish(self, topic: str, message: dict) -> None:
        if self.should_fail:
            raise RuntimeError("Kafka unavailable")
        self.published.append((topic, message))


@pytest.fixture(scope="session")
def postgres_url() -> Generator[str, None, None]:
    with PostgresContainer("postgres:16-alpine") as container:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://"
        )


@pytest.fixture(scope="session")
def kafka_bootstrap_servers() -> Generator[str, None, None]:
    """Session-scoped real Kafka broker for end-to-end tests."""
    with KafkaContainer() as container:
        yield container.get_bootstrap_server()


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


class FakeScheduler:
    """Test double for RetryScheduler. Records calls for assertion."""

    def __init__(self) -> None:
        self.scheduled: list[tuple[UUID, UUID, int, dict[str, Any]]] = []
        self.cancelled: list[tuple[UUID, UUID, int]] = []

    async def schedule(
        self,
        event_id: UUID,
        endpoint_id: UUID,
        attempt_number: int,
        message: dict[str, Any],
    ) -> None:
        self.scheduled.append((event_id, endpoint_id, attempt_number, message))

    async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
        return []

    async def cancel(self, event_id: UUID, endpoint_id: UUID, attempt_number: int) -> None:
        self.cancelled.append((event_id, endpoint_id, attempt_number))


@pytest.fixture()
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture()
def failing_producer() -> FakeProducer:
    return FakeProducer(should_fail=True)


@pytest.fixture()
def fake_scheduler() -> FakeScheduler:
    return FakeScheduler()


@pytest.fixture()
def worker_session_factory(async_engine: AsyncEngine):
    """Patch AsyncSessionLocal in worker modules to use the test database engine."""
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    with (
        patch("hookrelay.worker.delivery.AsyncSessionLocal", factory),
        patch("hookrelay.worker.dlq.AsyncSessionLocal", factory),
        patch("hookrelay.worker.outbox.AsyncSessionLocal", factory),
    ):
        yield factory


def _make_client(
    async_engine: AsyncEngine,
    producer: FakeProducer,
    *,
    raise_server_exceptions: bool = True,
) -> AsyncClient:
    app = create_app()

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with AsyncSession(async_engine, expire_on_commit=False) as session:
            yield session

    async def override_get_producer() -> FakeProducer:
        return producer

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_producer_dep] = override_get_producer

    return AsyncClient(
        transport=ASGITransport(app, raise_app_exceptions=raise_server_exceptions),  # type: ignore[arg-type]
        base_url="http://test",
    )


@pytest.fixture()
async def client(
    async_engine: AsyncEngine, fake_producer: FakeProducer
) -> AsyncGenerator[AsyncClient, None]:
    with patch("hookrelay.main.HookRelayProducer", new=lambda: fake_producer):
        async with _make_client(async_engine, fake_producer) as ac:
            yield ac


@pytest.fixture()
async def failing_client(
    async_engine: AsyncEngine, failing_producer: FakeProducer
) -> AsyncGenerator[AsyncClient, None]:
    with patch("hookrelay.main.HookRelayProducer", new=lambda: failing_producer):
        async with _make_client(
            async_engine, failing_producer, raise_server_exceptions=False
        ) as ac:
            yield ac
