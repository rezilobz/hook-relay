"""Integration tests for /dlq — requires PostgreSQL (testcontainers)."""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.config import settings
from hookrelay.db.models import DLQEntry, Endpoint, Event
from tests.integration.conftest import FakeProducer

AUTH = {settings.api_key_header: settings.api_key}


async def _seed_dlq_entry(
    db: AsyncSession,
    *,
    event_id: uuid.UUID | None = None,
    endpoint_id: uuid.UUID | None = None,
    reason: str = "max retries exceeded",
) -> DLQEntry:
    """Insert an Event, Endpoint, and DLQEntry directly, bypassing the API."""
    if endpoint_id is None:
        endpoint = Endpoint(
            url="https://example.com/hook",
            event_types=["order.created"],
            secret="secret",
        )
        db.add(endpoint)
        await db.flush()
        endpoint_id = endpoint.id

    if event_id is None:
        event = Event(
            event_type="order.created",
            idempotency_key=str(uuid.uuid4()),
            payload={"order_id": "001"},
            status="dlq",
        )
        db.add(event)
        await db.flush()
        event_id = event.id

    entry = DLQEntry(event_id=event_id, endpoint_id=endpoint_id, reason=reason)
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return entry


# ─── GET /dlq ─────────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestListDLQ:
    async def test_returns_empty_list_when_none_exist(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.get("/dlq", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_returns_all_dlq_entries(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        await _seed_dlq_entry(db)
        await _seed_dlq_entry(db)

        resp = await client.get("/dlq", headers=AUTH)

        assert resp.status_code == 200
        assert len(resp.json()) == 2

    async def test_filter_by_endpoint_id(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        target = await _seed_dlq_entry(db)
        await _seed_dlq_entry(db)  # different endpoint

        resp = await client.get(f"/dlq?endpoint_id={target.endpoint_id}", headers=AUTH)

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["endpoint_id"] == str(target.endpoint_id)

    async def test_missing_auth_returns_422(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.get("/dlq")
        assert resp.status_code == 422


# ─── POST /dlq/{id}/replay ────────────────────────────────────────────────────


@pytest.mark.integration
class TestReplayDLQEntry:
    async def test_returns_200_with_entry_data(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)

        resp = await client.post(f"/dlq/{entry.id}/replay", headers=AUTH)

        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(entry.id)
        assert data["event_id"] == str(entry.event_id)
        assert data["reason"] == entry.reason

    async def test_publishes_event_to_pending_topic(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)

        await client.post(f"/dlq/{entry.id}/replay", headers=AUTH)

        assert len(fake_producer.published) == 1
        topic, message = fake_producer.published[0]
        assert topic == settings.kafka_topic_pending
        assert message == {"event_id": str(entry.event_id)}

    async def test_removes_dlq_entry_after_replay(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)

        await client.post(f"/dlq/{entry.id}/replay", headers=AUTH)

        resp = await client.get("/dlq", headers=AUTH)
        ids = [e["id"] for e in resp.json()]
        assert str(entry.id) not in ids

    async def test_unknown_id_returns_404(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.post(f"/dlq/{uuid.uuid4()}/replay", headers=AUTH)
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)
        resp = await client.post(f"/dlq/{entry.id}/replay")
        assert resp.status_code == 422


# ─── DELETE /dlq/{id} ─────────────────────────────────────────────────────────


@pytest.mark.integration
class TestDeleteDLQEntry:
    async def test_returns_204(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)

        resp = await client.delete(f"/dlq/{entry.id}", headers=AUTH)

        assert resp.status_code == 204

    async def test_deleted_entry_is_gone(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)
        await client.delete(f"/dlq/{entry.id}", headers=AUTH)

        resp = await client.get("/dlq", headers=AUTH)
        ids = [e["id"] for e in resp.json()]
        assert str(entry.id) not in ids

    async def test_second_delete_returns_404(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)
        await client.delete(f"/dlq/{entry.id}", headers=AUTH)

        resp = await client.delete(f"/dlq/{entry.id}", headers=AUTH)
        assert resp.status_code == 404

    async def test_unknown_id_returns_404(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.delete(f"/dlq/{uuid.uuid4()}", headers=AUTH)
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        entry = await _seed_dlq_entry(db)
        resp = await client.delete(f"/dlq/{entry.id}")
        assert resp.status_code == 422
