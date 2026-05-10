"""Integration tests for /events — requires PostgreSQL (testcontainers)."""

import uuid

import pytest
from httpx import AsyncClient

from hookrelay.config import settings
from tests.integration.conftest import FakeProducer

AUTH = {settings.api_key_header: settings.api_key}

VALID_BODY = {
    "event_type": "order.created",
    "idempotency_key": "order-001",
    "payload": {"order_id": "001", "total": 99.99},
}


async def _ingest(client: AsyncClient, body: dict | None = None) -> dict:
    payload = VALID_BODY if body is None else body
    resp = await client.post("/events", json=payload, headers=AUTH)
    assert resp.status_code == 201
    return resp.json()  # type: ignore[no-any-return]


# ─── POST /events ─────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestIngestEvent:
    async def test_returns_201_with_all_fields(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.post("/events", json=VALID_BODY, headers=AUTH)

        assert resp.status_code == 201
        data = resp.json()
        assert data["event_type"] == "order.created"
        assert data["idempotency_key"] == "order-001"
        assert data["payload"] == {"order_id": "001", "total": 99.99}
        assert data["status"] == "pending"
        assert "id" in data
        assert "created_at" in data

    async def test_publishes_to_pending_topic(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.post("/events", json=VALID_BODY, headers=AUTH)
        event_id = resp.json()["id"]

        assert len(fake_producer.published) == 1
        topic, message = fake_producer.published[0]
        assert topic == settings.kafka_topic_pending
        assert message == {"event_id": event_id}

    async def test_duplicate_idempotency_key_returns_same_event(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        first = await _ingest(client)

        resp = await client.post("/events", json=VALID_BODY, headers=AUTH)

        assert resp.status_code == 201
        assert resp.json()["id"] == first["id"]

    async def test_duplicate_does_not_publish_to_kafka(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        await _ingest(client)
        fake_producer.published.clear()

        await client.post("/events", json=VALID_BODY, headers=AUTH)

        assert len(fake_producer.published) == 0

    async def test_missing_auth_header_returns_422(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.post("/events", json=VALID_BODY)
        assert resp.status_code == 422

    async def test_wrong_api_key_returns_401(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.post("/events", json=VALID_BODY, headers={"X-API-Key": "wrong"})
        assert resp.status_code == 401


# ─── GET /events/{id} ─────────────────────────────────────────────────────────


@pytest.mark.integration
class TestGetEvent:
    async def test_returns_event_by_id(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.status_code == 200
        assert resp.json()["id"] == created["id"]

    async def test_unknown_id_returns_404(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.get(f"/events/{uuid.uuid4()}", headers=AUTH)
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        resp = await client.get(f"/events/{created['id']}")
        assert resp.status_code == 422


# ─── GET /events/{id}/deliveries ──────────────────────────────────────────────


@pytest.mark.integration
class TestGetEventDeliveries:
    async def test_returns_empty_list_before_worker_runs(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)

        resp = await client.get(f"/events/{created['id']}/deliveries", headers=AUTH)

        assert resp.status_code == 200
        assert resp.json() == []

    async def test_unknown_event_returns_404(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.get(f"/events/{uuid.uuid4()}/deliveries", headers=AUTH)
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        resp = await client.get(f"/events/{created['id']}/deliveries")
        assert resp.status_code == 422


# ─── POST /events/{id}/retry ──────────────────────────────────────────────────


@pytest.mark.integration
class TestRetryEvent:
    async def test_republishes_to_pending_topic(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        fake_producer.published.clear()

        resp = await client.post(f"/events/{created['id']}/retry", headers=AUTH)

        assert resp.status_code == 200
        assert len(fake_producer.published) == 1
        topic, message = fake_producer.published[0]
        assert topic == settings.kafka_topic_pending
        assert message == {"event_id": created["id"]}

    async def test_resets_status_to_pending(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)

        resp = await client.post(f"/events/{created['id']}/retry", headers=AUTH)

        assert resp.json()["status"] == "pending"

    async def test_unknown_event_returns_404(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        resp = await client.post(f"/events/{uuid.uuid4()}/retry", headers=AUTH)
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        resp = await client.post(f"/events/{created['id']}/retry")
        assert resp.status_code == 422
