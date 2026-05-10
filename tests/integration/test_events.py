"""Integration tests for /events — requires PostgreSQL (testcontainers)."""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.config import settings
from hookrelay.db.models import DeliveryAttempt, DLQEntry, Endpoint
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
        resp = await client.post(
            "/events", json=VALID_BODY, headers={settings.api_key_header: "wrong"}
        )
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


# ─── GET /events/{id} — derived status ───────────────────────────────────────


async def _seed_endpoint(db: AsyncSession) -> Endpoint:
    endpoint = Endpoint(
        url="https://example.com/hook",
        event_types=["order.created"],
        secret="secret",
    )
    db.add(endpoint)
    await db.flush()
    return endpoint


async def _seed_attempt(
    db: AsyncSession,
    *,
    event_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    status: str,
    attempt_number: int = 1,
) -> None:
    db.add(
        DeliveryAttempt(
            event_id=event_id,
            endpoint_id=endpoint_id,
            attempt_number=attempt_number,
            status=status,
            http_status=200 if status == "success" else (None if status == "timeout" else 500),
            latency_ms=10.0,
        )
    )
    await db.commit()


async def _seed_dlq(
    db: AsyncSession,
    *,
    event_id: uuid.UUID,
    endpoint_id: uuid.UUID,
) -> None:
    db.add(DLQEntry(event_id=event_id, endpoint_id=endpoint_id, reason="max retries exceeded"))
    await db.commit()


@pytest.mark.integration
class TestGetEventDerivedStatus:
    # ── pending ──────────────────────────────────────────────────────────────

    async def test_no_attempts_no_dlq_returns_pending(
        self, client: AsyncClient, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "pending"

    async def test_single_failure_returns_pending(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # One failure with no DLQ means the endpoint is still in the retry cycle.
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        await _seed_attempt(
            db, event_id=uuid.UUID(created["id"]), endpoint_id=ep.id, status="failure"
        )

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "pending"

    async def test_multiple_failures_same_endpoint_returns_pending(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # Three failure attempts (attempt_number 1-3) — still retrying, not exhausted.
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        for n in range(1, 4):
            await _seed_attempt(
                db, event_id=eid, endpoint_id=ep.id, status="failure", attempt_number=n
            )

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "pending"

    async def test_timeout_attempt_returns_pending(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # timeout is not a success; endpoint is still in the retry cycle.
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        await _seed_attempt(
            db, event_id=uuid.UUID(created["id"]), endpoint_id=ep.id, status="timeout"
        )

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "pending"

    async def test_all_endpoints_retrying_returns_pending(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # Two endpoints each with failures but no DLQ — both still retrying.
        created = await _ingest(client)
        ep_a, ep_b = await _seed_endpoint(db), await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_a.id, status="failure")
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_b.id, status="failure")

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "pending"

    # ── delivered ─────────────────────────────────────────────────────────────

    async def test_single_success_returns_delivered(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        await _seed_attempt(
            db, event_id=uuid.UUID(created["id"]), endpoint_id=ep.id, status="success"
        )

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "delivered"

    async def test_retry_eventually_succeeds_returns_delivered(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # Attempt 1 fails, attempt 2 succeeds — endpoint is resolved as success.
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep.id, status="failure", attempt_number=1)
        await _seed_attempt(db, event_id=eid, endpoint_id=ep.id, status="success", attempt_number=2)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "delivered"

    async def test_all_endpoints_succeed_returns_delivered(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        ep_a, ep_b = await _seed_endpoint(db), await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_a.id, status="success")
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_b.id, status="success")

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "delivered"

    # ── dlq ───────────────────────────────────────────────────────────────────

    async def test_dlq_only_no_prior_attempts_returns_dlq(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # DLQ entry exists but no delivery_attempts rows — status column in the
        # UNION ALL carries NULL, so bool_or must be coalesced to false.
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        await _seed_dlq(db, event_id=uuid.UUID(created["id"]), endpoint_id=ep.id)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "dlq"

    async def test_failures_exhausted_into_dlq_returns_dlq(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # The normal retry-exhaustion flow: multiple failed attempts followed by a DLQ entry.
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        for n in range(1, 4):
            await _seed_attempt(
                db, event_id=eid, endpoint_id=ep.id, status="failure", attempt_number=n
            )
        await _seed_dlq(db, event_id=eid, endpoint_id=ep.id)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "dlq"

    async def test_timeout_then_dlq_returns_dlq(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        ep = await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep.id, status="timeout", attempt_number=1)
        await _seed_dlq(db, event_id=eid, endpoint_id=ep.id)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "dlq"

    async def test_all_endpoints_exhausted_into_dlq_returns_dlq(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        ep_a, ep_b = await _seed_endpoint(db), await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        for ep in (ep_a, ep_b):
            await _seed_attempt(db, event_id=eid, endpoint_id=ep.id, status="failure")
            await _seed_dlq(db, event_id=eid, endpoint_id=ep.id)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "dlq"

    # ── partially_delivered ───────────────────────────────────────────────────

    async def test_success_and_dlq_returns_partially_delivered(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        ep_a, ep_b = await _seed_endpoint(db), await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_a.id, status="success")
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_b.id, status="failure")
        await _seed_dlq(db, event_id=eid, endpoint_id=ep_b.id)

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "partially_delivered"

    async def test_success_and_retrying_returns_partially_delivered(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # ep_a delivered, ep_b still in the retry cycle.
        created = await _ingest(client)
        ep_a, ep_b = await _seed_endpoint(db), await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_a.id, status="success")
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_b.id, status="failure")

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "partially_delivered"

    async def test_success_and_timeout_retrying_returns_partially_delivered(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        created = await _ingest(client)
        ep_a, ep_b = await _seed_endpoint(db), await _seed_endpoint(db)
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_a.id, status="success")
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_b.id, status="timeout")

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "partially_delivered"

    async def test_three_endpoints_success_dlq_retrying_returns_partially_delivered(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # ep_a succeeded, ep_b exhausted into DLQ, ep_c still retrying.
        created = await _ingest(client)
        ep_a, ep_b, ep_c = (
            await _seed_endpoint(db),
            await _seed_endpoint(db),
            await _seed_endpoint(db),
        )
        eid = uuid.UUID(created["id"])
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_a.id, status="success")
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_b.id, status="failure")
        await _seed_dlq(db, event_id=eid, endpoint_id=ep_b.id)
        await _seed_attempt(db, event_id=eid, endpoint_id=ep_c.id, status="failure")

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "partially_delivered"

    # ── isolation ─────────────────────────────────────────────────────────────

    async def test_attempts_for_other_event_do_not_affect_status(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # event_a has no attempts; event_b has a success. event_a must still read as pending.
        event_a = await _ingest(
            client, {"event_type": "order.created", "idempotency_key": "iso-a", "payload": {}}
        )
        event_b = await _ingest(
            client, {"event_type": "order.created", "idempotency_key": "iso-b", "payload": {}}
        )
        ep = await _seed_endpoint(db)
        await _seed_attempt(
            db, event_id=uuid.UUID(event_b["id"]), endpoint_id=ep.id, status="success"
        )

        resp = await client.get(f"/events/{event_a['id']}", headers=AUTH)

        assert resp.json()["status"] == "pending"

    async def test_registered_but_unprocessed_endpoint_not_reflected_in_status(
        self, client: AsyncClient, db: AsyncSession, fake_producer: FakeProducer
    ) -> None:
        # ep_a succeeded; ep_b is a registered matching endpoint but has no rows in
        # delivery_attempts or dlq_entries (e.g. worker crashed before reaching it).
        # Status is derived purely from attempt records — the system has no visibility
        # into ep_b — so it reports "delivered" based on ep_a alone. This is the
        # fan-out-at-consumption trade-off: status reflects what was attempted, not
        # what was registered.
        created = await _ingest(client)
        ep_a, _ep_b = await _seed_endpoint(db), await _seed_endpoint(db)
        await _seed_attempt(
            db, event_id=uuid.UUID(created["id"]), endpoint_id=ep_a.id, status="success"
        )

        resp = await client.get(f"/events/{created['id']}", headers=AUTH)

        assert resp.json()["status"] == "delivered"
