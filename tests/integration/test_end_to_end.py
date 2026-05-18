"""End-to-end integration test: real Postgres + real Kafka via testcontainers.

Tests the full pipeline: produce message to Kafka → consumer reads it →
_process_record delivers to endpoint mock → DeliveryAttempt written to Postgres.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from sqlalchemy import select

from hookrelay.config import settings
from hookrelay.db.models import DeliveryAttempt, Endpoint, Event
from hookrelay.kafka.consumer import HookRelayConsumer
from hookrelay.kafka.producer import HookRelayProducer
from hookrelay.worker.delivery import _process_record
from tests.integration.conftest import FakeScheduler


def _ok_http_client() -> AsyncMock:
    response = MagicMock()
    response.status_code = 200
    response.is_success = True
    response.text = "OK"
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(return_value=response)
    return client


async def _seed_event(factory, *, event_type: str = "order.created") -> Event:
    async with factory() as session:
        event = Event(
            event_type=event_type,
            idempotency_key=str(uuid.uuid4()),
            payload={"e2e": True},
        )
        session.add(event)
        await session.commit()
    return event


async def _seed_endpoint(factory, *, event_types: list[str] | None = None) -> Endpoint:
    async with factory() as session:
        endpoint = Endpoint(
            url="https://example.com/e2e-hook",
            event_types=event_types or ["order.created"],
            secret="e2e-secret",
        )
        session.add(endpoint)
        await session.commit()
    return endpoint


async def _get_attempts(factory, event_id: uuid.UUID) -> list[DeliveryAttempt]:
    async with factory() as session:
        result = await session.execute(
            select(DeliveryAttempt).where(DeliveryAttempt.event_id == event_id)
        )
        return list(result.scalars().all())


@pytest.mark.integration
@pytest.mark.slow
class TestKafkaEndToEnd:
    """Full pipeline: publish to Kafka, consume, process, verify DeliveryAttempt."""

    async def test_ingest_event_writes_delivery_attempt(
        self,
        kafka_bootstrap_servers: str,
        worker_session_factory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "kafka_bootstrap_servers", kafka_bootstrap_servers)
        # Unique consumer group per test run so offset tracking is clean.
        monkeypatch.setattr(settings, "kafka_consumer_group", f"test-{uuid.uuid4()}")

        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        # Publish the event message as the ingestion API would.
        producer = HookRelayProducer()
        await producer.start()
        await producer.publish(settings.kafka_topic_pending, {"event_id": str(event.id)})
        await producer.stop()

        # Consume the message with a real consumer and process it.
        consumer = HookRelayConsumer()
        await consumer.start()
        try:
            batch = await consumer.getmany(timeout_ms=10_000, max_records=1)
            assert batch, "No message received from Kafka within timeout"
            records = next(iter(batch.values()))
            assert len(records) == 1

            fake_producer = MagicMock()
            fake_producer.publish = AsyncMock()
            fake_scheduler = FakeScheduler()

            await _process_record(
                records[0].value,
                _ok_http_client(),
                fake_producer,
                fake_scheduler,
            )
        finally:
            await consumer.stop()

        attempts = await _get_attempts(worker_session_factory, event.id)
        assert len(attempts) == 1
        assert attempts[0].status == "success"
        assert attempts[0].endpoint_id == endpoint.id

    async def test_kafka_message_roundtrip_preserves_event_id(
        self,
        kafka_bootstrap_servers: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Producer serializes and consumer deserializes the event_id correctly.

        A fresh consumer group reads from the earliest offset, so earlier test
        messages may be present. We consume all available messages and assert that
        our message is among them.
        """
        monkeypatch.setattr(settings, "kafka_bootstrap_servers", kafka_bootstrap_servers)
        monkeypatch.setattr(settings, "kafka_consumer_group", f"test-{uuid.uuid4()}")

        event_id = str(uuid.uuid4())

        producer = HookRelayProducer()
        await producer.start()
        await producer.publish(settings.kafka_topic_pending, {"event_id": event_id})
        await producer.stop()

        consumer = HookRelayConsumer()
        await consumer.start()
        try:
            # Drain all available messages — earlier test runs may have published
            # to the same topic and a fresh group reads from the earliest offset.
            all_values: list[dict] = []
            for _ in range(20):
                batch = await consumer.getmany(timeout_ms=2_000, max_records=50)
                if not batch:
                    break
                for records in batch.values():
                    all_values.extend(r.value for r in records)
                if {"event_id": event_id} in all_values:
                    break
        finally:
            await consumer.stop()

        assert {"event_id": event_id} in all_values, (
            f"Published message not found in consumed records. Got: {all_values}"
        )
