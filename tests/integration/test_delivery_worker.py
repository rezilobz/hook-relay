"""Integration tests for the delivery worker: _deliver_to_endpoint, _process_record, move_to_dlq."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from sqlalchemy import select

from hookrelay.config import settings
from hookrelay.db.models import DeliveryAttempt, DLQEntry, Endpoint, Event
from hookrelay.worker.delivery import PoisonPillError, _deliver_to_endpoint, _process_record
from hookrelay.worker.dlq import move_to_dlq
from tests.integration.conftest import FakeProducer, FakeScheduler


def _http_client(*, status_code: int = 200) -> AsyncMock:
    response = MagicMock()
    response.status_code = status_code
    response.is_success = 200 <= status_code < 300
    response.text = "OK" if response.is_success else "Server Error"
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(return_value=response)
    return client


def _timeout_client() -> AsyncMock:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
    return client


async def _seed_event(factory, *, event_type: str = "order.created") -> Event:
    async with factory() as session:
        event = Event(
            event_type=event_type,
            idempotency_key=str(uuid.uuid4()),
            payload={"test": True},
        )
        session.add(event)
        await session.commit()
    return event


async def _seed_endpoint(
    factory,
    *,
    event_types: list[str] | None = None,
    enabled: bool = True,
) -> Endpoint:
    async with factory() as session:
        endpoint = Endpoint(
            url="https://example.com/webhook",
            event_types=event_types or ["order.created"],
            secret="test-secret",
            enabled=enabled,
        )
        session.add(endpoint)
        await session.commit()
    return endpoint


async def _seed_attempt(
    factory,
    *,
    event_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    status: str,
    attempt_number: int = 0,
) -> None:
    async with factory() as session:
        session.add(
            DeliveryAttempt(
                event_id=event_id,
                endpoint_id=endpoint_id,
                attempt_number=attempt_number,
                status=status,
                http_status=200 if status == "success" else None,
                latency_ms=5.0,
            )
        )
        await session.commit()


async def _get_attempts(factory, event_id: uuid.UUID) -> list[DeliveryAttempt]:
    async with factory() as session:
        result = await session.execute(
            select(DeliveryAttempt).where(DeliveryAttempt.event_id == event_id)
        )
        return list(result.scalars().all())


async def _get_dlq_entries(factory, event_id: uuid.UUID) -> list[DLQEntry]:
    async with factory() as session:
        result = await session.execute(select(DLQEntry).where(DLQEntry.event_id == event_id))
        return list(result.scalars().all())


@pytest.mark.integration
class TestDeliverToEndpoint:
    async def test_success_writes_attempt_with_correct_fields(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=200), fake_producer, fake_scheduler
        )

        attempts = await _get_attempts(worker_session_factory, event.id)
        assert len(attempts) == 1
        assert attempts[0].status == "success"
        assert attempts[0].http_status == 200
        assert attempts[0].attempt_number == 0
        assert attempts[0].latency_ms >= 0

    async def test_success_does_not_schedule_retry(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=200), fake_producer, fake_scheduler
        )

        assert fake_scheduler.scheduled == []

    async def test_http_failure_writes_failure_attempt(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=500), fake_producer, fake_scheduler
        )

        attempts = await _get_attempts(worker_session_factory, event.id)
        assert len(attempts) == 1
        assert attempts[0].status == "failure"
        assert attempts[0].http_status == 500

    async def test_http_failure_schedules_retry(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=500), fake_producer, fake_scheduler
        )

        assert len(fake_scheduler.scheduled) == 1
        _, _, next_attempt, msg = fake_scheduler.scheduled[0]
        assert next_attempt == 1
        assert msg["event_id"] == str(event.id)
        assert msg["endpoint_id"] == str(endpoint.id)
        assert msg["attempt_number"] == 1

    async def test_timeout_writes_timeout_attempt_and_schedules_retry(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _timeout_client(), fake_producer, fake_scheduler
        )

        attempts = await _get_attempts(worker_session_factory, event.id)
        assert attempts[0].status == "timeout"
        assert attempts[0].http_status is None
        assert len(fake_scheduler.scheduled) == 1

    async def test_exhausted_writes_dlq_entry(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "max_retry_attempts", 1)
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=500), fake_producer, fake_scheduler
        )

        entries = await _get_dlq_entries(worker_session_factory, event.id)
        assert len(entries) == 1
        assert entries[0].event_id == event.id
        assert entries[0].endpoint_id == endpoint.id

    async def test_exhausted_cancels_redis_entry(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "max_retry_attempts", 1)
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=500), fake_producer, fake_scheduler
        )

        assert (event.id, endpoint.id, 0) in fake_scheduler.cancelled

    async def test_exhausted_publishes_to_dlq_topic(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings, "max_retry_attempts", 1)
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=500), fake_producer, fake_scheduler
        )

        assert len(fake_producer.published) == 1
        topic, msg = fake_producer.published[0]
        assert topic == settings.kafka_topic_dlq
        assert msg["event_id"] == str(event.id)
        assert msg["endpoint_id"] == str(endpoint.id)

    async def test_permanent_4xx_skips_retry_and_goes_to_dlq(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=405), fake_producer, fake_scheduler
        )

        assert fake_scheduler.scheduled == []
        entries = await _get_dlq_entries(worker_session_factory, event.id)
        assert len(entries) == 1
        assert "405" in entries[0].reason

    async def test_429_is_retried_not_dlqd(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await _deliver_to_endpoint(
            event, endpoint, 0, _http_client(status_code=429), fake_producer, fake_scheduler
        )

        assert len(fake_scheduler.scheduled) == 1
        entries = await _get_dlq_entries(worker_session_factory, event.id)
        assert entries == []

    async def test_idempotency_skips_if_already_succeeded(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)
        await _seed_attempt(
            worker_session_factory,
            event_id=event.id,
            endpoint_id=endpoint.id,
            status="success",
        )
        http_client = _http_client(status_code=200)

        await _deliver_to_endpoint(event, endpoint, 1, http_client, fake_producer, fake_scheduler)

        http_client.post.assert_not_called()


@pytest.mark.integration
class TestProcessRecord:
    async def test_fanout_delivers_to_all_matching_endpoints(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        ep_a = await _seed_endpoint(worker_session_factory, event_types=["order.created"])
        ep_b = await _seed_endpoint(worker_session_factory, event_types=["order.created"])
        # ep_c does not match
        await _seed_endpoint(worker_session_factory, event_types=["order.shipped"])
        http_client = _http_client(status_code=200)

        await _process_record(
            {"event_id": str(event.id)}, http_client, fake_producer, fake_scheduler
        )

        assert http_client.post.call_count == 2
        attempts = await _get_attempts(worker_session_factory, event.id)
        endpoint_ids = {a.endpoint_id for a in attempts}
        assert endpoint_ids == {ep_a.id, ep_b.id}

    async def test_fanout_skips_disabled_endpoint(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        await _seed_endpoint(worker_session_factory, enabled=True)
        await _seed_endpoint(worker_session_factory, enabled=False)
        http_client = _http_client(status_code=200)

        await _process_record(
            {"event_id": str(event.id)}, http_client, fake_producer, fake_scheduler
        )

        assert http_client.post.call_count == 1

    async def test_targeted_retry_delivers_only_to_specified_endpoint(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        ep_target = await _seed_endpoint(worker_session_factory)
        ep_other = await _seed_endpoint(worker_session_factory)
        http_client = _http_client(status_code=200)

        await _process_record(
            {"event_id": str(event.id), "endpoint_id": str(ep_target.id), "attempt_number": 1},
            http_client,
            fake_producer,
            fake_scheduler,
        )

        assert http_client.post.call_count == 1
        attempts = await _get_attempts(worker_session_factory, event.id)
        assert all(a.endpoint_id == ep_target.id for a in attempts)
        assert ep_other.id not in {a.endpoint_id for a in attempts}

    async def test_targeted_retry_skips_disabled_endpoint(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        disabled = await _seed_endpoint(worker_session_factory, enabled=False)
        http_client = _http_client(status_code=200)

        await _process_record(
            {"event_id": str(event.id), "endpoint_id": str(disabled.id), "attempt_number": 1},
            http_client,
            fake_producer,
            fake_scheduler,
        )

        http_client.post.assert_not_called()

    async def test_orphan_event_id_returns_silently(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        http_client = _http_client(status_code=200)

        # Should not raise even though the event doesn't exist in the DB.
        await _process_record(
            {"event_id": str(uuid.uuid4())}, http_client, fake_producer, fake_scheduler
        )

        http_client.post.assert_not_called()

    async def test_missing_event_id_raises_poison_pill(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        with pytest.raises(PoisonPillError):
            await _process_record({}, _http_client(status_code=200), fake_producer, fake_scheduler)

    async def test_invalid_event_id_uuid_raises_poison_pill(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        with pytest.raises(PoisonPillError):
            await _process_record(
                {"event_id": "not-a-uuid"},
                _http_client(status_code=200),
                fake_producer,
                fake_scheduler,
            )

    async def test_invalid_attempt_number_raises_poison_pill(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        with pytest.raises(PoisonPillError):
            await _process_record(
                {"event_id": str(uuid.uuid4()), "attempt_number": "bad"},
                _http_client(status_code=200),
                fake_producer,
                fake_scheduler,
            )

    async def test_unexpected_http_exception_writes_failure_attempt(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        """A non-Timeout exception from the HTTP client is treated as a failure."""
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        error_client = AsyncMock(spec=httpx.AsyncClient)
        error_client.post = AsyncMock(side_effect=ConnectionError("reset by peer"))

        await _deliver_to_endpoint(event, endpoint, 0, error_client, fake_producer, fake_scheduler)

        attempts = await _get_attempts(worker_session_factory, event.id)
        assert len(attempts) == 1
        assert attempts[0].status == "failure"
        assert attempts[0].http_status is None
        assert len(fake_scheduler.scheduled) == 1


@pytest.mark.integration
class TestMoveToDeadLetterQueue:
    async def test_writes_dlq_entry_to_postgres(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await move_to_dlq(fake_producer, fake_scheduler, event.id, endpoint.id, "test reason")

        entries = await _get_dlq_entries(worker_session_factory, event.id)
        assert len(entries) == 1
        assert entries[0].reason == "test reason"

    async def test_cancels_redis_retry(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await move_to_dlq(fake_producer, fake_scheduler, event.id, endpoint.id, "exhausted", 0)

        assert (event.id, endpoint.id, 0) in fake_scheduler.cancelled

    async def test_publishes_to_dlq_kafka_topic(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await move_to_dlq(fake_producer, fake_scheduler, event.id, endpoint.id, "exhausted")

        assert len(fake_producer.published) == 1
        topic, msg = fake_producer.published[0]
        assert topic == settings.kafka_topic_dlq
        assert msg == {"event_id": str(event.id), "endpoint_id": str(endpoint.id)}

    async def test_idempotent_second_call_does_not_create_duplicate_entry(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
        fake_scheduler: FakeScheduler,
    ) -> None:
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await move_to_dlq(fake_producer, fake_scheduler, event.id, endpoint.id, "exhausted")
        await move_to_dlq(fake_producer, fake_scheduler, event.id, endpoint.id, "exhausted")

        entries = await _get_dlq_entries(worker_session_factory, event.id)
        assert len(entries) == 1

    async def test_cancel_failure_is_tolerated(
        self,
        worker_session_factory,
        fake_producer: FakeProducer,
    ) -> None:
        """Redis cancel failure must not prevent the DLQ entry from being written."""

        class FailingScheduler:
            async def cancel(self, *_args, **_kwargs) -> None:
                raise RuntimeError("Redis down")

        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await move_to_dlq(fake_producer, FailingScheduler(), event.id, endpoint.id, "test")

        entries = await _get_dlq_entries(worker_session_factory, event.id)
        assert len(entries) == 1

    async def test_kafka_publish_failure_is_tolerated(
        self,
        worker_session_factory,
        fake_scheduler: FakeScheduler,
        failing_producer: FakeProducer,
    ) -> None:
        """Kafka publish failure must not prevent the DLQ entry from being written."""
        event = await _seed_event(worker_session_factory)
        endpoint = await _seed_endpoint(worker_session_factory)

        await move_to_dlq(failing_producer, fake_scheduler, event.id, endpoint.id, "test")

        entries = await _get_dlq_entries(worker_session_factory, event.id)
        assert len(entries) == 1
