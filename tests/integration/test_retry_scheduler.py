"""Integration tests for RedisRetryScheduler and run_scheduler."""

import asyncio
import contextlib
import time
import uuid
from typing import Any
from unittest.mock import patch

import fakeredis
import pytest

from hookrelay.config import settings
from hookrelay.worker.retry import RedisRetryScheduler
from hookrelay.worker.scheduler import run_scheduler
from tests.integration.conftest import FakeProducer


@pytest.fixture()
async def redis_client():
    client = fakeredis.FakeAsyncRedis()
    yield client
    await client.aclose()


@pytest.fixture()
async def retry_scheduler(redis_client) -> RedisRetryScheduler:
    return RedisRetryScheduler(redis_client)


def _msg(event_id: uuid.UUID, endpoint_id: uuid.UUID, attempt: int = 1) -> dict[str, Any]:
    return {
        "event_id": str(event_id),
        "endpoint_id": str(endpoint_id),
        "attempt_number": attempt,
    }


@pytest.mark.integration
class TestRedisRetryScheduler:
    async def test_schedule_and_poll_returns_message(
        self, retry_scheduler: RedisRetryScheduler
    ) -> None:
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()
        msg = _msg(event_id, endpoint_id)

        await retry_scheduler.schedule(event_id, endpoint_id, 0, msg)
        due = await retry_scheduler.poll_due(now=time.time() + 10_000)

        assert due == [msg]

    async def test_poll_before_due_returns_empty(
        self, retry_scheduler: RedisRetryScheduler
    ) -> None:
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()

        await retry_scheduler.schedule(event_id, endpoint_id, 0, _msg(event_id, endpoint_id))
        # Poll at a past timestamp — the entry is not due yet.
        due = await retry_scheduler.poll_due(now=time.time() - 10_000)

        assert due == []

    async def test_cancel_removes_entry_before_poll(
        self, retry_scheduler: RedisRetryScheduler
    ) -> None:
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()

        await retry_scheduler.schedule(event_id, endpoint_id, 0, _msg(event_id, endpoint_id))
        await retry_scheduler.cancel(event_id, endpoint_id, 0)
        due = await retry_scheduler.poll_due(now=time.time() + 10_000)

        assert due == []

    async def test_poll_does_not_remove_entry(self, retry_scheduler: RedisRetryScheduler) -> None:
        """poll_due fetches without removing; cancel() is what removes the entry."""
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()
        await retry_scheduler.schedule(event_id, endpoint_id, 0, _msg(event_id, endpoint_id))

        future = time.time() + 10_000
        first = await retry_scheduler.poll_due(now=future)
        second = await retry_scheduler.poll_due(now=future)

        # Both polls return the same entry — no removal on fetch.
        assert len(first) == 1
        assert second == first

        # cancel() is the only way to remove the entry.
        await retry_scheduler.cancel(event_id, endpoint_id, 0)
        assert await retry_scheduler.poll_due(now=future) == []

    async def test_multiple_due_entries_all_returned(
        self, retry_scheduler: RedisRetryScheduler
    ) -> None:
        pairs = [(uuid.uuid4(), uuid.uuid4()) for _ in range(3)]
        messages = [_msg(eid, epid) for eid, epid in pairs]

        for (eid, epid), msg in zip(pairs, messages, strict=True):
            await retry_scheduler.schedule(eid, epid, 0, msg)

        due = await retry_scheduler.poll_due(now=time.time() + 10_000)

        assert len(due) == 3
        for msg in messages:
            assert msg in due

    async def test_cancel_is_idempotent(self, retry_scheduler: RedisRetryScheduler) -> None:
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()
        # Cancelling an entry that was never scheduled must not raise.
        await retry_scheduler.cancel(event_id, endpoint_id, 0)
        await retry_scheduler.cancel(event_id, endpoint_id, 0)

    async def test_reschedule_overwrites_previous_entry(
        self, retry_scheduler: RedisRetryScheduler
    ) -> None:
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()
        msg_v1 = _msg(event_id, endpoint_id, attempt=1)
        msg_v2 = _msg(event_id, endpoint_id, attempt=2)

        await retry_scheduler.schedule(event_id, endpoint_id, 0, msg_v1)
        await retry_scheduler.schedule(event_id, endpoint_id, 0, msg_v2)
        due = await retry_scheduler.poll_due(now=time.time() + 10_000)

        # Same member key → only one entry; latest write wins.
        assert len(due) == 1
        assert due[0] == msg_v2

    async def test_corrupt_json_payload_is_skipped(
        self, retry_scheduler: RedisRetryScheduler, redis_client
    ) -> None:
        """A non-JSON value in the hash must be skipped without crashing poll_due."""
        from hookrelay.worker.retry import _DATA_KEY, _ZSET_KEY

        # Inject a corrupt entry directly into Redis.
        await redis_client.hset(_DATA_KEY, "bad:entry", "not-valid-json{{{")
        await redis_client.zadd(_ZSET_KEY, {"bad:entry": time.time() - 1})

        # A valid entry to confirm poll still returns good entries.
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()
        msg = _msg(event_id, endpoint_id)
        await retry_scheduler.schedule(event_id, endpoint_id, 0, msg)

        due = await retry_scheduler.poll_due(now=time.time() + 10_000)

        assert due == [msg]

    async def test_orphaned_zset_member_is_cleaned_up(
        self, retry_scheduler: RedisRetryScheduler, redis_client
    ) -> None:
        """A ZSET member with no hash entry must be removed (not re-scanned forever)."""
        from hookrelay.worker.retry import _ZSET_KEY

        # Add a member to the ZSET but not to the data hash.
        await redis_client.zadd(_ZSET_KEY, {"orphan:member": time.time() - 1})

        due = await retry_scheduler.poll_due(now=time.time() + 10_000)

        assert due == []
        # The orphan should be gone from the ZSET.
        score = await redis_client.zscore(_ZSET_KEY, "orphan:member")
        assert score is None


@pytest.mark.integration
class TestRunScheduler:
    async def test_republishes_due_entries_to_pending_topic(self) -> None:
        event_id, endpoint_id = uuid.uuid4(), uuid.uuid4()
        msg = _msg(event_id, endpoint_id)
        producer = FakeProducer()
        iteration = 0
        cancelled: list[tuple[uuid.UUID, uuid.UUID, int]] = []

        async def mock_sleep(_: float) -> None:
            nonlocal iteration
            iteration += 1
            if iteration > 1:
                raise asyncio.CancelledError

        class OneIterScheduler:
            async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
                return [msg] if iteration == 1 else []

            async def cancel(self, eid: uuid.UUID, epid: uuid.UUID, attempt_number: int) -> None:
                cancelled.append((eid, epid, attempt_number))

        with patch("hookrelay.worker.scheduler.asyncio.sleep", side_effect=mock_sleep):
            with contextlib.suppress(asyncio.CancelledError):
                await run_scheduler(producer, OneIterScheduler())

        assert len(producer.published) == 1
        topic, published_msg = producer.published[0]
        assert topic == settings.kafka_topic_pending
        assert published_msg == msg

        # cancel() must be called after each successful publish, with the attempt from the message.
        assert cancelled == [(event_id, endpoint_id, 1)]

    async def test_cancel_called_for_each_published_entry(self) -> None:
        pairs = [(uuid.uuid4(), uuid.uuid4()) for _ in range(3)]
        messages = [_msg(eid, epid) for eid, epid in pairs]
        producer = FakeProducer()
        iteration = 0
        cancelled: list[tuple[uuid.UUID, uuid.UUID, int]] = []

        async def mock_sleep(_: float) -> None:
            nonlocal iteration
            iteration += 1
            if iteration > 1:
                raise asyncio.CancelledError

        class MultiEntryScheduler:
            async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
                return messages if iteration == 1 else []

            async def cancel(self, eid: uuid.UUID, epid: uuid.UUID, attempt_number: int) -> None:
                cancelled.append((eid, epid, attempt_number))

        with patch("hookrelay.worker.scheduler.asyncio.sleep", side_effect=mock_sleep):
            with contextlib.suppress(asyncio.CancelledError):
                await run_scheduler(producer, MultiEntryScheduler())

        assert len(producer.published) == 3
        assert cancelled == [(eid, epid, 1) for eid, epid in pairs]

    async def test_failed_publish_does_not_cancel(self) -> None:
        """If Kafka publish raises, cancel() must not be called so the entry
        stays in Redis and is retried on the next scheduler poll."""
        msg = _msg(uuid.uuid4(), uuid.uuid4())
        producer = FakeProducer(should_fail=True)
        cancelled: list[tuple[uuid.UUID, uuid.UUID, int]] = []

        async def mock_sleep(_: float) -> None:
            pass

        class OneShotScheduler:
            async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
                return [msg]

            async def cancel(self, eid: uuid.UUID, epid: uuid.UUID, attempt_number: int) -> None:
                cancelled.append((eid, epid))

        with patch("hookrelay.worker.scheduler.asyncio.sleep", side_effect=mock_sleep):
            with pytest.raises(RuntimeError, match="Kafka unavailable"):
                await run_scheduler(producer, OneShotScheduler())

        assert cancelled == []

    async def test_malformed_uuid_entry_is_skipped_not_raised(self) -> None:
        """A message with an invalid UUID must be logged and skipped, not crash the scheduler."""
        bad_msg = {"event_id": "not-a-uuid", "endpoint_id": "also-bad", "attempt_number": 1}
        good_event_id, good_endpoint_id = uuid.uuid4(), uuid.uuid4()
        good_msg = _msg(good_event_id, good_endpoint_id)
        producer = FakeProducer()
        iteration = 0

        async def mock_sleep(_: float) -> None:
            nonlocal iteration
            iteration += 1
            if iteration > 1:
                raise asyncio.CancelledError

        class MixedScheduler:
            async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
                return [bad_msg, good_msg] if iteration == 1 else []

            async def cancel(self, eid: uuid.UUID, epid: uuid.UUID, attempt_number: int) -> None:
                pass

        with patch("hookrelay.worker.scheduler.asyncio.sleep", side_effect=mock_sleep):
            with contextlib.suppress(asyncio.CancelledError):
                await run_scheduler(producer, MixedScheduler())

        # Only the good message should have been published.
        assert len(producer.published) == 1
        _, published_msg = producer.published[0]
        assert published_msg == good_msg

    async def test_empty_poll_publishes_nothing(self) -> None:
        producer = FakeProducer()
        iteration = 0

        async def mock_sleep(_: float) -> None:
            nonlocal iteration
            iteration += 1
            if iteration > 1:
                raise asyncio.CancelledError

        class EmptyScheduler:
            async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
                return []

            async def cancel(self, eid: uuid.UUID, epid: uuid.UUID, attempt_number: int) -> None:
                pass

        with patch("hookrelay.worker.scheduler.asyncio.sleep", side_effect=mock_sleep):
            with contextlib.suppress(asyncio.CancelledError):
                await run_scheduler(producer, EmptyScheduler())

        assert producer.published == []
