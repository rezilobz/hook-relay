"""Unit tests for the outbox relay — pure logic, no real DB or Kafka."""

import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hookrelay.worker.outbox import _relay_batch


def _make_entry(event_id: uuid.UUID | None = None) -> MagicMock:
    entry = MagicMock()
    entry.id = uuid.uuid4()
    entry.event_id = event_id or uuid.uuid4()
    return entry


def _make_session_factory(entries: list) -> callable:
    """Returns a factory whose sessions yield the given outbox entries on SELECT."""

    @asynccontextmanager
    async def _session():
        session = AsyncMock()

        select_result = MagicMock()
        select_result.scalars.return_value.all.return_value = entries
        delete_result = MagicMock()

        session.execute.side_effect = [select_result] + [delete_result] * len(entries)
        yield session

    return _session


@pytest.mark.asyncio
class TestRelayBatch:
    async def test_empty_outbox_publishes_nothing(self) -> None:
        producer = AsyncMock()
        factory = _make_session_factory([])

        with patch("hookrelay.worker.outbox.AsyncSessionLocal", factory):
            await _relay_batch(producer)

        producer.publish.assert_not_called()

    async def test_single_entry_publishes_to_pending_topic(self) -> None:
        from hookrelay.config import settings

        producer = AsyncMock()
        event_id = uuid.uuid4()
        factory = _make_session_factory([_make_entry(event_id)])

        with patch("hookrelay.worker.outbox.AsyncSessionLocal", factory):
            await _relay_batch(producer)

        producer.publish.assert_called_once_with(
            settings.kafka_topic_pending, {"event_id": str(event_id)}
        )

    async def test_multiple_entries_all_published(self) -> None:
        producer = AsyncMock()
        entries = [_make_entry() for _ in range(3)]
        factory = _make_session_factory(entries)

        with patch("hookrelay.worker.outbox.AsyncSessionLocal", factory):
            await _relay_batch(producer)

        assert producer.publish.call_count == 3
        published_ids = {call.args[1]["event_id"] for call in producer.publish.call_args_list}
        assert published_ids == {str(e.event_id) for e in entries}

    async def test_session_committed_after_batch(self) -> None:
        @asynccontextmanager
        async def _session():
            session = AsyncMock()
            select_result = MagicMock()
            select_result.scalars.return_value.all.return_value = [_make_entry()]
            session.execute.side_effect = [select_result, MagicMock()]
            yield session
            assert session.commit.called

        with patch("hookrelay.worker.outbox.AsyncSessionLocal", _session):
            await _relay_batch(AsyncMock())

    async def test_empty_outbox_does_not_commit(self) -> None:
        committed = False

        @asynccontextmanager
        async def _session():
            nonlocal committed
            session = AsyncMock()
            select_result = MagicMock()
            select_result.scalars.return_value.all.return_value = []
            session.execute.return_value = select_result

            original_commit = session.commit

            async def track_commit():
                nonlocal committed
                committed = True
                await original_commit()

            session.commit.side_effect = track_commit
            yield session

        with patch("hookrelay.worker.outbox.AsyncSessionLocal", _session):
            await _relay_batch(AsyncMock())

        assert not committed

    async def test_execute_called_once_per_delete(self) -> None:
        producer = AsyncMock()
        entries = [_make_entry(), _make_entry()]
        factory = _make_session_factory(entries)

        captured_session = None

        @asynccontextmanager
        async def _capturing_session():
            async with factory() as session:
                nonlocal captured_session
                captured_session = session
                yield session

        with patch("hookrelay.worker.outbox.AsyncSessionLocal", _capturing_session):
            await _relay_batch(producer)

        # 1 SELECT + 2 DELETEs
        assert captured_session.execute.call_count == 3
