"""Unit tests for worker_cli._run() — startup/shutdown lifecycle."""

from unittest.mock import AsyncMock, patch

import pytest


async def _noop_loop(*_args, **_kwargs) -> None:
    """Stand-in for run_delivery_loop / run_scheduler that exits immediately."""


class TestWorkerCliRun:
    async def test_starts_and_stops_all_components(self) -> None:
        from hookrelay.worker_cli import _run

        mock_consumer = AsyncMock()
        mock_producer = AsyncMock()
        mock_scheduler = AsyncMock()

        with (
            patch("hookrelay.worker_cli.HookRelayConsumer", return_value=mock_consumer),
            patch("hookrelay.worker_cli.HookRelayProducer", return_value=mock_producer),
            patch(
                "hookrelay.worker_cli.RedisRetryScheduler.from_url",
                return_value=mock_scheduler,
            ),
            patch("hookrelay.worker_cli.run_delivery_loop", side_effect=_noop_loop),
            patch("hookrelay.worker_cli.run_scheduler", side_effect=_noop_loop),
        ):
            await _run()

        mock_consumer.start.assert_called_once()
        mock_producer.start.assert_called_once()
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        mock_scheduler.close.assert_called_once()

    async def test_stops_all_components_if_producer_start_fails(self) -> None:
        """consumer.start() succeeds but producer.start() raises — both should still stop."""
        from hookrelay.worker_cli import _run

        mock_consumer = AsyncMock()
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = RuntimeError("broker unavailable")
        mock_scheduler = AsyncMock()

        with (
            patch("hookrelay.worker_cli.HookRelayConsumer", return_value=mock_consumer),
            patch("hookrelay.worker_cli.HookRelayProducer", return_value=mock_producer),
            patch(
                "hookrelay.worker_cli.RedisRetryScheduler.from_url",
                return_value=mock_scheduler,
            ),
        ):
            with pytest.raises(RuntimeError, match="broker unavailable"):
                await _run()

        mock_consumer.start.assert_called_once()
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        mock_scheduler.close.assert_called_once()

    async def test_stops_all_components_if_consumer_start_fails(self) -> None:
        """consumer.start() raises before producer.start() — cleanup still runs."""
        from hookrelay.worker_cli import _run

        mock_consumer = AsyncMock()
        mock_consumer.start.side_effect = RuntimeError("consumer failed")
        mock_producer = AsyncMock()
        mock_scheduler = AsyncMock()

        with (
            patch("hookrelay.worker_cli.HookRelayConsumer", return_value=mock_consumer),
            patch("hookrelay.worker_cli.HookRelayProducer", return_value=mock_producer),
            patch(
                "hookrelay.worker_cli.RedisRetryScheduler.from_url",
                return_value=mock_scheduler,
            ),
        ):
            with pytest.raises(RuntimeError, match="consumer failed"):
                await _run()

        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()
        mock_scheduler.close.assert_called_once()
