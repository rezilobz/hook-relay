"""Kafka consumer lag reporter: updates kafka_consumer_lag Prometheus gauge."""

import asyncio

import structlog

from hookrelay import metrics
from hookrelay.kafka.consumer import HookRelayConsumer

log = structlog.get_logger()

_POLL_INTERVAL_SECONDS = 10.0


async def run_lag_reporter(consumer: HookRelayConsumer) -> None:
    """Poll consumer group lag every 10 s and update the Prometheus gauge.

    Runs as a peer coroutine in the worker's asyncio.TaskGroup. Errors are
    caught and logged — a transient Kafka broker issue must not take down the
    delivery loop.
    """
    while True:
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        try:
            lags = await consumer.fetch_lags()
            for tp, lag in lags.items():
                metrics.kafka_consumer_lag.labels(topic=tp.topic, partition=str(tp.partition)).set(
                    lag
                )
        except Exception as exc:
            log.warning("lag_reporter.error", error=str(exc))
