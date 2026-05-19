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
    active_labels: set[tuple[str, str]] = set()

    while True:
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        try:
            lags = await consumer.fetch_lags()
            current_labels: set[tuple[str, str]] = set()
            for tp, lag in lags.items():
                label = (tp.topic, str(tp.partition))
                current_labels.add(label)
                metrics.kafka_consumer_lag.labels(topic=label[0], partition=label[1]).set(lag)

            # Remove gauges for partitions no longer assigned to this consumer
            # (e.g. after a rebalance) so stale values don't linger in Prometheus.
            for topic, partition in active_labels - current_labels:
                metrics.kafka_consumer_lag.remove(topic, partition)

            active_labels = current_labels
        except Exception as exc:
            log.warning("lag_reporter.error", error=str(exc))
