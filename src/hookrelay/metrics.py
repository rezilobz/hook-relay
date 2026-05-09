"""Prometheus metrics definitions for HookRelay."""

from prometheus_client import Counter, Gauge, Histogram

events_ingested_total = Counter(
    "hookrelay_events_ingested_total",
    "Total events accepted by the ingestion API",
)

deliveries_attempted_total = Counter(
    "hookrelay_deliveries_attempted_total",
    "Total delivery attempts",
    labelnames=["outcome"],
)

delivery_latency_seconds = Histogram(
    "hookrelay_delivery_latency_seconds",
    "HTTP delivery round-trip latency in seconds",
)

retry_depth = Histogram(
    "hookrelay_retry_depth",
    "Attempt number at time of successful delivery",
    buckets=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
)

dlq_entries_total = Gauge(
    "hookrelay_dlq_entries_total",
    "Current number of entries in the dead letter queue",
)

kafka_consumer_lag = Gauge(
    "hookrelay_kafka_consumer_lag",
    "Kafka consumer group lag per partition",
    labelnames=["topic", "partition"],
)
