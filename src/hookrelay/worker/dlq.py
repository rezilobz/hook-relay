"""DLQ handler: moves exhausted deliveries to the dead letter queue."""

from uuid import UUID

import structlog

from hookrelay import metrics
from hookrelay.config import settings
from hookrelay.db.models import DLQEntry
from hookrelay.db.session import AsyncSessionLocal
from hookrelay.kafka.producer import HookRelayProducer
from hookrelay.worker.retry import RetryScheduler

log = structlog.get_logger()


async def move_to_dlq(
    producer: HookRelayProducer,
    scheduler: RetryScheduler,
    event_id: UUID,
    endpoint_id: UUID,
    reason: str,
) -> None:
    """Write a DLQEntry, cancel any pending Redis retry, and publish a DLQ Kafka message.

    Order matters:
    1. Cancel Redis retry first — prevents a due retry from racing the DLQ write.
    2. DB commit — DLQEntry is now durable.
    3. Kafka publish — informational notification for external consumers; failure here
       is acceptable since the DLQEntry in Postgres is the source of truth.
    """
    await scheduler.cancel(event_id, endpoint_id)

    async with AsyncSessionLocal() as session:
        session.add(DLQEntry(event_id=event_id, endpoint_id=endpoint_id, reason=reason))
        await session.commit()

    await producer.publish(
        settings.kafka_topic_dlq,
        {"event_id": str(event_id), "endpoint_id": str(endpoint_id)},
    )
    metrics.dlq_entries_total.inc()

    log.info("dlq.moved", event_id=str(event_id), endpoint_id=str(endpoint_id), reason=reason)
