"""DLQ handler: moves exhausted deliveries to the dead letter queue."""

from uuid import UUID

import structlog
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
    1. DB commit first — DLQEntry is durable before any side effects. ON CONFLICT DO NOTHING
       makes this idempotent: if a racing retry re-delivers and calls move_to_dlq again,
       the second write is a safe no-op.
    2. Cancel Redis retry — best-effort; if this fails the entry may fire again, but the
       duplicate delivery attempt will hit the idempotent DLQ write and stop there.
    3. Kafka publish — informational notification; failure here is acceptable since the
       DLQEntry in Postgres is the source of truth.
    """
    async with AsyncSessionLocal() as session:
        await session.execute(
            pg_insert(DLQEntry)
            .values(event_id=event_id, endpoint_id=endpoint_id, reason=reason)
            .on_conflict_do_nothing(constraint="uq_dlq_entry")
        )
        await session.commit()

    try:
        await scheduler.cancel(event_id, endpoint_id)
    except Exception:
        log.warning(
            "dlq.cancel_retry_failed",
            event_id=str(event_id),
            endpoint_id=str(endpoint_id),
        )

    await producer.publish(
        settings.kafka_topic_dlq,
        {"event_id": str(event_id), "endpoint_id": str(endpoint_id)},
    )
    metrics.dlq_entries_total.inc()

    log.info("dlq.moved", event_id=str(event_id), endpoint_id=str(endpoint_id), reason=reason)
