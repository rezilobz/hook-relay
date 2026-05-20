"""Outbox relay: polls the outbox table and publishes pending events to Kafka."""

import asyncio

import structlog
from sqlalchemy import delete, select

from hookrelay.config import settings
from hookrelay.db.models import OutboxEntry
from hookrelay.db.session import AsyncSessionLocal
from hookrelay.kafka.producer import HookRelayProducer

log = structlog.get_logger()

_POLL_INTERVAL = 0.5  # seconds
_BATCH_SIZE = 100


async def run_outbox_relay(producer: HookRelayProducer) -> None:
    """Continuously drain the outbox table into Kafka.

    Uses SELECT ... FOR UPDATE SKIP LOCKED so multiple relay instances (e.g. during
    a rolling restart) never double-publish the same row.
    """
    log.info("outbox_relay.started", poll_interval=_POLL_INTERVAL, batch_size=_BATCH_SIZE)
    while True:
        await asyncio.sleep(_POLL_INTERVAL)
        await _relay_batch(producer)


async def _relay_batch(producer: HookRelayProducer) -> None:
    async with AsyncSessionLocal() as session:
        rows = (
            (
                await session.execute(
                    select(OutboxEntry)
                    .order_by(OutboxEntry.created_at)
                    .limit(_BATCH_SIZE)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )

        if not rows:
            return

        for entry in rows:
            await producer.publish(
                settings.kafka_topic_pending,
                {"event_id": str(entry.event_id)},
            )
            await session.execute(delete(OutboxEntry).where(OutboxEntry.id == entry.id))
            log.debug("outbox_relay.published", event_id=str(entry.event_id))

        await session.commit()
