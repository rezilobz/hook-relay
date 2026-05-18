"""Retry scheduler: polls Redis for due retries and republishes to the pending topic."""

import asyncio
from uuid import UUID

import structlog

from hookrelay.config import settings
from hookrelay.kafka.producer import HookRelayProducer
from hookrelay.worker.retry import RetryScheduler

log = structlog.get_logger()

_POLL_INTERVAL_SECONDS = 1.0


async def run_scheduler(producer: HookRelayProducer, scheduler: RetryScheduler) -> None:
    """Poll the Redis ZSET every second and republish due retries to the pending topic.

    Runs as a peer coroutine alongside run_delivery_loop inside an asyncio.TaskGroup.
    A crash here propagates out of the TaskGroup and takes down the worker process,
    triggering a clean restart — no stale state left behind.

    At-least-once semantics: poll_due() fetches entries without removing them.
    Each entry is removed from Redis (via cancel()) only after a successful Kafka
    publish. A crash between publish and cancel leaves the entry in Redis; it will
    be re-fetched on the next poll and produce a duplicate Kafka message, which the
    delivery worker's idempotency check suppresses.
    """
    log.info("scheduler.started", poll_interval=_POLL_INTERVAL_SECONDS)
    while True:
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        due = await scheduler.poll_due()
        for message in due:
            try:
                event_id = UUID(message["event_id"])
                endpoint_id = UUID(message["endpoint_id"])
            except (KeyError, ValueError):
                log.error("scheduler.malformed_entry", message=str(message))
                continue
            await producer.publish(settings.kafka_topic_pending, message)
            await scheduler.cancel(event_id, endpoint_id)
            log.debug(
                "scheduler.republished",
                event_id=str(event_id),
                endpoint_id=str(endpoint_id),
                attempt=message.get("attempt_number"),
            )
        if due:
            log.info("scheduler.poll", republished=len(due))
