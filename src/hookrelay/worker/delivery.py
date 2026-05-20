"""Delivery worker: consumes events from Kafka and dispatches HTTPS POST to endpoints."""

import asyncio
import hashlib
import hmac
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import httpx
import structlog
from aiokafka import ConsumerRecord
from aiokafka.structs import TopicPartition
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hookrelay import metrics
from hookrelay.config import settings
from hookrelay.db.models import DeliveryAttempt, Endpoint, Event
from hookrelay.db.session import AsyncSessionLocal
from hookrelay.kafka.consumer import HookRelayConsumer
from hookrelay.kafka.producer import HookRelayProducer
from hookrelay.worker.dlq import move_to_dlq
from hookrelay.worker.retry import RetryScheduler

log = structlog.get_logger()


@dataclass
class PartitionWatermark:
    """Tracks in-flight offsets for one Kafka partition.

    Ensures we only commit offset N after all offsets <= N are processed,
    giving at-least-once delivery on worker restart.
    """

    in_flight: set[int] = field(default_factory=set)
    last_seen: int = -1
    last_committed: int = -1

    def start(self, offset: int) -> None:
        self.in_flight.add(offset)
        if offset > self.last_seen:
            self.last_seen = offset

    def done(self, offset: int) -> int | None:
        """Mark offset as processed. Returns the new watermark to commit, or None."""
        self.in_flight.discard(offset)
        candidate = self.last_seen if not self.in_flight else min(self.in_flight) - 1
        if candidate > self.last_committed:
            self.last_committed = candidate
            return candidate
        return None


def _hmac_signature(secret: str, payload_bytes: bytes) -> str:
    return hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()


class PoisonPillError(Exception):
    """Raised when a Kafka message is structurally invalid and will always fail.

    Unlike infrastructure failures, retrying a poison pill message on worker
    restart will produce the same error indefinitely. The correct response is
    to log it and commit the offset so the partition can advance.
    """


async def _deliver_to_endpoint(
    event: Event,
    endpoint: Endpoint,
    attempt_number: int,
    http_client: httpx.AsyncClient,
    producer: HookRelayProducer,
    scheduler: RetryScheduler,
) -> None:
    """Deliver one (event, endpoint) pair. Handles all errors internally."""
    bound = log.bind(
        event_id=str(event.id),
        endpoint_id=str(endpoint.id),
        attempt=attempt_number,
    )

    # Idempotency: skip if a successful attempt already exists.
    async with AsyncSessionLocal() as session:
        prior = (
            await session.execute(
                select(DeliveryAttempt)
                .where(
                    DeliveryAttempt.event_id == event.id,
                    DeliveryAttempt.endpoint_id == endpoint.id,
                    DeliveryAttempt.status == "success",
                )
                .limit(1)
            )
        ).scalar_one_or_none()
    if prior:
        bound.debug("delivery.skipped_idempotent")
        return

    # Build signed request.
    payload_bytes = json.dumps(event.payload, separators=(",", ":")).encode()
    sig = _hmac_signature(endpoint.secret, payload_bytes)

    # HTTP POST.
    t0 = time.monotonic()
    http_status: int | None = None
    response_body: str | None = None
    attempt_status = "failure"

    try:
        resp = await http_client.post(
            str(endpoint.url),
            content=payload_bytes,
            headers={
                "Content-Type": "application/json",
                settings.signature_header: f"sha256={sig}",
                "X-HookRelay-Event-ID": str(event.id),
                "X-HookRelay-Event-Type": event.event_type,
            },
            timeout=settings.delivery_timeout_seconds,
        )
        http_status = resp.status_code
        response_body = resp.text[:1024]
        attempt_status = "success" if resp.is_success else "failure"
    except httpx.TimeoutException:
        attempt_status = "timeout"
    except Exception as exc:
        bound.warning("delivery.http_error", error=str(exc))

    latency_ms = (time.monotonic() - t0) * 1000
    succeeded = attempt_status == "success"

    bound.info(
        "delivery.attempted",
        status=attempt_status,
        http_status=http_status,
        latency_ms=round(latency_ms, 1),
    )

    metrics.deliveries_attempted_total.labels(outcome=attempt_status).inc()
    metrics.delivery_latency_seconds.observe(latency_ms / 1000)
    if succeeded:
        metrics.retry_depth.observe(attempt_number)

    # Write DeliveryAttempt. ON CONFLICT DO NOTHING handles idempotent re-processing
    # (Kafka offset not committed before crash → same message redelivered on restart).
    async with AsyncSessionLocal() as session:
        await session.execute(
            pg_insert(DeliveryAttempt)
            .values(
                event_id=event.id,
                endpoint_id=endpoint.id,
                attempt_number=attempt_number,
                status=attempt_status,
                http_status=http_status,
                response_body=response_body,
                latency_ms=latency_ms,
            )
            .on_conflict_do_nothing()
        )
        await session.commit()

    if succeeded:
        return

    # Failure path: schedule retry or exhaust to DLQ.
    next_attempt = attempt_number + 1

    # 4xx errors (except 429 Too Many Requests) are permanent client errors —
    # retrying will never succeed, so skip straight to DLQ.
    is_permanent = http_status is not None and 400 <= http_status < 500 and http_status != 429

    if is_permanent or next_attempt >= settings.max_retry_attempts:
        reason = (
            f"Permanent error: HTTP {http_status}"
            if is_permanent
            else f"Exhausted {settings.max_retry_attempts} attempts. Last status: {attempt_status}"
        )
        await move_to_dlq(producer, scheduler, event.id, endpoint.id, reason, attempt_number)
    else:
        await scheduler.schedule(
            event.id,
            endpoint.id,
            next_attempt,
            {
                "event_id": str(event.id),
                "endpoint_id": str(endpoint.id),
                "attempt_number": next_attempt,
            },
        )
        bound.debug("delivery.retry_scheduled", next_attempt=next_attempt)


async def _process_record(
    message: dict[str, Any],
    http_client: httpx.AsyncClient,
    producer: HookRelayProducer,
    scheduler: RetryScheduler,
) -> None:
    """Resolve event + target endpoints from a Kafka message, then deliver."""
    try:
        event_id = UUID(message["event_id"])
        attempt_number = int(message.get("attempt_number", 0))
        endpoint_id_raw: str | None = message.get("endpoint_id")
        endpoint_id: UUID | None = UUID(endpoint_id_raw) if endpoint_id_raw else None
    except (KeyError, ValueError) as exc:
        raise PoisonPillError(f"malformed message: {exc}") from exc

    async with AsyncSessionLocal() as session:
        event = await session.get(Event, event_id)
        if event is None:
            # Orphan: ingestion transaction rolled back after Kafka publish.
            log.warning("delivery.orphan_event", event_id=str(event_id))
            return

        if endpoint_id is not None:
            # Targeted retry — deliver to one specific endpoint.
            endpoint = await session.get(Endpoint, endpoint_id)
            targets: list[Endpoint] = [endpoint] if (endpoint and endpoint.enabled) else []
        else:
            # Fresh fanout — deliver to all matching enabled endpoints.
            result = await session.execute(
                select(Endpoint).where(
                    Endpoint.enabled.is_(True),
                    Endpoint.event_types.contains([event.event_type]),
                )
            )
            targets = list(result.scalars().all())
        # event and endpoints are detached after session closes; attributes
        # remain readable because AsyncSessionLocal uses expire_on_commit=False.

    for endpoint in targets:
        await _deliver_to_endpoint(
            event,
            endpoint,
            attempt_number,
            http_client,
            producer,
            scheduler,
        )


async def run_delivery_loop(
    consumer: HookRelayConsumer,
    producer: HookRelayProducer,
    scheduler: RetryScheduler,
) -> None:
    """Consume from Kafka and fan-out deliveries under a bounded concurrency semaphore.

    Each consumed record is dispatched as an asyncio task. The semaphore caps
    concurrent in-flight HTTP calls. Per-partition watermarks ensure Kafka offsets
    are committed only after all preceding records in that partition are processed,
    giving at-least-once delivery on worker restart.
    """
    semaphore = asyncio.Semaphore(settings.worker_concurrency)
    watermarks: defaultdict[tuple[str, int], PartitionWatermark] = defaultdict(PartitionWatermark)

    # Infrastructure failures (DB down, Redis down, etc.) set this so the main
    # loop can exit cleanly after the current getmany batch drains. The worker
    # process then restarts and aiokafka replays from the last committed offset,
    # preserving at-least-once delivery.
    _fatal_exc: Exception | None = None

    async def _process_and_commit(record: ConsumerRecord) -> None:
        nonlocal _fatal_exc
        tp = (record.topic, record.partition)
        advance = False
        try:
            await _process_record(record.value, http_client, producer, scheduler)
            advance = True
        except PoisonPillError:
            # Message is structurally broken and will always fail. Commit and move on;
            # retrying on restart would loop forever on the same broken payload.
            log.error(
                "delivery.poison_pill",
                offset=record.offset,
                topic=record.topic,
                partition=record.partition,
            )
            advance = True
        except Exception as exc:
            # Infrastructure failure — do not commit. Signal the main loop to
            # exit so the process restarts and replays from the last committed offset.
            log.exception("delivery.unhandled_error", offset=record.offset, topic=record.topic)
            if _fatal_exc is None:
                _fatal_exc = exc
        finally:
            semaphore.release()
            if advance:
                commit_at = watermarks[tp].done(record.offset)
                if commit_at is not None:
                    try:
                        await consumer.commit_partition(
                            TopicPartition(record.topic, record.partition), commit_at
                        )
                    except Exception:
                        log.exception("delivery.commit_error", tp=tp, offset=commit_at)

    async with httpx.AsyncClient() as http_client:
        log.info("delivery_loop.started", concurrency=settings.worker_concurrency)
        # TaskGroup owns all per-record tasks. When the loop body raises (fatal
        # infrastructure error or external cancellation), the group cancels every
        # in-flight task and awaits them before __aexit__ returns — so the HTTP
        # client stays open until all tasks have wound down cleanly.
        async with asyncio.TaskGroup() as tg:
            while True:
                if _fatal_exc is not None:
                    raise _fatal_exc
                batch = await consumer.getmany(
                    timeout_ms=500, max_records=settings.worker_concurrency * 4
                )
                for tp_obj, records in batch.items():
                    tp_key = (tp_obj.topic, tp_obj.partition)
                    for record in records:
                        await semaphore.acquire()
                        watermarks[tp_key].start(record.offset)
                        tg.create_task(_process_and_commit(record))
