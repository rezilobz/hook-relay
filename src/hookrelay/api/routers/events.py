"""Router: /events — event ingestion and delivery history."""

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hookrelay import metrics
from hookrelay.api.dependencies import DB, Producer, require_api_key
from hookrelay.api.schemas.events import DeliveryAttemptResponse, EventCreate, EventResponse
from hookrelay.config import settings
from hookrelay.db.models import DeliveryAttempt, DLQEntry, Event

router = APIRouter(
    prefix="/events",
    tags=["events"],
    dependencies=[Depends(require_api_key)],
)


@router.post("", response_model=EventResponse, status_code=201)
async def ingest_event(body: EventCreate, db: DB, producer: Producer) -> Event:
    # RETURNING only yields a row when the INSERT lands; conflict → None (no Kafka publish)
    result = await db.execute(
        pg_insert(Event)
        .values(
            event_type=body.event_type,
            idempotency_key=body.idempotency_key,
            payload=body.payload,
        )
        .on_conflict_do_nothing(index_elements=["idempotency_key"])
        .returning(Event.id)
    )
    inserted_id = result.scalar_one_or_none()

    event_result = await db.execute(
        select(Event).where(Event.idempotency_key == body.idempotency_key)
    )
    event = event_result.scalar_one()

    if inserted_id is not None:
        # Publish before commit: if Kafka fails, the exception propagates and
        # the transaction rolls back, so the event is never persisted without a
        # worker signal.
        await producer.publish(settings.kafka_topic_pending, {"event_id": str(event.id)})
        metrics.events_ingested_total.inc()

    await db.commit()
    return event


@router.get("/{event_id}", response_model=EventResponse)
async def get_event(event_id: uuid.UUID, db: DB) -> Event:
    event = await db.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    row = (
        await db.execute(
            text("""
                SELECT
                    COUNT(CASE WHEN has_success                    THEN 1 END) AS succeeded,
                    COUNT(CASE WHEN in_dlq AND NOT has_success     THEN 1 END) AS exhausted,
                    COUNT(CASE WHEN NOT has_success AND NOT in_dlq THEN 1 END) AS retrying
                FROM (
                    SELECT
                        endpoint_id,
                        coalesce(bool_or(status = 'success'), false) AS has_success,
                        bool_or(from_dlq)                            AS in_dlq
                    FROM (
                        SELECT endpoint_id, status, false AS from_dlq
                          FROM delivery_attempts WHERE event_id = :eid
                        UNION ALL
                        SELECT endpoint_id, NULL, true
                          FROM dlq_entries WHERE event_id = :eid
                    ) src
                    GROUP BY endpoint_id
                ) states
            """),
            {"eid": event_id},
        )
    ).one()

    succeeded, exhausted, retrying = int(row.succeeded), int(row.exhausted), int(row.retrying)

    if succeeded + exhausted + retrying == 0:
        event.status = "pending"
    elif retrying > 0:
        event.status = "pending" if succeeded == 0 else "partially_delivered"
    elif succeeded > 0 and exhausted == 0:
        event.status = "delivered"
    elif exhausted > 0 and succeeded == 0:
        event.status = "dlq"
    else:
        event.status = "partially_delivered"

    return event


@router.get("/{event_id}/deliveries", response_model=list[DeliveryAttemptResponse])
async def get_event_deliveries(event_id: uuid.UUID, db: DB) -> list[DeliveryAttempt]:
    event = await db.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    result = await db.execute(
        select(DeliveryAttempt)
        .where(DeliveryAttempt.event_id == event_id)
        .order_by(DeliveryAttempt.attempted_at)
    )
    return list(result.scalars().all())


@router.post("/{event_id}/retry", response_model=EventResponse)
async def retry_event(event_id: uuid.UUID, db: DB, producer: Producer) -> Event:
    event = await db.get(Event, event_id, with_for_update=True)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    await producer.publish(settings.kafka_topic_pending, {"event_id": str(event.id)})
    await db.execute(delete(DLQEntry).where(DLQEntry.event_id == event_id))
    event.status = "pending"
    await db.commit()
    await db.refresh(event)
    return event
