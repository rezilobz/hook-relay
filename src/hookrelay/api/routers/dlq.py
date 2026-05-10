"""Router: /dlq — dead letter queue inspection and replay."""

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from hookrelay.api.dependencies import DB, Producer, require_api_key
from hookrelay.api.schemas.dlq import DLQEntryResponse
from hookrelay.config import settings
from hookrelay.db.models import DLQEntry, Event

router = APIRouter(
    prefix="/dlq",
    tags=["dlq"],
    dependencies=[Depends(require_api_key)],
)


@router.get("", response_model=list[DLQEntryResponse])
async def list_dlq(
    db: DB,
    endpoint_id: uuid.UUID | None = None,
) -> list[DLQEntry]:
    stmt = select(DLQEntry).order_by(DLQEntry.created_at.desc())
    if endpoint_id is not None:
        stmt = stmt.where(DLQEntry.endpoint_id == endpoint_id)
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.post("/{dlq_id}/replay", response_model=DLQEntryResponse)
async def replay_dlq_entry(dlq_id: uuid.UUID, db: DB, producer: Producer) -> DLQEntry:
    entry = await db.get(DLQEntry, dlq_id, with_for_update=True)
    if entry is None:
        raise HTTPException(status_code=404, detail="DLQ entry not found")

    event = await db.get(Event, entry.event_id)
    if event is not None:
        event.status = "pending"

    await db.delete(entry)
    # Publish after DB work but before commit: if Kafka fails, the exception
    # propagates and the transaction rolls back, leaving the DLQ entry intact
    # for a safe retry with no duplicate Kafka messages.
    await producer.publish(settings.kafka_topic_pending, {"event_id": str(entry.event_id)})
    await db.commit()

    # expire_on_commit=False keeps entry attributes readable after deletion
    return entry


@router.delete("/{dlq_id}", status_code=204)
async def delete_dlq_entry(dlq_id: uuid.UUID, db: DB) -> None:
    entry = await db.get(DLQEntry, dlq_id, with_for_update=True)
    if entry is None:
        raise HTTPException(status_code=404, detail="DLQ entry not found")
    await db.delete(entry)
    await db.commit()
