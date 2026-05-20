"""Router: /endpoints — CRUD for webhook endpoint registration."""

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, select

from hookrelay.api.dependencies import DB, require_api_key
from hookrelay.api.schemas.endpoints import EndpointCreate, EndpointResponse, EndpointUpdate
from hookrelay.db.models import DeliveryAttempt, DLQEntry, Endpoint

router = APIRouter(
    prefix="/endpoints",
    tags=["endpoints"],
    dependencies=[Depends(require_api_key)],
)


@router.post("", response_model=EndpointResponse, status_code=201)
async def create_endpoint(body: EndpointCreate, db: DB) -> Endpoint:
    endpoint = Endpoint(**body.model_dump(mode="json"))
    db.add(endpoint)
    await db.flush()
    await db.refresh(endpoint)
    await db.commit()
    return endpoint


@router.get("", response_model=list[EndpointResponse])
async def list_endpoints(db: DB) -> list[Endpoint]:
    result = await db.execute(select(Endpoint).order_by(Endpoint.created_at.desc()))
    return list(result.scalars().all())


@router.get("/{endpoint_id}", response_model=EndpointResponse)
async def get_endpoint(endpoint_id: uuid.UUID, db: DB) -> Endpoint:
    endpoint = await db.get(Endpoint, endpoint_id)
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    return endpoint


@router.patch("/{endpoint_id}", response_model=EndpointResponse)
async def update_endpoint(endpoint_id: uuid.UUID, body: EndpointUpdate, db: DB) -> Endpoint:
    result = await db.execute(select(Endpoint).where(Endpoint.id == endpoint_id).with_for_update())
    endpoint = result.scalar_one_or_none()
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    updates = body.model_dump(mode="json", exclude_unset=True)
    for field, value in updates.items():
        setattr(endpoint, field, value)
    await db.flush()
    await db.refresh(endpoint)
    await db.commit()
    return endpoint


@router.delete("/{endpoint_id}", status_code=204)
async def delete_endpoint(endpoint_id: uuid.UUID, db: DB) -> None:
    result = await db.execute(select(Endpoint).where(Endpoint.id == endpoint_id).with_for_update())
    endpoint = result.scalar_one_or_none()
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    await db.execute(delete(DeliveryAttempt).where(DeliveryAttempt.endpoint_id == endpoint_id))
    await db.execute(delete(DLQEntry).where(DLQEntry.endpoint_id == endpoint_id))
    await db.delete(endpoint)
    await db.commit()
