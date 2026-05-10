"""Pydantic schemas for event ingestion requests and delivery history responses."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class EventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_type: str
    idempotency_key: str
    payload: dict[str, Any]
    status: str
    created_at: datetime


class DeliveryAttemptResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    attempt_number: int
    status: str
    http_status: int | None
    response_body: str | None
    latency_ms: float
    attempted_at: datetime
