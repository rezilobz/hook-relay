"""Pydantic schemas for DLQ entry responses."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class DLQEntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    reason: str
    created_at: datetime
