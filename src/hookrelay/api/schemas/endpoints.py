"""Pydantic schemas for endpoint registration requests and responses."""

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_serializer


class EndpointCreate(BaseModel):
    url: AnyHttpUrl
    description: str | None = None
    event_types: Annotated[list[str], Field(min_length=1)]
    secret: Annotated[str, Field(min_length=1)]

    @field_serializer("url")
    def serialize_url(self, value: AnyHttpUrl) -> str:
        return str(value)


class EndpointUpdate(BaseModel):
    url: AnyHttpUrl | None = None
    description: str | None = None
    event_types: list[str] | None = None
    secret: str | None = None
    enabled: bool | None = None

    @field_serializer("url")
    def serialize_url(self, value: AnyHttpUrl | None) -> str | None:
        return str(value) if value is not None else None


class EndpointResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    url: str
    description: str | None
    event_types: list[str]
    secret: str
    enabled: bool
    created_at: datetime
    updated_at: datetime
