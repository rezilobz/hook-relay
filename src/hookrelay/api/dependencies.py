"""Shared FastAPI dependencies (auth, DB session, settings injection)."""

from typing import Annotated

from fastapi import Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.config import settings
from hookrelay.db.session import get_db as _get_db

get_db = _get_db


async def require_api_key(
    x_api_key: Annotated[str, Header(alias=settings.api_key_header)],
) -> None:
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


DB = Annotated[AsyncSession, Depends(get_db)]
