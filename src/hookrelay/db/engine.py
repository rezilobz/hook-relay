"""SQLAlchemy async engine creation."""

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hookrelay.config import settings

async_engine: AsyncEngine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    echo=False,
)
