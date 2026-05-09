from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from prometheus_client import make_asgi_app as make_prometheus_asgi_app

from hookrelay import __version__

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("startup", version=__version__)
    yield
    logger.info("shutdown")


def create_app() -> FastAPI:
    app = FastAPI(
        title="HookRelay",
        description="Self-hosted webhook delivery engine",
        version=__version__,
        lifespan=lifespan,
    )
    metrics_app = make_prometheus_asgi_app()
    app.mount("/metrics", metrics_app)
    return app


app = create_app()
