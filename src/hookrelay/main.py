from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from prometheus_client import make_asgi_app as make_prometheus_asgi_app

from hookrelay import __version__
from hookrelay.api.routers import dlq as dlq_router
from hookrelay.api.routers import endpoints as endpoints_router
from hookrelay.api.routers import events as events_router
from hookrelay.kafka.producer import HookRelayProducer

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("startup", version=__version__)
    producer = HookRelayProducer()
    await producer.start()
    app.state.producer = producer
    yield
    await producer.stop()
    logger.info("shutdown")


def create_app() -> FastAPI:
    app = FastAPI(
        title="HookRelay",
        description="Self-hosted webhook delivery engine",
        version=__version__,
        lifespan=lifespan,
    )
    app.include_router(endpoints_router.router)
    app.include_router(events_router.router)
    app.include_router(dlq_router.router)
    app.mount("/metrics", make_prometheus_asgi_app())
    return app


app = create_app()
