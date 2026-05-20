"""Worker entry point: delivery loop and retry scheduler as asyncio.TaskGroup peers."""

import asyncio

import structlog
from prometheus_client import start_http_server as start_metrics_server
from sqlalchemy import func, select

from hookrelay import metrics
from hookrelay.db.models import DLQEntry
from hookrelay.db.session import AsyncSessionLocal
from hookrelay.kafka.consumer import HookRelayConsumer
from hookrelay.kafka.producer import HookRelayProducer
from hookrelay.worker.delivery import run_delivery_loop
from hookrelay.worker.lag_reporter import run_lag_reporter
from hookrelay.worker.outbox import run_outbox_relay
from hookrelay.worker.retry import RedisRetryScheduler
from hookrelay.worker.scheduler import run_scheduler

log = structlog.get_logger()


_METRICS_PORT = 8001


async def _run() -> None:
    consumer = HookRelayConsumer()
    producer = HookRelayProducer()
    scheduler = await RedisRetryScheduler.from_url()

    # Expose worker metrics on a dedicated port so Prometheus can scrape them
    # independently of the API's /metrics endpoint.
    start_metrics_server(_METRICS_PORT)
    log.info("worker.metrics_server_started", port=_METRICS_PORT)

    # Sync DLQ gauge from DB so it reflects real depth after a worker restart.
    # Without this, the gauge resets to 0 on restart and under-reports until new
    # entries are added or removed.
    async with AsyncSessionLocal() as session:
        dlq_count = (await session.execute(select(func.count()).select_from(DLQEntry))).scalar_one()
    metrics.dlq_entries_total.set(dlq_count)
    log.info("worker.dlq_gauge_initialized", count=dlq_count)

    try:
        await consumer.start()
        await producer.start()
        log.info("worker.started")
        # TaskGroup semantics: if any task raises, all others are cancelled and
        # the exception propagates — worker process exits and is restarted cleanly.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(run_delivery_loop(consumer, producer, scheduler))
            tg.create_task(run_scheduler(producer, scheduler))
            tg.create_task(run_outbox_relay(producer))
            tg.create_task(run_lag_reporter(consumer))
    finally:
        log.info("worker.stopping")
        await consumer.stop()
        await producer.stop()
        await scheduler.close()


def main() -> None:
    asyncio.run(_run())
