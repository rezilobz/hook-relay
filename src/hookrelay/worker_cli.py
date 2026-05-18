"""Worker entry point: delivery loop and retry scheduler as asyncio.TaskGroup peers."""

import asyncio

import structlog

from hookrelay.kafka.consumer import HookRelayConsumer
from hookrelay.kafka.producer import HookRelayProducer
from hookrelay.worker.delivery import run_delivery_loop
from hookrelay.worker.retry import RedisRetryScheduler
from hookrelay.worker.scheduler import run_scheduler

log = structlog.get_logger()


async def _run() -> None:
    consumer = HookRelayConsumer()
    producer = HookRelayProducer()
    scheduler = await RedisRetryScheduler.from_url()

    try:
        await consumer.start()
        await producer.start()
        log.info("worker.started")
        # TaskGroup semantics: if either task raises, the other is cancelled and
        # the exception propagates — worker process exits and is restarted cleanly.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(run_delivery_loop(consumer, producer, scheduler))
            tg.create_task(run_scheduler(producer, scheduler))
    finally:
        log.info("worker.stopping")
        await consumer.stop()
        await producer.stop()
        await scheduler.close()


def main() -> None:
    asyncio.run(_run())
