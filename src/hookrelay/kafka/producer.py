"""Kafka producer: publishes events to the pending, retry, and DLQ topics."""

import json
from typing import Any

from aiokafka import AIOKafkaProducer

from hookrelay.config import settings


class HookRelayProducer:
    def __init__(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, message: dict[str, Any]) -> None:
        value = json.dumps(message).encode("utf-8")
        await self._producer.send_and_wait(topic, value=value)
