"""Kafka consumer: reads from the pending topic for the delivery worker pool."""

import json
from collections.abc import AsyncIterator
from typing import Any, cast

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from hookrelay.config import settings


class HookRelayConsumer:
    """aiokafka consumer with manual offset management for at-least-once delivery.

    Offsets are committed per-partition via commit_partition() only after the
    delivery worker writes a DeliveryAttempt to PostgreSQL — never before. This
    ensures that a worker crash causes re-processing from the last committed
    offset, and the idempotency check in the worker prevents double delivery.
    """

    def __init__(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic_pending,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        return cast(AsyncIterator[ConsumerRecord], self._consumer.__aiter__())

    async def getmany(
        self,
        timeout_ms: int = 1000,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[ConsumerRecord]]:
        """Fetch a batch of records keyed by TopicPartition.

        Returns an empty dict when no records are available within timeout_ms.
        Mirrors the aiokafka API so callers can do per-partition watermark tracking.
        """
        return cast(
            dict[TopicPartition, list[ConsumerRecord]],
            await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records),
        )

    async def commit_partition(self, tp: TopicPartition, offset: int) -> None:
        """Commit offset+1 for a single partition.

        Commits the *next* offset to fetch (Kafka convention), so passing the
        offset of the last successfully processed record is always correct.
        Only called by the delivery worker after the attempt is durably written.
        """
        await self._consumer.commit({tp: OffsetAndMetadata(offset + 1, "")})

    def assignment(self) -> frozenset[TopicPartition]:
        """Return the set of partitions currently assigned to this consumer."""
        return cast(frozenset[TopicPartition], self._consumer.assignment())

    async def seek_to_beginning(self, *partitions: TopicPartition) -> None:
        await self._consumer.seek_to_beginning(*partitions)

    def deserialize(self, raw: bytes) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(raw.decode("utf-8")))
