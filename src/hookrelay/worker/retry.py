"""Retry scheduling: exponential backoff and Redis ZSET-backed scheduler."""

import json
import random
import time
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

import redis.asyncio as aioredis

from hookrelay.config import settings

# Backoff parameters (seconds)
_BASE: float = 1.0
_CAP: float = 4 * 3600  # 4 hours

# Redis key names
_ZSET_KEY = "hookrelay:retries"
_DATA_KEY = "hookrelay:retry:data"


def backoff_seconds(attempt: int) -> float:
    """Compute delay for the nth retry attempt.

    Formula: min(cap, base * 2^n) + uniform_random(0, base)
    Starts at ~2s, doubles each attempt, caps at 4h.
    15 attempts span ~8.5 hours total.
    """
    exponential: float = _BASE * (2**attempt)
    return float(min(_CAP, exponential) + random.uniform(0, _BASE))  # noqa: S311


@runtime_checkable
class RetryScheduler(Protocol):
    async def schedule(
        self,
        event_id: UUID,
        endpoint_id: UUID,
        attempt_number: int,
        message: dict[str, Any],
    ) -> None:
        """Schedule a retry for event+endpoint after the appropriate backoff delay.

        message is the full Kafka payload that will be re-published when the
        entry becomes due. attempt_number is used to compute the backoff.
        """
        ...

    async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
        """Fetch (without removing) retry entries whose retry_after <= now.

        Returns a list of message dicts ready to republish to Kafka.
        The caller must call cancel() after each successful publish so the
        entry is removed from Redis. A crash before cancel() leaves the entry
        in Redis and it will be re-fetched on the next poll, producing a
        duplicate Kafka message that the delivery worker's idempotency check
        will suppress.
        """
        ...

    async def cancel(self, event_id: UUID, endpoint_id: UUID) -> None:
        """Remove a scheduled retry entry, e.g. when moving an attempt to DLQ."""
        ...


# Batch size for each poll_due call. Keeps each Lua script execution bounded:
# Lua 5.1's unpack() overflows at ~8000 args, and large batches block Redis
# (single-threaded) for other clients. Remaining due entries are picked up on
# the next scheduler tick.
_POLL_BATCH = 100

# Fetch up to _POLL_BATCH ZSET entries due by `now`, pulling their payloads
# from the data hash. Does NOT remove entries — the caller must call cancel()
# after a successful Kafka publish so that a crash between fetch and publish
# leaves entries in Redis and they are retried on the next poll (at-least-once).
# Members present in the ZSET but missing from the hash (should not happen;
# defensive) are silently skipped.
_POLL_SCRIPT = """
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #members == 0 then return {} end
local results = {}
for _, member in ipairs(members) do
    local data = redis.call('HGET', KEYS[2], member)
    if data then
        table.insert(results, data)
    end
end
return results
"""

# Atomically write the data hash entry then add to the ZSET so the scheduler
# can never observe a ZSET member whose payload is missing.
_SCHEDULE_SCRIPT = """
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return 1
"""


class RedisRetryScheduler:
    """Redis ZSET-backed retry scheduler.

    ZSET key ``hookrelay:retries`` — score = Unix timestamp when retry is due.
    Hash key ``hookrelay:retry:data`` — member → JSON-encoded Kafka message.
    Member key — ``{event_id}:{endpoint_id}`` (stable; enables O(1) cancel).

    Both schedule() and poll_due() use Lua scripts for atomicity so a crash
    mid-operation cannot leave the ZSET and hash in an inconsistent state.
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis
        self._poll_script = self._redis.register_script(_POLL_SCRIPT)
        self._schedule_script = self._redis.register_script(_SCHEDULE_SCRIPT)

    @classmethod
    async def from_url(cls, url: str | None = None) -> "RedisRetryScheduler":
        r = aioredis.from_url(url or settings.redis_url, decode_responses=True)
        return cls(r)

    async def close(self) -> None:
        await self._redis.aclose()

    def _member(self, event_id: UUID, endpoint_id: UUID) -> str:
        return f"{event_id}:{endpoint_id}"

    async def schedule(
        self,
        event_id: UUID,
        endpoint_id: UUID,
        attempt_number: int,
        message: dict[str, Any],
    ) -> None:
        retry_after = time.time() + backoff_seconds(attempt_number)
        member = self._member(event_id, endpoint_id)
        payload = json.dumps(message)
        await self._schedule_script(
            keys=[_DATA_KEY, _ZSET_KEY],
            args=[member, payload, retry_after],
        )

    async def poll_due(self, now: float | None = None) -> list[dict[str, Any]]:
        timestamp = now if now is not None else time.time()
        raw_entries: list[str] = await self._poll_script(
            keys=[_ZSET_KEY, _DATA_KEY],
            args=[timestamp, _POLL_BATCH],
        )
        return [json.loads(entry) for entry in raw_entries]

    async def cancel(self, event_id: UUID, endpoint_id: UUID) -> None:
        member = self._member(event_id, endpoint_id)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zrem(_ZSET_KEY, member)
            pipe.hdel(_DATA_KEY, member)
            await pipe.execute()
