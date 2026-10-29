from __future__ import annotations

import asyncio
from collections import deque
from typing import Optional

from ..messages import FlashSaleMessage


class LocalOrderQueue:
    def __init__(self, capacity: int = 0):
        self._queue: asyncio.Queue[FlashSaleMessage] = asyncio.Queue(maxsize=max(0, capacity))

    async def push(self, message: FlashSaleMessage) -> None:
        await self._queue.put(message)

    async def pop(self) -> FlashSaleMessage:
        return await self._queue.get()

    def pending(self) -> int:
        return self._queue.qsize()


class LocalDeadLetterQueue:
    """Process-local dead-letter sink matching the C++ in-memory queue pair."""

    def __init__(self):
        self._messages: deque[bytes] = deque()

    async def dead_letter(self, payload: bytes) -> None:
        self._messages.append(bytes(payload))


class RedisOrderQueue:
    """Redis List adapter compatible with the C++ FOQ1 payload and list keys."""

    def __init__(self, redis, queue_key: str, max_length: int = 100000, dlq_key: Optional[str] = None):
        self.redis = redis
        self.queue_key = queue_key
        self.max_length = max_length
        self.dlq_key = dlq_key or queue_key.replace("flash_orders:{", "flash_orders_dlq:{")

    async def push(self, message: FlashSaleMessage) -> None:
        # Use one Lua operation for the same bounded LPUSH behavior as the C++ adapter.
        script = """
        if redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[1]) then return 0 end
        redis.call('LPUSH', KEYS[1], ARGV[2]); return 1
        """
        accepted = await self.redis.eval(script, 1, self.queue_key, self.max_length, message.encode())
        if int(accepted) != 1:
            raise OverflowError("Redis order queue is full")

    async def pop(self) -> FlashSaleMessage:
        item = await self.redis.brpop(self.queue_key, timeout=1)
        if item is None:
            raise asyncio.TimeoutError
        return FlashSaleMessage.decode(item[1])

    async def pending(self) -> int:
        return int(await self.redis.llen(self.queue_key))

    async def dead_letter(self, payload: bytes) -> None:
        await self.redis.lpush(self.dlq_key, payload)

    async def inspect_dlq(self, limit: int) -> list[bytes]:
        return list(await self.redis.lrange(self.dlq_key, 0, limit - 1))

    async def replay_dlq(self, target: "RedisOrderQueue", limit: int) -> int:
        script = """
        local length=redis.call('LLEN', KEYS[1]);
        if length==0 then return 0 end;
        local value=redis.call('LINDEX',KEYS[1],-1);
        local target_length=redis.call('LLEN',KEYS[2]);
        if target_length>=tonumber(ARGV[1]) then return -1 end;
        redis.call('RPOP',KEYS[1]); redis.call('LPUSH',KEYS[2],value); return 1;
        """
        moved = 0
        for _ in range(max(1, min(limit, 1000))):
            raw = await self.redis.lindex(self.dlq_key, -1)
            if raw is None:
                break
            FlashSaleMessage.decode(raw)  # Refuse to move a malformed oldest entry.
            result = int(await self.redis.eval(script, 2, self.dlq_key, target.queue_key, target.max_length))
            if result == -1:
                raise OverflowError("destination Redis order queue is full")
            if result == 0:
                break
            moved += 1
        return moved
