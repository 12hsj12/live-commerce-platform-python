from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Awaitable, Callable


@dataclass(frozen=True)
class _Entry:
    value: str
    expires_at: float


class AsyncShardedCache:
    """Async sharded LRU with TTL and per-key singleflight loading."""

    def __init__(self, shard_count: int = 16, capacity_per_shard: int = 1024):
        count = max(1, shard_count)
        capacity = max(1, capacity_per_shard)
        self._entries = [OrderedDict() for _ in range(count)]
        self._inflight: list[dict[str, asyncio.Task[str]]] = [{} for _ in range(count)]
        self._locks = [asyncio.Lock() for _ in range(count)]
        self._capacity = capacity
        self._ttl_seconds = 30.0
        self.hits = 0
        self.misses = 0
        self.singleflight = 0

    def _index(self, key: str) -> int:
        return hash(key) % len(self._entries)

    async def get(self, key: str) -> str | None:
        index = self._index(key)
        async with self._locks[index]:
            entries = self._entries[index]
            entry = entries.get(key)
            if entry is None or (entry.expires_at and time.monotonic() >= entry.expires_at):
                if entry is not None:
                    entries.pop(key, None)
                self.misses += 1
                return None
            entries.move_to_end(key, last=False)
            self.hits += 1
            return entry.value

    async def set(self, key: str, value: str, ttl_seconds: float | None = None) -> None:
        if not key:
            raise ValueError("cache key must not be empty")
        index = self._index(key)
        ttl = self._ttl_seconds if ttl_seconds is None else ttl_seconds
        expiry = time.monotonic() + ttl if ttl > 0 else 0.0
        async with self._locks[index]:
            entries = self._entries[index]
            entries[key] = _Entry(value, expiry)
            entries.move_to_end(key, last=False)
            while len(entries) > self._capacity:
                entries.popitem(last=True)

    async def erase(self, key: str) -> None:
        index = self._index(key)
        async with self._locks[index]:
            self._entries[index].pop(key, None)

    async def _load_and_cache(self, index: int, key: str,
                              loader: Callable[[], Awaitable[str]]) -> str:
        try:
            value = await loader()
            await self.set(key, value)
            return value
        finally:
            async with self._locks[index]:
                self._inflight[index].pop(key, None)

    async def get_or_load(self, key: str, loader: Callable[[], Awaitable[str]]) -> str:
        cached = await self.get(key)
        if cached is not None:
            return cached
        index = self._index(key)
        async with self._locks[index]:
            task = self._inflight[index].get(key)
            if task is not None:
                self.singleflight += 1
            else:
                task = asyncio.create_task(self._load_and_cache(index, key, loader))
                self._inflight[index][key] = task
        return await asyncio.shield(task)
