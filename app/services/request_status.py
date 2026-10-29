from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace

from sqlalchemy import select

from ..models import FlashSaleRequest


STATES = ("QUEUED", "PROCESSING", "SOLD_OUT", "REJECTED", "PENDING_PAYMENT",
          "PAID", "EXPIRED", "CANCELLED", "WAITING", "SUBMITTING")
TERMINAL = {"SOLD_OUT", "REJECTED", "PAID", "EXPIRED", "CANCELLED"}


@dataclass
class RequestStatus:
    request_id: str
    user_id: str
    order_id: str = ""
    state: str = "QUEUED"
    message: str = ""
    promotion_id: str = ""


class RequestStatusStore:
    TTL_SECONDS = 24 * 60 * 60

    def __init__(self, database, redis=None, durable: bool = False, *, write_durable: bool | None = None,
                 local_state: dict[str, RequestStatus] | None = None,
                 local_lock: asyncio.Lock | None = None):
        self.database = database
        self.redis = redis
        self.durable = durable
        self.sql_read_status = durable or (redis is None and database.settings.mysql_mode)
        self.sql_write_status = (durable if write_durable is None else write_durable) or (
            redis is None and database.settings.mysql_mode)
        self.local_mode = redis is None and not database.settings.mysql_mode
        self.local_state = local_state if local_state is not None else {}
        self.local_lock = local_lock if local_lock is not None else asyncio.Lock()

    @staticmethod
    def _serialize(value: RequestStatus) -> bytes:
        state_id = STATES.index(value.state)
        result = bytearray(f"{state_id};".encode())
        for field in (value.request_id, value.user_id, value.order_id, value.message, value.promotion_id):
            encoded = field.encode()
            result.extend(str(len(encoded)).encode() + b":" + encoded)
        return bytes(result)

    @staticmethod
    def _deserialize(raw: bytes | str) -> RequestStatus:
        value = raw.encode() if isinstance(raw, str) else raw
        semicolon = value.find(b";")
        if semicolon <= 0:
            raise ValueError("corrupt flash-sale request status")
        state_id = int(value[:semicolon])
        if not 0 <= state_id < len(STATES):
            raise ValueError("corrupt flash-sale request status")
        offset = semicolon + 1
        fields: list[str] = []
        while offset < len(value):
            colon = value.find(b":", offset)
            if colon < 0:
                raise ValueError("corrupt flash-sale request status")
            length = int(value[offset:colon])
            offset = colon + 1
            if length < 0 or offset + length > len(value):
                raise ValueError("corrupt flash-sale request status")
            fields.append(value[offset:offset + length].decode())
            offset += length
        if len(fields) not in (4, 5):
            raise ValueError("corrupt flash-sale request status")
        if len(fields) == 4:
            fields.append("")
        return RequestStatus(fields[0], fields[1], fields[2], STATES[state_id], fields[3], fields[4])

    async def get(self, request_id: str) -> RequestStatus | None:
        if self.local_mode:
            async with self.local_lock:
                value = self.local_state.get(request_id)
                return replace(value) if value is not None else None
        cached: RequestStatus | None = None
        if self.redis is not None:
            raw = await self.redis.get("flashsale:request:" + request_id)
            if raw is not None:
                cached = self._deserialize(raw)
                if not self.durable or cached.state not in TERMINAL:
                    return cached
        if self.sql_read_status:
            async with self.database.sessions() as session:
                row = await session.get(FlashSaleRequest, request_id)
                if row is not None:
                    result = RequestStatus(row.request_id, row.user_id, row.order_id, row.state, row.message,
                                           cached.promotion_id if cached else "")
                    return result
        return cached

    async def put_if_absent(self, value: RequestStatus, *, durable: bool = False) -> bool:
        if self.local_mode:
            async with self.local_lock:
                if value.request_id in self.local_state:
                    return False
                self.local_state[value.request_id] = replace(value)
                return True
        inserted = True
        if self.redis is not None:
            inserted = bool(await self.redis.set("flashsale:request:" + value.request_id,
                                                 self._serialize(value), nx=True, ex=self.TTL_SECONDS))
        if inserted and self.sql_write_status and (durable or self.redis is None):
            async with self.database.sessions.begin() as session:
                existing = await session.get(FlashSaleRequest, value.request_id, with_for_update=True)
                if existing is None:
                    session.add(FlashSaleRequest(request_id=value.request_id, user_id=value.user_id,
                        order_id=value.order_id, state=value.state, message=value.message[:500]))
                else:
                    inserted = False
        return inserted

    async def put_ingress(self, value: RequestStatus) -> None:
        """Gateway fast path: Redis-only when configured, as in C++ ReadThrough store."""
        if self.redis is not None:
            await self.redis.set("flashsale:request:" + value.request_id, self._serialize(value), ex=self.TTL_SECONDS)
        else:
            await self.put(value)

    async def put(self, value: RequestStatus) -> None:
        if self.local_mode:
            async with self.local_lock:
                self.local_state[value.request_id] = replace(value)
            return
        if self.redis is not None:
            await self.redis.set("flashsale:request:" + value.request_id, self._serialize(value), ex=self.TTL_SECONDS)
        if self.sql_write_status:
            async with self.database.sessions.begin() as session:
                row = await session.get(FlashSaleRequest, value.request_id, with_for_update=True)
                if row is None:
                    row = FlashSaleRequest(request_id=value.request_id, user_id=value.user_id,
                                           order_id=value.order_id, state=value.state, message=value.message[:500])
                    session.add(row)
                elif row.user_id == value.user_id:
                    row.order_id = value.order_id
                    row.message = value.message[:500]
                    if row.state not in TERMINAL:
                        row.state = value.state

    async def transition(self, request_id: str, expected: str, next_state: str) -> bool:
        if self.local_mode:
            async with self.local_lock:
                current = self.local_state.get(request_id)
                if current is None or current.state != expected:
                    return False
                current.state = next_state
                return True
        current = await self.get(request_id)
        if current is None or current.state != expected:
            return False
        previous = self._serialize(current)
        current.state = next_state
        following = self._serialize(current)
        updated = True
        if self.redis is not None:
            script = "if redis.call('GET',KEYS[1])==ARGV[1] then redis.call('SET',KEYS[1],ARGV[2],'EX',ARGV[3]); return 1 end; return 0"
            updated = int(await self.redis.eval(script, 1, "flashsale:request:" + request_id,
                                                 previous, following, self.TTL_SECONDS)) == 1
        # The C++ tiered store attempts the durable transition even when the
        # primary Redis CAS did not update; either store can establish success.
        durable_updated = False
        if self.sql_write_status:
            async with self.database.sessions.begin() as session:
                row = await session.get(FlashSaleRequest, request_id, with_for_update=True)
                if row is not None and row.state == expected:
                    row.state = next_state
                    durable_updated = True
        return updated or durable_updated
