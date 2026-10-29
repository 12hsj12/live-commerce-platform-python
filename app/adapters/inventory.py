from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update

from ..models import Inventory, InventoryOperation


@dataclass(frozen=True)
class Stock:
    available: int
    reserved: int
    sold: int


class InventoryGateway:
    _initialize = """
    if redis.call('EXISTS', KEYS[1])==1 then
      if redis.call('HGET', KEYS[1], 'total')~=ARGV[1] then return 0 end; return 2
    end
    redis.call('HSET', KEYS[1], 'total', ARGV[1], 'available', ARGV[1], 'reserved', '0', 'sold', '0'); return 1
    """
    _reserve = """
    local old=redis.call('HGET', KEYS[2], ARGV[2]);
    if old then if old==ARGV[1]..'|reserved' then return 2 else return -2 end end;
    local available=tonumber(redis.call('HGET', KEYS[1], 'available')) or -1;
    local quantity=tonumber(ARGV[1]); if available<quantity then return 0 end;
    redis.call('HINCRBY', KEYS[1], 'available', -quantity);
    redis.call('HINCRBY', KEYS[1], 'reserved', quantity);
    redis.call('HSET', KEYS[2], ARGV[2], ARGV[1]..'|reserved'); return 1
    """
    _confirm = """
    local record=redis.call('HGET', KEYS[2], ARGV[1]); if not record then return -1 end;
    local sep=string.find(record, '|'); local qty=tonumber(string.sub(record,1,sep-1));
    local state=string.sub(record,sep+1); if state=='confirmed' then return 1 end;
    if state~='reserved' then return -2 end;
    redis.call('HINCRBY',KEYS[1],'reserved',-qty); redis.call('HINCRBY',KEYS[1],'sold',qty);
    redis.call('HSET',KEYS[2],ARGV[1],qty..'|confirmed'); return 1
    """
    _release = """
    local record=redis.call('HGET', KEYS[2], ARGV[1]); if not record then return -1 end;
    local sep=string.find(record, '|'); local qty=tonumber(string.sub(record,1,sep-1));
    local state=string.sub(record,sep+1); if state=='released' then return 1 end;
    if state~='reserved' then return -2 end;
    redis.call('HINCRBY',KEYS[1],'reserved',-qty); redis.call('HINCRBY',KEYS[1],'available',qty);
    redis.call('HSET',KEYS[2],ARGV[1],qty..'|released'); return 1
    """
    _rollback_confirm = """
    local record=redis.call('HGET',KEYS[2],ARGV[1]); if not record then return -1 end;
    local sep=string.find(record,'|'); local qty=tonumber(string.sub(record,1,sep-1));
    if string.sub(record,sep+1)~='confirmed' then return -2 end;
    redis.call('HINCRBY',KEYS[1],'sold',-qty); redis.call('HINCRBY',KEYS[1],'reserved',qty);
    redis.call('HSET',KEYS[2],ARGV[1],qty..'|reserved'); return 1
    """
    _rollback_release = """
    local record=redis.call('HGET',KEYS[2],ARGV[1]); if not record then return -1 end;
    local sep=string.find(record,'|'); local qty=tonumber(string.sub(record,1,sep-1));
    if string.sub(record,sep+1)~='released' then return -2 end;
    redis.call('HINCRBY',KEYS[1],'available',-qty); redis.call('HINCRBY',KEYS[1],'reserved',qty);
    redis.call('HSET',KEYS[2],ARGV[1],qty..'|reserved'); return 1
    """

    def __init__(self, database, redis=None):
        self.database = database
        self.redis = redis

    @staticmethod
    def _keys(sku_id: str) -> tuple[str, str]:
        return f"flash_inventory:{{{sku_id}}}:stock", f"flash_inventory:{{{sku_id}}}:operations"

    async def initialize(self, sku_id: str, quantity: int) -> None:
        if self.redis is not None:
            stock_key, _ = self._keys(sku_id)
            result = int(await self.redis.eval(self._initialize, 1, stock_key, quantity))
            if result == 0:
                raise RuntimeError("Redis inventory total does not match initialization")
            return
        async with self.database.sessions.begin() as session:
            row = await session.get(Inventory, sku_id)
            if row is None:
                session.add(Inventory(sku_id=sku_id, available=quantity, reserved=0, sold=0, version=0))

    async def query(self, sku_id: str) -> Stock | None:
        if self.redis is not None:
            values = await self.redis.hmget(self._keys(sku_id)[0], "available", "reserved", "sold")
            if not values or values[0] is None:
                return None
            return Stock(*(int(self._text(value)) for value in values))
        async with self.database.sessions() as session:
            row = await session.get(Inventory, sku_id)
            return None if row is None else Stock(row.available, row.reserved, row.sold)

    async def reserve(self, sku_id: str, quantity: int, operation_id: str) -> str:
        if self.redis is not None:
            stock_key, operation_key = self._keys(sku_id)
            result = int(await self.redis.eval(self._reserve, 2, stock_key, operation_key, quantity, operation_id))
            return {1: "success", 2: "duplicate", 0: "insufficient", -2: "conflict"}.get(result, "missing")
        async with self.database.sessions.begin() as session:
            operation = await session.get(InventoryOperation, operation_id)
            if operation is not None:
                if operation.sku_id != sku_id or operation.quantity != quantity:
                    return "conflict"
                return "duplicate"
            stock = await session.get(Inventory, sku_id, with_for_update=True)
            if stock is None:
                return "missing"
            if stock.available < quantity:
                return "insufficient"
            stock.available -= quantity
            stock.reserved += quantity
            stock.version += 1
            session.add(InventoryOperation(operation_id=operation_id, sku_id=sku_id,
                                             quantity=quantity, state="reserved"))
            return "success"

    async def _transition(self, sku_id: str, operation_id: str, *, action: str) -> bool:
        if self.redis is not None:
            script = self._confirm if action == "confirm" else self._release
            stock_key, operation_key = self._keys(sku_id)
            result = int(await self.redis.eval(script, 2, stock_key, operation_key, operation_id))
            if result in (1,):
                return True
            if action == "confirm" and result == -2:
                raise RuntimeError("released reservation cannot be confirmed")
            if action == "release" and result == -2:
                raise RuntimeError("confirmed reservation cannot be released")
            raise LookupError("reservation not found")
        async with self.database.sessions.begin() as session:
            operation = await session.get(InventoryOperation, operation_id, with_for_update=True)
            if operation is None:
                raise LookupError("reservation not found")
            stock = await session.get(Inventory, operation.sku_id, with_for_update=True)
            if stock is None:
                raise LookupError("sku not found")
            if action == "confirm":
                if operation.state == "confirmed":
                    return True
                if operation.state != "reserved":
                    raise RuntimeError("released reservation cannot be confirmed")
                stock.reserved -= operation.quantity
                stock.sold += operation.quantity
                operation.state = "confirmed"
            else:
                if operation.state == "released":
                    return True
                if operation.state != "reserved":
                    raise RuntimeError("confirmed reservation cannot be released")
                stock.reserved -= operation.quantity
                stock.available += operation.quantity
                operation.state = "released"
            stock.version += 1
            return True

    async def confirm(self, sku_id: str, operation_id: str) -> bool:
        return await self._transition(sku_id, operation_id, action="confirm")

    async def release(self, sku_id: str, operation_id: str) -> bool:
        return await self._transition(sku_id, operation_id, action="release")

    async def rollback(self, sku_id: str, operation_id: str, *, action: str) -> None:
        if self.redis is not None:
            script = self._rollback_confirm if action == "confirm" else self._rollback_release
            stock_key, operation_key = self._keys(sku_id)
            result = int(await self.redis.eval(script, 2, stock_key, operation_key, operation_id))
            if result != 1:
                raise RuntimeError("inventory rollback did not apply")
            return
        async with self.database.sessions.begin() as session:
            operation = await session.get(InventoryOperation, operation_id, with_for_update=True)
            if operation is None or operation.sku_id != sku_id:
                raise LookupError("reservation not found")
            stock = await session.get(Inventory, sku_id, with_for_update=True)
            if stock is None:
                raise LookupError("sku not found")
            expected = "confirmed" if action == "confirm" else "released"
            if operation.state != expected:
                raise RuntimeError("reservation is not in rollback state")
            if action == "confirm":
                stock.sold -= operation.quantity
                stock.reserved += operation.quantity
            else:
                stock.available -= operation.quantity
                stock.reserved += operation.quantity
            operation.state = "reserved"
            stock.version += 1

    @staticmethod
    def _text(value) -> str:
        return value.decode() if isinstance(value, bytes) else str(value)
