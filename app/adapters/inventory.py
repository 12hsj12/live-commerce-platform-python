from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

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
        lock = self.database.sqlite_memory_lock
        if lock is not None:
            async with lock:
                return await self._reserve_sql(sku_id, quantity, operation_id)
        return await self._reserve_sql(sku_id, quantity, operation_id)

    async def _reserve_sql(self, sku_id: str, quantity: int, operation_id: str) -> str:
        try:
            async with self.database.sessions.begin() as session:
                operation = await session.get(InventoryOperation, operation_id)
                if operation is not None:
                    if operation.sku_id != sku_id or operation.quantity != quantity:
                        return "conflict"
                    return "duplicate"
                # A conditional UPDATE is atomic on both SQLite and MySQL. SELECT
                # FOR UPDATE is ignored by SQLite, which allowed concurrent local
                # requests to all observe and reserve the same available units.
                result = await session.execute(
                    update(Inventory)
                    .where(Inventory.sku_id == sku_id, Inventory.available >= quantity)
                    .values(available=Inventory.available - quantity,
                            reserved=Inventory.reserved + quantity,
                            version=Inventory.version + 1)
                    .execution_options(synchronize_session=False)
                )
                if result.rowcount == 0:
                    # A concurrent request with the same id may have committed
                    # its journal before this conditional inventory update ran.
                    operation = await session.get(InventoryOperation, operation_id)
                    if operation is not None:
                        if operation.sku_id != sku_id or operation.quantity != quantity:
                            return "conflict"
                        return "duplicate"
                    stock = await session.get(Inventory, sku_id)
                    return "missing" if stock is None else "insufficient"
                session.add(InventoryOperation(operation_id=operation_id, sku_id=sku_id,
                                                 quantity=quantity, state="reserved"))
                await session.flush()
                return "success"
        except IntegrityError:
            # Two identical request IDs may race past the initial read. The
            # unique journal key rolls back the losing stock update as part of
            # the same transaction; report the established idempotent outcome.
            async with self.database.sessions() as session:
                operation = await session.get(InventoryOperation, operation_id)
            if operation is None:
                raise
            if operation.sku_id != sku_id or operation.quantity != quantity:
                return "conflict"
            return "duplicate"

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
        lock = self.database.sqlite_memory_lock
        if lock is not None:
            async with lock:
                return await self._transition_sql(sku_id, operation_id, action=action)
        return await self._transition_sql(sku_id, operation_id, action=action)

    async def _transition_sql(self, sku_id: str, operation_id: str, *, action: str) -> bool:
        async with self.database.sessions.begin() as session:
            if action == "confirm":
                expected, target = "reserved", "confirmed"
            else:
                expected, target = "reserved", "released"
            changed = await session.execute(
                update(InventoryOperation)
                .where(InventoryOperation.operation_id == operation_id,
                       InventoryOperation.state == expected)
                .values(state=target)
                .execution_options(synchronize_session=False)
            )
            if changed.rowcount == 0:
                operation = await session.get(InventoryOperation, operation_id)
                if operation is None:
                    raise LookupError("reservation not found")
                if action == "confirm":
                    if operation.state == "confirmed":
                        return True
                    raise RuntimeError("released reservation cannot be confirmed")
                if operation.state == "released":
                    return True
                raise RuntimeError("confirmed reservation cannot be released")
            operation = await session.get(InventoryOperation, operation_id)
            if operation is None:
                raise LookupError("reservation not found")
            if action == "confirm":
                values = {"reserved": Inventory.reserved - operation.quantity,
                          "sold": Inventory.sold + operation.quantity}
            else:
                values = {"reserved": Inventory.reserved - operation.quantity,
                          "available": Inventory.available + operation.quantity}
            values["version"] = Inventory.version + 1
            stock_changed = await session.execute(
                update(Inventory).where(Inventory.sku_id == operation.sku_id).values(**values)
                .execution_options(synchronize_session=False)
            )
            if stock_changed.rowcount == 0:
                raise LookupError("sku not found")
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
        lock = self.database.sqlite_memory_lock
        if lock is not None:
            async with lock:
                await self._rollback_sql(sku_id, operation_id, action=action)
            return
        await self._rollback_sql(sku_id, operation_id, action=action)

    async def _rollback_sql(self, sku_id: str, operation_id: str, *, action: str) -> None:
        async with self.database.sessions.begin() as session:
            expected = "confirmed" if action == "confirm" else "released"
            changed = await session.execute(
                update(InventoryOperation)
                .where(InventoryOperation.operation_id == operation_id,
                       InventoryOperation.sku_id == sku_id,
                       InventoryOperation.state == expected)
                .values(state="reserved")
                .execution_options(synchronize_session=False)
            )
            if changed.rowcount == 0:
                operation = await session.get(InventoryOperation, operation_id)
                if operation is None or operation.sku_id != sku_id:
                    raise LookupError("reservation not found")
                raise RuntimeError("reservation is not in rollback state")
            # The inventory operation row changed earlier in this transaction;
            # read its quantity once for the corresponding stock update.
            operation = await session.get(InventoryOperation, operation_id)
            if operation is None:
                raise LookupError("reservation not found")
            if action == "confirm":
                values = {"sold": Inventory.sold - operation.quantity,
                          "reserved": Inventory.reserved + operation.quantity}
            else:
                values = {"available": Inventory.available - operation.quantity,
                          "reserved": Inventory.reserved + operation.quantity}
            values["version"] = Inventory.version + 1
            stock_changed = await session.execute(
                update(Inventory).where(Inventory.sku_id == sku_id).values(**values)
                .execution_options(synchronize_session=False)
            )
            if stock_changed.rowcount == 0:
                raise LookupError("sku not found")

    @staticmethod
    def _text(value) -> str:
        return value.decode() if isinstance(value, bytes) else str(value)
