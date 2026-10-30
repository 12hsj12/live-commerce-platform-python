from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import struct
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from starlette.requests import Request

from app import main as main_module
from app.agent.providers import Completion, ToolCall
from app.agent.service import AgentService
from app.config import Settings
from app.database import Database
from app.models import LegacyKVRecord, OutboxEvent
from app.services.commerce import OrderService
from app.storage.cpp_local_kv import (
    CorruptCppStorage,
    OP_BATCH,
    OP_DELETE,
    OP_SET,
    SNAPSHOT_MAGIC,
    WAL_MAGIC,
    decode_snapshot,
    recover_cpp_local_kv,
    replay_wal,
)
from app.security import verify_timestamped_hmac
from tools.import_cpp_local_kv import import_records


def _snapshot(records: dict[bytes, bytes]) -> bytes:
    body = struct.pack("<Q", len(records))
    for key, value in records.items():
        body += struct.pack("<II", len(key), len(value)) + key + value
    return struct.pack("<I", SNAPSHOT_MAGIC) + body + struct.pack("<I", zlib.crc32(body))


def _wal_record(operation: int, key: bytes, value: bytes) -> bytes:
    body = struct.pack("<IBBBII", WAL_MAGIC, 1, operation, 0, len(key), len(value)) + key + value
    return body + struct.pack("<I", zlib.crc32(body[4:]))


def _batch(mutations: list[tuple[int, bytes, bytes]]) -> bytes:
    value = struct.pack("<I", len(mutations))
    for operation, key, item_value in mutations:
        value += bytes([operation]) + struct.pack("<I", len(key)) + key
        value += struct.pack("<I", len(item_value)) + item_value
    return value


def test_cpp_snapshot_wal_batch_delete_and_torn_tail_recovery(tmp_path: Path) -> None:
    snapshot = tmp_path / "legacy.snapshot"
    wal = tmp_path / "legacy.wal"
    snapshot.write_bytes(_snapshot({b"inventory/stock/sku-1": b"100|0|0|0", b"old": b"x"}))
    wal_bytes = _wal_record(OP_SET, b"old", b"updated")
    wal_bytes += _wal_record(OP_BATCH, b"__batch__", _batch([
        (OP_SET, b"orders/order-1", b"PENDING"),
        (OP_DELETE, b"old", b""),
    ]))
    wal_bytes += b"\x32\x4b"  # C++ recovery ignores an incomplete final header.
    wal.write_bytes(wal_bytes)

    recovered, torn_at = recover_cpp_local_kv(snapshot, wal)
    assert recovered == {
        b"inventory/stock/sku-1": b"100|0|0|0",
        b"orders/order-1": b"PENDING",
    }
    assert torn_at == len(wal_bytes) - 2
    assert snapshot.read_bytes() == _snapshot({b"inventory/stock/sku-1": b"100|0|0|0", b"old": b"x"})
    assert wal.read_bytes().endswith(b"\x32\x4b")  # recovery is read-only


def test_cpp_snapshot_and_complete_bad_wal_crc_are_rejected() -> None:
    snapshot = bytearray(_snapshot({b"a": b"b"}))
    snapshot[-1] ^= 1
    with pytest.raises(CorruptCppStorage, match="checksum"):
        decode_snapshot(bytes(snapshot))

    wal = bytearray(_wal_record(OP_SET, b"a", b"b"))
    wal[-1] ^= 1
    with pytest.raises(CorruptCppStorage, match="checksum"):
        replay_wal(bytes(wal), {})


def test_cpp_kv_import_archives_raw_records_and_is_idempotent(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{(tmp_path / 'archive.sqlite').as_posix()}"
    records = {b"users/9": b"9\tuser\t1", b"binary\x00key": b"\xff\x00"}
    assert asyncio.run(import_records(records, url)) == 2
    assert asyncio.run(import_records(records, url)) == 0

    async def read_back() -> dict[bytes, bytes]:
        database = Database(Settings(database_url=url))
        try:
            async with database.sessions() as session:
                rows = (await session.execute(select(LegacyKVRecord))).scalars()
                return {row.key: row.value for row in rows}
        finally:
            await database.close()

    assert asyncio.run(read_back()) == records


def test_mysql_ordered_candidate_selection_requires_full_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.database as database_module

    class FakeEngine:
        def __init__(self, url: str):
            from sqlalchemy.engine import make_url

            self.url = make_url(url)
            self.disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    engines: list[FakeEngine] = []

    def make_engine(url, **_options):
        engine = FakeEngine(str(url))
        engines.append(engine)
        return engine

    monkeypatch.setattr(database_module, "create_async_engine", make_engine)
    monkeypatch.setattr(database_module, "async_sessionmaker", lambda *_args, **_kwargs: object())
    settings = Settings(
        mysql_mode=True,
        database_url="mysql+asyncmy://user:secret@stale-url:3306/commerce",
        mysql_hosts=("unavailable", "healthy"),
        mysql_port=3306,
        mysql_pool_size=3,
    )
    database = Database(settings)
    probes: list[tuple[str, int]] = []

    async def probe(engine):
        probes.append((engine.url.host, settings.mysql_pool_size))
        if engine.url.host == "unavailable":
            raise OSError("simulated unreachable host")

    monkeypatch.setattr(database, "_probe_mysql_engine", probe)
    try:
        asyncio.run(database._select_mysql_host())
        assert probes == [("unavailable", 3), ("healthy", 3)]
        assert database.active_mysql_host == "healthy"
        assert database._mysql_selected is True
        assert engines[0].disposed is True
    finally:
        asyncio.run(database.close())


def test_mysql_host_list_and_custom_url_port_are_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlalchemy.engine import make_url

    monkeypatch.setenv("ECOMMERCE_DATABASE_URL", "mysql+asyncmy://user:test%40password@first:4406/commerce")
    monkeypatch.setenv("ECOMMERCE_MYSQL_HOSTS", " first,second,first ")
    monkeypatch.delenv("ECOMMERCE_MYSQL_PORT", raising=False)
    monkeypatch.setenv("ECOMMERCE_MYSQL_POOL_SIZE", "3")
    settings = Settings.from_env()
    assert settings.mysql_hosts == ("first", "second")
    assert settings.mysql_port == 4406
    assert settings.mysql_pool_size == 3
    assert make_url(settings.database_url).host == "first"
    assert make_url(settings.database_url).password == "test@password"


class _StatusStore:
    def __init__(self):
        self.rows = {}

    async def get(self, request_id):
        return self.rows.get(request_id)

    async def put_if_absent(self, status):
        if status.request_id in self.rows:
            return False
        self.rows[status.request_id] = status
        return True

    async def put(self, status):
        self.rows[status.request_id] = status


class _Admission:
    async def take_redis_bucket(self, key, _rate, _burst):
        return not key.startswith("flashsale:{global}:entry_rate")


def test_waiting_room_http_handler_reproducibly_rejects_missing_order_id(monkeypatch) -> None:
    """Reproduce the C++ HTTP waiting-room message/FOQ1 validation mismatch."""
    runtime = SimpleNamespace(
        redis=object(),
        settings=Settings(waiting_room_enabled=True, waiting_capacity=16),
        queue_mode="redis",
        admission=_Admission(),
        ingress_statuses=_StatusStore(),
    )

    async def current_user(_request):
        return SimpleNamespace(id="user-1")

    monkeypatch.setattr(main_module, "_current_user", current_user)

    async def submit(request_id: str):
        body = (f"promotion_id=promo-1&sku_id=sku-1&quantity=1&"
                f"idempotency_key={request_id}").encode()
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        scope = {
            "type": "http", "http_version": "1.1", "method": "POST", "scheme": "http",
            "path": "/flash-sale/orders", "raw_path": b"/flash-sale/orders", "query_string": b"",
            "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
            "client": ("127.0.0.1", 1234), "server": ("test", 80),
            "app": SimpleNamespace(state=SimpleNamespace(runtime=runtime)),
        }
        return await main_module.submit_flash_sale(Request(scope, receive))

    async def run_repetitions():
        return [await submit("waiting-1"), await submit("waiting-2")]

    responses = asyncio.run(run_repetitions())
    assert [response.status_code for response in responses] == [503, 503]
    assert [json.loads(response.body)["error"] for response in responses] == [
        "invalid flash-sale order message", "invalid flash-sale order message"
    ]
    assert [runtime.ingress_statuses.rows[key].state for key in ("waiting-1", "waiting-2")] == [
        "REJECTED", "REJECTED"
    ]


def test_order_service_filters_by_owner() -> None:
    """The Agent's order tool can only retrieve records owned by its authenticated user."""
    from app.models import Order, OrderItem

    async def scenario():
        database = Database(Settings(database_url="sqlite+aiosqlite:///:memory:"))
        await database.initialize(seed=False)
        try:
            async with database.sessions.begin() as session:
                session.add(Order(id="order-owned", user_id="alice", idempotency_key="idem-owned",
                                  state="PENDING_PAYMENT"))
                session.add(OrderItem(order_id="order-owned", sku_id="sku-1", quantity=1, unit_price_cents=100))
            service = OrderService(database, None, None)
            assert await service.get_for_user("order-owned", "alice") is not None
            assert await service.get_for_user("order-owned", "bob") is None
            assert await service.list_for_user("bob") == []
        finally:
            await database.close()

    asyncio.run(scenario())


def test_local_inventory_concurrency_never_oversells() -> None:
    from app.adapters.inventory import InventoryGateway

    async def scenario():
        database = Database(Settings(database_url="sqlite+aiosqlite:///:memory:"))
        await database.initialize(seed=False)
        try:
            inventory = InventoryGateway(database)
            await inventory.initialize("concurrent-sku", 25)
            outcomes = await asyncio.gather(*(inventory.reserve("concurrent-sku", 1, f"op-{i}")
                                               for i in range(100)))
            stock = await inventory.query("concurrent-sku")
            assert outcomes.count("success") == 25
            assert outcomes.count("insufficient") == 75
            assert stock is not None and (stock.available, stock.reserved, stock.sold) == (0, 25, 0)
        finally:
            await database.close()

    asyncio.run(scenario())


def test_local_inventory_duplicate_reserve_and_confirm_are_idempotent() -> None:
    from app.adapters.inventory import InventoryGateway

    async def scenario():
        database = Database(Settings(database_url="sqlite+aiosqlite:///:memory:"))
        await database.initialize(seed=False)
        try:
            inventory = InventoryGateway(database)
            await inventory.initialize("duplicate-sku", 10)
            reserved = await asyncio.gather(*(inventory.reserve("duplicate-sku", 1, "same-operation")
                                                for _ in range(16)))
            assert reserved.count("success") == 1
            assert reserved.count("duplicate") == 15
            confirmed = await asyncio.gather(*(inventory.confirm("duplicate-sku", "same-operation")
                                                  for _ in range(16)))
            assert all(confirmed)
            stock = await inventory.query("duplicate-sku")
            assert stock is not None and (stock.available, stock.reserved, stock.sold) == (9, 0, 1)
        finally:
            await database.close()

    asyncio.run(scenario())


def test_local_event_broker_retries_then_dead_letters() -> None:
    from app.adapters.event_broker import LocalEventBroker

    broker = LocalEventBroker()
    broker.subscribe("orders", lambda _event: False, max_attempts=2)
    broker.publish("orders", "order-1", b"OrderCreated")
    with pytest.raises(RuntimeError, match="delivery failed"):
        broker.deliver("orders")
    assert broker.pending("orders") == 1
    with pytest.raises(RuntimeError, match="delivery failed"):
        broker.deliver("orders")
    assert broker.pending("orders") == 0
    assert broker.dead_letter_pending("orders") == 1


def test_outbox_is_marked_published_only_after_broker_ack(tmp_path: Path) -> None:
    from app.workers.service import outbox_loop

    class Broker:
        def __init__(self):
            self.sent = []

        async def send_and_wait(self, topic, *, key, value):
            self.sent.append((topic, key, value))

    async def scenario():
        database_url = f"sqlite+aiosqlite:///{(tmp_path / 'outbox-test.sqlite').as_posix()}"
        database = Database(Settings(database_url=database_url))
        await database.initialize(seed=False)
        broker = Broker()
        async with database.sessions.begin() as session:
            session.add(OutboxEvent(event_id="test-event", topic="order.events", event_key="order-1",
                payload=b"OrderCreated|order-1", published=False, attempts=0))
        runtime = SimpleNamespace(database=database, settings=Settings(), rocketmq_queue=None,
                                  kafka_producer=broker, local_event_broker=None, metrics={})
        task = asyncio.create_task(outbox_loop(runtime))
        try:
            for _ in range(100):
                async with database.sessions() as session:
                    event = await session.get(OutboxEvent, "test-event")
                if event is not None and event.published:
                    break
                await asyncio.sleep(0.01)
            assert broker.sent == [("order.events", b"order-1", b"OrderCreated|order-1")]
            assert event is not None and event.published and event.attempts == 1
            assert event.claimed_by is None and event.lease_until is None and event.last_error is None
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await database.close()

    asyncio.run(scenario())


def test_payment_callback_hmac_matches_signed_request_bytes() -> None:
    secret = "test-only-secret"
    timestamp = "1790000000"
    body = b'{"order_id":"order-1","amount_cents":19900}'
    signature = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    assert verify_timestamped_hmac(secret, timestamp, body, signature)
    assert not verify_timestamped_hmac(secret, timestamp, body + b" ", signature)
