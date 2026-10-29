from __future__ import annotations

import asyncio
import sqlite3
import time

from app.config import Settings
from app.database import Database
from app.messages import FlashSaleMessage
from app.models import Inventory
from app.adapters.inventory import InventoryGateway


def test_foq1_message_round_trip_and_rejects_trailing_bytes() -> None:
    message = FlashSaleMessage("promo-1", "order-1", "request-1", "user-1", "sku-1", 1)
    assert FlashSaleMessage.decode(message.encode()) == message
    try:
        FlashSaleMessage.decode(message.encode() + b"x")
    except ValueError:
        pass
    else:
        raise AssertionError("trailing bytes must be rejected")


def test_inventory_reservation_is_idempotent_and_terminal() -> None:
    async def scenario() -> None:
        database_url = "sqlite+aiosqlite:///:memory:"
        database = Database(Settings(database_url=database_url))
        await database.initialize(seed=False)
        inventory = InventoryGateway(database)
        await inventory.initialize("sku-test", 2)
        assert await inventory.reserve("sku-test", 1, "op-1") == "success"
        assert await inventory.reserve("sku-test", 1, "op-1") == "duplicate"
        assert await inventory.reserve("sku-test", 2, "op-2") == "insufficient"
        await inventory.confirm("sku-test", "op-1")
        assert await inventory.confirm("sku-test", "op-1") is True
        try:
            await inventory.release("sku-test", "op-1")
        except RuntimeError:
            pass
        else:
            raise AssertionError("confirmed inventory cannot be released")
        stock = await inventory.query("sku-test")
        assert stock is not None
        assert (stock.available, stock.reserved, stock.sold) == (1, 0, 1)
        await database.close()

    asyncio.run(scenario())


def test_sqlite_snapshot_contains_a_consistent_database_copy(tmp_path) -> None:
    async def scenario() -> tuple[str, str]:
        database_path = tmp_path / "snapshot-test.db"
        snapshot_path = tmp_path / "snapshot-test.snapshot.sqlite"
        settings = Settings(database_url=f"sqlite+aiosqlite:///{database_path.as_posix()}",
                            storage_snapshot_path=str(snapshot_path))
        database = Database(settings)
        await database.initialize(seed=False)
        async with database.sessions.begin() as session:
            session.add(Inventory(sku_id="snapshot-sku", available=3, reserved=1, sold=2, version=6))
        await database.close()
        result = await asyncio.to_thread(database.snapshot)
        assert result == snapshot_path.resolve()
        return str(snapshot_path), str(database_path)

    snapshot_path, _database_path = asyncio.run(scenario())
    with sqlite3.connect(snapshot_path) as connection:
        row = connection.execute(
            "SELECT available, reserved, sold FROM inventory WHERE sku_id = ?", ("snapshot-sku",)
        ).fetchone()
    assert row == (3, 1, 2)


def test_http_flash_sale_to_paid_order(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "api-test.db"
    snapshot_path = tmp_path / "api-test.snapshot.sqlite"
    monkeypatch.setenv("ECOMMERCE_DATABASE_URL", f"sqlite+aiosqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("ECOMMERCE_STORAGE_SNAPSHOT", str(snapshot_path))
    monkeypatch.setenv("ECOMMERCE_INVENTORY_MODE", "local")
    monkeypatch.setenv("ECOMMERCE_FLASH_SALE_QUEUE", "local")
    monkeypatch.setenv("ECOMMERCE_DEMO_STOCK", "2")
    monkeypatch.setenv("ECOMMERCE_DEMO_CAMPAIGN_LIMIT", "2")
    monkeypatch.setenv("ECOMMERCE_FLASH_SALE_WAITING_ROOM", "false")
    for key in ("ECOMMERCE_MYSQL_HOST", "ECOMMERCE_MYSQL_HOSTS", "ECOMMERCE_REDIS_URL",
                "ECOMMERCE_REDIS_HOST", "ECOMMERCE_KAFKA_BROKERS", "ECOMMERCE_ROCKETMQ_ENDPOINT"):
        monkeypatch.delenv(key, raising=False)

    from fastapi.testclient import TestClient
    from app.main import app

    suffix = str(time.time_ns())
    with TestClient(app) as client:
        registered = client.post("/users/register", data={"username": "test_" + suffix,
                                                          "password": "Test-Password-2026"})
        assert registered.status_code == 201
        login = client.post("/users/login", data={"username": "test_" + suffix,
                                                  "password": "Test-Password-2026"})
        assert login.status_code == 200
        headers = {"Authorization": "Bearer " + login.json()["token"]}
        request_id = "request-" + suffix
        submitted = client.post("/flash-sale/orders", headers=headers, data={
            "promotion_id": "promo-1", "sku_id": "sku-1", "quantity": "1",
            "idempotency_key": request_id,
        })
        assert submitted.status_code == 202
        deadline = time.monotonic() + 5
        status = {}
        while time.monotonic() < deadline:
            response = client.get("/flash-sale/requests", headers=headers, params={"request_id": request_id})
            assert response.status_code == 200
            status = response.json()
            if status.get("state") == "PENDING_PAYMENT":
                break
            time.sleep(0.02)
        assert status.get("state") == "PENDING_PAYMENT"
        paid = client.post("/orders/pay", headers=headers, data={"order_id": status["order_id"]})
        assert paid.status_code == 200
        assert paid.json()["state"] == "PAID"
        stock = client.get("/stock/sku-1").json()
        assert (stock["available"], stock["reserved"], stock["sold"]) == (99, 0, 1)
