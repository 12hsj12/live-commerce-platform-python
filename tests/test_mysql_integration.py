from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from sqlalchemy.engine import make_url

from app.adapters.inventory import InventoryGateway
from app.config import Settings
from app.database import Database
from app.messages import FlashSaleMessage
from app.services.commerce import OrderService
from app.services.request_status import RequestStatusStore


@pytest.mark.skipif(not os.getenv("ECOMMERCE_MYSQL_TEST_URL"),
                    reason="set ECOMMERCE_MYSQL_TEST_URL and load deploy/mysql/schema.sql")
def test_mysql_inventory_order_and_owner_scoped_read() -> None:
    async def scenario() -> None:
        url = os.environ["ECOMMERCE_MYSQL_TEST_URL"]
        parsed = make_url(url)
        hosts = tuple(item.strip() for item in os.getenv("ECOMMERCE_MYSQL_TEST_HOSTS", parsed.host or "").split(",")
                      if item.strip())
        settings = Settings(mysql_mode=True, database_url=url, mysql_hosts=hosts,
            mysql_port=int(os.getenv("ECOMMERCE_MYSQL_TEST_PORT", parsed.port or 3306)),
            mysql_pool_size=int(os.getenv("ECOMMERCE_MYSQL_TEST_POOL_SIZE", "4")))
        database = Database(settings)
        suffix = uuid.uuid4().hex[:16]
        sku = "py-mysql-test-" + suffix
        order_id = "py-mysql-order-" + suffix
        request_id = "py-mysql-request-" + suffix
        user_id = "py-mysql-user-" + suffix
        try:
            await database.initialize(seed=False)
            inventory = InventoryGateway(database)
            await inventory.initialize(sku, 2)
            assert await inventory.reserve(sku, 1, order_id) == "success"
            orders = OrderService(database, inventory, RequestStatusStore(database, None, durable=True))
            message = FlashSaleMessage("py-mysql-campaign", order_id, request_id, user_id, sku, 1)
            created = await orders.persist_queued_order(message)
            assert created.id == order_id and created.state == "PENDING_PAYMENT"
            assert await orders.get_for_user(order_id, user_id) is not None
            assert await orders.get_for_user(order_id, "different-user") is None
            stock = await inventory.query(sku)
            assert stock is not None and (stock.available, stock.reserved, stock.sold) == (1, 1, 0)
        finally:
            await database.close()

    asyncio.run(scenario())
