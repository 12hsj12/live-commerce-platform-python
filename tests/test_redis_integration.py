from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from app.adapters.inventory import InventoryGateway
from app.adapters.queue import RedisOrderQueue
from app.messages import FlashSaleMessage


@pytest.mark.skipif(not os.getenv("ECOMMERCE_REDIS_TEST_URL"),
                    reason="set ECOMMERCE_REDIS_TEST_URL to run Redis integration coverage")
def test_redis_atomic_inventory_and_foq1_queue() -> None:
    from redis.asyncio import from_url

    async def scenario() -> None:
        client = from_url(os.environ["ECOMMERCE_REDIS_TEST_URL"], decode_responses=False)
        suffix = uuid.uuid4().hex[:16]
        inventory = InventoryGateway(None, client)
        sku = "redis-test-" + suffix
        stock_key, operation_key = inventory._keys(sku)
        queue_key = "redis-test-queue:" + suffix
        try:
            await inventory.initialize(sku, 25)
            outcomes = await asyncio.gather(*(inventory.reserve(sku, 1, f"op-{suffix}-{i}")
                                               for i in range(100)))
            stock = await inventory.query(sku)
            assert outcomes.count("success") == 25
            assert outcomes.count("insufficient") == 75
            assert stock is not None and (stock.available, stock.reserved, stock.sold) == (0, 25, 0)

            queue = RedisOrderQueue(client, queue_key, max_length=4, dlq_key=queue_key + ":dlq")
            message = FlashSaleMessage("promo-1", "order-" + suffix, "request-" + suffix,
                                        "redis-user", sku, 1)
            await queue.push(message)
            assert await queue.pop() == message
        finally:
            await client.delete(stock_key, operation_key, queue_key, queue_key + ":dlq")
            await client.aclose()

    asyncio.run(scenario())
