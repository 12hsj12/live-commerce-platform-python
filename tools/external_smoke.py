from __future__ import annotations

import argparse
import asyncio
import json
import os
import uuid

from app.config import Settings
from app.database import Database


async def check_dependencies() -> dict[str, object]:
    settings = Settings.from_env()
    result: dict[str, object] = {}
    database = Database(settings)
    try:
        await database.ping()
        result["database"] = "reachable"
        if database.active_mysql_host:
            result["database_selected_host"] = database.active_mysql_host
    except Exception as error:
        result["database"] = f"failed: {type(error).__name__}"
    finally:
        await database.close()

    if settings.redis_url:
        client = None
        cleanup_keys: list[str] = []
        try:
            from redis.asyncio import from_url
            from app.adapters.queue import RedisOrderQueue
            from app.adapters.inventory import InventoryGateway
            from app.messages import FlashSaleMessage

            client = from_url(settings.redis_url, decode_responses=False)
            await client.ping()
            suffix = uuid.uuid4().hex[:16]
            inventory = InventoryGateway(database, client)
            sku_id = "smoke-" + suffix
            cleanup_keys.extend(inventory._keys(sku_id))
            await inventory.initialize(sku_id, 2)
            first = await inventory.reserve(sku_id, 1, "smoke-op-" + suffix)
            duplicate = await inventory.reserve(sku_id, 1, "smoke-op-" + suffix)
            await inventory.confirm(sku_id, "smoke-op-" + suffix)
            stock = await inventory.query(sku_id)
            queue_key = "smoke:flash-orders:" + suffix
            queue = RedisOrderQueue(client, queue_key, max_length=4, dlq_key=queue_key + ":dlq")
            message = FlashSaleMessage("smoke-campaign", "smoke-order-" + suffix,
                "smoke-request-" + suffix, "smoke-user", sku_id, 1)
            await queue.push(message)
            popped = await queue.pop()
            if first != "success" or duplicate != "duplicate" or popped != message or stock is None or \
                    (stock.available, stock.reserved, stock.sold) != (1, 0, 1):
                raise RuntimeError("Redis queue or inventory behavior mismatch")
            if cleanup_keys:
                await client.delete(*cleanup_keys)
            await client.aclose()
            client = None
            result["redis"] = "atomic inventory and FOQ1 queue round-trip passed"
        except Exception as error:
            result["redis"] = f"failed: {type(error).__name__}"
        finally:
            if client is not None:
                if cleanup_keys:
                    await client.delete(*cleanup_keys)
                await client.aclose()

    if settings.kafka_brokers:
        producer = None
        consumer = None
        try:
            from aiokafka import AIOKafkaProducer
            from aiokafka import AIOKafkaConsumer

            suffix = uuid.uuid4().hex[:12]
            topic = os.getenv("ECOMMERCE_KAFKA_SMOKE_TOPIC", "ecommerce-smoke")
            consumer = AIOKafkaConsumer(topic, bootstrap_servers=settings.kafka_brokers,
                group_id="ecommerce-smoke-" + suffix, auto_offset_reset="latest", enable_auto_commit=False)
            await consumer.start()
            assignment_deadline = asyncio.get_running_loop().time() + 5
            while not consumer.assignment() and asyncio.get_running_loop().time() < assignment_deadline:
                await asyncio.sleep(0.05)
            if not consumer.assignment():
                raise TimeoutError("Kafka smoke consumer received no partition assignment")
            producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers,
                request_timeout_ms=5000, acks="all", enable_idempotence=True)
            await producer.start()
            payload = b"ecommerce-smoke-" + suffix.encode()
            await producer.send_and_wait(topic, key=suffix.encode(), value=payload)
            received = False
            deadline = asyncio.get_running_loop().time() + 10
            while asyncio.get_running_loop().time() < deadline and not received:
                batches = await consumer.getmany(timeout_ms=500, max_records=10)
                received = any(record.value == payload for records in batches.values() for record in records)
            if not received:
                raise TimeoutError("Kafka smoke event was not received")
            await consumer.commit()
            result["kafka"] = "producer/consumer round-trip passed"
        except Exception as error:
            result["kafka"] = f"failed: {type(error).__name__}"
        finally:
            if producer is not None:
                await producer.stop()
            if consumer is not None:
                await consumer.stop()

    if settings.rocketmq_endpoint:
        try:
            from tools.rocketmq_smoke import run_smoke

            result["rocketmq"] = await run_smoke(settings.rocketmq_endpoint,
                settings.rocketmq_command_topic, settings.rocketmq_consumer_group)
        except Exception as error:
            result["rocketmq"] = f"failed: {type(error).__name__}"

    if os.getenv("DEMO_BASE_URL"):
        try:
            from urllib.request import urlopen

            with urlopen(os.environ["DEMO_BASE_URL"].rstrip("/") + "/ready", timeout=3) as response:
                result["http_ready"] = f"HTTP {response.status}"
        except Exception as error:
            result["http_ready"] = f"failed: {type(error).__name__}"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Check configured dependency reachability without printing credentials.")
    parser.parse_args()
    checks = asyncio.run(check_dependencies())
    print(json.dumps(checks, ensure_ascii=False, indent=2, sort_keys=True))
    if any(str(value).startswith("failed:") for value in checks.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
