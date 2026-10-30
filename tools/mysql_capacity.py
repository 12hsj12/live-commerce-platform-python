from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid

from app.adapters.inventory import InventoryGateway
from app.config import Settings
from app.database import Database
from app.messages import FlashSaleMessage
from app.services.commerce import OrderService
from app.services.request_status import RequestStatusStore


def _positive_env(name: str, default: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw else default
    except ValueError:
        return default
    return value if 1 <= value <= maximum else default


async def run_capacity(requests: int, concurrency: int) -> dict[str, object]:
    settings = Settings.from_env()
    if not settings.mysql_mode:
        raise RuntimeError("MySQL capacity check requires ECOMMERCE_DATABASE_URL or ECOMMERCE_MYSQL_HOST(S)")
    database = Database(settings)
    latencies: list[float] = []
    failure_types: dict[str, int] = {}
    success = 0
    run_id = uuid.uuid4().hex[:16]
    try:
        await database.initialize(seed=False)
        inventory = InventoryGateway(database)
        await inventory.initialize("capacity-" + run_id, requests)
        orders = OrderService(database, inventory, RequestStatusStore(database, None, durable=True))
        semaphore = asyncio.Semaphore(concurrency)

        async def one(index: int) -> None:
            nonlocal success
            message = FlashSaleMessage("capacity-campaign", f"capacity-{run_id}-{index}",
                f"capacity-key-{run_id}-{index}", "capacity-user", "capacity-" + run_id, 1)
            async with semaphore:
                started = time.perf_counter()
                try:
                    decision = await inventory.reserve(message.sku_id, message.quantity, message.order_id)
                    if decision == "success":
                        await orders.persist_queued_order(message)
                        success += 1
                    else:
                        failure_types[decision] = failure_types.get(decision, 0) + 1
                except Exception as exc:
                    failure_types[type(exc).__name__] = failure_types.get(type(exc).__name__, 0) + 1
                    try:
                        await inventory.release(message.sku_id, message.order_id)
                    except Exception:
                        pass
                finally:
                    latencies.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        await asyncio.gather(*(one(index) for index in range(requests)))
        elapsed = time.perf_counter() - started
        ordered = sorted(latencies)

        def percentile(ratio: float) -> float:
            if not ordered:
                return 0.0
            return round(ordered[min(len(ordered) - 1, int(ratio * (len(ordered) - 1)))], 3)

        return {
            "mysql_active_host": database.active_mysql_host,
            "requests": requests,
            "concurrency": concurrency,
            "success": success,
            "failures": failure_types,
            "elapsed_seconds": round(elapsed, 3),
            "throughput_rps": round(requests / elapsed, 2) if elapsed else 0.0,
            "latency_ms": {"p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)},
            "note": "Python result for this run only; not comparable to the C++ benchmark.",
        }
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Exercise MySQL reservation and order persistence; use an isolated test schema.")
    parser.add_argument("--requests", type=int, default=_positive_env("MYSQL_CAPACITY_REQUESTS", 1000, 100000))
    parser.add_argument("--concurrency", type=int, default=_positive_env("MYSQL_CAPACITY_WORKERS", 8, 128))
    args = parser.parse_args()
    if not 1 <= args.requests <= 100000 or not 1 <= args.concurrency <= 128:
        parser.error("requests must be 1..100000 and concurrency 1..128")
    print(json.dumps(asyncio.run(run_capacity(args.requests, args.concurrency)), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
