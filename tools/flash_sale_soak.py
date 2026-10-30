from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
import uuid
from collections import Counter
from pathlib import Path

from sqlalchemy import update

from app.config import Settings
from app.models import Campaign, Inventory
from app.runtime import Runtime
from app.workers.service import consume_one


async def run_soak(seconds: int, worker_count: int) -> dict[str, object]:
    # SQLite's in-memory StaticPool shares one connection across all sessions.
    # A file-backed temporary DB is required for concurrent producer/consumer
    # sessions and mirrors the local storage mode without polluting the repo.
    database_path = Path(tempfile.gettempdir()) / f"live-commerce-soak-{uuid.uuid4().hex}.db"
    settings = Settings(database_url="sqlite+aiosqlite:///" + database_path.as_posix(),
                       demo_stock=100, demo_campaign_limit=100,
                       queue_mode="local", inventory_mode="local")
    runtime = Runtime(settings)
    await runtime.start()
    initial_stock = 1_000_000
    try:
        # The source C++ soak harness provisions a million local units. Adjust only
        # this isolated runtime's fixture to the same invariant before traffic.
        async with runtime.database.sessions.begin() as session:
            await session.execute(update(Inventory).where(Inventory.sku_id == "sku-1").values(
                available=initial_stock, reserved=0, sold=0, version=0))
            await session.execute(update(Campaign).where(Campaign.id == "promo-1").values(
                participant_limit=initial_stock, state="RUNNING"))

        running = True
        counters = Counter()
        sequence = 0

        async def producer(worker: int) -> None:
            nonlocal sequence
            while running:
                index = sequence
                sequence += 1
                try:
                    result = await runtime.flash_sale.submit("promo-1", "sku-1",
                        f"soak-user-{worker}-{index}", f"soak-key-{worker}-{index}", 1)
                    counters[result["code"]] += 1
                except Exception as exc:
                    counters["error:" + type(exc).__name__] += 1

        async def consumer(worker: int) -> None:
            while running or runtime.queue.pending():
                try:
                    # The local queue pop does not block. A timeout around the
                    # entire handler could cancel it after reserving stock but
                    # before persisting the order/status.
                    await consume_one(runtime, worker)
                except asyncio.TimeoutError:
                    continue

        consumers = [asyncio.create_task(consumer(i)) for i in range(worker_count)]
        producers = [asyncio.create_task(producer(i)) for i in range(worker_count)]
        started = time.perf_counter()
        await asyncio.sleep(seconds)
        running = False
        await asyncio.gather(*producers)
        await asyncio.gather(*consumers)
        elapsed = time.perf_counter() - started
        stock = await runtime.inventory.query("sku-1")
        request_states = Counter(value.state for value in runtime._local_status_state.values())
        queued = counters["QUEUED"]
        processed = runtime.metrics["flash_sale_consumer_processed"]
        invariant_ok = (stock is not None and stock.available + stock.reserved + stock.sold == initial_stock and
                        stock.reserved == processed and stock.sold == 0 and runtime.queue.pending() == 0 and
                        queued == processed)
        return {
            "seconds": seconds,
            "workers": worker_count,
            "submitted": sum(counters.values()),
            "queued": queued,
            "processed": processed,
            "request_states": dict(request_states),
            "consumer_metrics": {key: value for key, value in runtime.metrics.items()
                                 if key.startswith("flash_sale_consumer_")},
            "local_dlq_depth": len(runtime.dead_letter_queue._messages),
            "queue_drained": runtime.queue.pending() == 0,
            "stock": None if stock is None else {"available": stock.available, "reserved": stock.reserved,
                                                  "sold": stock.sold},
            "invariant_ok": invariant_ok,
            "elapsed_seconds": round(elapsed, 3),
            "offered_rps": round(sum(counters.values()) / elapsed, 2) if elapsed else 0.0,
            "note": "This run measures this Python environment only; not comparable to C++ results.",
        }
    finally:
        await runtime.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="In-process local flash-sale soak test.")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300 or not 1 <= args.workers <= 64:
        parser.error("seconds must be 1..300 and workers 1..64")
    result = asyncio.run(run_soak(args.seconds, args.workers))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if not result["invariant_ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
