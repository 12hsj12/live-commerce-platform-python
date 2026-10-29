from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from collections import Counter

from app.config import Settings
from app.runtime import Runtime


async def run(requests: int, concurrency: int, stock_size: int) -> dict:
    runtime = Runtime(Settings.from_env())
    await runtime.start()
    sku_id = "bench-" + uuid.uuid4().hex[:16]
    try:
        await runtime.inventory.initialize(sku_id, stock_size)
        semaphore = asyncio.Semaphore(concurrency)

        async def reserve(index: int) -> tuple[int, str, str]:
            async with semaphore:
                operation_id = f"{sku_id}-{index}"
                return index, operation_id, await runtime.inventory.reserve(sku_id, 1, operation_id)

        started = time.perf_counter()
        outcomes = await asyncio.gather(*(reserve(index) for index in range(requests)), return_exceptions=True)
        elapsed = time.perf_counter() - started
        failures = [type(value).__name__ for value in outcomes if isinstance(value, Exception)]
        successful = [(index, operation_id) for value in outcomes if not isinstance(value, Exception)
                      for index, operation_id, decision in (value,) if decision == "success"]
        counts = Counter(value[2] for value in outcomes if not isinstance(value, Exception))
        release_errors = []
        for _index, operation_id in successful:
            try:
                await runtime.inventory.release(sku_id, operation_id)
            except Exception as error:
                release_errors.append(type(error).__name__)
        stock = await runtime.inventory.query(sku_id)
        report = {
            "sku_id": sku_id,
            "requests": requests,
            "concurrency_limit": concurrency,
            "initial_stock": stock_size,
            "elapsed_seconds": round(elapsed, 4),
            "throughput_rps": round(requests / elapsed, 2) if elapsed else 0,
            "decisions": dict(sorted(counts.items())),
            "operation_errors": dict(Counter(failures)),
            "release_errors": dict(Counter(release_errors)),
            "post_cleanup_stock": None if stock is None else stock.__dict__,
            "stock_invariant": stock is not None and stock.available + stock.reserved + stock.sold == stock_size,
            "note": "Python runtime measurement only; no C++ benchmark numbers are reused.",
        }
        return report
    finally:
        await runtime.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Concurrent local/Redis inventory reservation benchmark.")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--stock", type=int, default=1000)
    args = parser.parse_args()
    if min(args.requests, args.concurrency, args.stock) <= 0:
        parser.error("requests, concurrency and stock must be positive")
    report = asyncio.run(run(args.requests, args.concurrency, args.stock))
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["operation_errors"] or report["release_errors"] or not report["stock_invariant"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
