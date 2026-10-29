from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
import urllib.error
import urllib.request
from collections import Counter


PATHS = ("/health", "/ready", "/products/sku-1", "/stock/sku-1", "/campaigns?campaign_id=promo-1",
         "/promotions/promo-1", "/metrics", "/debug/stats")


def _get(base_url: str, path: str, timeout: float) -> tuple[str, int, float]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(base_url + path, timeout=timeout) as response:
            response.read()
            return path, response.status, (time.perf_counter() - started) * 1000
    except urllib.error.HTTPError as error:
        error.read()
        return path, error.code, (time.perf_counter() - started) * 1000
    except Exception:
        return path, 0, (time.perf_counter() - started) * 1000


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only mixed business endpoint load generator.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if min(args.requests, args.concurrency) <= 0 or args.timeout <= 0:
        parser.error("requests, concurrency and timeout must be positive")
    base_url = args.base_url.rstrip("/")
    jobs = [PATHS[index % len(PATHS)] for index in range(args.requests)]
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(lambda path: _get(base_url, path, args.timeout), jobs))
    elapsed = time.perf_counter() - started
    by_path: dict[str, Counter] = {}
    latencies: dict[str, list[float]] = {}
    for path, status, latency in results:
        by_path.setdefault(path, Counter())[status] += 1
        latencies.setdefault(path, []).append(latency)
    report = {"elapsed_seconds": round(elapsed, 3), "requests": args.requests,
              "throughput_rps": round(args.requests / elapsed, 2) if elapsed else 0,
              "endpoints": {path: {"status_counts": dict(counts),
                                  "mean_latency_ms": round(sum(latencies[path]) / len(latencies[path]), 3)}
                            for path, counts in by_path.items()},
              "note": "Read-only Python service sample; no C++ benchmark numbers are reused."}
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if any(status >= 500 or status == 0 for _path, status, _latency in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
