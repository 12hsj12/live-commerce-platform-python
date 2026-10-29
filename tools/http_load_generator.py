from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.error
import urllib.request
from collections import Counter


def _one(url: str, timeout: float) -> tuple[int, float, str | None]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            response.read()
            return response.status, (time.perf_counter() - started) * 1000, None
    except urllib.error.HTTPError as error:
        error.read()
        return error.code, (time.perf_counter() - started) * 1000, None
    except Exception as error:
        return 0, (time.perf_counter() - started) * 1000, type(error).__name__


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int((len(ordered) - 1) * quantile)))] if ordered else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Concurrent HTTP GET load generator; results describe this Python run only.")
    parser.add_argument("--url", default="http://127.0.0.1:8080/ready")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if min(args.requests, args.concurrency) <= 0 or args.timeout <= 0:
        parser.error("requests, concurrency and timeout must be positive")

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(lambda _: _one(args.url, args.timeout), range(args.requests)))
    elapsed = time.perf_counter() - started
    statuses = Counter(status for status, _latency, _error in results)
    errors = Counter(error for _status, _latency, error in results if error)
    latencies = [latency for _status, latency, _error in results]
    report = {
        "url": args.url,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_rps": round(args.requests / elapsed, 2) if elapsed else 0,
        "http_status_counts": dict(sorted(statuses.items())),
        "transport_errors": dict(errors),
        "latency_ms": {"p50": round(_percentile(latencies, .50), 3),
                       "p95": round(_percentile(latencies, .95), 3),
                       "p99": round(_percentile(latencies, .99), 3),
                       "max": round(max(latencies, default=0), 3)},
        "mean_latency_ms": round(statistics.fmean(latencies), 3) if latencies else 0,
        "note": "Python run only; no C++ benchmark numbers are reused.",
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if errors or any(status >= 500 for status in statuses):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
