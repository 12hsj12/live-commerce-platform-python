#!/usr/bin/env python3
"""Measure authenticated flash-sale submissions; setup and drain are excluded."""

import argparse
import concurrent.futures
import json
import os
import platform
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter


def call(base_url, path, method="GET", body=None, token=None):
    headers = {}
    data = None
    if body is not None:
        data = urllib.parse.urlencode(body).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        try:
            return error.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return error.code, {"body": raw}
    except Exception as error:  # transport failures are counted separately from HTTP responses
        return 0, {"error": str(error)}


def percentile(values, ratio):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * ratio))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=os.environ.get("DEMO_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--setup-workers", type=int, default=8)
    parser.add_argument("--drain-timeout", type=int, default=90)
    parser.add_argument("--rate", type=float, default=0.0,
                        help="optional paced submission rate in requests/second; 0 means send as fast as possible")
    args = parser.parse_args()
    if min(args.requests, args.concurrency, args.setup_workers, args.drain_timeout) <= 0 or args.rate < 0:
        parser.error("counts and timeouts must be positive; rate must be non-negative")

    base_url = args.base_url.rstrip("/")
    status, ready = call(base_url, "/ready")
    if status != 200:
        raise SystemExit(f"Gateway is not ready: HTTP {status} {ready}")

    prefix = "bench_" + uuid.uuid4().hex[:12]
    password = "Bench-Only-Password-2026"
    setup_errors = []
    setup_lock = threading.Lock()

    def provision(index):
        username = f"{prefix}_{index}"
        status, result = call(base_url, "/users/register", "POST", {"username": username, "password": password})
        if status != 201:
            with setup_lock:
                setup_errors.append(f"register {index}: HTTP {status} {result}")
            return None
        status, result = call(base_url, "/users/login", "POST", {"username": username, "password": password})
        if status != 200:
            with setup_lock:
                setup_errors.append(f"login {index}: HTTP {status} {result}")
            return None
        return result["token"]

    setup_started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.setup_workers) as pool:
        tokens = list(pool.map(provision, range(args.requests)))
    setup_seconds = time.monotonic() - setup_started
    if setup_errors or any(token is None for token in tokens):
        raise SystemExit(f"Could not provision all benchmark sessions: {setup_errors[:5]}")

    run_id = uuid.uuid4().hex[:16]
    latencies = [0.0] * args.requests
    statuses = [0] * args.requests

    def submit(index):
        request_id = f"{run_id}-{index}"
        start = time.perf_counter()
        status, payload = call(base_url, "/flash-sale/orders", "POST", {
            "promotion_id": "promo-1", "sku_id": "sku-1", "quantity": "1",
            "idempotency_key": request_id,
        }, tokens[index])
        elapsed_ms = (time.perf_counter() - start) * 1000
        latencies[index] = elapsed_ms
        statuses[index] = status
        return request_id, status, payload

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        if args.rate == 0:
            results = list(pool.map(submit, range(args.requests)))
        else:
            futures = []
            for index in range(args.requests):
                futures.append(pool.submit(submit, index))
                if index + 1 < args.requests:
                    target = started + (index + 1) / args.rate
                    delay = target - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
            results = [future.result() for future in futures]
    elapsed = time.monotonic() - started

    status_counts = Counter(status for status in statuses)
    accepted = [(request_id, tokens[index]) for index, (request_id, status, _) in enumerate(results) if status == 202]
    drain_deadline = time.monotonic() + args.drain_timeout
    terminal_states = Counter()
    unsettled = list(accepted)
    poll_workers = max(1, min(args.concurrency, 64))

    def read_request_state(entry):
        request_id, token = entry
        status, payload = call(base_url, "/flash-sale/requests?request_id=" + urllib.parse.quote(request_id), token=token)
        if status == 200 and payload.get("state") in {"PENDING_PAYMENT", "SOLD_OUT", "REJECTED", "PAID", "EXPIRED", "CANCELLED"}:
            return request_id, token, payload["state"]
        return request_id, token, None

    while unsettled and time.monotonic() < drain_deadline:
        next_unsettled = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=poll_workers) as pool:
            for request_id, token, state in pool.map(read_request_state, unsettled):
                if state is not None:
                    terminal_states[state] += 1
                else:
                    next_unsettled.append((request_id, token))
        unsettled = next_unsettled
        if unsettled:
            time.sleep(0.1)

    _, stock = call(base_url, "/stock/sku-1")
    _, stats = call(base_url, "/debug/stats")
    response_count = sum(count for status, count in status_counts.items() if status != 0)
    transport_errors = status_counts.get(0, 0)
    server_errors = sum(count for status, count in status_counts.items() if status >= 500)
    throttled = status_counts.get(429, 0)
    business_rejections = status_counts.get(409, 0)
    initial_stock = int(os.environ.get("ECOMMERCE_DEMO_STOCK", "100"))
    stock_invariant = stock.get("available", -1) + stock.get("reserved", -1) + stock.get("sold", -1) == initial_stock
    dead_lettered = stats.get("flash_sale_consumer_dead_lettered", 0)

    report = {
        "run_id": run_id,
        "environment": {
            "platform": platform.platform(),
            "processor": platform.processor() or "unknown",
            "logical_cpu_count": os.cpu_count(),
            "python_version": platform.python_version(),
            "container_images": {
                "gateway": "locally built Docker image",
                "redis": "redis:7-alpine",
                "mysql": "mysql:8.4",
                "redpanda": "redpandadata/redpanda:v24.1.2",
                "rocketmq": "apache/rocketmq:5.3.2 (single broker; RocketMQ mode only)",
            },
        },
        "mode": os.environ.get("ECOMMERCE_FLASH_SALE_QUEUE", "redis"),
        "compose_project": os.environ.get("ECOMMERCE_BENCH_PROJECT", "unspecified"),
        "application_configuration": {
            "gateway_workers": int(os.environ.get("ECOMMERCE_HTTP_WORKERS", "8")),
            "consumer_workers": int(os.environ.get("ECOMMERCE_FLASH_CONSUMER_WORKERS", "4")),
            "mysql_pool_size": int(os.environ.get("ECOMMERCE_MYSQL_POOL_SIZE", "16")),
            "initial_stock": initial_stock,
            "campaign_limit": int(os.environ.get("ECOMMERCE_DEMO_CAMPAIGN_LIMIT", str(initial_stock))),
        },
        "requests": args.requests,
        "concurrency": args.concurrency,
        "target_submission_rate_rps": args.rate if args.rate > 0 else None,
        "session_setup_seconds_excluded": round(setup_seconds, 3),
        "measured_seconds": round(elapsed, 3),
        "throughput_rps": round(args.requests / elapsed, 2) if elapsed else 0,
        "accepted_admission_rps": round(status_counts.get(202, 0) / elapsed, 2) if elapsed else 0,
        "latency_ms": {
            "measured_operation": "authenticated HTTP admission and command publish; setup and order drain excluded",
            "p50": round(percentile(latencies, 0.50), 3),
            "p95": round(percentile(latencies, 0.95), 3),
            "p99": round(percentile(latencies, 0.99), 3),
            "max": round(max(latencies, default=0), 3),
        },
        "http_status_counts": dict(sorted(status_counts.items())),
        "server_error_rate": round(server_errors / max(1, args.requests), 6),
        "transport_error_rate": round(transport_errors / max(1, args.requests), 6),
        "throttled_429": throttled,
        "business_rejected_409": business_rejections,
        "accepted_202": status_counts.get(202, 0),
        "accepted_requests_reached_terminal_state": sum(terminal_states.values()),
        "accepted_requests_not_settled": len(unsettled),
        "terminal_request_states": dict(terminal_states),
        "stock": stock,
        "stock_invariant_available_plus_reserved_plus_sold_equals_initial": stock_invariant,
        "consumer_stats": stats,
        "dead_lettered_commands": dead_lettered,
        "rocketmq_broker_dlq_retry_and_replay": "not verified by this load run",
        "http_responses_observed": response_count,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if transport_errors or server_errors or dead_lettered or unsettled or not stock_invariant or not accepted or sum(terminal_states.values()) != len(accepted):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
