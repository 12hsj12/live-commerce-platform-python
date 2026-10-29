from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def request(base_url: str, path: str, method: str = "GET", body: dict | None = None,
            token: str | None = None) -> tuple[int, dict]:
    headers = {}
    data = None
    if body is not None:
        data = urllib.parse.urlencode(body).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = "Bearer " + token
    outgoing = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(outgoing, timeout=10) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        try:
            return error.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return error.code, {"body": raw}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one end-to-end flash-sale request.")
    parser.add_argument("--base-url", default=os.environ.get("DEMO_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--settle", choices=("pay", "cancel", "none"), default="pay")
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")
    status, ready = request(base_url, "/ready")
    if status != 200:
        raise SystemExit(f"服务未就绪：HTTP {status} {ready}")

    suffix = uuid.uuid4().hex[:16]
    username = "python_demo_" + suffix
    password = "Demo-Only-Password-2026"
    status, payload = request(base_url, "/users/register", "POST", {"username": username, "password": password})
    if status != 201:
        raise SystemExit(f"注册失败：HTTP {status} {payload}")
    status, login = request(base_url, "/users/login", "POST", {"username": username, "password": password})
    if status != 200:
        raise SystemExit(f"登录失败：HTTP {status} {login}")

    request_id = "demo-" + suffix
    status, submitted = request(base_url, "/flash-sale/orders", "POST", {
        "promotion_id": "promo-1", "sku_id": "sku-1", "quantity": "1", "idempotency_key": request_id,
    }, login["token"])
    if status != 202:
        raise SystemExit(f"秒杀提交失败：HTTP {status} {submitted}")

    terminal = {"PENDING_PAYMENT", "SOLD_OUT", "REJECTED", "PAID", "EXPIRED", "CANCELLED"}
    deadline = time.monotonic() + args.timeout
    state = submitted
    while time.monotonic() < deadline:
        status, state = request(base_url, "/flash-sale/requests?" + urllib.parse.urlencode({"request_id": request_id}),
                                token=login["token"])
        if status != 200:
            raise SystemExit(f"请求状态查询失败：HTTP {status} {state}")
        if state.get("state") in terminal:
            break
        time.sleep(0.1)
    else:
        raise SystemExit(f"请求未在 {args.timeout:g}s 内到达终态：{state}")

    final_state = state.get("state")
    if final_state == "PENDING_PAYMENT" and args.settle != "none":
        action = "pay" if args.settle == "pay" else "cancel"
        status, state = request(base_url, "/orders/" + action, "POST", {"order_id": state["order_id"]}, login["token"])
        if status != 200:
            raise SystemExit(f"订单{action}操作失败：HTTP {status} {state}")
        final_state = state.get("state")

    _, stock = request(base_url, "/stock/sku-1")
    print(json.dumps({"request_id": request_id, "order_id": state.get("order_id"),
                      "final_state": final_state, "stock": stock}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
