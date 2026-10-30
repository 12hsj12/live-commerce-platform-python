from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from sqlalchemy.engine import make_url

from app.config import Settings
from app.database import Database
from app.models import FlashSaleRequest
from app.services.commerce import OrderService
from app.services.request_status import RequestStatus, RequestStatusStore
from tools.rocketmq_smoke import make_smoke_message, run_smoke


ROOT = Path(__file__).resolve().parents[1]


def _compose(project: str, *args: str) -> None:
    subprocess.run(["docker", "compose", "-p", project, "--env-file", ".env.example",
        "-f", "compose.yaml", "-f", "compose.rocketmq.yaml",
        "--profile", "rocketmq", *args], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)


def _http(base_url: str, path: str, *, token: str = "", body: dict | None = None) -> tuple[int, dict]:
    headers = {}
    data = None
    if token:
        headers["x-admin-token"] = token
    if body is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = urllib.parse.urlencode(body).encode()
    request = urllib.request.Request(base_url.rstrip("/") + path, data=data, headers=headers)
    try:
        # RocketMQ DLQ receive uses a broker long-poll and may take 30 seconds.
        with urllib.request.urlopen(request, timeout=45) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read() or b"{}")
        except Exception:
            payload = {}
        return exc.code, payload


async def _order_exists(database_url: str, order_id: str) -> bool:
    parsed = make_url(database_url)
    settings = Settings(mysql_mode=True, database_url=database_url,
        mysql_hosts=(parsed.host,) if parsed.host else (), mysql_port=parsed.port or 3306, mysql_pool_size=2)
    database = Database(settings)
    try:
        order = await OrderService(database, None, RequestStatusStore(database, None, durable=True)).get(order_id)
        return order is not None
    finally:
        await database.close()


async def _seed_queued_request(database_url: str, message) -> None:
    """Seed the durable QUEUED state that normal API admission writes before enqueue."""
    parsed = make_url(database_url)
    settings = Settings(mysql_mode=True, database_url=database_url,
        mysql_hosts=(parsed.host,) if parsed.host else (), mysql_port=parsed.port or 3306,
        mysql_pool_size=2)
    database = Database(settings)
    try:
        await database.initialize(seed=False)
        async with database.sessions.begin() as session:
            session.add(FlashSaleRequest(request_id=message.idempotency_key,
                user_id=message.user_id, order_id=message.order_id, state="QUEUED",
                message="RocketMQ DLQ recovery validation"))
        # Exercise the same service-level status writer used by the worker.
        await RequestStatusStore(database, None, durable=True).put(RequestStatus(
            message.idempotency_key, message.user_id, message.order_id, "QUEUED",
            "RocketMQ DLQ recovery validation", message.promotion_id))
    finally:
        await database.close()


def _wait_worker_ready(project: str, timeout_seconds: int = 120) -> None:
    container = f"{project}-worker-1"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        logs = subprocess.run(["docker", "logs", container], capture_output=True,
                              text=True, check=False)
        if "worker_ready queue_mode=rocketmq" in (logs.stdout + logs.stderr):
            return
        state = subprocess.run(["docker", "inspect", "--format", "{{.State.Status}}", container],
                               capture_output=True, text=True, check=False)
        if state.returncode == 0 and state.stdout.strip() in {"exited", "dead"}:
            raise RuntimeError("RocketMQ worker exited before signaling readiness")
        time.sleep(1)
    raise TimeoutError("RocketMQ worker did not signal readiness before MySQL fault injection")


def _publish_from_api_container(project: str, message) -> dict[str, str]:
    container = f"{project}-api-1"
    command = ["docker", "exec", container, "python", "-m", "tools.rocketmq_smoke",
        "--endpoint", "rocketmq-proxy:8081", "--topic", "flashsale.command",
        "--group", "flashsale-command-consumers", "--publish-only",
        "--promotion-id", message.promotion_id, "--order-id", message.order_id,
        "--request-id", message.idempotency_key, "--user-id", message.user_id,
        "--sku-id", message.sku_id, "--quantity", str(message.quantity)]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=True)
    return json.loads(result.stdout.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Inject a MySQL outage and verify RocketMQ application-DLQ replay.")
    parser.add_argument("--project", default="live-commerce-python-rocketmq-dlq")
    parser.add_argument("--base-url", default=os.getenv("DEMO_BASE_URL", "http://127.0.0.1:18080"))
    parser.add_argument("--endpoint", default=os.getenv("ECOMMERCE_ROCKETMQ_TEST_ENDPOINT", "127.0.0.1:8081"))
    parser.add_argument("--admin-token", default=os.getenv("ECOMMERCE_ADMIN_TOKEN", "local-demo-admin-token"))
    parser.add_argument("--mysql-url", default=os.getenv("ECOMMERCE_MYSQL_TEST_URL",
                        "mysql+asyncmy://ecommerce:local-app-password-change-me@127.0.0.1:13316/ecommerce"))
    parser.add_argument("--start-compose", action="store_true",
                        help="start an isolated local compose stack and stop/restore only its mysql service")
    args = parser.parse_args()

    if args.start_compose:
        env = os.environ.copy()
        env.setdefault("ECOMMERCE_GATEWAY_PUBLISHED_PORT", "18080")
        env.setdefault("ECOMMERCE_MYSQL_PUBLISHED_PORT", "13316")
        env.setdefault("ECOMMERCE_REDIS_PUBLISHED_PORT", "16379")
        env.setdefault("ECOMMERCE_ROCKETMQ_PUBLISHED_PORT", "8081")
        env.setdefault("ECOMMERCE_ADMIN_TOKEN", args.admin_token)
        subprocess.run(["docker", "compose", "-p", args.project, "--env-file", ".env.example",
            "-f", "compose.yaml", "-f",
            "compose.rocketmq.yaml", "--profile", "rocketmq", "up", "--build", "-d", "--wait",
            "--wait-timeout", "900"], cwd=ROOT, env=env, check=True)
        _wait_worker_ready(args.project)
    mysql_stopped = False
    try:
        message = make_smoke_message()
        if args.start_compose:
            # Normal API admission has already committed this durable state by
            # the time it enqueues a command. The standalone RocketMQ smoke
            # producer bypasses that HTTP path, so seed the equivalent record
            # before inducing a database outage.
            asyncio.run(_seed_queued_request(args.mysql_url, message))
            _compose(args.project, "stop", "mysql")
            mysql_stopped = True
            published = _publish_from_api_container(args.project, message)
        else:
            published = asyncio.run(run_smoke(args.endpoint, "flashsale.command",
                "flashsale-command-consumers", publish_only=True, message=message))
        deadline = time.monotonic() + 120
        dlq_before: dict = {}
        while time.monotonic() < deadline:
            code, dlq_before = _http(args.base_url, "/admin/dlq?limit=10", token=args.admin_token)
            if code == 200 and len(dlq_before.get("messages", [])) >= 1:
                break
            time.sleep(1)
        if len(dlq_before.get("messages", [])) < 1:
            raise RuntimeError("no RocketMQ application DLQ message arrived within 120 seconds")
        if mysql_stopped:
            _compose(args.project, "start", "mysql")
            mysql_stopped = False

        ready = False
        for _ in range(90):
            try:
                code, _ = _http(args.base_url, "/ready")
                if code == 200:
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(1)
        if not ready:
            raise RuntimeError("API did not become ready after MySQL recovery")

        code, replay = _http(args.base_url, "/admin/dlq/replay", token=args.admin_token,
                             body={"max_messages": "10"})
        if code != 200 or replay.get("replayed", 0) < 1:
            raise RuntimeError("application DLQ replay did not accept a message")

        persisted = False
        for _ in range(90):
            persisted = asyncio.run(_order_exists(args.mysql_url, published["order_id"]))
            if persisted:
                break
            time.sleep(1)
        if not persisted:
            raise RuntimeError("replayed RocketMQ command did not persist an order")
        code, dlq_after = _http(args.base_url, "/admin/dlq?limit=10", token=args.admin_token)
        report = {"status": "rocketmq-dlq-replay-passed", "order_id": published["order_id"],
                  "dead_lettered_before_replay": len(dlq_before.get("messages", [])),
                  "replayed": replay.get("replayed"), "persisted_after_replay": persisted,
                  "dlq_after_count": len(dlq_after.get("messages", [])) if code == 200 else None,
                  "external_provider_or_performance_claim": None}
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        if args.start_compose:
            print(f"Compose project {args.project} remains running for inspection; no volumes were removed.")
    finally:
        if mysql_stopped:
            _compose(args.project, "start", "mysql")


if __name__ == "__main__":
    main()
