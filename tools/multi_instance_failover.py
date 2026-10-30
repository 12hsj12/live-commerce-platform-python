from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _request(base_url: str, path: str, *, method: str = "GET", body: dict | None = None,
             token: str | None = None) -> tuple[int, dict]:
    headers = {}
    data = None
    if body is not None:
        data = urllib.parse.urlencode(body).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(base_url.rstrip("/") + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read() or b"{}")
        except Exception:
            payload = {}
        return exc.code, payload


def _compose(project: str, *args: str) -> None:
    subprocess.run(["docker", "compose", "-p", project, "--env-file", ".env.example",
                    "-f", "compose.yaml", "-f", "compose.multi.yaml",
                    *args], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Redis-backed session continuity across two Python gateways.")
    parser.add_argument("--project", default="live-commerce-python-multi")
    parser.add_argument("--base-url", default=os.getenv("BASE_URL", "http://127.0.0.1:18080"))
    parser.add_argument("--second-url", default=os.getenv("SECOND_URL", "http://127.0.0.1:18081"))
    parser.add_argument("--start-compose", action="store_true", help="build/start the isolated Compose stack first")
    args = parser.parse_args()

    if args.start_compose:
        _compose(args.project, "up", "--build", "-d", "--wait", "--wait-timeout", "240")
    for base_url in (args.base_url, args.second_url):
        ready = False
        for _ in range(60):
            try:
                code, _ = _request(base_url, "/ready")
                if code == 200:
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(1)
        if not ready:
            raise SystemExit(f"gateway did not become ready: {base_url}")

    suffix = uuid.uuid4().hex[:12]
    username = "multi-" + suffix
    status, _ = _request(args.base_url, "/users/register", method="POST",
                         body={"username": username, "password": "Local-Test-Password-2026"})
    if status != 201:
        raise SystemExit(f"registration failed with HTTP {status}")
    status, login = _request(args.base_url, "/users/login", method="POST",
                             body={"username": username, "password": "Local-Test-Password-2026"})
    token = login.get("token")
    if status != 200 or not token:
        raise SystemExit(f"login failed with HTTP {status}")

    def assert_shared_session() -> None:
        code, _ = _request(args.second_url, "/orders?order_id=missing", token=token)
        if code != 404:
            raise SystemExit(f"expected authenticated missing-order response 404, got {code}")

    assert_shared_session()
    if args.start_compose:
        _compose(args.project, "stop", "api")
    try:
        code, _ = _request(args.second_url, "/ready")
        if code != 200:
            raise SystemExit(f"second gateway became unavailable: HTTP {code}")
        assert_shared_session()
    except Exception:
        if args.start_compose:
            print(f"Compose project {args.project} remains running for inspection.")
        raise
    print(json.dumps({"status": "shared-session-and-gateway-failover-passed",
                      "first_gateway": args.base_url, "second_gateway": args.second_url,
                      "compose_project": args.project if args.start_compose else None}, sort_keys=True))
    if args.start_compose:
        print(f"Compose project {args.project} is left running for inspection; stop it with docker compose -p {args.project} -f compose.yaml -f compose.multi.yaml stop.")


if __name__ == "__main__":
    main()
