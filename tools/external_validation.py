from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from pathlib import Path

from sqlalchemy.engine import URL


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Start isolated MySQL/Redis/Redpanda services and validate adapters.")
    parser.add_argument("--project", default="live-commerce-python-external-" + uuid.uuid4().hex[:8])
    args = parser.parse_args()
    env = os.environ.copy()
    env.update({
        "ECOMMERCE_GATEWAY_PUBLISHED_PORT": env.get("ECOMMERCE_GATEWAY_PUBLISHED_PORT", "18080"),
        "ECOMMERCE_MYSQL_PUBLISHED_PORT": env.get("ECOMMERCE_MYSQL_PUBLISHED_PORT", "13316"),
        "ECOMMERCE_REDIS_PUBLISHED_PORT": env.get("ECOMMERCE_REDIS_PUBLISHED_PORT", "16379"),
        "ECOMMERCE_REDPANDA_PUBLISHED_PORT": env.get("ECOMMERCE_REDPANDA_PUBLISHED_PORT", "19092"),
        # Keep this isolated test stack independent from any user credentials or
        # inherited environment secrets. The value is a local placeholder only.
        "ECOMMERCE_MYSQL_PASSWORD": "local-app-password-change-me",
        "MYSQL_ROOT_PASSWORD": "local-root-password-change-me",
    })
    url = URL.create("mysql+asyncmy", username="ecommerce", password=env["ECOMMERCE_MYSQL_PASSWORD"],
        host="127.0.0.1", port=int(env.get("ECOMMERCE_MYSQL_PUBLISHED_PORT", "13316")), database="ecommerce")
    mysql_url = url.render_as_string(hide_password=False)
    env.update({
        "ECOMMERCE_DATABASE_URL": mysql_url,
        "ECOMMERCE_MYSQL_HOSTS": "unreachable-validation-host.invalid,127.0.0.1",
        "ECOMMERCE_REDIS_URL": f"redis://127.0.0.1:{env['ECOMMERCE_REDIS_PUBLISHED_PORT']}/0",
        "ECOMMERCE_REDIS_TEST_URL": f"redis://127.0.0.1:{env['ECOMMERCE_REDIS_PUBLISHED_PORT']}/0",
        "ECOMMERCE_MYSQL_TEST_URL": mysql_url,
        "ECOMMERCE_MYSQL_TEST_HOSTS": "unreachable-validation-host.invalid,127.0.0.1",
        "ECOMMERCE_MYSQL_TEST_PORT": env["ECOMMERCE_MYSQL_PUBLISHED_PORT"],
        "ECOMMERCE_KAFKA_BROKERS": f"127.0.0.1:{env['ECOMMERCE_REDPANDA_PUBLISHED_PORT']}",
    })
    env.pop("ECOMMERCE_ROCKETMQ_ENDPOINT", None)
    env.pop("DEMO_BASE_URL", None)
    subprocess.run(["docker", "compose", "-p", args.project, "--env-file", ".env.example",
        "-f", "compose.yaml", "-f", "compose.kafka.yaml",
        "--profile", "kafka", "up", "--build", "-d", "--wait", "--wait-timeout", "300"],
        cwd=ROOT, env=env, check=True)
    smoke = subprocess.run([sys.executable, "-m", "tools.external_smoke"], cwd=ROOT, env=env,
                           check=False, capture_output=True, text=True)
    print(smoke.stdout.strip())
    if smoke.returncode != 0:
        raise SystemExit(f"external dependency smoke failed with exit code {smoke.returncode}")
    tests = subprocess.run([sys.executable, "-m", "pytest", "-q", "tests/test_redis_integration.py",
        "tests/test_mysql_integration.py"], cwd=ROOT, env=env, check=False, capture_output=True, text=True)
    print(tests.stdout.strip())
    if tests.returncode != 0:
        raise SystemExit(f"external pytest integrations failed with exit code {tests.returncode}")
    print(f"External stack {args.project} remains running for inspection; no database or Docker volume was removed.")


if __name__ == "__main__":
    main()
