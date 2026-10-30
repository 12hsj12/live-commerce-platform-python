from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the authenticated HTTP benchmark against RocketMQ mode.")
    parser.add_argument("--project", default="live-commerce-python-rocketmq-bench")
    parser.add_argument("--base-url", default=os.getenv("DEMO_BASE_URL", "http://127.0.0.1:18080"))
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--rate", type=float, default=0)
    parser.add_argument("--start-compose", action="store_true",
                        help="start the local MySQL/Redis/RocketMQ stack before benchmarking")
    args = parser.parse_args()
    env = os.environ.copy()
    env.update({"ECOMMERCE_FLASH_SALE_QUEUE": "rocketmq",
                "ECOMMERCE_BENCH_PROJECT": args.project,
                "DEMO_BASE_URL": args.base_url,
                "ECOMMERCE_DEMO_STOCK": str(max(100, args.requests)),
                "ECOMMERCE_DEMO_CAMPAIGN_LIMIT": str(max(100, args.requests))})
    if args.start_compose:
        subprocess.run(["docker", "compose", "-p", args.project, "--env-file", ".env.example",
            "-f", "compose.yaml", "-f", "compose.rocketmq.yaml", "--profile", "rocketmq",
            "up", "--build", "-d", "--wait", "--wait-timeout", "900"], cwd=ROOT, env=env, check=True)
    command = [sys.executable, "-m", "tools.http_flash_sale_benchmark", "--base-url", args.base_url,
               "--requests", str(args.requests), "--concurrency", str(args.concurrency), "--rate", str(args.rate)]
    subprocess.run(command, cwd=ROOT, env=env, check=True)
    if args.start_compose:
        print(f"Compose project {args.project} remains running for inspection; no volumes were removed.")


if __name__ == "__main__":
    main()
