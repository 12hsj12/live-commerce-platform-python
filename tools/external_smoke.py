from __future__ import annotations

import argparse
import asyncio
import json
import os
from urllib.parse import urlsplit

from app.config import Settings
from app.database import Database


async def check_dependencies() -> dict[str, object]:
    settings = Settings.from_env()
    result: dict[str, object] = {}
    database = Database(settings)
    try:
        await database.ping()
        result["database"] = "reachable"
    except Exception as error:
        result["database"] = f"failed: {type(error).__name__}"
    finally:
        await database.close()

    if settings.redis_url:
        try:
            from redis.asyncio import from_url

            client = from_url(settings.redis_url, decode_responses=False)
            await client.ping()
            await client.aclose()
            result["redis"] = "reachable"
        except Exception as error:
            result["redis"] = f"failed: {type(error).__name__}"

    if settings.kafka_brokers:
        producer = None
        try:
            from aiokafka import AIOKafkaProducer

            producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers, request_timeout_ms=5000)
            await producer.start()
            result["kafka"] = "reachable; no message was published"
        except Exception as error:
            result["kafka"] = f"failed: {type(error).__name__}"
        finally:
            if producer is not None:
                await producer.stop()

    if settings.rocketmq_endpoint:
        try:
            import socket

            endpoint = settings.rocketmq_endpoint
            parsed = urlsplit(endpoint if "://" in endpoint else "//" + endpoint)
            host, port = parsed.hostname, parsed.port or 8081
            if not host:
                raise ValueError("endpoint has no host")
            with socket.create_connection((host, port), timeout=3):
                pass
            result["rocketmq"] = "TCP reachable; gRPC publish/consume not verified"
        except Exception as error:
            result["rocketmq"] = f"failed: {type(error).__name__}"

    if os.getenv("DEMO_BASE_URL"):
        try:
            from urllib.request import urlopen

            with urlopen(os.environ["DEMO_BASE_URL"].rstrip("/") + "/ready", timeout=3) as response:
                result["http_ready"] = f"HTTP {response.status}"
        except Exception as error:
            result["http_ready"] = f"failed: {type(error).__name__}"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Check configured dependency reachability without printing credentials.")
    parser.parse_args()
    checks = asyncio.run(check_dependencies())
    print(json.dumps(checks, ensure_ascii=False, indent=2, sort_keys=True))
    if any(str(value).startswith("failed:") for value in checks.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
