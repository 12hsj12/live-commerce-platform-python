from __future__ import annotations

import argparse
import asyncio
import json
import os
import uuid

from app.adapters.rocketmq_queue import RocketMqOrderQueue
from app.messages import FlashSaleMessage


def make_smoke_message() -> FlashSaleMessage:
    suffix = uuid.uuid4().hex[:16]
    return FlashSaleMessage("rocketmq-smoke", "rocketmq-order-" + suffix,
        "rocketmq-request-" + suffix, "rocketmq-smoke-user", "sku-1", 1)


async def run_smoke(endpoint: str, topic: str, group: str, *, publish_only: bool = False,
                    message: FlashSaleMessage | None = None) -> dict[str, str]:
    queue = RocketMqOrderQueue(endpoint, topic, group, enable_consumer=not publish_only,
                               enable_dlq_operations=False)
    try:
        await asyncio.to_thread(queue.start)
        sent = message or make_smoke_message()
        await queue.push(sent)
        if publish_only:
            return {"status": "published", "request_id": sent.idempotency_key, "order_id": sent.order_id}
        received = await asyncio.wait_for(queue.pop(), timeout=30)
        if received != sent:
            raise RuntimeError("RocketMQ command round-trip payload mismatch")
        await queue.acknowledge(received, True)
        return {"status": "round-trip-passed", "request_id": received.idempotency_key,
                "order_id": received.order_id}
    finally:
        await queue.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Send/read/ack one isolated RocketMQ FOQ1 command.")
    parser.add_argument("--endpoint", default=os.getenv("ECOMMERCE_ROCKETMQ_TEST_ENDPOINT") or
                        os.getenv("ECOMMERCE_ROCKETMQ_ENDPOINT"))
    parser.add_argument("--topic", default=os.getenv("ECOMMERCE_ROCKETMQ_TEST_TOPIC", "flashsale.command"))
    parser.add_argument("--group", default=os.getenv("ECOMMERCE_ROCKETMQ_TEST_GROUP", "flashsale-smoke"))
    parser.add_argument("--publish-only", action="store_true")
    parser.add_argument("--promotion-id")
    parser.add_argument("--order-id")
    parser.add_argument("--request-id")
    parser.add_argument("--user-id")
    parser.add_argument("--sku-id")
    parser.add_argument("--quantity", type=int)
    args = parser.parse_args()
    if not args.endpoint:
        parser.error("--endpoint or ECOMMERCE_ROCKETMQ_TEST_ENDPOINT is required")
    overrides = (args.promotion_id, args.order_id, args.request_id, args.user_id, args.sku_id, args.quantity)
    if any(value is not None for value in overrides):
        if any(value is None for value in overrides):
            parser.error("message overrides require promotion, order, request, user, SKU and quantity")
        message = FlashSaleMessage(args.promotion_id, args.order_id, args.request_id,
                                   args.user_id, args.sku_id, args.quantity)
    else:
        message = None
    result = asyncio.run(run_smoke(args.endpoint, args.topic, args.group,
                                   publish_only=args.publish_only, message=message))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
