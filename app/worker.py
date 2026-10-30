from __future__ import annotations

import asyncio
import signal

from .runtime import Runtime
from .workers.service import consumer_loop, expiry_loop, notification_loop, outbox_loop, waiting_room_loop


async def main() -> None:
    runtime = Runtime()
    await runtime.start()
    if runtime.settings.process_role != "worker":
        await runtime.close()
        raise SystemExit("set ECOMMERCE_PROCESS_ROLE=worker before running python -m app.worker")
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopping.set)
        except NotImplementedError:
            pass
    worker_count = 1 if runtime.queue_mode == "rocketmq" else runtime.settings.consumer_workers
    tasks = [asyncio.create_task(consumer_loop(runtime, i), name=f"flash-consumer-{i}")
             for i in range(worker_count)]
    tasks.extend((asyncio.create_task(outbox_loop(runtime), name="outbox-dispatcher"),
                  asyncio.create_task(expiry_loop(runtime), name="order-expiry")))
    if runtime.redis is not None and runtime.settings.waiting_room_enabled and runtime.queue_mode != "local":
        tasks.extend(asyncio.create_task(waiting_room_loop(runtime, i), name=f"waiting-room-{i}")
                     for i in range(runtime.settings.waiting_workers))
    if runtime.kafka_producer is not None:
        tasks.append(asyncio.create_task(notification_loop(runtime), name="notification-fanout"))
    # Compose's `--wait` does not know when the background consumers have
    # connected to their queue.  This marker lets migration/soak tools inject
    # dependency failures only after worker initialization has completed.
    print(f"worker_ready queue_mode={runtime.queue_mode}", flush=True)
    try:
        await stopping.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
