from __future__ import annotations

import asyncio
import os
import socket
import time
from datetime import datetime, timedelta

from sqlalchemy import or_, select

from ..messages import FlashSaleMessage
from ..models import FlashSaleRequest, Order, OrderItem, OutboxEvent
from ..services.commerce import ServiceFault
from ..services.request_status import RequestStatus


async def process_command(runtime, message: FlashSaleMessage) -> None:
    previous = await runtime.statuses.get(message.idempotency_key)
    if previous is not None and previous.state in {"REJECTED", "SOLD_OUT", "PAID", "EXPIRED", "CANCELLED"}:
        return
    await runtime.statuses.put(RequestStatus(message.idempotency_key, message.user_id, message.order_id,
                                            "PROCESSING", "request is being processed"))
    decision = await runtime.inventory.reserve(message.sku_id, message.quantity, message.order_id)
    if decision == "insufficient":
        await runtime.admission.rollback(message.promotion_id, message.user_id)
        await runtime.statuses.put(RequestStatus(message.idempotency_key, message.user_id, message.order_id,
            "SOLD_OUT", "campaign inventory is exhausted", message.promotion_id))
        runtime.metrics["flash_sale_consumer_rejected"] += 1
        return
    if decision == "conflict":
        raise RuntimeError("operation id was reused with different data")
    if decision == "missing":
        raise RuntimeError("inventory SKU not found")
    await runtime.orders.persist_queued_order(message)
    await runtime.statuses.put(RequestStatus(message.idempotency_key, message.user_id, message.order_id,
        "PENDING_PAYMENT", "order created; payment is pending", message.promotion_id))
    runtime.metrics["flash_sale_consumer_processed"] += 1


async def compensate(runtime, message: FlashSaleMessage) -> None:
    try:
        persisted = await runtime.orders.get(message.order_id)
    except Exception:
        # An indeterminate repository read is not proof that releasing is safe.
        return
    if persisted is not None:
        return
    try:
        await runtime.inventory.release(message.sku_id, message.order_id)
    except Exception:
        pass
    try:
        await runtime.admission.rollback(message.promotion_id, message.user_id)
    except Exception:
        pass


async def consume_one(runtime, worker_index: int) -> None:
    try:
        message = await runtime.queue.pop()
    except asyncio.TimeoutError:
        return
    except ValueError as exc:
        runtime.metrics["flash_sale_consumer_bad_messages"] += 1
        return
    except Exception:
        runtime.metrics["flash_sale_consumer_receive_errors"] += 1
        await asyncio.sleep(0.1)
        return
    last_error: Exception | None = None
    processed = False
    for attempt in range(runtime.settings.consumer_max_attempts):
        try:
            await process_command(runtime, message)
            processed = True
            break
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0)
    if processed:
        if hasattr(runtime.queue, "acknowledge"):
            try:
                await runtime.queue.acknowledge(message, True)
            except Exception:
                runtime.metrics["flash_sale_consumer_ack_errors"] += 1
                return
        if hasattr(runtime.queue, "_queue"):
            runtime.queue._queue.task_done()
        return
    raw = message.encode()
    moved = False
    if runtime.dead_letter_queue is not None:
        try:
            await runtime.dead_letter_queue.dead_letter(raw)
            moved = True
        except Exception:
            pass
    if moved:
        runtime.metrics["flash_sale_consumer_dead_lettered"] += 1
        try:
            durable = await runtime.orders.get(message.order_id)
            if durable is not None:
                state = durable.state
                request_state = state if state in {"PAID", "EXPIRED", "CANCELLED"} else "PENDING_PAYMENT"
                detail = "message moved to DLQ after order persistence"
            else:
                request_state = "REJECTED"
                detail = "message moved to DLQ after retries"
            await runtime.statuses.put(RequestStatus(message.idempotency_key, message.user_id, message.order_id,
                                                      request_state, detail, message.promotion_id))
        except Exception:
            pass
    await compensate(runtime, message)
    if hasattr(runtime.queue, "acknowledge"):
        try:
            await runtime.queue.acknowledge(message, moved)
        except Exception:
            runtime.metrics["flash_sale_consumer_ack_errors"] += 1
    if hasattr(runtime.queue, "_queue"):
        runtime.queue._queue.task_done()
    if not moved:
        runtime.metrics["flash_sale_consumer_dlq_unavailable"] += 1
    if last_error is not None:
        runtime.metrics["flash_sale_consumer_last_error"] += 1


async def consumer_loop(runtime, worker_index: int) -> None:
    while True:
        await consume_one(runtime, worker_index)


async def waiting_room_loop(runtime, worker_index: int) -> None:
    if runtime.redis is None or not runtime.settings.waiting_room_enabled:
        return
    from sqlalchemy import select
    from ..adapters.admission import RedisWaitingRoom
    from ..models import Campaign

    owner = f"waiting-{socket.gethostname()}-{os.getpid()}-{worker_index}"
    campaigns: list[str] = []
    refresh_at = 0.0
    while True:
        now = time.monotonic()
        if now >= refresh_at:
            async with runtime.database.sessions() as session:
                campaigns = list((await session.execute(select(Campaign.id))).scalars())
            refresh_at = now + 2.0
        made_progress = False
        for campaign_id in campaigns:
            room = RedisWaitingRoom(runtime.redis, campaign_id, runtime.settings.waiting_capacity)
            claimed = await room.claim(owner)
            if claimed is None:
                continue
            ticket, message = claimed
            allowed = await runtime.admission.take_redis_bucket(
                "flashsale:{global}:entry_rate", runtime.settings.global_rate, runtime.settings.global_burst)
            if allowed:
                allowed = await runtime.admission.take_redis_bucket(
                    f"flashsale:{{{campaign_id}}}:entry_rate", runtime.settings.campaign_rate,
                    runtime.settings.campaign_burst)
            if not allowed:
                await room.release(ticket, owner)
                continue
            try:
                result = await runtime.flash_sale.submit(message.promotion_id, message.sku_id, message.user_id,
                                                         message.idempotency_key, message.quantity)
            except ServiceFault as exc:
                if exc.status_code in {400, 404, 409}:
                    await runtime.statuses.put(RequestStatus(ticket, message.user_id, "", "REJECTED",
                        exc.message, message.promotion_id))
                    made_progress = await room.complete(ticket, owner) or made_progress
                else:
                    await room.release(ticket, owner)
                continue
            if result["code"] == "NOT_STARTED":
                await room.release(ticket, owner)
                continue
            if result["code"] == "REJECTED":
                current = await runtime.promotions.get(message.promotion_id)
                if current is not None and current.state in {"CREATED", "PREHEATING", "PAUSED"}:
                    await room.release(ticket, owner)
                    continue
            if result["code"] == "DUPLICATE":
                current = await runtime.statuses.get(ticket)
                if current is not None and current.state == "WAITING":
                    await runtime.statuses.put(RequestStatus(ticket, message.user_id, result["order_id"],
                        "REJECTED", result.get("message") or "user was already admitted to this campaign", campaign_id))
            elif result["code"] not in {"QUEUED"}:
                final_state = "SOLD_OUT" if result["code"] == "EXHAUSTED" else "REJECTED"
                await runtime.statuses.put(RequestStatus(ticket, message.user_id, result.get("order_id", ""),
                    final_state, result.get("message", ""), campaign_id))
            made_progress = await room.complete(ticket, owner) or made_progress
        if not made_progress:
            await asyncio.sleep(0.01)


async def outbox_loop(runtime) -> None:
    owner = f"outbox-{socket.gethostname()}-{os.getpid()}"
    while True:
        now = datetime.utcnow()
        async with runtime.database.sessions.begin() as session:
            statement = (select(OutboxEvent).where(OutboxEvent.published.is_(False),
                or_(OutboxEvent.lease_until.is_(None), OutboxEvent.lease_until < now))
                .order_by(OutboxEvent.created_at, OutboxEvent.event_id).limit(100).with_for_update(skip_locked=True))
            events = list((await session.execute(statement)).scalars())
            for event in events:
                event.claimed_by = owner
                event.lease_until = now + timedelta(seconds=30)
                event.attempts += 1
            copies = [(event.event_id, event.topic, event.event_key, event.payload) for event in events]
        for event_id, topic, key, payload in copies:
            error: Exception | None = None
            try:
                if runtime.rocketmq_queue is not None:
                    await runtime.rocketmq_queue.publish(topic, payload, key, event_id)
                elif runtime.kafka_producer is not None:
                    await runtime.kafka_producer.send_and_wait(topic, key=key.encode(), value=payload)
                else:
                    runtime.local_event_broker.publish(topic, key, payload, event_id)
            except Exception as exc:
                error = exc
            async with runtime.database.sessions.begin() as session:
                event = await session.get(OutboxEvent, event_id, with_for_update=True)
                if event is None or event.claimed_by != owner:
                    continue
                if error is None:
                    event.published = True
                    event.claimed_by = None
                    event.lease_until = None
                    event.last_error = None
                else:
                    event.claimed_by = None
                    event.lease_until = datetime.utcnow() + timedelta(seconds=5)
                    event.last_error = str(error)[:1000]
            if error is not None:
                runtime.metrics["outbox_publish_errors"] += 1
                break
        if not copies:
            await asyncio.sleep(0.025)


async def expiry_loop(runtime) -> None:
    if not runtime.settings.mysql_mode:
        return
    owner = f"expiry-{socket.gethostname()}-{os.getpid()}"
    while True:
        now = datetime.utcnow()
        async with runtime.database.sessions.begin() as session:
            rows = (await session.execute(select(Order, OrderItem).join(OrderItem, OrderItem.order_id == Order.id)
                .where(Order.state == "PENDING_PAYMENT", Order.expires_at <= now,
                    or_(Order.expiry_lease_until.is_(None), Order.expiry_lease_until < now))
                .order_by(Order.expires_at, Order.id).limit(100).with_for_update(skip_locked=True))).all()
            for order, _item in rows:
                order.expiry_claimed_by = owner[:128]
                order.expiry_lease_until = now + timedelta(seconds=30)
            candidates = [(order.id, order.idempotency_key, order.user_id, item.sku_id)
                          for order, item in rows]
        for order_id, request_id, user_id, sku_id in candidates:
            try:
                order = await runtime.orders.transition(order_id, "EXPIRED")
            except ServiceFault:
                continue  # Payment or cancellation won the conditional transition.
            await runtime.statuses.put(RequestStatus(request_id, user_id, order_id, "EXPIRED",
                "payment window expired; inventory released"))
            try:
                await runtime.inventory.release(sku_id, order_id)
            except Exception:
                runtime.metrics["expiry_inventory_release_pending"] += 1
        await asyncio.sleep(1)


async def notification_loop(runtime) -> None:
    if runtime.kafka_producer is None:
        return
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer("order.events", bootstrap_servers=runtime.settings.kafka_brokers,
        group_id=runtime.settings.notification_group, enable_auto_commit=False, auto_offset_reset="earliest")
    await consumer.start()
    runtime.kafka_consumer = consumer
    try:
        while True:
            try:
                message = await consumer.getone()
                payload = (message.value or b"").decode(errors="replace")
                prefix = "OrderCreated|"
                if payload.startswith(prefix):
                    parts = payload.split("|", 3)
                    if len(parts) >= 3:
                        order_id, user_id = parts[1], parts[2]
                        await runtime.kafka_producer.send_and_wait(runtime.settings.notification_topic,
                            key=order_id.encode(), value=f"OrderConfirmation|{order_id}|{user_id}".encode())
                await consumer.commit()
            except asyncio.CancelledError:
                raise
            except Exception:
                # C++ logs the poll/handler error and keeps the fanout thread alive.
                # The failed record is not explicitly committed; a later successful
                # commit may advance the group position just as commit_message can.
                runtime.metrics["notification_fanout_errors"] += 1
                await asyncio.sleep(0.1)
    finally:
        await consumer.stop()
