from __future__ import annotations

import asyncio
import hashlib
import os
import re
import threading
import time
from dataclasses import dataclass

from ..messages import FlashSaleMessage


@dataclass(frozen=True)
class RocketMqDlqMessage:
    message_id: str
    payload: bytes


def _physical_topic(logical_topic: str) -> str:
    if not logical_topic:
        raise ValueError("RocketMQ topic is required")
    physical = logical_topic.replace(".", "_")
    if re.fullmatch(r"[A-Za-z0-9_-]+", physical, flags=re.ASCII) is None:
        raise ValueError("RocketMQ topic contains unsupported characters")
    return physical


def _invisible_duration_ms() -> int:
    value = os.getenv("ECOMMERCE_ROCKETMQ_INVISIBLE_MS", "")
    try:
        parsed = int(value) if value else 120000
    except ValueError:
        return 120000
    return parsed if 1000 <= parsed <= 3600000 else 120000


class RocketMqOrderQueue:
    """RocketMQ 5.x adapter; blocking SDK calls run outside the ASGI loop."""

    def __init__(self, endpoint: str, command_topic: str, consumer_group: str,
                 dlq_topic: str | None = None, *, enable_consumer: bool,
                 enable_dlq_operations: bool):
        self.endpoint = endpoint
        self.command_logical_topic = command_topic
        self.command_topic = _physical_topic(command_topic)
        self.dlq_topic = _physical_topic(dlq_topic or f"{command_topic}.dlq")
        self.consumer_group = consumer_group
        self.dlq_group = consumer_group[:38] + "-dlq-" + hashlib.sha256(
            consumer_group.encode()).hexdigest()[:16]
        self.enable_consumer = enable_consumer
        self.enable_dlq_operations = enable_dlq_operations
        self.tls = os.getenv("ECOMMERCE_ROCKETMQ_TLS") not in {"0", "false", "off"}
        self.invisible_ms = _invisible_duration_ms()
        self._producer = None
        self._dlq_producer = None
        self._consumer = None
        self._dlq_consumer = None
        self._delivery_by_order: dict[str, object] = {}
        self._dlq_deliveries: dict[str, tuple[object, float]] = {}
        self._delivery_lock = threading.Lock()
        self._dlq_client_lock = threading.Lock()
        self._dlq_operation_lock = asyncio.Lock()

    def start(self) -> None:
        if not self.endpoint or not self.command_topic or not self.consumer_group:
            raise ValueError("RocketMQ endpoint, topic and consumer group are required")
        try:
            from rocketmq import ClientConfiguration, Credentials, FilterExpression, Producer, SimpleConsumer
        except ImportError as exc:
            raise RuntimeError(
                "RocketMQ mode requires rocketmq-python-client 5.1.2 or newer"
            ) from exc

        configuration = ClientConfiguration(self.endpoint, Credentials())
        event_topics = ("order_events", "inventory_events", "notification_events")
        producer_topics = tuple(dict.fromkeys((self.command_topic, self.dlq_topic, *event_topics)))
        self._producer = Producer(configuration, producer_topics, tls_enable=self.tls)
        self._dlq_producer = Producer(configuration, (self.dlq_topic, *event_topics), tls_enable=self.tls)
        self._producer.startup()
        self._dlq_producer.startup()
        if self.enable_consumer:
            self._consumer = SimpleConsumer(
                configuration, self.consumer_group,
                {self.command_topic: FilterExpression("*")},
                await_duration=10, tls_enable=self.tls,
            )
            self._consumer.startup()

    @staticmethod
    def _message(topic: str, payload: bytes, tag: str, key: str):
        from rocketmq import Message

        message = Message()
        message.topic = topic
        message.body = payload
        message.tag = tag
        message.keys = key
        return message

    @staticmethod
    def _send_sync(producer, message) -> None:
        # The official SDK exposes send_async; waiting on its future preserves
        # the C++ adapter's wait-for-broker-ACK behavior.
        producer.send_async(message).result()

    async def push(self, message: FlashSaleMessage) -> None:
        payload = message.encode()
        request = self._message(self.command_topic, payload, "FlashSaleCommand", message.idempotency_key)
        await asyncio.to_thread(self._send_sync, self._producer, request)

    async def pop(self) -> FlashSaleMessage:
        if self._consumer is None:
            raise RuntimeError("RocketMQ consumer is not ready")
        messages = await asyncio.to_thread(
            self._consumer.receive, 1, max(1, self.invisible_ms // 1000))
        if not messages:
            raise asyncio.TimeoutError
        delivered = messages[0]
        message = FlashSaleMessage.decode(delivered.body)
        with self._delivery_lock:
            self._delivery_by_order[message.order_id] = delivered
        return message

    async def acknowledge(self, message: FlashSaleMessage, processed: bool) -> None:
        if self._consumer is None:
            raise RuntimeError("RocketMQ consumer is not ready")
        if not processed:
            # Leave the lease unacknowledged so the broker redelivers it.
            return
        with self._delivery_lock:
            delivered = self._delivery_by_order.get(message.order_id)
        if delivered is None:
            raise LookupError("RocketMQ delivery handle not found")
        await asyncio.to_thread(self._consumer.ack, delivered)
        with self._delivery_lock:
            self._delivery_by_order.pop(message.order_id, None)

    async def dead_letter(self, payload: bytes) -> None:
        message = FlashSaleMessage.decode(payload)
        request = self._message(self.dlq_topic, payload, "FlashSaleCommand", message.idempotency_key)
        await asyncio.to_thread(self._send_sync, self._dlq_producer, request)

    async def publish(self, topic: str, payload: bytes, key: str, event_id: str) -> None:
        physical_topic = _physical_topic(topic)
        request = self._message(physical_topic, payload, topic,
                                event_id or key)
        await asyncio.to_thread(self._send_sync, self._producer, request)

    def _ensure_dlq_consumer(self):
        if not self.enable_dlq_operations:
            raise RuntimeError("RocketMQ DLQ operations are not enabled")
        if self._dlq_consumer is not None:
            return self._dlq_consumer
        with self._dlq_client_lock:
            if self._dlq_consumer is not None:
                return self._dlq_consumer
            from rocketmq import ClientConfiguration, Credentials, FilterExpression, SimpleConsumer

            consumer = SimpleConsumer(
                ClientConfiguration(self.endpoint, Credentials()), self.dlq_group,
                {self.dlq_topic: FilterExpression("*")},
                await_duration=10, tls_enable=self.tls,
            )
            consumer.startup()
            self._dlq_consumer = consumer
            return consumer

    def _receive_dlq(self, count: int, invisible_seconds: int) -> list[object]:
        consumer = self._ensure_dlq_consumer()
        return consumer.receive(count, invisible_seconds) or []

    @staticmethod
    def _message_id(delivered) -> str:
        value = getattr(delivered, "message_id", "")
        return str(value)

    @staticmethod
    def _message_body(delivered) -> bytes:
        body = delivered.body
        return body if isinstance(body, bytes) else str(body).encode()

    def _collect_dlq(self, limit: int) -> list[RocketMqDlqMessage]:
        delivered = self._receive_dlq(min(limit, 32), 30)
        now = time.monotonic()
        for item in delivered:
            message_id = self._message_id(item)
            if len(self._dlq_deliveries) >= 1000 and message_id not in self._dlq_deliveries:
                continue
            self._dlq_deliveries[message_id] = (item, now)
        result: list[RocketMqDlqMessage] = []
        for message_id in sorted(self._dlq_deliveries):
            if len(result) >= limit:
                break
            item, _received_at = self._dlq_deliveries[message_id]
            result.append(RocketMqDlqMessage(message_id, self._message_body(item)))
        return result

    async def inspect_dlq(self, limit: int) -> list[RocketMqDlqMessage]:
        if limit <= 0:
            raise ValueError("DLQ output and positive limit are required")
        async with self._dlq_operation_lock:
            return await asyncio.to_thread(self._collect_dlq, min(limit, 1000))

    def _replay_dlq(self, limit: int) -> int:
        delivered = self._receive_dlq(min(limit, 32), 30)
        now = time.monotonic()
        for item in delivered:
            message_id = self._message_id(item)
            if len(self._dlq_deliveries) < 1000 or message_id in self._dlq_deliveries:
                self._dlq_deliveries[message_id] = (item, now)
        replayed = 0
        consumer = self._dlq_consumer
        for message_id in sorted(self._dlq_deliveries):
            if replayed >= limit:
                break
            item, received_at = self._dlq_deliveries[message_id]
            if time.monotonic() - received_at >= 25:
                continue
            payload = self._message_body(item)
            try:
                command = FlashSaleMessage.decode(payload)
            except ValueError:
                continue
            request = self._message(self.command_topic, payload, "FlashSaleCommand",
                                    command.idempotency_key)
            self._send_sync(self._producer, request)
            consumer.ack(item)
            self._dlq_deliveries.pop(message_id, None)
            replayed += 1
        return replayed

    async def replay_dlq(self, limit: int) -> int:
        if limit <= 0:
            raise ValueError("DLQ replay output and positive limit are required")
        async with self._dlq_operation_lock:
            return await asyncio.to_thread(self._replay_dlq, min(limit, 1000))

    async def pending(self) -> int | None:
        # RocketMQ does not expose a non-mutating queue depth in this adapter.
        return None

    async def close(self) -> None:
        for client in (self._dlq_consumer, self._consumer, self._dlq_producer, self._producer):
            if client is not None:
                await asyncio.to_thread(client.shutdown)
        self._dlq_consumer = self._consumer = self._dlq_producer = self._producer = None
