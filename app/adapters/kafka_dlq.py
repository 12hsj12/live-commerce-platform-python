from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaDlqEvent:
    topic: str
    event_id: str
    key: bytes | None
    payload: bytes


class KafkaDlqOperator:
    """Per-request Kafka DLQ reader/replayer matching KafkaDlqOperator in C++."""

    def __init__(self, brokers: str):
        self.brokers = brokers

    async def _consumer(self, topic: str, operation: str):
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.brokers,
            group_id=f"dlq-{operation}-{uuid.uuid4().hex}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        return consumer

    @staticmethod
    def _event(record) -> KafkaDlqEvent:
        return KafkaDlqEvent(record.topic, f"{record.topic}-{record.offset}", record.key, record.value or b"")

    async def peek(self, topic: str, max_messages: int) -> list[KafkaDlqEvent]:
        if not self.brokers or not topic or max_messages <= 0:
            raise ValueError("invalid DLQ inspection arguments")
        consumer = await self._consumer(topic, "inspect")
        events: list[KafkaDlqEvent] = []
        last_message = time.monotonic()
        try:
            while len(events) < max_messages and time.monotonic() - last_message < 2.0:
                batches = await consumer.getmany(timeout_ms=250, max_records=max_messages - len(events))
                for records in batches.values():
                    for record in records:
                        events.append(self._event(record))
                        last_message = time.monotonic()
                        if len(events) >= max_messages:
                            break
                    if len(events) >= max_messages:
                        break
            return events
        finally:
            await consumer.stop()

    async def replay(self, dlq_topic: str, target_topic: str, max_messages: int) -> int:
        if not self.brokers or not dlq_topic or not target_topic or max_messages <= 0:
            raise ValueError("invalid DLQ replay arguments")
        from aiokafka import AIOKafkaProducer
        from aiokafka.structs import OffsetAndMetadata, TopicPartition

        consumer = await self._consumer(dlq_topic, "replay")
        producer = AIOKafkaProducer(
            bootstrap_servers=self.brokers,
            client_id="dlq-replay-producer",
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=5000,
        )
        replayed = 0
        last_message = time.monotonic()
        try:
            await producer.start()
            while replayed < max_messages and time.monotonic() - last_message < 2.0:
                batches = await consumer.getmany(timeout_ms=250, max_records=max_messages - replayed)
                for partition, records in batches.items():
                    for record in records:
                        # C++ sets a replay event_id, but KafkaProducer publishes only
                        # the original key and payload, so the wire data stays intact.
                        await producer.send_and_wait(target_topic, key=record.key, value=record.value)
                        await consumer.commit({
                            TopicPartition(record.topic, record.partition):
                                OffsetAndMetadata(record.offset + 1, "")
                        })
                        replayed += 1
                        last_message = time.monotonic()
                        if replayed >= max_messages:
                            break
                    if replayed >= max_messages:
                        break
            return replayed
        finally:
            await producer.stop()
            await consumer.stop()
