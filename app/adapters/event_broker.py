from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class LocalEvent:
    event_id: str
    topic: str
    key: str
    payload: bytes
    sequence: int


@dataclass(frozen=True)
class _Subscription:
    subscription_id: int
    handler: Callable[[LocalEvent], object]
    max_attempts: int
    dead_letter_topic: str


class LocalEventBroker:
    """Process-local event broker matching the C++ in-memory broker."""

    def __init__(self):
        self._next_sequence = 1
        self._next_subscription = 1
        self._events: dict[str, list[LocalEvent]] = {}
        self._subscriptions: dict[str, list[_Subscription]] = {}
        self._delivery_attempts: dict[tuple[str, int, str], int] = {}
        self._lock = threading.Lock()

    def publish(self, topic: str, key: str, payload: bytes, event_id: str = "") -> LocalEvent:
        if not topic:
            raise ValueError("topic must not be empty")
        with self._lock:
            sequence = self._next_sequence
            self._next_sequence += 1
            item = LocalEvent(event_id or f"{topic}-{sequence}", topic, key, bytes(payload), sequence)
            self._events.setdefault(topic, []).append(item)
        return item

    def subscribe(self, topic: str, handler: Callable[[LocalEvent], object],
                  max_attempts: int = 5, dead_letter_topic: str | None = None) -> int:
        if not topic or handler is None:
            raise ValueError("topic and event handler are required")
        with self._lock:
            subscription_id = self._next_subscription
            self._next_subscription += 1
            self._subscriptions.setdefault(topic, []).append(_Subscription(
                subscription_id, handler, max(1, max_attempts), dead_letter_topic or f"{topic}.DLQ"))
            return subscription_id

    def deliver(self, topic: str, max_events: int = 0) -> None:
        with self._lock:
            available = list(self._events.get(topic, ()))
            events = available if max_events == 0 else available[:max_events]
            subscriptions = list(self._subscriptions.get(topic, ()))
        if not subscriptions:
            raise RuntimeError("no consumer subscribed to topic")

        delivered: set[str] = set()
        dead_letters: list[tuple[LocalEvent, str, int]] = []
        failed = False
        for event in events:
            event_ok = True
            for subscription in subscriptions:
                attempt_key = (topic, subscription.subscription_id, event.event_id)
                try:
                    result = subscription.handler(event)
                    ok = result is not False
                except Exception:
                    ok = False
                if ok:
                    with self._lock:
                        self._delivery_attempts.pop(attempt_key, None)
                    continue
                failed = True
                with self._lock:
                    attempts = self._delivery_attempts.get(attempt_key, 0) + 1
                    if attempts >= subscription.max_attempts:
                        self._delivery_attempts.pop(attempt_key, None)
                    else:
                        self._delivery_attempts[attempt_key] = attempts
                if attempts >= subscription.max_attempts:
                    dead_letters.append((event, subscription.dead_letter_topic, attempts))
                else:
                    event_ok = False
            if event_ok:
                delivered.add(event.event_id)

        with self._lock:
            if delivered:
                self._events[topic] = [event for event in self._events.get(topic, ())
                                       if event.event_id not in delivered]
            for event, dead_letter_topic, attempts in dead_letters:
                sequence = self._next_sequence
                self._next_sequence += 1
                dead_letter = LocalEvent(
                    f"{dead_letter_topic}-{event.event_id}-{attempts}", dead_letter_topic,
                    event.key, event.payload, sequence)
                self._events.setdefault(dead_letter_topic, []).append(dead_letter)
        if failed:
            raise RuntimeError("message delivery failed; event retained or dead-lettered")

    def pending(self, topic: str) -> int:
        with self._lock:
            return len(self._events.get(topic, ()))

    def dead_letter_pending(self, topic: str) -> int:
        return self.pending(f"{topic}.DLQ")
