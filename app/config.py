from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _positive_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _positive_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


@dataclass(frozen=True)
class Settings:
    host: str = "0.0.0.0"
    port: int = 8080
    process_role: str = "all"
    database_url: str = "sqlite+aiosqlite:///./python-commerce.db"
    storage_snapshot_path: Optional[str] = None
    snapshot_interval_seconds: int = 30
    redis_url: Optional[str] = None
    inventory_mode: Optional[str] = None
    queue_mode: Optional[str] = None
    kafka_brokers: Optional[str] = None
    kafka_flash_sale_topic: str = "flash-sale-orders"
    rocketmq_endpoint: Optional[str] = None
    rocketmq_command_topic: str = "flashsale.command"
    rocketmq_consumer_group: str = "flashsale-command-consumers"
    rocketmq_dlq_topic: Optional[str] = None
    notification_topic: str = "notification.events"
    notification_group: str = "ecommerce-notification-fanout"
    command_queue_key: str = "flash_orders:{promo-1}"
    dlq_queue_key: str = "flash_orders_dlq:{promo-1}"
    admin_token: Optional[str] = None
    admin_credentials: str = ""
    payment_callback_secret: Optional[str] = None
    demo_stock: int = 100
    demo_campaign_limit: int = 100
    consumer_workers: int = 4
    local_queue_capacity: int = 0
    waiting_room_enabled: bool = True
    waiting_capacity: int = 100000
    waiting_workers: int = 4
    trusted_proxy_ips: tuple[str, ...] = ()
    global_rate: float = 10000.0
    global_burst: float = 2000.0
    campaign_rate: float = 10000.0
    campaign_burst: float = 2000.0
    user_rate: float = 2.0
    user_burst: float = 5.0
    ip_rate: float = 200.0
    ip_burst: float = 500.0
    consumer_max_attempts: int = 5
    mysql_mode: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        database_url = os.getenv("ECOMMERCE_DATABASE_URL")
        mysql_host = os.getenv("ECOMMERCE_MYSQL_HOST")
        mysql_hosts = os.getenv("ECOMMERCE_MYSQL_HOSTS")
        mysql_mode = bool(mysql_host or mysql_hosts)
        if database_url:
            mysql_mode = database_url.startswith("mysql+")
        elif mysql_mode:
            host = (mysql_hosts or mysql_host or "").split(",", 1)[0].strip()
            port = _positive_int("ECOMMERCE_MYSQL_PORT", 3306)
            user = os.getenv("ECOMMERCE_MYSQL_USER", "ecommerce")
            password = os.getenv("ECOMMERCE_MYSQL_PASSWORD", "ecommerce")
            database = os.getenv("ECOMMERCE_MYSQL_DATABASE", "ecommerce")
            from urllib.parse import quote_plus

            database_url = (
                f"mysql+asyncmy://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/"
                f"{quote_plus(database)}?charset=utf8mb4"
            )
        else:
            database_url = "sqlite+aiosqlite:///./python-commerce.db"

        redis_host = os.getenv("ECOMMERCE_REDIS_HOST")
        redis_url = os.getenv("ECOMMERCE_REDIS_URL")
        if redis_url is None and redis_host:
            redis_port = _positive_int("ECOMMERCE_REDIS_PORT", 6379)
            redis_db = os.getenv("ECOMMERCE_REDIS_DB", "0")
            redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

        return cls(
            host=os.getenv("ECOMMERCE_HOST", "0.0.0.0"),
            port=_positive_int("ECOMMERCE_PORT", 8080),
            process_role=os.getenv("ECOMMERCE_PROCESS_ROLE", "all"),
            database_url=database_url,
            storage_snapshot_path=os.getenv("ECOMMERCE_STORAGE_SNAPSHOT") or None,
            snapshot_interval_seconds=_positive_int("ECOMMERCE_SNAPSHOT_INTERVAL_SECONDS", 30),
            redis_url=redis_url,
            inventory_mode=os.getenv("ECOMMERCE_INVENTORY_MODE"),
            queue_mode=os.getenv("ECOMMERCE_FLASH_SALE_QUEUE"),
            kafka_brokers=os.getenv("ECOMMERCE_KAFKA_BROKERS") or None,
            kafka_flash_sale_topic=os.getenv("ECOMMERCE_FLASH_SALE_TOPIC", "flash-sale-orders"),
            rocketmq_endpoint=os.getenv("ECOMMERCE_ROCKETMQ_ENDPOINT") or None,
            rocketmq_command_topic=os.getenv("ECOMMERCE_ROCKETMQ_COMMAND_TOPIC", "flashsale.command"),
            rocketmq_consumer_group=os.getenv("ECOMMERCE_ROCKETMQ_CONSUMER_GROUP", "flashsale-command-consumers"),
            rocketmq_dlq_topic=os.getenv("ECOMMERCE_ROCKETMQ_DLQ_TOPIC") or None,
            notification_topic=os.getenv("ECOMMERCE_NOTIFICATION_TOPIC", "notification.events"),
            notification_group=os.getenv("ECOMMERCE_NOTIFICATION_GROUP", "ecommerce-notification-fanout"),
            admin_token=os.getenv("ECOMMERCE_ADMIN_TOKEN") or None,
            admin_credentials=os.getenv("ECOMMERCE_ADMIN_CREDENTIALS", ""),
            payment_callback_secret=os.getenv("ECOMMERCE_PAYMENT_CALLBACK_SECRET") or None,
            demo_stock=_positive_int("ECOMMERCE_DEMO_STOCK", 100),
            demo_campaign_limit=_positive_int("ECOMMERCE_DEMO_CAMPAIGN_LIMIT", 100),
            consumer_workers=_positive_int("ECOMMERCE_FLASH_CONSUMER_WORKERS", 4),
            local_queue_capacity=int(os.getenv("ECOMMERCE_HTTP_QUEUE_CAPACITY", "0")),
            waiting_room_enabled=os.getenv("ECOMMERCE_FLASH_SALE_WAITING_ROOM", "true").lower() not in {"0", "false"},
            waiting_capacity=_positive_int("ECOMMERCE_FLASH_SALE_WAITING_CAPACITY", 100000),
            waiting_workers=_positive_int("ECOMMERCE_FLASH_SALE_WAITING_WORKERS", 4),
            trusted_proxy_ips=tuple(x.strip() for x in os.getenv("ECOMMERCE_TRUSTED_PROXY_IPS", "").split(",") if x.strip()),
            global_rate=_positive_float("ECOMMERCE_FLASH_SALE_RATE_PER_SECOND", 10000.0),
            global_burst=_positive_float("ECOMMERCE_FLASH_SALE_BURST", 2000.0),
            campaign_rate=_positive_float("ECOMMERCE_FLASH_SALE_CAMPAIGN_RATE_PER_SECOND", 10000.0),
            campaign_burst=_positive_float("ECOMMERCE_FLASH_SALE_CAMPAIGN_BURST", 2000.0),
            user_rate=_positive_float("ECOMMERCE_FLASH_SALE_USER_RATE_PER_SECOND", 2.0),
            user_burst=_positive_float("ECOMMERCE_FLASH_SALE_USER_BURST", 5.0),
            ip_rate=_positive_float("ECOMMERCE_FLASH_SALE_IP_RATE_PER_SECOND", 200.0),
            ip_burst=_positive_float("ECOMMERCE_FLASH_SALE_IP_BURST", 500.0),
            mysql_mode=mysql_mode,
        )
