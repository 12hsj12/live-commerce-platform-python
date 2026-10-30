from __future__ import annotations

import asyncio
from collections import Counter

from .adapters.admission import AdmissionService, LocalTokenBucket
from .adapters.cache import AsyncShardedCache
from .adapters.event_broker import LocalEventBroker
from .adapters.inventory import InventoryGateway
from .adapters.queue import LocalDeadLetterQueue, LocalOrderQueue, RedisOrderQueue
from .config import Settings
from .database import Database
from .services.auth import AuthService
from .services.commerce import FlashSaleService, OrderService, PromotionService
from .services.product import ProductService
from .services.request_status import RequestStatusStore
from .storage import LocalSnapshotWorker


class Runtime:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings.from_env()
        self.database = Database(self.settings)
        self.snapshot_worker = LocalSnapshotWorker(self.database, self.settings.snapshot_interval_seconds)
        self.redis = None
        self.kafka_producer = None
        self.kafka_consumer = None
        self.kafka_dlq = None
        self.rocketmq_queue = None
        self.local_event_broker = LocalEventBroker()
        self.inventory: InventoryGateway | None = None
        self.admission: AdmissionService | None = None
        self.queue = None
        self.dead_letter_queue = None
        self.statuses: RequestStatusStore | None = None
        self.ingress_statuses: RequestStatusStore | None = None
        self._local_status_state = {}
        self._local_status_lock = asyncio.Lock()
        self.auth: AuthService | None = None
        self.promotions: PromotionService | None = None
        self.flash_sale: FlashSaleService | None = None
        self.orders: OrderService | None = None
        self.products: ProductService | None = None
        self.agent = None
        self.product_cache = AsyncShardedCache(16, 1024)
        self.global_limiter: LocalTokenBucket | None = None
        self.metrics: Counter[str] = Counter()
        self._redis_owns_fast_inventory = False
        self.inventory_mode = "local"
        self.queue_mode = "local"

    async def start(self) -> None:
        if self.settings.process_role not in {"all", "gateway", "worker"}:
            raise RuntimeError("ECOMMERCE_PROCESS_ROLE must be all, gateway or worker")
        await self.database.initialize(seed=self.settings.process_role != "worker")
        if self.settings.redis_url:
            from redis.asyncio import from_url

            self.redis = from_url(self.settings.redis_url, decode_responses=False, health_check_interval=30)
            await self.redis.ping()
        inventory_mode = self.settings.inventory_mode or ("redis" if self.redis is not None else "local")
        if inventory_mode not in {"local", "redis"}:
            raise RuntimeError("ECOMMERCE_INVENTORY_MODE must be local or redis")
        if inventory_mode == "redis" and self.redis is None:
            raise RuntimeError("ECOMMERCE_INVENTORY_MODE=redis requires a configured Redis connection")
        self._redis_owns_fast_inventory = inventory_mode == "redis"
        self.inventory_mode = inventory_mode

        queue_mode = self.settings.queue_mode or ("redis" if inventory_mode == "redis" else "local")
        if queue_mode not in {"redis", "local", "rocketmq"}:
            raise RuntimeError("ECOMMERCE_FLASH_SALE_QUEUE must be redis, rocketmq or local")
        if self.settings.process_role != "all":
            if queue_mode == "local":
                raise RuntimeError("split gateway/worker roles require a shared Redis command queue")
            if self.redis is None:
                raise RuntimeError("split gateway/worker roles require Redis for shared request state and admission")
            if not self.settings.mysql_mode:
                raise RuntimeError("split gateway/worker roles require MySQL for shared order persistence")
        self.queue_mode = queue_mode
        if queue_mode == "rocketmq":
            if not self.settings.rocketmq_endpoint:
                raise RuntimeError("ECOMMERCE_ROCKETMQ_ENDPOINT is required for RocketMQ queue mode")
            from .adapters.rocketmq_queue import RocketMqOrderQueue

            self.rocketmq_queue = RocketMqOrderQueue(
                self.settings.rocketmq_endpoint,
                self.settings.rocketmq_command_topic,
                self.settings.rocketmq_consumer_group,
                self.settings.rocketmq_dlq_topic,
                enable_consumer=self.settings.process_role in {"all", "worker"},
                enable_dlq_operations=self.settings.process_role in {"all", "gateway"},
            )
            await asyncio.to_thread(self.rocketmq_queue.start)
            self.queue = self.rocketmq_queue
            self.dead_letter_queue = self.rocketmq_queue
        elif queue_mode == "redis":
            if inventory_mode != "redis" or self.redis is None:
                raise RuntimeError("Redis queue requires Redis inventory mode and connection")
            self.queue = RedisOrderQueue(self.redis, self.settings.command_queue_key, dlq_key=self.settings.dlq_queue_key)
            self.dead_letter_queue = self.queue
        else:
            self.queue = LocalOrderQueue()
            self.dead_letter_queue = LocalDeadLetterQueue()

        self.inventory = InventoryGateway(self.database, self.redis if self._redis_owns_fast_inventory else None)
        initial_stock = self.settings.demo_stock if (self.settings.mysql_mode or self._redis_owns_fast_inventory) else 100
        await self.inventory.initialize("sku-1", initial_stock)
        self.admission = AdmissionService(self.database, self.redis if self._redis_owns_fast_inventory else None,
                                          rate_redis=self.redis)
        async with self.database.sessions() as session:
            from sqlalchemy import select
            from .models import Campaign

            campaigns = list((await session.execute(select(Campaign))).scalars())
        for campaign in campaigns:
            await self.admission.configure(campaign)

        self.statuses = RequestStatusStore(self.database, self.redis, durable=self.settings.mysql_mode,
            local_state=self._local_status_state, local_lock=self._local_status_lock)
        self.ingress_statuses = RequestStatusStore(self.database, self.redis, durable=self.settings.mysql_mode,
            write_durable=self.settings.mysql_mode and self.redis is None,
            local_state=self._local_status_state, local_lock=self._local_status_lock)
        self.auth = AuthService(self.database, self.redis)
        self.promotions = PromotionService(self.database, self.inventory, self.admission)
        self.flash_sale = FlashSaleService(self.database, self.queue, self.admission,
                                           self.ingress_statuses, self.promotions)
        self.orders = OrderService(self.database, self.inventory, self.statuses)
        self.products = ProductService(self.database, self.redis, self.product_cache)
        from .agent.service import AgentService
        from .agent.providers import build_provider

        self.agent = AgentService(
            provider=build_provider(self.settings),
            products=self.products,
            promotions=self.promotions,
            inventory=self.inventory,
            orders=self.orders,
            timeout_seconds=self.settings.agent_timeout_seconds,
            max_tool_rounds=self.settings.agent_max_tool_rounds,
        )
        self.global_limiter = LocalTokenBucket(self.settings.global_rate, self.settings.global_burst)

        if self.settings.kafka_brokers:
            from aiokafka import AIOKafkaProducer
            from .adapters.kafka_dlq import KafkaDlqOperator

            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.kafka_brokers,
                client_id="ecommerce-api",
                acks="all",
                enable_idempotence=True,
                request_timeout_ms=5000,
            )
            await self.kafka_producer.start()
            self.kafka_dlq = KafkaDlqOperator(self.settings.kafka_brokers)
        self.metrics["runtime_started"] += 1
        if self.settings.process_role in {"all", "gateway"}:
            await self.snapshot_worker.start()

    async def check_ready(self) -> list[str]:
        unavailable: list[str] = []
        try:
            await self.database.ping()
        except Exception:
            unavailable.append("mysql" if self.settings.mysql_mode else "database")
        if self.redis is not None:
            try:
                await self.redis.ping()
            except Exception:
                unavailable.append("redis")
        return unavailable

    async def close(self) -> None:
        if self.kafka_consumer is not None:
            await self.kafka_consumer.stop()
        if self.kafka_producer is not None:
            await self.kafka_producer.stop()
        if self.redis is not None:
            await self.redis.aclose()
        if self.rocketmq_queue is not None:
            await self.rocketmq_queue.close()
        await self.snapshot_worker.stop()
        await self.database.close()
