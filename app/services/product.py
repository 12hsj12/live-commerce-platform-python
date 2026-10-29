from __future__ import annotations

from dataclasses import dataclass

from ..adapters.cache import AsyncShardedCache
from ..models import Product


@dataclass(frozen=True)
class ProductView:
    id: str
    name: str
    price_cents: int
    on_sale: bool


class ProductService:
    def __init__(self, database, redis, cache: AsyncShardedCache):
        self.database = database
        self.redis = redis
        self.cache = cache

    @staticmethod
    def _serialize(product: Product) -> str:
        return f"{product.id}\t{product.name}\t{product.price_cents}\t{1 if product.on_sale else 0}"

    @staticmethod
    def _deserialize(value: str) -> ProductView:
        fields = value.split("\t")
        if len(fields) < 4 or not fields[0] or not fields[1]:
            raise ValueError("invalid cached product")
        try:
            price_cents = int(fields[2])
            on_sale = int(fields[3]) != 0
        except ValueError as exc:
            raise ValueError("invalid cached product") from exc
        return ProductView(fields[0], fields[1], price_cents, on_sale)

    async def _load(self, product_id: str) -> str:
        redis_key = f"product:{product_id}"
        if self.redis is not None:
            cached = await self.redis.get(redis_key)
            if cached is not None:
                return cached.decode() if isinstance(cached, bytes) else str(cached)
        async with self.database.sessions() as session:
            product = await session.get(Product, product_id)
        if product is None:
            raise LookupError("product not found")
        encoded = self._serialize(product)
        if self.redis is not None:
            await self.redis.set(redis_key, encoded.encode(), ex=24 * 60 * 60)
        return encoded

    async def get(self, product_id: str) -> ProductView:
        if not product_id:
            raise ValueError("invalid product query")
        encoded = await self.cache.get_or_load(product_id, lambda: self._load(product_id))
        return self._deserialize(encoded)
