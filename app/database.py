from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.engine import make_url

from .config import Settings
from .models import AdminAuditLog, Base, Campaign, Inventory, Product


class Database:
    def __init__(self, settings: Settings):
        self.settings = settings
        engine_options = {"pool_pre_ping": True}
        if settings.mysql_mode:
            engine_options.update(pool_size=16, max_overflow=16, isolation_level="READ COMMITTED")
        self.engine: AsyncEngine = create_async_engine(settings.database_url, **engine_options)
        url = make_url(settings.database_url)
        self.sqlite_path: Path | None = None
        if url.get_backend_name() == "sqlite" and url.database not in (None, "", ":memory:"):
            self.sqlite_path = Path(url.database).expanduser().resolve()
            event.listen(self.engine.sync_engine, "connect", self._configure_sqlite)
        self.snapshot_path: Path | None = None
        if self.sqlite_path is not None:
            configured_snapshot = settings.storage_snapshot_path
            self.snapshot_path = (Path(configured_snapshot).expanduser().resolve() if configured_snapshot
                                  else self.sqlite_path.with_name(self.sqlite_path.stem + ".snapshot.sqlite"))
            if self.snapshot_path == self.sqlite_path:
                raise ValueError("ECOMMERCE_STORAGE_SNAPSHOT must differ from the SQLite database path")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    @staticmethod
    def _configure_sqlite(connection, _record) -> None:
        cursor = connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=FULL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
        finally:
            cursor.close()

    async def initialize(self, seed: bool = True) -> None:
        if not self.settings.mysql_mode:
            async with self.engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
        else:
            # The C++ deployment initializes the business schema through schema.sql.
            # These two Python adapter tables hold local-gateway operation/admission
            # state when Redis is not selected; create_all leaves existing tables intact.
            async with self.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync_connection: Base.metadata.create_all(
                        sync_connection,
                        tables=[Base.metadata.tables["python_inventory_operations"],
                                Base.metadata.tables["python_campaign_admissions"]],
                    )
                )

        if not seed:
            return
        async with self.sessions.begin() as session:
            product = await session.get(Product, "sku-1")
            if product is None:
                session.add(Product(id="sku-1", name="C++ 高并发实战课程", price_cents=19900, on_sale=True))

            stock = await session.get(Inventory, "sku-1")
            inventory_mode = self.settings.inventory_mode or ("redis" if self.settings.redis_url else "local")
            initial_stock = self.settings.demo_stock if (self.settings.mysql_mode or inventory_mode == "redis") else 100
            if stock is None:
                stock = Inventory(sku_id="sku-1", available=initial_stock, reserved=0, sold=0, version=0)
                session.add(stock)

            campaign = await session.get(Campaign, "promo-1")
            if campaign is None:
                if self.settings.mysql_mode and stock.available < self.settings.demo_campaign_limit:
                    raise RuntimeError("campaign quota exceeds unallocated SKU stock")
                initial = f"sku-1:{self.settings.demo_campaign_limit}:0:0"
                campaign = Campaign(id="promo-1", sku_id="sku-1",
                    participant_limit=self.settings.demo_campaign_limit, state="CREATED", starts_at=0, ends_at=0)
                session.add(campaign)
                if self.settings.mysql_mode:
                    session.add(AdminAuditLog(actor="system", action="CAMPAIGN_CREATED", resource_type="campaign",
                        resource_id="promo-1", previous_value="", new_value=initial))
                campaign.state = "PREHEATING"
                if self.settings.mysql_mode:
                    session.add(AdminAuditLog(actor="system", action="CAMPAIGN_STATE_CHANGED", resource_type="campaign",
                        resource_id="promo-1", previous_value="CREATED", new_value="PREHEATING"))
                campaign.state = "RUNNING"
                if self.settings.mysql_mode:
                    session.add(AdminAuditLog(actor="system", action="CAMPAIGN_STATE_CHANGED", resource_type="campaign",
                        resource_id="promo-1", previous_value="PREHEATING", new_value="RUNNING"))

    async def ping(self) -> bool:
        async with self.engine.connect() as connection:
            await connection.execute(select(1))
        return True

    def snapshot(self) -> Path | None:
        """Create a consistent, atomically published SQLite backup snapshot."""
        if self.sqlite_path is None or self.snapshot_path is None or not self.sqlite_path.exists():
            return None
        destination = self.snapshot_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary_name = tempfile.mkstemp(prefix=destination.name + ".", suffix=".tmp",
                                                   dir=destination.parent)
        os.close(handle)
        temporary = Path(temporary_name)
        source_connection = None
        target_connection = None
        try:
            source_connection = sqlite3.connect(self.sqlite_path, timeout=30)
            target_connection = sqlite3.connect(temporary, timeout=30)
            source_connection.backup(target_connection, pages=128, sleep=0.01)
            integrity = target_connection.execute("PRAGMA integrity_check").fetchone()
            if not integrity or integrity[0] != "ok":
                raise RuntimeError("SQLite snapshot failed integrity_check")
            target_connection.close()
            target_connection = None
            source_connection.close()
            source_connection = None
            with temporary.open("rb+") as snapshot_file:
                os.fsync(snapshot_file.fileno())
            os.replace(temporary, destination)
            return destination
        finally:
            if target_connection is not None:
                target_connection.close()
            if source_connection is not None:
                source_connection.close()
            if temporary.exists():
                temporary.unlink()

    async def close(self) -> None:
        await self.engine.dispose()
