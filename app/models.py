from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, LargeBinary, BigInteger, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="customer")
    password_hash: Mapped[bytes] = mapped_column(LargeBinary(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Product(Base):
    __tablename__ = "products"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    price_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    on_sale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class Inventory(Base):
    __tablename__ = "inventory"
    sku_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    available: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reserved: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    sold: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)


class InventoryOperation(Base):
    """Local inventory reservation journal; Redis mode uses the C++ compatible Lua keys."""
    __tablename__ = "python_inventory_operations"
    operation_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    sku_id: Mapped[str] = mapped_column(String(64), nullable=False)
    quantity: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)


class Campaign(Base):
    __tablename__ = "campaigns"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    sku_id: Mapped[str] = mapped_column(String(64), nullable=False)
    participant_limit: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    starts_at: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    ends_at: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class CampaignAdmission(Base):
    __tablename__ = "python_campaign_admissions"
    campaign_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)


class LegacyKVRecord(Base):
    """Unmapped C++ LocalKV bytes preserved with a redacted classification."""
    __tablename__ = "legacy_kv_records"
    key: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    value: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="local_kv")
    reason: Mapped[str] = mapped_column(String(64), nullable=False, default="unknown_keyspace")


class AdminAuditLog(Base):
    __tablename__ = "admin_audit_logs"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(128), nullable=False)
    previous_value: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    new_value: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    campaign_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    expiry_claimed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    expiry_lease_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class OrderItem(Base):
    __tablename__ = "order_items"
    order_id: Mapped[str] = mapped_column(String(64), ForeignKey("orders.id"), primary_key=True)
    sku_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    quantity: Mapped[int] = mapped_column(BigInteger, nullable=False)
    unit_price_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    event_key: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claimed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Payment(Base):
    __tablename__ = "payments"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(64), ForeignKey("orders.id"), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_transaction_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    amount_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class FlashSaleRequest(Base):
    __tablename__ = "flash_sale_requests"
    request_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    order_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    message: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class CppMigrationBatch(Base):
    """Idempotency marker and aggregate-only report for a C++ data import."""
    __tablename__ = "cpp_migration_batches"
    source_fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_summary: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


Index("idx_campaigns_state_updated", Campaign.state, Campaign.updated_at)
Index("idx_campaigns_sku_state", Campaign.sku_id, Campaign.state)
Index("idx_admin_audit_resource", AdminAuditLog.resource_type, AdminAuditLog.resource_id, AdminAuditLog.created_at)
Index("idx_admin_audit_actor", AdminAuditLog.actor, AdminAuditLog.created_at)
Index("idx_orders_expiry", Order.state, Order.expires_at)
Index("idx_orders_expiry_lease", Order.state, Order.expires_at, Order.expiry_lease_until)
Index("idx_orders_campaign_id", Order.campaign_id)
Index("idx_outbox_pending", OutboxEvent.published, OutboxEvent.created_at, OutboxEvent.event_id)
