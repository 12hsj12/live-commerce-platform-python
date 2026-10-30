from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import struct
import zlib
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import select

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import (
    AdminAuditLog,
    Campaign,
    FlashSaleRequest,
    Inventory,
    InventoryOperation,
    LegacyKVRecord,
    Order,
    OrderItem,
    OutboxEvent,
    Payment,
    Product,
    User,
    Base,
)
from app.security import verify_password
from app.storage.cpp_business_migration import (
    LOCAL_ORDER_CREATED_AT,
    LOCAL_ORDER_EXPIRES_AT,
    MigrationConflict,
    build_migration_plan,
    verify_password_hash_fixture,
)
from app.storage.cpp_local_kv import SNAPSHOT_MAGIC
from app.storage.cpp_outbox import CorruptCppOutbox, OUTBOX_MAGIC, decode_outbox
from tools import import_cpp_business as migration_cli
from tools.import_cpp_business import _run, apply_plan


def _test_database(url: str):
    engine = create_async_engine(url)

    def enable_foreign_keys(connection, _record) -> None:
        cursor = connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
        finally:
            cursor.close()

    event.listen(engine.sync_engine, "connect", enable_foreign_keys)
    return engine, async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def _create_test_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)


def _password_hash(password: str = "fixture-only-password") -> bytes:
    salt = bytes(range(16))
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 210_000, dklen=32)
    return f"pbkdf2-sha256$210000${salt.hex()}${digest.hex()}".encode("ascii")


def _field(value: bytes) -> bytes:
    return struct.pack("<I", len(value)) + value


def _obx2(event_id: bytes, topic: bytes, key: bytes, body: bytes, sequence: int) -> bytes:
    payload = _field(event_id) + _field(topic) + _field(key) + _field(body) + struct.pack("<Q", sequence)
    return struct.pack("<II", OUTBOX_MAGIC, len(payload)) + payload + struct.pack("<I", zlib.crc32(payload))


def _snapshot(records: dict[bytes, bytes]) -> bytes:
    payload = struct.pack("<Q", len(records))
    for key, value in records.items():
        payload += struct.pack("<II", len(key), len(value)) + key + value
    return struct.pack("<I", SNAPSHOT_MAGIC) + payload + struct.pack("<I", zlib.crc32(payload))


def _local_records() -> dict[bytes, bytes]:
    return {
        b"users/user-1": b"user-1\talice\t" + _password_hash().decode().encode() + b"\t0\t1",
        b"products/sku-1": b"sku-1\tCourse\t19900\t1",
        b"inventory/stock/sku-1": b"7|1|2",
        b"inventory/reservation/order-1": b"sku-1|1|0",
        b"inventory/reservation/sold-op": b"sku-1|2|1",
        b"orders/order-1": b"order-1\tidem-1\tuser-1\tsku-1\t1\t0",
        b"unknown/opaque": b"\xff\x00secret-like-bytes",
    }


def _mysql_rows() -> dict[str, list[dict]]:
    created = datetime(2024, 1, 2, 3, 4, 5)
    return {
        "users": [{"id": "user-2", "username": "bob", "role": "customer",
                   "password_hash": _password_hash(), "enabled": True, "created_at": created}],
        "products": [{"id": "sku-2", "name": "Book", "price_cents": 2500, "on_sale": True,
                      "version": 4, "updated_at": created}],
        "inventory": [{"sku_id": "sku-2", "available": 5, "reserved": 1, "sold": 0, "version": 8}],
        "campaigns": [{"id": "promo-2", "sku_id": "sku-2", "participant_limit": 10,
                       "state": "RUNNING", "starts_at": 1700000000, "ends_at": 1800000000,
                       "created_at": created, "updated_at": created}],
        "admin_audit_logs": [{"id": 7, "actor": "admin", "action": "CAMPAIGN_CREATED",
                              "resource_type": "campaign", "resource_id": "promo-2",
                              "previous_value": "", "new_value": "RUNNING", "created_at": created}],
        "orders": [{"id": "order-2", "user_id": "user-2", "campaign_id": "promo-2",
                    "idempotency_key": "idem-2", "state": "PENDING_PAYMENT",
                    "expires_at": created, "expiry_claimed_by": None, "expiry_lease_until": None,
                    "created_at": created}],
        "order_items": [{"order_id": "order-2", "sku_id": "sku-2", "quantity": 1,
                         "unit_price_cents": 2500}],
        "outbox_events": [{"event_id": "event-2", "topic": "order.events", "event_key": "order-2",
                           "payload": b"OrderCreated|order-2", "published": False, "attempts": 3,
                           "claimed_by": None, "lease_until": None, "last_error": "retry", "created_at": created}],
        "payments": [{"id": "payment-2", "order_id": "order-2", "provider": "demo",
                      "provider_transaction_id": "txn-2", "amount_cents": 2500,
                      "state": "CREATED", "created_at": created, "updated_at": created}],
        "flash_sale_requests": [{"request_id": "request-2", "user_id": "user-2", "order_id": "order-2",
                                 "state": "PENDING_PAYMENT", "message": "created", "updated_at": created}],
    }


def test_cpp_user_password_format_is_compatible_with_python_verifier() -> None:
    assert verify_password_hash_fixture()
    assert verify_password("fixture-only-password", _password_hash())


def test_local_kv_conversion_maps_active_rows_and_archives_only_unknown_records() -> None:
    plan = build_migration_plan(kv_records=_local_records())
    assert not plan.conflicts
    assert plan.counts() == {
        "inventory": 1,
        "orders": 1,
        "order_items": 1,
        "products": 1,
        "python_inventory_operations": 2,
        "users": 1,
    }
    assert len(plan.archives) == 1
    assert plan.archives[0][2:] == ("unknown_keyspace", "local_kv")
    user = next(iter(plan.rows["users"].values()))
    assert user["id"] == "user-1" and user["role"] == "customer"
    assert verify_password("fixture-only-password", user["password_hash"])
    assert next(iter(plan.rows["inventory"].values())) == {
        "sku_id": "sku-1", "available": 7, "reserved": 1, "sold": 2, "version": 0,
    }
    local_order = next(iter(plan.rows["orders"].values()))
    assert local_order["created_at"] == LOCAL_ORDER_CREATED_AT
    assert local_order["expires_at"] == LOCAL_ORDER_EXPIRES_AT
    assert next(iter(plan.rows["order_items"].values()))["unit_price_cents"] == 0


def test_mysql_business_rows_map_all_persisted_fields_without_normalizing_states() -> None:
    rows = _mysql_rows()
    plan = build_migration_plan(mysql_tables=rows)
    assert not plan.conflicts
    assert plan.counts() == {
        "admin_audit_logs": 1,
        "campaigns": 1,
        "flash_sale_requests": 1,
        "inventory": 1,
        "order_items": 1,
        "orders": 1,
        "outbox_events": 1,
        "payments": 1,
        "products": 1,
        "users": 1,
    }
    assert plan.rows["users"][("user-2",)]["created_at"] == datetime(2024, 1, 2, 3, 4, 5)
    assert plan.rows["campaigns"][("promo-2",)]["starts_at"] == 1700000000
    assert plan.rows["outbox_events"][("event-2",)]["attempts"] == 3
    assert plan.rows["outbox_events"][("event-2",)]["sequence"] == 0
    assert plan.rows["payments"][("payment-2",)]["amount_cents"] == 2500


def test_cross_source_conflict_and_inventory_ledger_mismatch_block_import() -> None:
    records = {b"products/sku-1": b"sku-1\tCourse\t19900\t1"}
    mysql = {"products": [{"id": "sku-1", "name": "Course", "price_cents": 20000,
                           "on_sale": True, "version": 0, "updated_at": datetime(2024, 1, 1)}]}
    plan = build_migration_plan(records, mysql)
    assert plan.conflicts["duplicate_products_key"] == 1

    broken = _local_records()
    broken[b"inventory/stock/sku-1"] = b"8|0|2"
    plan = build_migration_plan(kv_records=broken)
    assert plan.conflicts["local_inventory_ledger_mismatch"] == 1

    mysql_rows = _mysql_rows()
    mysql_rows["inventory"][0]["sold"] = 1
    mysql_plan = build_migration_plan(mysql_tables=mysql_rows)
    assert mysql_plan.conflicts["mysql_inventory_order_ledger_mismatch"] == 1


def test_cpp_obx2_pending_event_decode_crc_and_truncation() -> None:
    encoded = _obx2(b"event-1", b"order.events", b"order-1", b"created", 9)
    events = decode_outbox(encoded)
    assert len(events) == 1
    event = events[0]
    assert (event.event_id, event.topic, event.key, event.payload, event.sequence) == (
        "event-1", "order.events", b"order-1", b"created", 9,
    )
    plan = build_migration_plan(outbox_events=events)
    assert not plan.conflicts and plan.rows["outbox_events"][("event-1",)]["sequence"] == 9
    duplicate = build_migration_plan(outbox_events=[events[0], events[0]])
    assert duplicate.conflicts["duplicate_outbox_event_id"] == 1
    with pytest.raises(CorruptCppOutbox, match="truncated"):
        decode_outbox(encoded[:-1])
    damaged = bytearray(encoded)
    damaged[-1] ^= 1
    with pytest.raises(CorruptCppOutbox, match="checksum"):
        decode_outbox(bytes(damaged))


def test_unrepresentable_outbox_fields_are_archived_without_exposing_payload() -> None:
    event = decode_outbox(_obx2(b"event-raw", b"order.events", b"\xff", b"opaque", 1))[0]
    plan = build_migration_plan(outbox_events=[event])
    assert not plan.conflicts
    assert len(plan.archives) == 1
    assert plan.archives[0][0] == b"OBX2/record/0"
    assert plan.archives[0][1] == event.raw_record
    assert plan.archives[0][2:] == ("outbox_key_not_utf8", "file_outbox")

    invalid_id = decode_outbox(_obx2(b"\xff", b"order.events", b"key", b"opaque", 2))[0]
    invalid_topic = decode_outbox(_obx2(b"event-3", b"\xff", b"key", b"opaque", 3))[0]
    archived = build_migration_plan(outbox_events=[invalid_id, invalid_topic])
    assert [entry[2] for entry in archived.archives] == [
        "outbox_event_id_not_utf8", "outbox_topic_not_utf8",
    ]

    # C++ recovery permits an empty event ID in a valid record, though append()
    # normally assigns one; retain this source state rather than rewriting it.
    empty_id = decode_outbox(_obx2(b"", b"order.events", b"key", b"opaque", 4))[0]
    assert empty_id.event_id == ""


def test_local_kv_import_writes_runtime_tables_and_is_idempotent(tmp_path: Path) -> None:
    target = tmp_path / "converted.sqlite"
    url = f"sqlite+aiosqlite:///{target.as_posix()}"
    plan = build_migration_plan(kv_records=_local_records())

    async def scenario() -> None:
        first = await apply_plan(url, plan)
        second = await apply_plan(url, plan)
        assert first["status"] == "applied" and first["verification"]["foreign_keys"] == "passed"
        assert second["status"] == "already_imported"
        engine, sessions = _test_database(url)
        try:
            async with sessions() as session:
                assert (await session.execute(select(User))).scalars().one().id == "user-1"
                assert (await session.execute(select(Product))).scalars().one().id == "sku-1"
                stock = (await session.execute(select(Inventory))).scalars().one()
                assert (stock.available, stock.reserved, stock.sold) == (7, 1, 2)
                order = (await session.execute(select(Order))).scalars().one()
                assert order.id == "order-1" and order.state == "PENDING_PAYMENT"
                item = (await session.execute(select(OrderItem))).scalars().one()
                assert item.order_id == "order-1" and item.quantity == 1
                operations = (await session.execute(select(InventoryOperation))).scalars().all()
                assert {operation.state for operation in operations} == {"reserved", "confirmed"}
                archive = (await session.execute(select(LegacyKVRecord))).scalars().one()
                assert archive.reason == "unknown_keyspace" and archive.value == b"\xff\x00secret-like-bytes"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_mysql_schema_rows_convert_to_sqlite_runtime_models(tmp_path: Path) -> None:
    target = tmp_path / "mysql-converted.sqlite"
    url = f"sqlite+aiosqlite:///{target.as_posix()}"
    plan = build_migration_plan(mysql_tables=_mysql_rows())

    async def scenario() -> None:
        imported = await apply_plan(url, plan)
        assert imported["verification"]["inventory_ledger"] is True
        engine, sessions = _test_database(url)
        try:
            async with sessions() as session:
                assert len((await session.execute(select(Campaign))).scalars().all()) == 1
                assert len((await session.execute(select(AdminAuditLog))).scalars().all()) == 1
                assert len((await session.execute(select(Payment))).scalars().all()) == 1
                assert len((await session.execute(select(FlashSaleRequest))).scalars().all()) == 1
                outbox = (await session.execute(select(OutboxEvent))).scalars().one()
                assert outbox.published is False and outbox.attempts == 3 and outbox.payload == b"OrderCreated|order-2"
        finally:
            await engine.dispose()

        mixed_url = f"sqlite+aiosqlite:///{(tmp_path / 'mixed-sources.sqlite').as_posix()}"
        mixed_plan = build_migration_plan(kv_records=_local_records(), mysql_tables=_mysql_rows())
        mixed_import = await apply_plan(mixed_url, mixed_plan)
        assert mixed_import["verification"]["inventory_ledger"] is True
        assert mixed_plan.inventory_ledgers == {"sku-1": (1, 2), "sku-2": (1, 0)}

    asyncio.run(scenario())


def test_apply_transaction_rolls_back_and_nonempty_target_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "rollback.sqlite"
    url = f"sqlite+aiosqlite:///{target.as_posix()}"
    plan = build_migration_plan(kv_records=_local_records())

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="injected"):
            await apply_plan(url, plan, fail_after_rows=1)
        connection = sqlite3.connect(target)
        try:
            assert connection.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
            assert connection.execute("SELECT COUNT(*) FROM cpp_migration_batches").fetchone()[0] == 0
        finally:
            connection.close()
        original_verify = migration_cli._verify_database

        async def fail_post_verification(*_args, **_kwargs):
            raise MigrationConflict("injected post-import verification failure")

        monkeypatch.setattr(migration_cli, "_verify_database", fail_post_verification)
        with pytest.raises(MigrationConflict, match="post-import"):
            await apply_plan(url, plan)
        connection = sqlite3.connect(target)
        try:
            assert connection.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
            assert connection.execute("SELECT COUNT(*) FROM cpp_migration_batches").fetchone()[0] == 0
        finally:
            connection.close()
        monkeypatch.setattr(migration_cli, "_verify_database", original_verify)
        await apply_plan(url, plan)
        blocked_target = tmp_path / "already-has-data.sqlite"
        blocked_url = f"sqlite+aiosqlite:///{blocked_target.as_posix()}"
        engine, sessions = _test_database(blocked_url)
        await _create_test_schema(engine)
        async with sessions.begin() as session:
            session.add(Product(id="existing", name="Existing", price_cents=1, on_sale=True))
        await engine.dispose()
        with pytest.raises(MigrationConflict, match="isolated"):
            await apply_plan(blocked_url, plan)

    asyncio.run(scenario())


def test_bad_password_hash_is_never_imported_or_reset() -> None:
    plan = build_migration_plan(mysql_tables={"users": [{
        "id": "unsafe-user", "username": "unsafe", "role": "customer",
        "password_hash": b"not-a-compatible-hash", "enabled": True,
        "created_at": datetime(2024, 1, 1),
    }]})
    assert plan.conflicts["incompatible_user_password_hash"] == 1
    assert not plan.rows.get("users")


def test_cli_dry_run_is_default_and_apply_writes_only_the_explicit_target(tmp_path: Path) -> None:
    snapshot = tmp_path / "synthetic.snapshot"
    snapshot.write_bytes(_snapshot(_local_records()))
    target = tmp_path / "cli-target.sqlite"
    target_url = f"sqlite+aiosqlite:///{target.as_posix()}"

    async def scenario() -> None:
        args = type("Args", (), {
            "snapshot": snapshot, "wal": None, "outbox": None,
            "source_mysql_url": None, "target_database_url": target_url,
            "apply": False,
        })()
        dry_run, dry_status = await _run(args)
        assert dry_status == 0 and dry_run["status"] == "ready_to_apply"
        assert dry_run["verified"] is False and not target.exists()
        args.apply = True
        applied, apply_status = await _run(args)
        assert apply_status == 0 and applied["status"] == "applied"
        assert applied["verified"] is True and target.exists()
        summary = json.dumps(applied, ensure_ascii=False, sort_keys=True)
        assert "fixture-only-password" not in summary and "\xff" not in summary

    asyncio.run(scenario())
