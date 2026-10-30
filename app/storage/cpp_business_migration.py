from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.models import (
    AdminAuditLog,
    Campaign,
    CppMigrationBatch,
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
)
from app.security import verify_password
from app.storage.cpp_local_kv import recover_cpp_local_kv
from app.storage.cpp_outbox import CppOutboxEvent


LOCAL_ORDER_CREATED_AT = datetime(1970, 1, 1)
LOCAL_ORDER_EXPIRES_AT = datetime(9999, 12, 31, 23, 59, 59)
ORDER_STATES = {"PENDING_PAYMENT", "PAID", "EXPIRED", "CANCELLED"}
REQUEST_STATES = {
    "QUEUED", "PROCESSING", "PENDING_PAYMENT", "PAID", "EXPIRED", "CANCELLED",
    "WAITING", "SUBMITTING", "REJECTED", "FAILED",
}
CAMPAIGN_STATES = {"CREATED", "PREHEATING", "RUNNING", "PAUSED", "FINISHED"}


class MigrationConflict(ValueError):
    """Raised for source data that cannot be imported without guessing."""


@dataclass
class MigrationPlan:
    rows: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = field(default_factory=lambda: defaultdict(dict))
    archives: list[tuple[bytes, bytes, str, str]] = field(default_factory=list)
    source_counts: dict[str, Any] = field(default_factory=dict)
    conflicts: Counter[str] = field(default_factory=Counter)
    differences: Counter[str] = field(default_factory=Counter)
    inventory_ledgers: dict[str, tuple[int, int]] = field(default_factory=dict)
    duplicate_consistent: int = 0
    fingerprint: str = ""

    def counts(self) -> dict[str, int]:
        return {table: len(rows) for table, rows in sorted(self.rows.items()) if rows}

    def summary(self) -> dict[str, Any]:
        return {
            "source_counts": self.source_counts,
            "converted_counts": self.counts(),
            "archived_unknown_records": len(self.archives),
            "archive_reasons": dict(sorted(Counter(item[2] for item in self.archives).items())),
            "consistent_cross_source_duplicates": self.duplicate_consistent,
            "inventory_ledger_skus_checked": len(self.inventory_ledgers),
            "documented_differences": dict(sorted(self.differences.items())),
            "blocking_conflicts": dict(sorted(self.conflicts.items())),
        }


MODEL_BY_TABLE = {
    "users": User,
    "products": Product,
    "inventory": Inventory,
    "python_inventory_operations": InventoryOperation,
    "campaigns": Campaign,
    "admin_audit_logs": AdminAuditLog,
    "orders": Order,
    "order_items": OrderItem,
    "outbox_events": OutboxEvent,
    "payments": Payment,
    "flash_sale_requests": FlashSaleRequest,
}
KEY_FIELDS = {
    "users": ("id",),
    "products": ("id",),
    "inventory": ("sku_id",),
    "python_inventory_operations": ("operation_id",),
    "campaigns": ("id",),
    "admin_audit_logs": ("id",),
    "orders": ("id",),
    "order_items": ("order_id", "sku_id"),
    "outbox_events": ("event_id",),
    "payments": ("id",),
    "flash_sale_requests": ("request_id",),
}
COMPARE_FIELDS = {
    "users": ("id", "username", "role", "password_hash", "enabled"),
    "products": ("id", "name", "price_cents", "on_sale"),
    "inventory": ("sku_id", "available", "reserved", "sold"),
    "orders": ("id", "user_id", "idempotency_key", "state"),
    "outbox_events": ("event_id", "topic", "event_key", "payload"),
}


def _fingerprint_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat(timespec="microseconds")}
    if isinstance(value, dict):
        return {str(key): _fingerprint_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def source_fingerprint(
    kv_records: dict[bytes, bytes],
    mysql_tables: dict[str, list[dict[str, Any]]],
    outbox_events: list[CppOutboxEvent],
) -> str:
    material = {
        "local_kv": [[key.hex(), value.hex()] for key, value in sorted(kv_records.items())],
        "mysql": {
            table: sorted(
                (_fingerprint_value(row) for row in rows),
                key=lambda row: json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            )
            for table, rows in sorted(mysql_tables.items())
        },
        "file_outbox": [
            [event.event_id, event.topic, event.key.hex(), event.payload.hex(), event.sequence]
            for event in outbox_events
        ],
    }
    encoded = json.dumps(material, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _password_hash_compatible(value: bytes) -> bool:
    try:
        text = value.decode("ascii")
        algorithm, rounds_text, salt_hex, digest_hex = text.split("$", 3)
        rounds = int(rounds_text)
        if algorithm != "pbkdf2-sha256" or not 10_000 <= rounds <= 10_000_000:
            return False
        salt, digest = bytes.fromhex(salt_hex), bytes.fromhex(digest_hex)
        # Check the exact format accepted by Python's verifier without knowing
        # the user's password or exposing the stored value.
        if len(salt) != 16 or len(digest) != 32:
            return False
        # The verifier's parser is also exercised against a known test fixture.
        return True
    except (ValueError, UnicodeError):
        return False


def _text(value: bytes) -> str:
    return value.decode("utf-8", errors="strict")


def _positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise ValueError
    return number


def _nonnegative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise ValueError
    return number


def _identity(table: str, row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row[field] for field in KEY_FIELDS[table])


def _semantic_equal(table: str, left: dict[str, Any], right: dict[str, Any]) -> bool:
    return all(left.get(field) == right.get(field) for field in COMPARE_FIELDS.get(table, tuple(left)))


def _add_row(plan: MigrationPlan, table: str, row: dict[str, Any], source: str) -> bool:
    key = _identity(table, row)
    previous = plan.rows[table].get(key)
    if previous is None:
        plan.rows[table][key] = row
        return True
    if table == "outbox_events" and source == "file_outbox":
        # C++ FileOutbox permits repeated event IDs. Do not silently collapse
        # two pending deliveries or combine active outboxes from different modes.
        plan.conflicts["duplicate_outbox_event_id"] += 1
        return False
    if source == "mysql" and _semantic_equal(table, previous, row):
        plan.rows[table][key] = row
        plan.duplicate_consistent += 1
        return False
    if source == "local_kv" and _semantic_equal(table, previous, row):
        # MySQL has the richer transaction record, so retain its timestamps,
        # campaign, unit price and expiry fields when both sources agree.
        if table == "outbox_events":
            previous["sequence"] = row.get("sequence", 0)
        plan.duplicate_consistent += 1
        return False
    plan.conflicts[f"duplicate_{table}_key"] += 1
    return False


def _archive(plan: MigrationPlan, key: bytes, value: bytes, reason: str, source: str = "local_kv") -> None:
    plan.archives.append((key, value, reason, source))


def _map_local_kv(plan: MigrationPlan, records: dict[bytes, bytes], mysql_order_ids: set[str]) -> None:
    stock_source: dict[str, tuple[int, int, int]] = {}
    reservation_totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    imported_local_orders: list[tuple[dict[str, Any], dict[str, Any]]] = []
    reservation_skus: set[str] = set()
    for key, value in sorted(records.items()):
        key_text = key.decode("utf-8", errors="replace")
        prefix, separator, suffix = key_text.partition("/")
        if not separator or prefix not in {"users", "products", "inventory", "orders"}:
            _archive(plan, key, value, "unknown_keyspace")
            continue
        try:
            text = _text(value)
            if prefix == "users" and suffix and "/" not in suffix:
                fields = text.split("\t")
                if len(fields) != 5:
                    raise ValueError
                user_id, username, encoded_hash, role_value, enabled_value = fields
                role = {"0": "customer", "1": "merchant", "2": "admin"}.get(role_value)
                password_hash = encoded_hash.encode("ascii")
                if (not user_id or user_id != suffix or not username or len(username) > 128 or role is None
                        or not _password_hash_compatible(password_hash)):
                    raise ValueError
                _add_row(plan, "users", {
                    "id": user_id, "username": username, "role": role,
                    "password_hash": password_hash, "enabled": int(enabled_value) != 0,
                }, "local_kv")
                plan.differences["local_users_created_at_not_stored"] += 1
                continue
            if prefix == "products" and suffix and "/" not in suffix:
                fields = text.split("\t")
                if len(fields) != 4:
                    raise ValueError
                product_id, name, price, on_sale = fields
                price_cents = _nonnegative_int(price)
                if not product_id or product_id != suffix or not name or len(name) > 255:
                    raise ValueError
                _add_row(plan, "products", {
                    "id": product_id, "name": name, "price_cents": price_cents,
                    "on_sale": int(on_sale) != 0, "version": 0,
                }, "local_kv")
                plan.differences["local_products_version_time_not_stored"] += 1
                continue
            if prefix == "inventory" and suffix.startswith("stock/") and suffix.count("/") == 1:
                sku_id = suffix.split("/", 1)[1]
                parts = text.split("|")
                if len(parts) != 3 or not sku_id:
                    raise ValueError
                available, reserved, sold = (_nonnegative_int(part) for part in parts)
                stock_source[sku_id] = (available, reserved, sold)
                _add_row(plan, "inventory", {
                    "sku_id": sku_id, "available": available, "reserved": reserved,
                    "sold": sold, "version": 0,
                }, "local_kv")
                plan.differences["local_inventory_version_not_stored"] += 1
                continue
            if prefix == "inventory" and suffix.startswith("reservation/") and suffix.count("/") == 1:
                operation_id = suffix.split("/", 1)[1]
                parts = text.split("|")
                if len(parts) != 3 or not operation_id:
                    raise ValueError
                sku_id, quantity_text, state_text = parts
                quantity = _positive_int(quantity_text)
                state = {"0": "reserved", "1": "confirmed", "2": "released"}.get(state_text)
                if not sku_id or state is None:
                    raise ValueError
                reservation_skus.add(sku_id)
                if state == "reserved":
                    reservation_totals[sku_id][0] += quantity
                elif state == "confirmed":
                    reservation_totals[sku_id][1] += quantity
                _add_row(plan, "python_inventory_operations", {
                    "operation_id": operation_id, "sku_id": sku_id,
                    "quantity": quantity, "state": state,
                }, "local_kv")
                continue
            if prefix == "orders" and suffix and "/" not in suffix:
                fields = text.split("\t")
                if len(fields) != 6:
                    raise ValueError
                order_id, idempotency_key, user_id, sku_id, quantity_text, state_text = fields
                quantity = _positive_int(quantity_text)
                state = {"0": "PENDING_PAYMENT", "1": "PAID", "2": "EXPIRED", "3": "CANCELLED"}.get(state_text)
                if not all((order_id, idempotency_key, user_id, sku_id)) or order_id != suffix or state is None:
                    raise ValueError
                row = {
                    "id": order_id, "user_id": user_id, "campaign_id": "",
                    "idempotency_key": idempotency_key, "state": state,
                    # Local C++ Order has no timestamps or expiry. A fixed
                    # non-expiring sentinel keeps its no-timeout semantics.
                    "created_at": LOCAL_ORDER_CREATED_AT,
                    "expires_at": LOCAL_ORDER_EXPIRES_AT,
                    "expiry_claimed_by": None, "expiry_lease_until": None,
                }
                before = plan.rows["orders"].get((order_id,))
                _add_row(plan, "orders", row, "local_kv")
                if order_id not in mysql_order_ids:
                    item = {"order_id": order_id, "sku_id": sku_id, "quantity": quantity, "unit_price_cents": 0}
                    imported_local_orders.append((row, item))
                elif before is None:
                    plan.conflicts["mysql_order_without_source_item"] += 1
                else:
                    source_item = plan.rows["order_items"].get((order_id, sku_id))
                    if source_item is None or source_item["quantity"] != quantity:
                        plan.conflicts["duplicate_order_item_mismatch"] += 1
                plan.differences["local_orders_campaign_price_created_at_not_stored"] += 1
                continue
            _archive(plan, key, value, "unrecognized_or_malformed_key")
        except (ValueError, UnicodeError, OverflowError):
            _archive(plan, key, value, f"unconvertible_{prefix}")

    for sku_id, (available, reserved, sold) in stock_source.items():
        totals = reservation_totals.get(sku_id, [0, 0])
        plan.inventory_ledgers[sku_id] = (reserved, sold)
        if totals != [reserved, sold]:
            plan.conflicts["local_inventory_ledger_mismatch"] += 1
    for sku_id in reservation_skus:
        if sku_id not in stock_source:
            plan.conflicts["local_reservation_missing_stock"] += 1

    for row, item in imported_local_orders:
        _add_row(plan, "order_items", item, "local_kv")


def _map_mysql(plan: MigrationPlan, tables: dict[str, list[dict[str, Any]]]) -> set[str]:
    table_map = {
        "users": "users",
        "products": "products",
        "inventory": "inventory",
        "campaigns": "campaigns",
        "admin_audit_logs": "admin_audit_logs",
        "orders": "orders",
        "order_items": "order_items",
        "outbox_events": "outbox_events",
        "payments": "payments",
        "flash_sale_requests": "flash_sale_requests",
    }
    mysql_order_ids: set[str] = set()
    for source_table, target_table in table_map.items():
        for source in tables.get(source_table, []):
            row = dict(source)
            if target_table == "users":
                password_hash = row.get("password_hash")
                if isinstance(password_hash, str):
                    password_hash = password_hash.encode("ascii", errors="strict")
                if not isinstance(password_hash, bytes) or not _password_hash_compatible(password_hash):
                    plan.conflicts["incompatible_user_password_hash"] += 1
                    continue
                role = str(row.get("role", "")).lower()
                if role not in {"customer", "merchant", "admin"}:
                    plan.conflicts["unsupported_user_role"] += 1
                    continue
                row["role"] = role
            elif target_table == "products":
                row["on_sale"] = bool(row["on_sale"])
            elif target_table == "inventory":
                if any(int(row[name]) < 0 for name in ("available", "reserved", "sold")):
                    plan.conflicts["negative_mysql_inventory"] += 1
                    continue
            elif target_table == "campaigns":
                if row.get("state") not in CAMPAIGN_STATES or int(row["participant_limit"]) <= 0:
                    plan.conflicts["unsupported_campaign_state_or_quota"] += 1
                    continue
            elif target_table == "orders":
                mysql_order_ids.add(str(row["id"]))
                if row.get("state") not in ORDER_STATES:
                    plan.conflicts["unsupported_order_state"] += 1
                    continue
            elif target_table == "order_items":
                if int(row["quantity"]) <= 0 or int(row["unit_price_cents"]) < 0:
                    plan.conflicts["invalid_mysql_order_item"] += 1
                    continue
            elif target_table == "outbox_events":
                row["sequence"] = 0  # MySQL Outbox schema has no sequence field.
            elif target_table == "flash_sale_requests":
                if row.get("state") not in REQUEST_STATES:
                    plan.conflicts["unsupported_request_state"] += 1
                    continue
            _add_row(plan, target_table, row, "mysql")
    mysql_stocks = {str(row["sku_id"]): row for row in tables.get("inventory", [])}
    mysql_order_states = {str(row["id"]): str(row["state"]) for row in tables.get("orders", [])}
    reserved_by_sku: dict[str, int] = defaultdict(int)
    sold_by_sku: dict[str, int] = defaultdict(int)
    for item in tables.get("order_items", []):
        order_state = mysql_order_states.get(str(item["order_id"]))
        sku_id = str(item["sku_id"])
        quantity = int(item["quantity"])
        if order_state == "PENDING_PAYMENT":
            reserved_by_sku[sku_id] += quantity
        elif order_state == "PAID":
            sold_by_sku[sku_id] += quantity
        if order_state in {"PENDING_PAYMENT", "PAID"} and sku_id not in mysql_stocks:
            plan.conflicts["mysql_order_stock_missing"] += 1
    for sku_id, stock in mysql_stocks.items():
        plan.inventory_ledgers[sku_id] = (int(stock["reserved"]), int(stock["sold"]))
        if (int(stock["reserved"]) != reserved_by_sku[sku_id]
                or int(stock["sold"]) != sold_by_sku[sku_id]):
            plan.conflicts["mysql_inventory_order_ledger_mismatch"] += 1
    return mysql_order_ids


def _validate_relationships_and_invariants(plan: MigrationPlan) -> None:
    users = {row["id"] for row in plan.rows["users"].values()}
    products = {row["id"] for row in plan.rows["products"].values()}
    campaigns = {row["id"] for row in plan.rows["campaigns"].values()}
    orders = {row["id"]: row for row in plan.rows["orders"].values()}
    _check_unique(plan, "users", "username", "duplicate_username")
    _check_unique(plan, "orders", "idempotency_key", "duplicate_idempotency_key")
    _check_unique(plan, "payments", "provider_transaction_id", "duplicate_provider_transaction_id")
    archive_keys = [item[0] for item in plan.archives]
    if len(archive_keys) != len(set(archive_keys)):
        plan.conflicts["duplicate_archive_key"] += len(archive_keys) - len(set(archive_keys))
    item_order_ids = {row["order_id"] for row in plan.rows["order_items"].values()}
    for row in orders.values():
        if row["user_id"] not in users:
            plan.conflicts["order_user_missing"] += 1
        if row["campaign_id"] and row["campaign_id"] not in campaigns:
            plan.conflicts["order_campaign_missing"] += 1
        if row["id"] not in item_order_ids:
            plan.conflicts["order_item_missing"] += 1
    for row in plan.rows["campaigns"].values():
        if row["sku_id"] not in products:
            plan.conflicts["campaign_product_missing"] += 1
    for row in plan.rows["order_items"].values():
        if row["order_id"] not in orders:
            plan.conflicts["item_order_missing"] += 1
        if row["sku_id"] not in products:
            plan.conflicts["order_product_missing"] += 1
    for row in plan.rows["inventory"].values():
        if row["sku_id"] not in products:
            plan.conflicts["inventory_product_missing"] += 1
    payment_orders = {row["order_id"] for row in plan.rows["payments"].values()}
    if payment_orders - orders.keys():
        plan.conflicts["payment_order_missing"] += len(payment_orders - orders.keys())
    for row in plan.rows["python_inventory_operations"].values():
        if row["sku_id"] not in {stock["sku_id"] for stock in plan.rows["inventory"].values()}:
            plan.conflicts["operation_stock_missing"] += 1

    # The C++ MySQL schema does not persist per-operation inventory rows.
    # When present, check its stock counters and order/payment associations.
    for row in plan.rows["inventory"].values():
        if min(row["available"], row["reserved"], row["sold"]) < 0:
            plan.conflicts["negative_inventory"] += 1


def _check_unique(plan: MigrationPlan, table: str, field: str, reason: str) -> None:
    values = [row[field] for row in plan.rows[table].values()]
    if len(values) != len(set(values)):
        plan.conflicts[reason] += len(values) - len(set(values))


def build_migration_plan(
    kv_records: dict[bytes, bytes] | None = None,
    mysql_tables: dict[str, list[dict[str, Any]]] | None = None,
    outbox_events: list[CppOutboxEvent] | None = None,
) -> MigrationPlan:
    kv_records = kv_records or {}
    mysql_tables = mysql_tables or {}
    outbox_events = outbox_events or []
    plan = MigrationPlan()
    plan.source_counts = {
        "local_kv_records": len(kv_records),
        "mysql_tables": {table: len(rows) for table, rows in sorted(mysql_tables.items())},
        "file_outbox_pending_events": len(outbox_events),
    }
    mysql_order_ids = _map_mysql(plan, mysql_tables)
    _map_local_kv(plan, kv_records, mysql_order_ids)
    for event_index, event in enumerate(outbox_events):
        archive_key = f"OBX2/record/{event_index}".encode("ascii")
        if event.event_id is None:
            _archive(plan, archive_key, event.raw_record, "outbox_event_id_not_utf8", "file_outbox")
            continue
        if event.topic is None:
            _archive(plan, archive_key, event.raw_record, "outbox_topic_not_utf8", "file_outbox")
            continue
        if event.sequence > 2**63 - 1:
            _archive(plan, archive_key, event.raw_record,
                     "outbox_sequence_exceeds_python_integer", "file_outbox")
            continue
        try:
            event_key = event.key.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            _archive(plan, archive_key, event.raw_record, "outbox_key_not_utf8", "file_outbox")
            continue
        row = {
            "event_id": event.event_id,
            "topic": event.topic,
            "event_key": event_key,
            "payload": event.payload,
            "sequence": event.sequence,
            "published": False,
            "attempts": 0,
            "claimed_by": None,
            "lease_until": None,
            "last_error": None,
        }
        _add_row(plan, "outbox_events", row, "file_outbox")
        plan.differences["file_outbox_created_at_attempts_not_stored"] += 1
    _validate_relationships_and_invariants(plan)
    plan.fingerprint = source_fingerprint(kv_records, mysql_tables, outbox_events)
    return plan


def recover_and_plan(
    snapshot_path=None,
    wal_path=None,
    outbox_path=None,
    mysql_tables: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[MigrationPlan, int | None]:
    from pathlib import Path
    from app.storage.cpp_outbox import decode_outbox

    snapshot = Path(snapshot_path) if snapshot_path else None
    wal = Path(wal_path) if wal_path else None
    records, torn_tail_at = recover_cpp_local_kv(snapshot, wal)
    events = decode_outbox(Path(outbox_path).read_bytes()) if outbox_path else []
    return build_migration_plan(records, mysql_tables or {}, events), torn_tail_at


def verify_password_hash_fixture() -> bool:
    """Exercise the shared C++/Python PBKDF2 format with test-only material."""
    import hashlib

    password = "migration-fixture-password"
    salt = bytes(range(16))
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 10_000, dklen=32)
    encoded = f"pbkdf2-sha256$10000${salt.hex()}${digest.hex()}".encode("ascii")
    return _password_hash_compatible(encoded) and verify_password(password, encoded)
