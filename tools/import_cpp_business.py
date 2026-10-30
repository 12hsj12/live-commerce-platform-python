from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

from sqlalchemy import event, inspect, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import Base, CppMigrationBatch, LegacyKVRecord
from app.storage.cpp_business_migration import (
    KEY_FIELDS,
    MODEL_BY_TABLE,
    MigrationConflict,
    MigrationPlan,
    build_migration_plan,
    recover_and_plan,
)


MYSQL_SOURCE_ORDER = {
    "users": "id",
    "products": "id",
    "inventory": "sku_id",
    "campaigns": "id",
    "admin_audit_logs": "id",
    "orders": "id",
    "order_items": "order_id,sku_id",
    "outbox_events": "event_id",
    "payments": "id",
    "flash_sale_requests": "request_id",
}


def _require_sqlite_target(database_url: str) -> Path:
    parsed = make_url(database_url)
    if (parsed.drivername != "sqlite+aiosqlite"
            or parsed.database in (None, "", ":memory:")):
        raise ValueError("target must be a new file-based SQLite database")
    path = Path(parsed.database).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def _make_target_engine(target_url: str):
    # Keep the migration tool independent of Settings.from_env(): the project
    # currently has an optional .env loader, while migrations must never read it.
    engine = create_async_engine(target_url, pool_pre_ping=True)

    def enable_foreign_keys(connection, _record) -> None:
        cursor = connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
        finally:
            cursor.close()

    event.listen(engine.sync_engine, "connect", enable_foreign_keys)
    return engine, async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def read_cpp_mysql(database_url: str) -> dict[str, list[dict[str, Any]]]:
    """Read only the C++ business schema in one read-only repeatable-read txn."""
    engine = create_async_engine(
        database_url,
        pool_size=1,
        max_overflow=0,
        connect_args={"connect_timeout": 3},
        isolation_level="REPEATABLE READ",
    )
    try:
        async with engine.connect() as connection:
            await connection.exec_driver_sql("SET SESSION TRANSACTION READ ONLY")
            await connection.commit()
            tables: dict[str, list[dict[str, Any]]] = {}
            async with connection.begin():
                available = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
                missing = set(MYSQL_SOURCE_ORDER) - available
                if missing:
                    raise ValueError("source MySQL schema is missing expected C++ tables")
                for table, order_by in MYSQL_SOURCE_ORDER.items():
                    result = await connection.exec_driver_sql(
                        f"SELECT * FROM `{table}` ORDER BY {order_by}"
                    )
                    tables[table] = [dict(row) for row in result.mappings().all()]
            return tables
    finally:
        await engine.dispose()


def _read_target_state(target_url: str, plan: MigrationPlan) -> dict[str, Any]:
    path = _require_sqlite_target(target_url)
    if not path.exists() or path.stat().st_size == 0:
        return {"state": "new", "row_counts": {}}
    uri = f"{path.as_uri()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=3)
    except sqlite3.Error as exc:
        raise ValueError("target SQLite database cannot be inspected read-only") from exc
    try:
        table_names = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        row_counts = {
            name: int(connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0])
            for name in table_names
        }
        if not table_names:
            return {"state": "new", "row_counts": {}}
        batch_rows = []
        if "cpp_migration_batches" in table_names:
            columns = {row[1] for row in connection.execute('PRAGMA table_info("cpp_migration_batches")')}
            if {"source_fingerprint", "source_summary"} <= columns:
                batch_rows = connection.execute(
                    "SELECT source_fingerprint, source_summary FROM cpp_migration_batches"
                ).fetchall()
        matched = [row for row in batch_rows if row[0] == plan.fingerprint]
        if len(batch_rows) == 1 and len(matched) == 1 and set(table_names) <= set(Base.metadata.tables):
            expected_counts = {name: len(plan.rows.get(name, {})) for name in Base.metadata.tables}
            expected_counts.update({
                "legacy_kv_records": len(plan.archives),
                "cpp_migration_batches": 1,
            })
            if all(row_counts.get(name, 0) == expected for name, expected in expected_counts.items()):
                return {"state": "matching_batch", "row_counts": row_counts}
        if any(row_counts.values()):
            return {"state": "nonempty_conflict", "row_counts": row_counts}
        if not set(table_names) <= set(Base.metadata.tables):
            return {"state": "incompatible_empty_schema", "row_counts": row_counts}
        expected_columns = {table.name: {column.name for column in table.columns}
                            for table in Base.metadata.sorted_tables}
        for name in table_names:
            columns = {row[1] for row in connection.execute(f'PRAGMA table_info("{name}")')}
            if not expected_columns.get(name, set()) <= columns:
                return {"state": "incompatible_empty_schema", "row_counts": row_counts}
        return {"state": "compatible_empty_schema", "row_counts": row_counts}
    finally:
        connection.close()


def _clean(value: Any) -> Any:
    if isinstance(value, bytes):
        return value
    if hasattr(value, "isoformat"):
        return value
    return value


async def _verify_database(engine, sessions, plan: MigrationPlan, *, session=None) -> dict[str, Any]:
    async def verify(current) -> dict[str, Any]:
        result: dict[str, Any] = {"row_counts": {}, "field_mappings_checked": 0}
        actual_by_table: dict[str, list[Any]] = {}
        for table, model in MODEL_BY_TABLE.items():
            expected = plan.rows.get(table, {})
            rows = (await current.execute(select(model))).scalars().all()
            actual_by_table[table] = rows
            result["row_counts"][table] = len(rows)
            if len(rows) != len(expected):
                raise MigrationConflict("post-import row count verification failed")
            indexed = {tuple(getattr(row, field) for field in KEY_FIELDS[table]): row for row in rows}
            for key, expected_row in expected.items():
                actual = indexed.get(key)
                if actual is None:
                    raise MigrationConflict("post-import primary-key verification failed")
                for field, value in expected_row.items():
                    actual_value = _clean(getattr(actual, field))
                    if actual_value != _clean(value):
                        raise MigrationConflict("post-import field mapping verification failed")
                    result["field_mappings_checked"] += 1
        expected_archives = {(item[0], item[1], item[2], item[3]) for item in plan.archives}
        archived = (await current.execute(select(LegacyKVRecord))).scalars().all()
        actual_archives = {(row.key, row.value, row.reason, row.source_kind) for row in archived}
        if actual_archives != expected_archives:
            raise MigrationConflict("post-import archive verification failed")
        result["row_counts"]["legacy_kv_records"] = len(archived)
        batch = await current.get(CppMigrationBatch, plan.fingerprint)
        if batch is None:
            raise MigrationConflict("migration batch marker is missing")
        connection = await current.connection()
        violations = await connection.exec_driver_sql("PRAGMA foreign_key_check")
        if violations.first() is not None:
            raise MigrationConflict("target foreign-key verification failed")
        result["foreign_keys"] = "passed"
        result["inventory_nonnegative"] = all(
            min(row["available"], row["reserved"], row["sold"]) >= 0
            for row in plan.rows.get("inventory", {}).values()
        )
        actual_inventory = actual_by_table.get("inventory", [])
        result["inventory_ledger"] = (
            {row.sku_id for row in actual_inventory} == set(plan.inventory_ledgers)
            and all(
                plan.inventory_ledgers.get(row.sku_id) == (row.reserved, row.sold)
                for row in actual_inventory
            )
        )
        result["order_states_valid"] = all(
            row["state"] in {"PENDING_PAYMENT", "PAID", "EXPIRED", "CANCELLED"}
            for row in plan.rows.get("orders", {}).values()
        )
        if (not result["inventory_nonnegative"] or not result["inventory_ledger"]
                or not result["order_states_valid"]):
            raise MigrationConflict("post-import business invariant verification failed")
        return result

    if session is not None:
        return await verify(session)
    async with sessions() as current:
        return await verify(current)


async def _return_existing_verified(target_url: str, plan: MigrationPlan) -> dict[str, Any]:
    engine, sessions = _make_target_engine(target_url)
    try:
        # Do not initialize or mutate an existing target during idempotency checks.
        verification = await _verify_database(engine, sessions, plan)
        return verification
    finally:
        await engine.dispose()


async def apply_plan(target_url: str, plan: MigrationPlan, *, fail_after_rows: int | None = None) -> dict[str, Any]:
    if plan.conflicts:
        raise MigrationConflict("source contains blocking conflicts")
    state = _read_target_state(target_url, plan)
    if state["state"] == "matching_batch":
        verification = await _return_existing_verified(target_url, plan)
        return {"status": "already_imported", "verification": verification}
    if state["state"] not in {"new", "compatible_empty_schema"}:
        raise MigrationConflict("target is not a new isolated database or has conflicting content")

    engine, sessions = _make_target_engine(target_url)
    inserted = 0
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        try:
            async with sessions.begin() as session:
                for table, keyed_rows in sorted(plan.rows.items()):
                    model = MODEL_BY_TABLE[table]
                    for row in keyed_rows.values():
                        session.add(model(**row))
                        inserted += 1
                        if fail_after_rows is not None and inserted >= fail_after_rows:
                            raise RuntimeError("injected transaction failure")
                for key, value, reason, source_kind in plan.archives:
                    session.add(LegacyKVRecord(key=key, value=value, reason=reason, source_kind=source_kind))
                    inserted += 1
                    if fail_after_rows is not None and inserted >= fail_after_rows:
                        raise RuntimeError("injected transaction failure")
                summary = plan.summary()
                summary.pop("fingerprint", None)
                session.add(CppMigrationBatch(
                    source_fingerprint=plan.fingerprint,
                    source_summary=json.dumps(summary, ensure_ascii=True, sort_keys=True),
                ))
                if fail_after_rows is not None and inserted >= fail_after_rows:
                    raise RuntimeError("injected transaction failure")
                verification = await _verify_database(engine, sessions, plan, session=session)
        except Exception:
            # The SQLAlchemy transaction context rolls back all source rows,
            # archives, and the idempotency marker together.
            raise
        return {"status": "applied", "inserted_rows": inserted, "verification": verification}
    finally:
        await engine.dispose()


def _write_report(path: Path | None, report: dict[str, Any]) -> None:
    if path is None:
        return
    with path.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(report, output, ensure_ascii=False, sort_keys=True, indent=2)
        output.write("\n")


async def _run(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.snapshot is None and args.wal is None and args.outbox is None and not args.source_mysql_url:
        raise ValueError("at least one read-only C++ data source is required")
    if not args.target_database_url:
        raise ValueError("an explicit file-based SQLite target is required")
    _require_sqlite_target(args.target_database_url)
    mysql_tables = await read_cpp_mysql(args.source_mysql_url) if args.source_mysql_url else {}
    plan, torn_tail_at = recover_and_plan(
        snapshot_path=args.snapshot,
        wal_path=args.wal,
        outbox_path=args.outbox,
        mysql_tables=mysql_tables,
    )
    report = plan.summary()
    report["mode"] = "apply" if args.apply else "dry-run"
    report["ignored_torn_wal_tail"] = torn_tail_at is not None
    report["target"] = _read_target_state(args.target_database_url, plan)
    report["verified"] = False
    if plan.conflicts:
        report["status"] = "blocked_by_source_conflicts"
        return report, 2
    if report["target"]["state"] not in {"new", "compatible_empty_schema", "matching_batch"}:
        report["status"] = "blocked_by_target_state"
        return report, 2
    if args.apply:
        applied = await apply_plan(args.target_database_url, plan)
        report["status"] = applied["status"]
        report["inserted_rows"] = applied.get("inserted_rows", 0)
        report["verification"] = applied["verification"]
        report["verified"] = True
    else:
        report["status"] = (
            "already_imported" if report["target"]["state"] == "matching_batch" else "ready_to_apply"
        )
        if report["target"]["state"] == "matching_batch":
            report["verification"] = await _return_existing_verified(args.target_database_url, plan)
            report["verified"] = True
    return report, 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only C++ snapshot/WAL, OBX2 and MySQL migration into an isolated Python SQLite database."
    )
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--wal", type=Path)
    parser.add_argument("--outbox", type=Path, help="C++ FileOutbox OBX2 file")
    parser.add_argument("--source-mysql-url", help="read-only C++ MySQL source URL (never printed)")
    parser.add_argument("--target-database-url", required=True, help="new file-based sqlite+aiosqlite URL")
    parser.add_argument("--apply", action="store_true", help="write data; default behavior is read-only dry-run")
    parser.add_argument("--report-json", type=Path, help="create a redacted aggregate-only report file")
    args = parser.parse_args()
    try:
        report, status = asyncio.run(_run(args))
    except MigrationConflict:
        report, status = {"status": "blocked", "verified": False}, 2
    except Exception:
        # Database URLs and source values can contain private data. Emit a
        # stable category only; never stringify exception messages here.
        report, status = {"status": "source_or_target_error", "verified": False}, 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    try:
        _write_report(args.report_json, report)
    except Exception:
        print(json.dumps({"report_file_status": "not_written"}, sort_keys=True))
        status = 2
    raise SystemExit(status)


if __name__ == "__main__":
    main()
