from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from pathlib import Path

from sqlalchemy.engine import make_url

from app.config import Settings
from app.database import Database
from app.models import LegacyKVRecord
from app.storage.cpp_local_kv import recover_cpp_local_kv


async def import_records(records: dict[bytes, bytes], database_url: str) -> int:
    if make_url(database_url).get_backend_name() != "sqlite":
        raise ValueError("C++ LocalKV import writes to SQLite only")
    settings = replace(Settings.from_env(), database_url=database_url, mysql_mode=False)
    database = Database(settings)
    inserted = 0
    try:
        await database.initialize(seed=False)
        async with database.sessions.begin() as session:
            for key, value in records.items():
                previous = await session.get(LegacyKVRecord, key)
                if previous is None:
                    session.add(LegacyKVRecord(key=key, value=value))
                    inserted += 1
                elif previous.value != value:
                    raise ValueError("target already contains a different value for an imported C++ key")
    finally:
        await database.close()
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only LVS2/LVK2 recovery and optional SQLite archival import.")
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--wal", type=Path)
    parser.add_argument("--database-url", help="SQLite async URL; defaults to ECOMMERCE_DATABASE_URL")
    parser.add_argument("--dry-run", action="store_true", help="validate and report without writing records")
    args = parser.parse_args()
    if args.snapshot is None and args.wal is None:
        parser.error("provide --snapshot, --wal, or both")
    records, torn_tail_at = recover_cpp_local_kv(args.snapshot, args.wal)
    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not args.dry_run:
        inserted = asyncio.run(import_records(records, database_url))
    else:
        inserted = 0
    prefix_counts: dict[str, int] = {}
    for key in records:
        category = key.decode("utf-8", errors="replace").split("/", 1)[0]
        prefix_counts[category] = prefix_counts.get(category, 0) + 1
    print(json.dumps({
        "records_recovered": len(records),
        "records_inserted": inserted,
        "record_groups": dict(sorted(prefix_counts.items())),
        "ignored_torn_wal_tail_offset": torn_tail_at,
        "mode": "dry-run" if args.dry_run else "sqlite-archive-import",
    }, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
