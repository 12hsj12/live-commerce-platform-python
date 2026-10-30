"""Compatibility entry point for the full C++ business-data importer.

The former archive-only importer is retained as ``import_records`` for callers
that explicitly archive opaque records, while command-line migrations now use
the deterministic business conversion pipeline.
"""

from __future__ import annotations

from tools.import_cpp_business import apply_plan, main as _business_main
from app.storage.cpp_business_migration import build_migration_plan


async def import_records(records: dict[bytes, bytes], database_url: str) -> int:
    """Import recoverable records into active Python tables and archive unknowns."""
    plan = build_migration_plan(kv_records=records)
    if plan.conflicts:
        raise ValueError("C++ LocalKV contains blocking business-data conflicts")
    outcome = await apply_plan(database_url, plan)
    if outcome["status"] == "already_imported":
        return 0
    return sum(plan.counts().values()) + len(plan.archives)


def main() -> None:
    _business_main()


if __name__ == "__main__":
    main()
