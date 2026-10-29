from __future__ import annotations

import asyncio
import logging


class LocalSnapshotWorker:
    """Periodically back up the local SQLite database using SQLite's backup API."""

    def __init__(self, database, interval_seconds: int = 30):
        if interval_seconds <= 0:
            raise ValueError("snapshot interval must be positive")
        self.database = database
        self.interval_seconds = interval_seconds
        self._stopping = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        if self.database.snapshot_path is None:
            return
        if self._task is not None:
            raise RuntimeError("snapshot worker is already running")
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="local-snapshot-worker")

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=self.interval_seconds)
                return
            except asyncio.TimeoutError:
                try:
                    await asyncio.to_thread(self.database.snapshot)
                except Exception:
                    self._logger.exception("local SQLite snapshot failed")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stopping.set()
        task, self._task = self._task, None
        await task
        try:
            await asyncio.to_thread(self.database.snapshot)
        except Exception:
            self._logger.exception("final local SQLite snapshot failed")
