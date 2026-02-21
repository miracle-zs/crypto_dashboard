import asyncio
from functools import partial

from app.core.async_utils import run_in_thread
from app.logger import read_logs
from app.repositories import SnapshotRepository, SyncRepository


class SystemApiService:
    async def get_logs(self, *, lines: int):
        log_lines = await run_in_thread(read_logs, lines)
        return {"logs": log_lines}

    async def get_noon_loss_review_history(self, *, db, limit: int):
        snapshot_repo = SnapshotRepository(db)
        rows, summary = await asyncio.gather(
            run_in_thread(partial(snapshot_repo.list_noon_loss_review_history, limit)),
            run_in_thread(snapshot_repo.get_noon_loss_review_history_summary),
        )
        return {"rows": rows, "summary": summary}

    async def get_sync_runs(self, *, db, limit: int):
        sync_repo = SyncRepository(db)
        rows = await run_in_thread(partial(sync_repo.list_sync_run_logs, limit))
        return {"rows": rows}
