import asyncio
from functools import partial

from app.logger import read_logs


class SystemApiService:
    async def get_logs(self, *, lines: int):
        loop = asyncio.get_event_loop()
        log_lines = await loop.run_in_executor(None, read_logs, lines)
        return {"logs": log_lines}

    async def get_noon_loss_review_history(self, *, db, limit: int):
        loop = asyncio.get_event_loop()
        rows, summary = await asyncio.gather(
            loop.run_in_executor(None, partial(db.list_noon_loss_review_history, limit)),
            loop.run_in_executor(None, db.get_noon_loss_review_history_summary),
        )
        return {"rows": rows, "summary": summary}

    async def get_sync_runs(self, *, db, limit: int):
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(None, partial(db.list_sync_run_logs, limit))
        return {"rows": rows}
