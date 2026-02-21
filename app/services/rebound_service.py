import os
from datetime import datetime
from typing import Optional

from app.core.cache import TTLCache
from app.core.symbols import normalize_futures_symbol
from app.core.time import UTC8
from app.core.async_utils import run_in_thread
from app.repositories import SnapshotRepository, SyncRepository


class ReboundService:
    def __init__(self):
        self._cache = TTLCache()
        self._cache_ttl_seconds = float(os.getenv("REBOUND_CACHE_TTL_SECONDS", "10") or 10)

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return normalize_futures_symbol(symbol)

    async def get_snapshot_response(
        self,
        *,
        db,
        date: Optional[str],
        window: str,
    ):
        snapshot_repo = SnapshotRepository(db)
        sync_repo = SyncRepository(db)
        cache_key = f"rebound:snapshot:{window}:{date or 'latest'}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        getter_map = {
            "7d": (snapshot_repo.get_rebound_7d_snapshot_by_date, snapshot_repo.get_latest_rebound_7d_snapshot),
            "30d": (snapshot_repo.get_rebound_30d_snapshot_by_date, snapshot_repo.get_latest_rebound_30d_snapshot),
            "60d": (snapshot_repo.get_rebound_60d_snapshot_by_date, snapshot_repo.get_latest_rebound_60d_snapshot),
        }
        by_date, latest = getter_map[window]

        if date:
            try:
                requested_date = datetime.strptime(date, "%Y-%m-%d").date()
            except ValueError:
                return {"ok": False, "reason": "invalid_date", "message": f"日期格式错误: {date}，请使用 YYYY-MM-DD"}
            today_utc8 = datetime.now(UTC8).date()
            if requested_date > today_utc8:
                return {
                    "ok": False,
                    "reason": "future_date",
                    "message": f"请求日期 {date} 超过今天 {today_utc8.strftime('%Y-%m-%d')}",
                }
            snapshot = await run_in_thread(by_date, date)
        else:
            snapshot = await run_in_thread(latest)

        if not snapshot:
            msg = {
                "7d": "暂无快照数据，请等待下一次07:30定时任务生成（14D）",
                "30d": "暂无快照数据，请等待下一次07:30定时任务生成（30D）",
                "60d": "暂无快照数据，请等待下一次07:30定时任务生成（60D）",
            }[window]
            return {"ok": False, "reason": "no_snapshot", "message": msg}

        open_positions = await run_in_thread(sync_repo.get_open_positions)
        held_symbols = set()
        for pos in open_positions:
            sym = str(pos.get("symbol", "")).upper().strip()
            if not sym:
                continue
            held_symbols.add(sym)
            held_symbols.add(self._normalize_symbol(sym))

        enriched_rows = []
        for idx, row in enumerate(snapshot.get("rows", []), start=1):
            symbol = str(row.get("symbol", "")).upper()
            enriched_rows.append({**row, "rank": idx, "is_held": symbol in held_symbols})

        snapshot["rows"] = enriched_rows
        snapshot["top_count"] = len(enriched_rows)
        snapshot.pop("all_rows", None)
        payload = {"ok": True, **snapshot}
        self._cache.set(cache_key, payload, ttl_seconds=self._cache_ttl_seconds)
        return payload

    async def list_dates(self, *, db, window: str, limit: int):
        snapshot_repo = SnapshotRepository(db)
        cache_key = f"rebound:dates:{window}:{int(limit)}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        list_map = {
            "7d": snapshot_repo.list_rebound_7d_snapshot_dates,
            "30d": snapshot_repo.list_rebound_30d_snapshot_dates,
            "60d": snapshot_repo.list_rebound_60d_snapshot_dates,
        }
        dates = await run_in_thread(list_map[window], limit)
        payload = {"dates": dates}
        self._cache.set(cache_key, payload, ttl_seconds=self._cache_ttl_seconds)
        return payload
