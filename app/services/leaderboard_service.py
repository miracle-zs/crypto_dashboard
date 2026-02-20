import asyncio
from datetime import datetime, timedelta
from typing import Optional

from app.core.time import UTC8


class LeaderboardService:
    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol if symbol.endswith("USDT") else f"{symbol}USDT"

    async def build_snapshot_response(self, db, date: Optional[str]):
        loop = asyncio.get_event_loop()
        if date:
            try:
                requested_date = datetime.strptime(date, "%Y-%m-%d").date()
            except ValueError:
                return {
                    "ok": False,
                    "reason": "invalid_date",
                    "message": f"日期格式错误: {date}，请使用 YYYY-MM-DD"
                }
            today_utc8 = datetime.now(UTC8).date()
            if requested_date > today_utc8:
                return {
                    "ok": False,
                    "reason": "future_date",
                    "message": f"请求日期 {date} 超过今天 {today_utc8.strftime('%Y-%m-%d')}"
                }
            snapshot = await loop.run_in_executor(None, db.get_leaderboard_snapshot_by_date, date)
        else:
            snapshot = await loop.run_in_executor(None, db.get_latest_leaderboard_snapshot)

        if not snapshot:
            return {
                "ok": False,
                "reason": "no_snapshot",
                "message": "暂无快照数据，请等待下一次07:40定时任务生成"
            }

        open_positions = await loop.run_in_executor(None, db.get_open_positions)
        held_symbols = set()
        for pos in open_positions:
            sym = str(pos.get("symbol", "")).upper().strip()
            if not sym:
                continue
            held_symbols.add(sym)
            held_symbols.add(self._normalize_symbol(sym))

        try:
            snap_date = datetime.strptime(snapshot["snapshot_date"], "%Y-%m-%d").date()
        except Exception:
            snap_date = None

        yesterday_snapshot = None
        if snap_date is not None:
            yesterday = (snap_date - timedelta(days=1)).strftime("%Y-%m-%d")
            yesterday_snapshot = await loop.run_in_executor(
                None, db.get_leaderboard_snapshot_by_date, yesterday
            )
        yesterday_rank = {}
        yesterday_losers_rank = {}
        if yesterday_snapshot:
            for idx, row in enumerate(yesterday_snapshot.get("rows", []), start=1):
                symbol = str(row.get("symbol", "")).upper()
                if symbol:
                    yesterday_rank[symbol] = idx
            for idx, row in enumerate(yesterday_snapshot.get("losers_rows", []), start=1):
                symbol = str(row.get("symbol", "")).upper()
                if symbol:
                    yesterday_losers_rank[symbol] = idx

        appearances_7d = {}
        losers_appearances_7d = {}
        if snap_date is not None:
            start_date = (snap_date - timedelta(days=6)).strftime("%Y-%m-%d")
            end_date = snap_date.strftime("%Y-%m-%d")
            snapshots_7d = await loop.run_in_executor(
                None, db.get_leaderboard_snapshots_between, start_date, end_date
            )
            for snap in snapshots_7d:
                seen = set()
                for row in snap.get("rows", []):
                    symbol = str(row.get("symbol", "")).upper()
                    if not symbol or symbol in seen:
                        continue
                    seen.add(symbol)
                    appearances_7d[symbol] = appearances_7d.get(symbol, 0) + 1
                seen_losers = set()
                for row in snap.get("losers_rows", []):
                    symbol = str(row.get("symbol", "")).upper()
                    if not symbol or symbol in seen_losers:
                        continue
                    seen_losers.add(symbol)
                    losers_appearances_7d[symbol] = losers_appearances_7d.get(symbol, 0) + 1

        enriched_rows = []
        for idx, row in enumerate(snapshot.get("rows", []), start=1):
            symbol = str(row.get("symbol", "")).upper()
            prev_rank = yesterday_rank.get(symbol)
            rank_delta = None if prev_rank is None else (prev_rank - idx)
            enriched_rows.append({
                **row,
                "is_held": symbol in held_symbols,
                "rank_delta_vs_yesterday": rank_delta,
                "appearances_7d": appearances_7d.get(symbol, 0),
            })

        enriched_losers_rows = []
        for idx, row in enumerate(snapshot.get("losers_rows", []), start=1):
            symbol = str(row.get("symbol", "")).upper()
            prev_rank = yesterday_losers_rank.get(symbol)
            rank_delta = None if prev_rank is None else (prev_rank - idx)
            prev_gainer_rank = yesterday_rank.get(symbol)
            was_prev_gainer_top = prev_gainer_rank is not None
            enriched_losers_rows.append({
                **row,
                "is_held": symbol in held_symbols,
                "rank_delta_vs_yesterday": rank_delta,
                "appearances_7d": losers_appearances_7d.get(symbol, 0),
                "was_prev_gainer_top": was_prev_gainer_top,
                "prev_gainer_rank": prev_gainer_rank,
            })

        snapshot["rows"] = enriched_rows
        snapshot["losers_rows"] = enriched_losers_rows
        snapshot["gainers_top_rows"] = enriched_rows
        snapshot["gainers_top_count"] = len(enriched_rows)
        snapshot["losers_top_count"] = len(enriched_losers_rows)
        metric_payload = await loop.run_in_executor(
            None, db.get_leaderboard_daily_metrics, str(snapshot.get("snapshot_date"))
        )
        if not metric_payload:
            metric_payload = await loop.run_in_executor(
                None, db.upsert_leaderboard_daily_metrics_for_date, str(snapshot.get("snapshot_date"))
            )

        metric1 = metric_payload.get("metric1", {}) if metric_payload else {}
        metric2 = metric_payload.get("metric2", {}) if metric_payload else {}
        metric3 = metric_payload.get("metric3", {}) if metric_payload else {}
        gainers_rank_map = {
            str(row.get("symbol", "")).upper(): idx
            for idx, row in enumerate(enriched_rows, start=1)
            if str(row.get("symbol", "")).upper()
        }
        continuation_rows = []
        for item in (metric2.get("details") or []):
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue
            next_change = item.get("next_change_pct")
            if next_change is None:
                continue
            try:
                next_change_val = float(next_change)
            except (TypeError, ValueError):
                continue
            if next_change_val <= 0:
                continue

            today_rank = gainers_rank_map.get(symbol)
            continuation_rows.append({
                "symbol": symbol,
                "prev_rank": item.get("prev_rank"),
                "next_change_pct": round(next_change_val, 4),
                "today_gainer_rank": today_rank,
                "still_in_gainers_top": today_rank is not None,
            })
        continuation_rows.sort(
            key=lambda x: (
                0 if x.get("still_in_gainers_top") else 1,
                -(x.get("next_change_pct") or 0.0),
                x.get("prev_rank") or 9999
            )
        )
        continuation_pool = {
            "base_snapshot_date": metric2.get("base_snapshot_date"),
            "target_snapshot_date": metric2.get("target_snapshot_date"),
            "still_up_count": len(continuation_rows),
            "still_in_gainers_top_count": sum(1 for x in continuation_rows if x.get("still_in_gainers_top")),
            "rows": continuation_rows,
        }
        metric2_with_pool = {**metric2, "continuation_pool": continuation_pool}
        snapshot["losers_reversal"] = metric1
        snapshot["next_day_drop_metric"] = metric2_with_pool
        snapshot["continuation_pool"] = continuation_pool
        snapshot["change_48h_metric"] = metric3
        snapshot["short_48h_metric"] = metric3
        snapshot["hold_48h_metric"] = metric3
        snapshot.pop("all_rows", None)
        return {"ok": True, **snapshot}
