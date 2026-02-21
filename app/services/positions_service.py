import asyncio
import json
import os
import threading
from collections import defaultdict
from datetime import datetime
from functools import partial

from app.core.async_utils import run_in_thread
from app.core.cache import TTLCache
from app.core.time import UTC8
from app.logger import logger
from app.repositories import SnapshotRepository, SyncRepository
from app.services.market_price_service import MarketPriceService


class PositionsService:
    def __init__(self):
        self._cache = TTLCache()
        self._cache_ttl_seconds = float(os.getenv("POSITIONS_CACHE_TTL_SECONDS", "3") or 3)
        self._version_lock = threading.Lock()
        self._latest_version = 0
        self._latest_signature = ""

    @staticmethod
    def _format_holding_time(total_minutes: int) -> str:
        if total_minutes <= 0:
            return "0m"
        if total_minutes < 60:
            return f"{total_minutes}m"
        hours = total_minutes // 60
        minutes = total_minutes % 60
        if hours < 24:
            return f"{hours}h {minutes}m"
        days = hours // 24
        hours = hours % 24
        return f"{days}d {hours}h"

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol if symbol.endswith("USDT") else f"{symbol}USDT"

    @staticmethod
    def _parse_local_snapshot_time(value):
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC8)
        except Exception:
            return None

    def _is_noon_review_ready(self, noon_snapshot, noon_review_snapshot) -> bool:
        if not noon_review_snapshot:
            return False
        review_dt = self._parse_local_snapshot_time(str(noon_review_snapshot.get("review_time") or ""))
        if review_dt is None:
            return False
        noon_dt = self._parse_local_snapshot_time(str((noon_snapshot or {}).get("snapshot_time") or ""))
        if noon_dt is None:
            return False
        return review_dt >= noon_dt

    def _compute_signature(self, payload: dict) -> str:
        return json.dumps(
            {
                "positions": payload.get("positions", []),
                "summary": payload.get("summary", {}),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def _get_or_bump_version(self, payload: dict) -> int:
        signature = self._compute_signature(payload)
        with self._version_lock:
            if signature != self._latest_signature:
                self._latest_version += 1
                self._latest_signature = signature
            return self._latest_version

    async def build_open_positions_response(self, db, client, since_version: int | None = None):
        sync_repo = SyncRepository(db)
        snapshot_repo = SnapshotRepository(db)
        profit_alert_threshold_pct = float(os.getenv("PROFIT_ALERT_THRESHOLD_PCT", "20") or 20)
        now = datetime.now(UTC8)
        today_snapshot_date = now.strftime("%Y-%m-%d")
        cache_key = f"positions:open:{today_snapshot_date}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            payload = cached
        else:
            raw_positions, noon_loss_snapshot, noon_review_snapshot = await asyncio.gather(
                run_in_thread(sync_repo.get_open_positions),
                run_in_thread(partial(snapshot_repo.get_noon_loss_snapshot_by_date, today_snapshot_date)),
                run_in_thread(partial(snapshot_repo.get_noon_loss_review_snapshot_by_date, today_snapshot_date)),
            )
            noon_loss_count = int(noon_loss_snapshot.get("loss_count", 0)) if noon_loss_snapshot else 0
            noon_stop_loss_total = float(noon_loss_snapshot.get("total_stop_loss", 0.0)) if noon_loss_snapshot else 0.0
            noon_stop_loss_pct = float(noon_loss_snapshot.get("pct_of_balance", 0.0)) if noon_loss_snapshot else 0.0
            noon_snapshot_time = str(noon_loss_snapshot.get("snapshot_time")) if noon_loss_snapshot else None
            noon_review_ready = self._is_noon_review_ready(noon_loss_snapshot, noon_review_snapshot)
            noon_review_time = (
                str(noon_review_snapshot.get("review_time")) if (noon_review_snapshot and noon_review_ready) else None
            )
            noon_review_not_cut_count = (
                int(noon_review_snapshot.get("not_cut_count", 0)) if (noon_review_snapshot and noon_review_ready) else 0
            )
            noon_review_noon_cut_loss_total = (
                float(noon_review_snapshot.get("noon_cut_loss_total", 0.0))
                if (noon_review_snapshot and noon_review_ready) else 0.0
            )
            noon_review_hold_loss_total = (
                float(noon_review_snapshot.get("hold_loss_total", 0.0))
                if (noon_review_snapshot and noon_review_ready) else 0.0
            )
            noon_review_delta_loss_total = (
                float(noon_review_snapshot.get("delta_loss_total", 0.0))
                if (noon_review_snapshot and noon_review_ready) else 0.0
            )
            noon_review_pct_of_balance = (
                float(noon_review_snapshot.get("pct_of_balance", 0.0))
                if (noon_review_snapshot and noon_review_ready) else 0.0
            )

            if not raw_positions:
                payload = {
                    "as_of": now.isoformat(),
                    "positions": [],
                    "summary": {
                        "total_positions": 0,
                        "long_count": 0,
                        "short_count": 0,
                        "total_notional": 0.0,
                        "long_notional": 0.0,
                        "short_notional": 0.0,
                        "net_exposure": 0.0,
                        "total_unrealized_pnl": 0.0,
                        "avg_holding_minutes": 0.0,
                        "avg_holding_time": "0m",
                        "concentration_top1": 0.0,
                        "concentration_top3": 0.0,
                        "concentration_hhi": 0.0,
                        "recent_loss_count": noon_loss_count,
                        "recent_loss_total_if_stop_now": noon_stop_loss_total,
                        "recent_loss_pct_of_balance": noon_stop_loss_pct,
                        "recent_loss_snapshot_date": today_snapshot_date if noon_loss_snapshot else None,
                        "recent_loss_snapshot_time": noon_snapshot_time,
                        "recent_loss_snapshot_ready": noon_loss_snapshot is not None,
                        "noon_review_snapshot_date": today_snapshot_date if noon_review_ready else None,
                        "noon_review_snapshot_time": noon_review_time,
                        "noon_review_snapshot_ready": noon_review_ready,
                        "noon_review_not_cut_count": noon_review_not_cut_count,
                        "noon_review_noon_cut_loss_total": noon_review_noon_cut_loss_total,
                        "noon_review_hold_loss_total": noon_review_hold_loss_total,
                        "noon_review_delta_loss_total": noon_review_delta_loss_total,
                        "noon_review_pct_of_balance": noon_review_pct_of_balance,
                        "profit_alert_threshold_pct": profit_alert_threshold_pct
                    }
                }
                self._cache.set(cache_key, payload, ttl_seconds=self._cache_ttl_seconds)
            else:
                symbols_full = [self._normalize_symbol(pos["symbol"]) for pos in raw_positions]
                mark_prices = await run_in_thread(MarketPriceService.get_mark_price_map, symbols_full, client)

                positions = []
                per_symbol_notional = defaultdict(float)
                total_notional = 0.0
                long_notional = 0.0
                short_notional = 0.0
                total_unrealized_pnl = 0.0
                total_holding_minutes = 0

                for pos in raw_positions:
                    symbol = str(pos.get("symbol", "")).upper()
                    side = str(pos.get("side", "")).upper()
                    qty = float(pos.get("qty", 0.0))
                    entry_price = float(pos.get("entry_price", 0.0))
                    entry_amount = float(pos.get("entry_amount") or (entry_price * qty))
                    entry_time_str = str(pos.get("entry_time"))

                    symbol_full = self._normalize_symbol(symbol)
                    mark_price = mark_prices.get(symbol_full)
                    price_for_notional = mark_price if mark_price is not None else entry_price
                    notional = float(price_for_notional * qty)

                    unrealized_pnl = None
                    unrealized_pnl_pct = None
                    if mark_price is not None and qty > 0:
                        if side == "SHORT":
                            unrealized_pnl = (entry_price - mark_price) * qty
                        else:
                            unrealized_pnl = (mark_price - entry_price) * qty
                        if entry_amount > 0:
                            unrealized_pnl_pct = (unrealized_pnl / entry_amount) * 100

                    try:
                        entry_dt = datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC8)
                    except ValueError:
                        entry_dt = now

                    holding_minutes = max(0, int((now - entry_dt).total_seconds() // 60))
                    holding_time = self._format_holding_time(holding_minutes)

                    total_notional += notional
                    total_holding_minutes += holding_minutes
                    if side == "SHORT":
                        short_notional += notional
                    else:
                        long_notional += notional

                    if unrealized_pnl is not None:
                        total_unrealized_pnl += unrealized_pnl

                    per_symbol_notional[symbol] += notional

                    positions.append({
                        "symbol": symbol,
                        "order_id": pos.get("order_id"),
                        "side": side,
                        "qty": qty,
                        "entry_price": entry_price,
                        "mark_price": mark_price,
                        "entry_time": entry_time_str,
                        "holding_minutes": holding_minutes,
                        "holding_time": holding_time,
                        "entry_amount": entry_amount,
                        "notional": notional,
                        "unrealized_pnl": unrealized_pnl,
                        "unrealized_pnl_pct": unrealized_pnl_pct,
                        "is_long_term": pos.get("is_long_term", 0) == 1,
                        "profit_alerted": pos.get("profit_alerted", 0) == 1,
                        "weight": 0.0
                    })

                if total_notional > 0:
                    for pos in positions:
                        pos["weight"] = pos["notional"] / total_notional

                positions.sort(key=lambda item: item["notional"], reverse=True)

                shares = []
                if total_notional > 0:
                    shares = sorted((value / total_notional for value in per_symbol_notional.values()), reverse=True)

                concentration_top1 = shares[0] if shares else 0.0
                concentration_top3 = sum(shares[:3]) if shares else 0.0
                concentration_hhi = sum(share ** 2 for share in shares) if shares else 0.0

                avg_holding_minutes = (total_holding_minutes / len(positions)) if positions else 0.0
                avg_holding_time = self._format_holding_time(int(avg_holding_minutes))

                summary = {
                    "total_positions": len(positions),
                    "long_count": sum(1 for p in positions if p["side"] == "LONG"),
                    "short_count": sum(1 for p in positions if p["side"] == "SHORT"),
                    "total_notional": total_notional,
                    "long_notional": long_notional,
                    "short_notional": short_notional,
                    "net_exposure": long_notional - short_notional,
                    "total_unrealized_pnl": total_unrealized_pnl,
                    "avg_holding_minutes": avg_holding_minutes,
                    "avg_holding_time": avg_holding_time,
                    "concentration_top1": concentration_top1,
                    "concentration_top3": concentration_top3,
                    "concentration_hhi": concentration_hhi,
                    "recent_loss_count": noon_loss_count,
                    "recent_loss_total_if_stop_now": noon_stop_loss_total,
                    "recent_loss_pct_of_balance": noon_stop_loss_pct,
                    "recent_loss_snapshot_date": today_snapshot_date if noon_loss_snapshot else None,
                    "recent_loss_snapshot_time": noon_snapshot_time,
                    "recent_loss_snapshot_ready": noon_loss_snapshot is not None,
                    "noon_review_snapshot_date": today_snapshot_date if noon_review_ready else None,
                    "noon_review_snapshot_time": noon_review_time,
                    "noon_review_snapshot_ready": noon_review_ready,
                    "noon_review_not_cut_count": noon_review_not_cut_count,
                    "noon_review_noon_cut_loss_total": noon_review_noon_cut_loss_total,
                    "noon_review_hold_loss_total": noon_review_hold_loss_total,
                    "noon_review_delta_loss_total": noon_review_delta_loss_total,
                    "noon_review_pct_of_balance": noon_review_pct_of_balance,
                    "profit_alert_threshold_pct": profit_alert_threshold_pct
                }

                payload = {
                    "as_of": now.isoformat(),
                    "positions": positions,
                    "summary": summary
                }
                self._cache.set(cache_key, payload, ttl_seconds=self._cache_ttl_seconds)

        version = self._get_or_bump_version(payload)
        if since_version is not None:
            requested_version = int(since_version)
            if requested_version >= version:
                return {
                    "version": version,
                    "incremental": True,
                    "changed": False,
                }
            return {
                "version": version,
                "incremental": True,
                "changed": True,
                "as_of": payload.get("as_of"),
                "positions": payload.get("positions", []),
                "summary": payload.get("summary", {}),
            }
        return payload
