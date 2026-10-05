from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from app.core.job_runtime import JobCategory, JobRuntimeController
from app.core.scheduler_config import SchedulerConfig
from app.core.symbols import normalize_futures_symbol
from app.core.time import UTC8
from app.services.market_price_service import MarketPriceService


@dataclass
class JobContext:
    """Explicit dependencies and state available to background job handlers.

    This is deliberately independent from the composing scheduler: jobs receive
    only scheduling configuration, collaborators, and the operations they need.
    """

    scheduler: Any
    config: SchedulerConfig
    processor: Any
    sync_repo: Any
    risk_repo: Any
    snapshot_repo: Any
    trade_repo: Any
    daily_kline_repo: Any
    runtime_controller: JobRuntimeController
    resolve_sync_window: Callable[..., tuple[int, int, bool]]
    fetch_and_analyze_closed_trades: Callable[..., Any]
    persist_closed_trades_and_watermarks: Callable[..., tuple[float, int]]
    _last_transfer_sync_ts: float = 0.0
    _last_synced_wallet_balance: float | None = None
    _pending_compensation_since_ms: dict[str, int] = field(default_factory=dict)

    def __getattr__(self, name: str):
        if hasattr(self.config, name):
            return getattr(self.config, name)
        raise AttributeError(name)

    def _is_api_cooldown_active(self, source: str) -> bool:
        return self.runtime_controller.is_cooldown_active(source=source)

    def _try_enter_api_job_slot(self, source: str, category: JobCategory | None = None) -> bool:
        return self.runtime_controller.try_acquire(source=source, category=category)

    def _release_api_job_slot(self, source: str | None = None, category: JobCategory | None = None):
        self.runtime_controller.release(source=source, category=category)

    def _format_ms_to_utc8(self, ts_ms: int) -> str:
        try:
            return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).astimezone(UTC8).strftime(
                "%Y-%m-%d %H:%M:%S%z"
            )
        except (TypeError, ValueError, OSError):
            return str(ts_ms)

    def _format_window_with_ms(self, start_ms: int, end_ms: int) -> str:
        return (
            f"[{self._format_ms_to_utc8(start_ms)} ~ {self._format_ms_to_utc8(end_ms)}] "
            f"({start_ms} ~ {end_ms})"
        )

    def _is_leaderboard_guard_window(self) -> bool:
        if not self.enable_leaderboard_alert:
            return False
        now = datetime.now(UTC8)
        target = now.replace(
            hour=self.leaderboard_alert_hour,
            minute=self.leaderboard_alert_minute,
            second=0,
            microsecond=0,
        )
        before = self.leaderboard_guard_before_minutes * 60
        after = self.leaderboard_guard_after_minutes * 60
        return -before <= (now - target).total_seconds() <= after

    def _normalize_futures_symbol(self, symbol: str) -> str:
        return normalize_futures_symbol(symbol, preserve_busd=True)

    def _get_mark_price_map(self, symbols: list[str]) -> dict[str, float]:
        if not symbols or not self.processor:
            return {}
        return MarketPriceService.get_mark_price_map(symbols, self.processor.client)

    def _resolve_sync_window(self, **kwargs):
        kwargs.setdefault("utc8", UTC8)
        return self.resolve_sync_window(self, **kwargs)

    def _fetch_and_analyze_closed_trades(self, **kwargs):
        return self.fetch_and_analyze_closed_trades(self, **kwargs)

    def _persist_closed_trades_and_watermarks(self, **kwargs):
        return self.persist_closed_trades_and_watermarks(self, **kwargs)

    def check_long_held_positions(self):
        from app.jobs.risk_jobs import run_long_held_positions_check

        return run_long_held_positions_check(self)

    def check_same_symbol_reentry_alert(self):
        from app.jobs.alert_jobs import run_reentry_alert_check

        return run_reentry_alert_check(self)

    def check_open_positions_profit_alert(self, threshold_pct: float):
        from app.jobs.alert_jobs import run_profit_alert_check

        return run_profit_alert_check(self, threshold_pct)

    def request_trades_compensation(self, symbols, *, reason: str):
        from app.jobs.trades_compensation_jobs import request_trades_compensation_job

        return request_trades_compensation_job(self, symbols, reason=reason)

    def sync_trades_compensation(self, **kwargs):
        from app.jobs.trades_compensation_jobs import sync_trades_compensation_job

        return sync_trades_compensation_job(self, **kwargs)

    def _run_pending_trades_compensation(self):
        from app.jobs.trades_compensation_jobs import run_pending_trades_compensation_job

        return run_pending_trades_compensation_job(self)
