from enum import Enum
import threading
import time
from typing import Optional

from app.binance_client import BinanceFuturesRestClient
from app.logger import logger


class JobCategory(str, Enum):
    HEAVY = "heavy"
    LIGHT = "light"
    DEFAULT = "default"


class JobRuntimeController:
    """
    Tiered job runtime controller:
    - Heavy jobs (batch historical syncs, daily klines, full compensation) acquire `_heavy_job_lock` to avoid
      stacking long-running API tasks.
    - Lightweight monitoring jobs (open positions, balance snapshots) acquire `_light_job_lock` without waiting
      on the heavy job lock, ensuring high-frequency monitoring is never starved by long batch syncs.
    """

    HEAVY_JOBS = {"交易同步", "交易补偿同步", "日K同步", "市场快照"}
    LIGHT_JOBS = {"未平仓同步", "余额同步"}

    CATEGORY_MAP = {
        "balance_sync": JobCategory.LIGHT,
        "open_positions_sync": JobCategory.LIGHT,
        "trades_sync": JobCategory.HEAVY,
        "compensation_sync": JobCategory.HEAVY,
        "kline_sync": JobCategory.HEAVY,
        "market_snapshot": JobCategory.HEAVY,
    }

    def __init__(self, lock_wait_seconds: int = 8):
        self.lock_wait_seconds = max(0, int(lock_wait_seconds))
        self._heavy_job_lock = threading.Lock()
        self._light_job_lock = threading.Lock()
        self._thread_local = threading.local()

    def is_cooldown_active(self, source: str) -> bool:
        remaining = BinanceFuturesRestClient.cooldown_remaining_seconds()
        if remaining > 0:
            logger.warning(
                f"Binance API冷却中，跳过{source}: remaining={remaining:.1f}s"
            )
            return True
        return False

    def resolve_category(self, source: str, category: Optional[JobCategory] = None) -> JobCategory:
        if category is not None:
            return category
        if source in self.CATEGORY_MAP:
            return self.CATEGORY_MAP[source]
        if any(heavy in source for heavy in self.HEAVY_JOBS):
            return JobCategory.HEAVY
        if any(light in source for light in self.LIGHT_JOBS):
            return JobCategory.LIGHT
        return JobCategory.DEFAULT

    def is_heavy_job(self, source: str) -> bool:
        return self.resolve_category(source) == JobCategory.HEAVY

    def is_light_job(self, source: str) -> bool:
        return self.resolve_category(source) == JobCategory.LIGHT

    def try_acquire(self, source: str, category: Optional[JobCategory] = None) -> bool:
        if self.lock_wait_seconds <= 0:
            return True

        resolved = self.resolve_category(source, category)

        # Heavy job tier
        if resolved == JobCategory.HEAVY:
            acquired = self._heavy_job_lock.acquire(timeout=self.lock_wait_seconds)
            if not acquired:
                logger.warning(
                    f"{source}跳过: 重型API任务互斥锁繁忙(等待{self.lock_wait_seconds}s后超时)"
                )
                return False
            self._thread_local.held_lock = "heavy"
            return True

        # Light job tier (never blocks on heavy job lock)
        if resolved == JobCategory.LIGHT:
            wait_time = min(2.0, float(self.lock_wait_seconds))
            acquired = self._light_job_lock.acquire(timeout=wait_time)
            if not acquired:
                logger.warning(
                    f"{source}跳过: 轻量监控任务互斥锁繁忙"
                )
                return False
            self._thread_local.held_lock = "light"
            return True

        # Fallback for unclassified sources
        acquired = self._heavy_job_lock.acquire(timeout=self.lock_wait_seconds)
        if not acquired:
            logger.warning(
                f"{source}跳过: API任务互斥锁繁忙(等待{self.lock_wait_seconds}s后超时)"
            )
            return False
        self._thread_local.held_lock = "heavy"
        return True

    def release(self, source: Optional[str] = None, category: Optional[JobCategory] = None):
        if self.lock_wait_seconds <= 0:
            return

        held = getattr(self._thread_local, "held_lock", None)
        self._thread_local.held_lock = None

        resolved = None
        if category is not None:
            resolved = category
        elif source is not None:
            resolved = self.resolve_category(source)

        if resolved == JobCategory.LIGHT or (held == "light" and resolved != JobCategory.HEAVY):
            if self._light_job_lock.locked():
                try:
                    self._light_job_lock.release()
                except RuntimeError:
                    pass
        else:
            if self._heavy_job_lock.locked():
                try:
                    self._heavy_job_lock.release()
                except RuntimeError:
                    pass

    @staticmethod
    def remaining_budget_seconds(started_at: float, total_budget_seconds: float) -> float:
        elapsed = time.perf_counter() - started_at
        return max(0.0, float(total_budget_seconds) - elapsed)


def try_enter_slot(scheduler, source: str, category: JobCategory) -> bool:
    try:
        return scheduler._try_enter_api_job_slot(source=source, category=category)
    except TypeError:
        return scheduler._try_enter_api_job_slot(source=source)

