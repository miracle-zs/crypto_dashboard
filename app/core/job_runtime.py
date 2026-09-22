import threading
import time
from typing import Optional

from app.binance_client import BinanceFuturesRestClient
from app.logger import logger


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

    def is_heavy_job(self, source: str) -> bool:
        return any(heavy in source for heavy in self.HEAVY_JOBS)

    def is_light_job(self, source: str) -> bool:
        return any(light in source for light in self.LIGHT_JOBS)

    def try_acquire(self, source: str) -> bool:
        if self.lock_wait_seconds <= 0:
            return True

        # Heavy job tier
        if self.is_heavy_job(source):
            acquired = self._heavy_job_lock.acquire(timeout=self.lock_wait_seconds)
            if not acquired:
                logger.warning(
                    f"{source}跳过: 重型API任务互斥锁繁忙(等待{self.lock_wait_seconds}s后超时)"
                )
                return False
            self._thread_local.held_lock = "heavy"
            return True

        # Light job tier (never blocks on heavy job lock)
        if self.is_light_job(source):
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

    def release(self, source: Optional[str] = None):
        if self.lock_wait_seconds <= 0:
            return

        held = getattr(self._thread_local, "held_lock", None)
        self._thread_local.held_lock = None

        if (source and self.is_light_job(source)) or held == "light":
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
