import threading
import time

from app.binance_client import BinanceFuturesRestClient
from app.logger import logger


class JobRuntimeController:
    def __init__(self, lock_wait_seconds: int = 8):
        self.lock_wait_seconds = max(0, int(lock_wait_seconds))
        self._api_job_lock = threading.Lock()

    def is_cooldown_active(self, source: str) -> bool:
        remaining = BinanceFuturesRestClient.cooldown_remaining_seconds()
        if remaining > 0:
            logger.warning(
                f"Binance API冷却中，跳过{source}: remaining={remaining:.1f}s"
            )
            return True
        return False

    def try_acquire(self, source: str) -> bool:
        if self.lock_wait_seconds <= 0:
            return True

        acquired = self._api_job_lock.acquire(timeout=self.lock_wait_seconds)
        if not acquired:
            logger.warning(
                f"{source}跳过: API任务互斥锁繁忙(等待{self.lock_wait_seconds}s后超时)"
            )
            return False
        return True

    def release(self):
        if self.lock_wait_seconds <= 0:
            return
        if self._api_job_lock.locked():
            self._api_job_lock.release()

    @staticmethod
    def remaining_budget_seconds(started_at: float, total_budget_seconds: float) -> float:
        elapsed = time.perf_counter() - started_at
        return max(0.0, float(total_budget_seconds) - elapsed)
