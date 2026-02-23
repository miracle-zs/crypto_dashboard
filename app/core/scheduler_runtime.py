import os
from typing import Callable, TypeVar


T = TypeVar("T")

_scheduler_instance = None


def env_is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def resolve_worker_count() -> int:
    raw = os.getenv("WEB_CONCURRENCY") or os.getenv("UVICORN_WORKERS") or "1"
    try:
        count = int(raw)
    except (TypeError, ValueError):
        count = 1
    return max(1, count)


def should_start_scheduler_runtime() -> tuple[bool, str]:
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        return False, "missing_api_keys"

    worker_count = resolve_worker_count()
    allow_multi_worker = env_is_truthy(os.getenv("SCHEDULER_ALLOW_MULTI_WORKER"))
    if worker_count > 1 and not allow_multi_worker:
        return False, "multi_worker_unsupported"

    return True, "ok"


def get_scheduler_singleton(factory: Callable[[], T]) -> T:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = factory()
    return _scheduler_instance
