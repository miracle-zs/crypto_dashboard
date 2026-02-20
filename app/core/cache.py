from __future__ import annotations

import threading
import time
from copy import deepcopy


class TTLCache:
    def __init__(self):
        self._lock = threading.RLock()
        self._store: dict[str, tuple[float, object]] = {}

    def get(self, key: str):
        now = time.monotonic()
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            expires_at, value = item
            if expires_at <= now:
                self._store.pop(key, None)
                return None
            return deepcopy(value)

    def set(self, key: str, value, ttl_seconds: float):
        ttl = max(float(ttl_seconds), 0.0)
        expires_at = time.monotonic() + ttl
        with self._lock:
            self._store[key] = (expires_at, deepcopy(value))

    def invalidate(self, key: str | None = None, prefix: str | None = None):
        with self._lock:
            if key is not None:
                self._store.pop(key, None)
                return
            if prefix is not None:
                keys = [k for k in self._store.keys() if k.startswith(prefix)]
                for k in keys:
                    self._store.pop(k, None)
                return
            self._store.clear()
