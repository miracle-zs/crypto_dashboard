"""Binance request weights and exact rolling-window throttling."""

from __future__ import annotations

from collections import deque
import threading
import time
from typing import Iterable, Mapping, MutableSequence


RequestSpec = tuple[str, Mapping[str, object] | None]


def binance_request_weight(path: str, params: Mapping[str, object] | None = None) -> int:
    """Return the configured USD-M request weight for one emitted request."""
    endpoint = str(path or "").split("?", 1)[0]
    request_params = params or {}

    if endpoint == "/fapi/v1/klines":
        try:
            limit = int(request_params.get("limit", 500))
        except (TypeError, ValueError):
            limit = 500
        if limit < 100:
            return 1
        if limit < 500:
            return 2
        if limit <= 1000:
            return 5
        return 10

    if endpoint == "/fapi/v1/ticker/24hr":
        return 1 if request_params.get("symbol") else 40
    if endpoint == "/fapi/v1/income":
        return 30
    if endpoint in {
        "/fapi/v1/allOrders",
        "/fapi/v1/userTrades",
        "/fapi/v1/account",
        "/fapi/v2/account",
        "/fapi/v3/account",
        "/fapi/v1/positionRisk",
        "/fapi/v2/positionRisk",
        "/fapi/v3/positionRisk",
    }:
        return 5
    return 1


def calculate_request_weight(requests: Iterable[RequestSpec]) -> int:
    """Sum every request in an emitted request plan without per-symbol shortcuts."""
    return sum(binance_request_weight(path, params) for path, params in requests)


class RollingWeightLimiter:
    """Thread-safe limiter enforcing a weighted rolling window exactly."""

    def __init__(
        self,
        max_weight: int,
        *,
        window_seconds: float = 60.0,
        clock=time.monotonic,
        sleeper=time.sleep,
    ):
        if int(max_weight) <= 0:
            raise ValueError("max_weight must be positive")
        if float(window_seconds) <= 0:
            raise ValueError("window_seconds must be positive")
        self.max_weight = int(max_weight)
        self.window_seconds = float(window_seconds)
        self._clock = clock
        self._sleeper = sleeper
        self._events: MutableSequence[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] <= cutoff:
            self._events.popleft()

    def acquire(self, weight: int) -> float:
        """Reserve weight, sleeping until the complete rolling window has room."""
        requested = int(weight)
        if requested <= 0:
            return 0.0
        if requested > self.max_weight:
            raise ValueError(
                f"request weight {requested} exceeds rolling limit {self.max_weight}"
            )

        waited = 0.0
        while True:
            with self._lock:
                now = float(self._clock())
                self._prune(now)
                used = sum(event_weight for _timestamp, event_weight in self._events)
                if used + requested <= self.max_weight:
                    self._events.append((now, requested))
                    return waited
                sleep_for = max(
                    0.001,
                    self._events[0][0] + self.window_seconds - now,
                )
            self._sleeper(sleep_for)
            waited += sleep_for

    def usage(self) -> int:
        """Return reserved weight in the current rolling window."""
        with self._lock:
            now = float(self._clock())
            self._prune(now)
            return sum(event_weight for _timestamp, event_weight in self._events)

    def reset(self, *, max_weight: int | None = None) -> None:
        with self._lock:
            if max_weight is not None:
                if int(max_weight) <= 0:
                    raise ValueError("max_weight must be positive")
                self.max_weight = int(max_weight)
            self._events.clear()

    def set_max_weight(self, max_weight: int) -> None:
        """Change capacity without erasing requests still inside the window."""
        if int(max_weight) <= 0:
            raise ValueError("max_weight must be positive")
        with self._lock:
            self.max_weight = int(max_weight)


class BulkRequestAborted(RuntimeError):
    """Raised when a 429/418 cooldown makes a batch unsafe to continue."""


class WeightedRequestSession:
    """Client adapter that records every emitted request and applies a job cap."""

    def __init__(self, client, *, limiter: RollingWeightLimiter | None = None):
        self._client = client
        self._limiter = limiter
        self.requests: list[RequestSpec] = []

    @property
    def total_weight(self) -> int:
        return calculate_request_weight(self.requests)

    def _call(self, method_name: str, path: str, params=None):
        request_params = dict(params) if params else None
        weight = binance_request_weight(path, request_params)
        if self._limiter is not None:
            self._limiter.acquire(weight)
        self.requests.append((path, request_params))
        result = getattr(self._client, method_name)(path, params)
        cooldown_check = getattr(self._client, "is_global_cooldown_active", None)
        if result is None and callable(cooldown_check) and cooldown_check():
            raise BulkRequestAborted(f"Binance cooldown activated while requesting {path}")
        return result

    def public_get(self, path: str, params=None):
        return self._call("public_get", path, params)

    def signed_get(self, path: str, params=None):
        return self._call("signed_get", path, params)
