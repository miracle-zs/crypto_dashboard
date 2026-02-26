"""
Binance Futures REST client (USD-M) with basic rate limiting and signing.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from app.logger import logger


class BinanceFuturesRestClient:
    """Lightweight REST client for Binance USD-M Futures."""
    _cooldown_until_ms = 0
    _cooldown_lock = threading.Lock()
    _throttle_lock = threading.Lock()
    _global_last_request_ts = 0.0
    _request_budget_enabled = False
    _request_budget_per_minute = 0
    _request_budget_tokens = 0.0
    _request_budget_last_refill_ts = 0.0
    _request_budget_path_weights: Dict[str, int] = {}

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
        min_request_interval: Optional[float] = None,
    ):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.base_url = base_url or os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")
        self._min_request_interval = (
            float(min_request_interval)
            if min_request_interval is not None
            else float(os.getenv("BINANCE_MIN_REQUEST_INTERVAL", 0.3))
        )
        self._recv_window = int(os.getenv("BINANCE_RECV_WINDOW", 10000))
        self._time_offset_ms = 0
        self._last_cooldown_log_ts = 0.0

    def _headers(self) -> Dict[str, str]:
        if not self.api_key:
            return {}
        return {"X-MBX-APIKEY": self.api_key}

    @classmethod
    def configure_global_request_budget(
        cls,
        *,
        enabled: bool,
        per_minute: int = 0,
        path_weights: Optional[Dict[str, int]] = None,
    ):
        with cls._throttle_lock:
            if not enabled:
                cls._request_budget_enabled = False
                cls._request_budget_per_minute = 0
                cls._request_budget_tokens = 0.0
                cls._request_budget_last_refill_ts = 0.0
                cls._request_budget_path_weights = {}
                return

            capacity = max(1, int(per_minute))
            clean_weights: Dict[str, int] = {}
            for path, weight in (path_weights or {}).items():
                try:
                    clean_weights[str(path)] = max(1, int(weight))
                except Exception:
                    continue
            cls._request_budget_enabled = True
            cls._request_budget_per_minute = capacity
            cls._request_budget_tokens = float(capacity)
            cls._request_budget_last_refill_ts = time.monotonic()
            cls._request_budget_path_weights = clean_weights

    @classmethod
    def _path_weight(cls, path: str) -> int:
        weights = cls._request_budget_path_weights
        if not weights:
            return 1
        direct = weights.get(path)
        if direct is not None:
            return max(1, int(direct))
        return 1

    @classmethod
    def _refill_budget_tokens(cls):
        if not cls._request_budget_enabled or cls._request_budget_per_minute <= 0:
            return
        now = time.monotonic()
        if cls._request_budget_last_refill_ts <= 0:
            cls._request_budget_last_refill_ts = now
            return
        elapsed = now - cls._request_budget_last_refill_ts
        if elapsed <= 0:
            return
        refill_rate_per_sec = cls._request_budget_per_minute / 60.0
        cls._request_budget_tokens = min(
            float(cls._request_budget_per_minute),
            cls._request_budget_tokens + elapsed * refill_rate_per_sec,
        )
        cls._request_budget_last_refill_ts = now

    def _sign_params(self, params: Dict[str, Any]) -> str:
        query_string = urlencode(params)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _throttle(self, path: str = ""):
        # 全局节流：所有 client 实例共享请求节奏，避免多线程实例级并发叠加打爆IP限额。
        with self.__class__._throttle_lock:
            while True:
                now = time.time()
                elapsed = now - self.__class__._global_last_request_ts
                wait_interval = max(0.0, self._min_request_interval - elapsed)

                wait_budget = 0.0
                need_weight = 0
                budget_ready = True
                if self.__class__._request_budget_enabled and self.__class__._request_budget_per_minute > 0:
                    self.__class__._refill_budget_tokens()
                    need_weight = self.__class__._path_weight(path)
                    if self.__class__._request_budget_tokens < need_weight:
                        budget_ready = False
                        refill_rate_per_sec = self.__class__._request_budget_per_minute / 60.0
                        deficit = need_weight - self.__class__._request_budget_tokens
                        wait_budget = max(0.0, deficit / refill_rate_per_sec) if refill_rate_per_sec > 0 else 0.1

                if wait_interval <= 0 and budget_ready:
                    if need_weight > 0:
                        self.__class__._request_budget_tokens = max(
                            0.0,
                            self.__class__._request_budget_tokens - need_weight,
                        )
                    self.__class__._global_last_request_ts = time.time()
                    return

                sleep_seconds = max(wait_interval, wait_budget, 0.001)
                time.sleep(sleep_seconds)

    def _sync_server_time(self) -> bool:
        """Sync local request timestamp offset with Binance server time."""
        try:
            self._throttle("/fapi/v1/time")
            response = requests.get(f"{self.base_url}/fapi/v1/time", timeout=10)
            response.raise_for_status()
            data = response.json()
            server_time = int(data.get("serverTime"))
            local_time = int(time.time() * 1000)
            self._time_offset_ms = server_time - local_time
            logger.warning(f"Synced Binance server time offset: {self._time_offset_ms} ms")
            return True
        except Exception as exc:
            logger.error(f"Failed to sync Binance server time: {exc}")
            return False

    @classmethod
    def _set_global_cooldown_until(cls, until_ms: int, reason: str):
        with cls._cooldown_lock:
            if until_ms > cls._cooldown_until_ms:
                cls._cooldown_until_ms = until_ms
                logger.warning(
                    f"Binance API cooldown activated until {until_ms} (epoch ms), reason={reason}"
                )

    @classmethod
    def cooldown_remaining_seconds(cls) -> float:
        with cls._cooldown_lock:
            now_ms = int(time.time() * 1000)
            return max(0.0, (cls._cooldown_until_ms - now_ms) / 1000)

    @classmethod
    def is_global_cooldown_active(cls) -> bool:
        return cls.cooldown_remaining_seconds() > 0

    @staticmethod
    def _extract_ban_until_ms(message: Optional[str]) -> Optional[int]:
        if not message:
            return None
        matched = re.search(r"banned until (\d+)", message)
        if matched:
            try:
                return int(matched.group(1))
            except Exception:
                return None
        return None

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Optional[Any]:
        base_params: Dict[str, Any] = dict(params) if params else {}
        cooldown_left = self.cooldown_remaining_seconds()
        if cooldown_left > 0:
            now = time.time()
            if now - self._last_cooldown_log_ts > 30:
                self._last_cooldown_log_ts = now
                logger.warning(
                    f"Skip Binance request {path}: cooldown active ({cooldown_left:.1f}s remaining)"
                )
            return None

        url = f"{self.base_url}{path}"

        max_retries = 4
        backoff_seconds = 1
        clock_synced = False
        for attempt in range(1, max_retries + 1):
            try:
                self._throttle(path)
                request_params = dict(base_params)
                if signed:
                    request_params["recvWindow"] = self._recv_window
                    request_params["timestamp"] = int(time.time() * 1000 + self._time_offset_ms)
                    request_params["signature"] = self._sign_params(request_params)

                response = requests.request(
                    method=method,
                    url=url,
                    headers=self._headers(),
                    params=request_params,
                    timeout=30,
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                err_code = None
                err_msg = None
                if exc.response is not None:
                    try:
                        payload = exc.response.json()
                        err_code = payload.get("code")
                        err_msg = payload.get("msg")
                    except (json.JSONDecodeError, ValueError, TypeError):
                        err_code = None

                if signed and err_code == -1021 and not clock_synced and attempt < max_retries:
                    logger.warning("Timestamp out of recvWindow (-1021), syncing server time and retrying...")
                    clock_synced = self._sync_server_time()
                    if clock_synced:
                        continue

                if err_code == -1003 or status_code in (418, 429):
                    now_ms = int(time.time() * 1000)
                    ban_until_ms = self._extract_ban_until_ms(err_msg or "")
                    if ban_until_ms is None:
                        fallback_seconds = 600 if status_code == 418 else 60
                        ban_until_ms = now_ms + fallback_seconds * 1000
                    self._set_global_cooldown_until(
                        until_ms=ban_until_ms,
                        reason=f"status={status_code}, code={err_code}"
                    )
                    # -1003 means request budget is exhausted; stop retrying to avoid prolonging ban.
                    logger.error(f"API request failed: {exc}")
                    if hasattr(exc, "response") and exc.response is not None:
                        logger.error(f"Response: {exc.response.text}")
                    return None

                if status_code == 429 and attempt < max_retries:
                    logger.warning(
                        f"Rate limited (429) on {path}. Backing off {backoff_seconds}s (attempt {attempt}/{max_retries})"
                    )
                    time.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                logger.error(f"API request failed: {exc}")
                if hasattr(exc, "response") and exc.response is not None:
                    logger.error(f"Response: {exc.response.text}")
                return None
            except requests.exceptions.RequestException as exc:
                logger.error(f"API request failed: {exc}")
                if hasattr(exc, "response") and exc.response is not None:
                    logger.error(f"Response: {exc.response.text}")
                return None

        return None

    def public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        return self.request("GET", path, params=params, signed=False)

    def signed_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        return self.request("GET", path, params=params, signed=True)

    def signed_post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        return self.request("POST", path, params=params, signed=True)

    def public_post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        return self.request("POST", path, params=params, signed=False)

    def public_put(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        return self.request("PUT", path, params=params, signed=False)
