"""
Binance Futures REST client (USD-M) with basic rate limiting and signing.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from app.logger import logger


class BinanceFuturesRestClient:
    """Lightweight REST client for Binance USD-M Futures."""

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
        self._last_request_ts = 0.0

    def _headers(self) -> Dict[str, str]:
        if not self.api_key:
            return {}
        return {"X-MBX-APIKEY": self.api_key}

    def _sign_params(self, params: Dict[str, Any]) -> str:
        query_string = urlencode(params)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Optional[Any]:
        if params is None:
            params = {}
        else:
            params = dict(params)

        # Rate limit spacing between requests
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_ts = time.time()

        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["signature"] = self._sign_params(params)

        url = f"{self.base_url}{path}"

        max_retries = 4
        backoff_seconds = 1
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self._headers(),
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
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
