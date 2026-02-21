"""
Binance USD-M Futures User Data Stream listener.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Optional

import requests
import websocket

from app.database import Database
from app.logger import logger
from app.repositories import TradeRepository


class BinanceUserDataStream:
    """Listen to Binance user data stream and persist key events."""

    def __init__(
        self,
        api_key: str,
        db: Optional[Database] = None,
        rest_base_url: Optional[str] = None,
        ws_base_url: Optional[str] = None,
    ):
        self.api_key = api_key
        self.db = db or Database()
        self.trade_repo = TradeRepository(self.db)
        self.rest_base_url = rest_base_url or os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")
        self.ws_base_url = ws_base_url or os.getenv("BINANCE_FAPI_WS_BASE_URL", "wss://fstream.binance.com")

        self.listen_key: Optional[str] = None
        self._running = False
        self._ws_app: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None

    def start(self):
        if self._running:
            return
        self._running = True

        if not self._create_listen_key():
            self._running = False
            return

        self._start_ws()
        self._keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self._keepalive_thread.start()
        logger.info("User data stream started")

    def stop(self):
        self._running = False
        if self._ws_app:
            try:
                self._ws_app.close()
            except Exception as exc:
                logger.warning(f"Failed to close user stream websocket: {exc}")
        logger.info("User data stream stopped")

    def _create_listen_key(self) -> bool:
        url = f"{self.rest_base_url}/fapi/v1/listenKey"
        try:
            response = requests.post(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.listen_key = data.get("listenKey")
            if not self.listen_key:
                logger.warning("listenKey missing in response")
                return False
            return True
        except Exception as exc:
            logger.warning(f"Failed to create listenKey: {exc}")
            return False

    def _keepalive_loop(self):
        # Keepalive every 30 minutes
        while self._running and self.listen_key:
            time.sleep(30 * 60)
            try:
                url = f"{self.rest_base_url}/fapi/v1/listenKey"
                response = requests.put(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=10)
                response.raise_for_status()
                logger.debug("listenKey keepalive success")
            except Exception as exc:
                logger.warning(f"listenKey keepalive failed: {exc}")

    def _start_ws(self):
        ws_url = f"{self.ws_base_url}/ws/{self.listen_key}"
        self._ws_app = websocket.WebSocketApp(
            ws_url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws_thread = threading.Thread(
            target=self._ws_app.run_forever,
            kwargs={"ping_interval": 20, "ping_timeout": 10},
            daemon=True,
        )
        self._ws_thread.start()

    def _on_message(self, _ws, message: str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logger.debug("Ignoring non-JSON user stream message")
            return

        event_type = payload.get("e", "UNKNOWN")
        event_time = payload.get("E") or int(time.time() * 1000)
        self.trade_repo.save_ws_event(event_type, event_time, payload)

        if event_type == "ACCOUNT_UPDATE":
            self._handle_account_update(payload)

    def _handle_account_update(self, payload: dict):
        account_info = payload.get("a", {})
        balances = account_info.get("B", [])

        usdt = None
        for balance in balances:
            if balance.get("a") == "USDT":
                usdt = balance
                break

        if not usdt:
            return

        try:
            wallet_balance = float(usdt.get("wb", 0))
            margin_balance = float(usdt.get("cw", wallet_balance))
        except (TypeError, ValueError):
            return

        # Persist as balance history snapshot
        self.trade_repo.save_balance_history(balance=margin_balance, wallet_balance=wallet_balance)

    def _on_error(self, _ws, error):
        logger.warning(f"User stream websocket error: {error}")

    def _on_close(self, _ws, status_code, msg):
        logger.warning(f"User stream websocket closed: {status_code} {msg}")
