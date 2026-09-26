"""
Binance USD-M Futures User Data Stream listener with auto-reconnection.
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
    """Listen to Binance user data stream with resilient auto-reconnect and persist key events."""

    def __init__(
        self,
        api_key: str,
        db: Optional[Database] = None,
        rest_base_url: Optional[str] = None,
        ws_base_url: Optional[str] = None,
        reconnect_delay_seconds: float = 2.0,
        max_reconnect_delay_seconds: float = 60.0,
    ):
        self.api_key = api_key
        self.db = db or Database()
        self.trade_repo = TradeRepository(self.db)
        self.rest_base_url = rest_base_url or os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")
        self.ws_base_url = ws_base_url or os.getenv("BINANCE_FAPI_WS_BASE_URL", "wss://fstream.binance.com")

        self.reconnect_delay_seconds = float(reconnect_delay_seconds)
        self.max_reconnect_delay_seconds = float(max_reconnect_delay_seconds)

        self.listen_key: Optional[str] = None
        self._running = False
        self._is_connected = False
        self._last_event_time_ms: int = 0
        self._ws_app: Optional[websocket.WebSocketApp] = None
        self._supervisor_thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None

    @property
    def is_connected(self) -> bool:
        return self._is_connected and self._running

    @property
    def last_event_time_ms(self) -> int:
        return self._last_event_time_ms

    def start(self):
        if self._running:
            return
        self._running = True

        self._supervisor_thread = threading.Thread(
            target=self._run_supervisor, daemon=True, name="user-stream-supervisor"
        )
        self._supervisor_thread.start()

        self._keepalive_thread = threading.Thread(
            target=self._keepalive_loop, daemon=True, name="user-stream-keepalive"
        )
        self._keepalive_thread.start()
        logger.info("User data stream supervisor started")

    def stop(self):
        if not self._running:
            return
        self._running = False
        self._is_connected = False

        if self._ws_app:
            try:
                self._ws_app.close()
            except Exception as exc:
                logger.warning(f"Failed to close user stream websocket: {exc}")

        # Delete listenKey on stop to release server-side resources
        if self.listen_key:
            self._delete_listen_key(self.listen_key)
            self.listen_key = None

        logger.info("User data stream stopped")

    def _create_listen_key(self) -> bool:
        url = f"{self.rest_base_url}/fapi/v1/listenKey"
        try:
            response = requests.post(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.listen_key = data.get("listenKey")
            if not self.listen_key:
                logger.warning("listenKey missing in Binance response")
                return False
            logger.info("Obtained Binance user stream listenKey")
            return True
        except Exception as exc:
            logger.warning(f"Failed to create listenKey: {exc}")
            return False

    def _delete_listen_key(self, key: str):
        url = f"{self.rest_base_url}/fapi/v1/listenKey"
        try:
            requests.delete(url, headers={"X-MBX-APIKEY": self.api_key}, params={"listenKey": key}, timeout=5)
        except Exception:
            pass

    def _keepalive_loop(self):
        # Keepalive every 25 minutes (Binance listenKey expires in 60 minutes)
        keepalive_interval = 25 * 60
        while self._running:
            time.sleep(keepalive_interval)
            if not self._running or not self.listen_key:
                continue

            try:
                url = f"{self.rest_base_url}/fapi/v1/listenKey"
                response = requests.put(
                    url,
                    headers={"X-MBX-APIKEY": self.api_key},
                    params={"listenKey": self.listen_key},
                    timeout=10,
                )
                response.raise_for_status()
                logger.debug("listenKey keepalive success")
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                logger.warning(f"listenKey keepalive HTTP error: {status_code}, invalidating key")
                # Invalid listenKey; close current WS so supervisor reconnects with fresh key
                self.listen_key = None
                if self._ws_app:
                    try:
                        self._ws_app.close()
                    except Exception:
                        pass
            except Exception as exc:
                logger.warning(f"listenKey keepalive failed: {exc}")

    def _run_supervisor(self):
        backoff = self.reconnect_delay_seconds
        while self._running:
            try:
                if not self.listen_key:
                    if not self._create_listen_key():
                        logger.warning(f"Failed to create listenKey, retrying in {backoff:.1f}s...")
                        time.sleep(backoff)
                        backoff = min(self.max_reconnect_delay_seconds, backoff * 2)
                        continue

                ws_url = f"{self.ws_base_url}/ws/{self.listen_key}"
                logger.info(f"Connecting user data stream to {self.ws_base_url}/ws/...")

                self._ws_app = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )

                # Reset backoff when connection loop runs
                backoff = self.reconnect_delay_seconds
                self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                logger.error(f"User stream exception in supervisor loop: {exc}")
            finally:
                self._is_connected = False

            if not self._running:
                break

            logger.warning(
                f"User data stream connection lost. Reconnecting in {backoff:.1f}s..."
            )
            # Invalidate old key so a fresh one is acquired upon reconnect
            self.listen_key = None
            time.sleep(backoff)
            backoff = min(self.max_reconnect_delay_seconds, backoff * 2)

    def _on_open(self, _ws):
        self._is_connected = True
        logger.info("User data stream WebSocket connection established")

    def _on_message(self, _ws, message: str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logger.debug("Ignoring non-JSON user stream message")
            return

        event_type = payload.get("e", "UNKNOWN")
        event_time = payload.get("E") or int(time.time() * 1000)
        self._last_event_time_ms = event_time

        try:
            self.trade_repo.save_ws_event(event_type, event_time, payload)
        except Exception as exc:
            logger.warning(f"Failed to save ws event: {exc}")

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
            cross_wallet = float(usdt.get("cw", wallet_balance))
        except (TypeError, ValueError):
            return

        positions = account_info.get("P", [])
        has_positions_pnl = False
        if positions:
            usdt_pnl = 0.0
            for p in positions:
                if p.get("ma") == "USDT" or str(p.get("s", "")).endswith("USDT"):
                    try:
                        usdt_pnl += float(p.get("up", 0.0))
                        has_positions_pnl = True
                    except (ValueError, TypeError):
                        pass
            if has_positions_pnl:
                self._last_unrealized_pnl = usdt_pnl

        if has_positions_pnl or hasattr(self, "_last_unrealized_pnl"):
            margin_balance = wallet_balance + getattr(self, "_last_unrealized_pnl", 0.0)
        else:
            # Fallback when no position context is known yet: use cross_wallet
            margin_balance = cross_wallet

        # Persist as balance history snapshot
        try:
            self.trade_repo.save_balance_history(balance=margin_balance, wallet_balance=wallet_balance)
            logger.info(f"User stream balance updated: {margin_balance:.2f} USDT (Wallet: {wallet_balance:.2f}, Cross: {cross_wallet:.2f})")
        except Exception as exc:
            logger.warning(f"Failed to persist balance history from user stream: {exc}")


    def _on_error(self, _ws, error):
        logger.warning(f"User stream websocket error: {error}")

    def _on_close(self, _ws, status_code, msg):
        self._is_connected = False
        logger.warning(f"User stream websocket closed: status={status_code}, msg={msg}")
