import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from app.user_stream import BinanceUserDataStream


def test_user_stream_account_update_parses_and_persists():
    mock_db = MagicMock()
    stream = BinanceUserDataStream(api_key="test_key", db=mock_db)
    stream.trade_repo = MagicMock()

    msg = json.dumps({
        "e": "ACCOUNT_UPDATE",
        "E": 1780000000000,
        "a": {
            "B": [
                {"a": "BTC", "wb": "1.0", "cw": "1.0"},
                {"a": "USDT", "wb": "15000.50", "cw": "14800.25"}
            ]
        }
    })

    stream._on_message(None, msg)

    # Verify event saved
    stream.trade_repo.save_ws_event.assert_called_once()
    call_args = stream.trade_repo.save_ws_event.call_args[0]
    assert call_args[0] == "ACCOUNT_UPDATE"
    assert call_args[1] == 1780000000000

    # Verify balance saved
    stream.trade_repo.save_balance_history.assert_called_once_with(
        balance=14800.25, wallet_balance=15000.50
    )


def test_user_stream_supervisor_reconnect_lifecycle(monkeypatch):
    post_calls = []
    close_calls = []

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            key = f"key_{len(post_calls)}"
            return {"listenKey": key}

    monkeypatch.setattr(
        requests,
        "post",
        lambda *args, **kwargs: post_calls.append(kwargs) or FakeResponse(),
    )
    monkeypatch.setattr(requests, "delete", lambda *args, **kwargs: None)

    runs = 0

    class FakeWebSocketApp:
        def __init__(self, url, on_open=None, on_message=None, on_error=None, on_close=None):
            self.url = url
            self.on_open = on_open
            self.on_close = on_close

        def run_forever(self, **kwargs):
            nonlocal runs
            runs += 1
            if self.on_open:
                self.on_open(self)
            # Simulate immediate close for first run
            if runs == 1:
                if self.on_close:
                    self.on_close(self, 1006, "abnormal")
                return
            # Second run stays until stopped
            time.sleep(0.1)

        def close(self):
            close_calls.append(True)

    monkeypatch.setattr("websocket.WebSocketApp", FakeWebSocketApp)

    stream = BinanceUserDataStream(
        api_key="test_key",
        reconnect_delay_seconds=0.01,
        max_reconnect_delay_seconds=0.05,
    )

    try:
        stream.start()
        # Allow supervisor thread to run, disconnect once, and reconnect
        time.sleep(0.15)
        assert runs >= 2, f"Expected at least 2 runs (reconnection), got {runs}"
        assert len(post_calls) >= 2, f"Expected at least 2 listenKey requests, got {len(post_calls)}"
    finally:
        stream.stop()


def test_user_stream_keepalive_failure_invalidates_key(monkeypatch):
    put_calls = []

    class FakeErrorResponse:
        status_code = 400

        def raise_for_status(self):
            exc = requests.exceptions.HTTPError("400 Client Error", response=self)
            raise exc

    monkeypatch.setattr(
        requests,
        "put",
        lambda *args, **kwargs: put_calls.append(kwargs) or FakeErrorResponse(),
    )

    stream = BinanceUserDataStream(api_key="test_key")
    stream.listen_key = "test_listen_key"
    stream._running = True

    mock_ws = MagicMock()
    stream._ws_app = mock_ws

    # Trigger one keepalive loop iteration directly
    with patch("time.sleep", return_value=None):
        def stop_after_one(*args, **kwargs):
            stream._running = False
            return FakeErrorResponse()
        monkeypatch.setattr(requests, "put", stop_after_one)
        stream._keepalive_loop()

    assert stream.listen_key is None
    mock_ws.close.assert_called()
