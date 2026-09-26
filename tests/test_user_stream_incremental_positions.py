from unittest.mock import MagicMock
from app.user_stream import BinanceUserDataStream


def test_user_stream_incremental_positions_does_not_wipe_other_positions():
    stream = BinanceUserDataStream(api_key="test")
    stream.trade_repo = MagicMock()

    # Event 1: Initial state has BTC (+100) and ETH (+200)
    event1 = {
        "e": "ACCOUNT_UPDATE",
        "E": 1000,
        "a": {
            "B": [{"a": "USDT", "wb": "10000", "cw": "10000"}],
            "P": [
                {"s": "BTCUSDT", "pa": "1.0", "up": "100.0", "ps": "BOTH"},
                {"s": "ETHUSDT", "pa": "10.0", "up": "200.0", "ps": "BOTH"},
            ],
        },
    }
    stream._handle_account_update(event1)
    # Total PnL should be 100 + 200 = 300, balance = 10000 + 300 = 10300
    assert stream.trade_repo.save_balance_history.call_args[1]["balance"] == 10300.0

    # Event 2: Only BTC updates to +150 (ETH is NOT present in P, as per Binance API spec)
    event2 = {
        "e": "ACCOUNT_UPDATE",
        "E": 2000,
        "a": {
            "B": [{"a": "USDT", "wb": "10000", "cw": "10000"}],
            "P": [
                {"s": "BTCUSDT", "pa": "1.0", "up": "150.0", "ps": "BOTH"},
            ],
        },
    }
    stream._handle_account_update(event2)
    # Total PnL MUST be 150 (BTC) + 200 (ETH retained) = 350 -> balance = 10350!
    # In the buggy code, it would be 10000 + 150 = 10150 because ETH was wiped!
    assert stream.trade_repo.save_balance_history.call_args[1]["balance"] == 10350.0

    # Event 3: ETH is closed (pa=0, up=0)
    event3 = {
        "e": "ACCOUNT_UPDATE",
        "E": 3000,
        "a": {
            "B": [{"a": "USDT", "wb": "10200", "cw": "10200"}],
            "P": [
                {"s": "ETHUSDT", "pa": "0", "up": "0", "ps": "BOTH"},
            ],
        },
    }
    stream._handle_account_update(event3)
    # ETH removed from map, only BTC (150) remains: balance = 10200 + 150 = 10350
    assert stream.trade_repo.save_balance_history.call_args[1]["balance"] == 10350.0
