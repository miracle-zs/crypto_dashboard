from unittest.mock import MagicMock
from app.user_stream import BinanceUserDataStream


def test_user_stream_incremental_positions_does_not_wipe_other_positions():
    stream = BinanceUserDataStream(api_key="test", db=MagicMock())
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

    # Event 3: ETH is closed while BTC remains open.
    event3 = {
        "e": "ACCOUNT_UPDATE",
        "E": 3000,
        "a": {
            "B": [{"a": "USDT", "wb": "10200", "cw": "10200"}],
            "P": [{"s": "ETHUSDT", "pa": "0", "up": "0", "ps": "BOTH"}],
        },
    }
    stream._handle_account_update(event3)
    assert stream.trade_repo.save_balance_history.call_args[1]["balance"] == 10350.0


def test_user_stream_primes_incremental_state_from_full_positions_and_refreshes_on_reconnect():
    class FakeProcessor:
        _latest_position_risk = [
            {"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "1", "unRealizedProfit": "100"},
            {"symbol": "ETHUSDT", "positionSide": "BOTH", "positionAmt": "2", "unRealizedProfit": "200"},
        ]

        def get_real_positions(self):
            return {"BTCUSDT": 1.0, "ETHUSDT": 2.0}

    processor = FakeProcessor()
    stream = BinanceUserDataStream(api_key="test", db=MagicMock(), processor=processor)
    stream.trade_repo = MagicMock()

    stream._on_open(None)
    assert stream._position_cache_ready is True
    assert sum(stream._position_pnl_map.values()) == 300.0
    stream._handle_account_update({"a": {
        "B": [{"a": "USDT", "wb": "10000", "cw": "10000"}],
        "P": [{"s": "BTCUSDT", "pa": "1", "up": "150", "ps": "BOTH"}],
    }})
    assert stream.trade_repo.save_balance_history.call_args.kwargs["balance"] == 10350.0

    stream._on_close(None, 1006, "reconnect")
    processor._latest_position_risk = [
        {"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "1", "unRealizedProfit": "120"},
        {"symbol": "ETHUSDT", "positionSide": "BOTH", "positionAmt": "2", "unRealizedProfit": "250"},
    ]
    stream._on_open(None)
    stream._handle_account_update({"a": {
        "B": [{"a": "USDT", "wb": "10000", "cw": "10000"}],
    }})

    assert sum(stream._position_pnl_map.values()) == 370.0
    assert stream.trade_repo.save_balance_history.call_args.kwargs["balance"] == 10370.0


def test_user_stream_skips_balance_rows_until_full_position_snapshot_is_available():
    class FailedSnapshotProcessor:
        _latest_position_risk = None

        def get_real_positions(self):
            return None

    stream = BinanceUserDataStream(api_key="test", db=MagicMock(), processor=FailedSnapshotProcessor())
    stream.trade_repo = MagicMock()
    stream._on_open(None)

    stream._handle_account_update({"a": {
        "B": [{"a": "USDT", "wb": "10000", "cw": "10000"}],
        "P": [{"s": "BTCUSDT", "pa": "1", "up": "150", "ps": "BOTH"}],
    }})

    stream.trade_repo.save_balance_history.assert_not_called()


def test_user_stream_removes_last_closed_position_pnl_from_later_balances():
    stream = BinanceUserDataStream(api_key="test", db=MagicMock())
    stream.trade_repo = MagicMock()
    balance = [{"a": "USDT", "wb": "1000", "cw": "1000"}]
    stream._handle_account_update({"a": {"B": balance, "P": [
        {"s": "BTCUSDT", "pa": "1", "up": "100", "ps": "BOTH"},
    ]}})
    assert stream.trade_repo.save_balance_history.call_args.kwargs["balance"] == 1100

    stream._handle_account_update({"a": {"B": balance, "P": [
        {"s": "BTCUSDT", "pa": "0", "up": "0", "ps": "BOTH"},
    ]}})
    assert stream.trade_repo.save_balance_history.call_args.kwargs["balance"] == 1000
    stream._handle_account_update({"a": {"B": [{"a": "USDT", "wb": "1000", "cw": "900"}]}})
    assert stream.trade_repo.save_balance_history.call_args.kwargs["balance"] == 1000


def test_missing_position_quantity_does_not_clear_known_position():
    stream = BinanceUserDataStream(api_key="test", db=MagicMock())
    stream.trade_repo = MagicMock()
    balance = [{"a": "USDT", "wb": "1000", "cw": "1000"}]
    stream._handle_account_update({"a": {"B": balance, "P": [
        {"s": "BTCUSDT", "pa": "1", "up": "100", "ps": "BOTH"},
    ]}})
    stream._handle_account_update({"a": {"B": balance, "P": [
        {"s": "BTCUSDT", "up": "0", "ps": "BOTH"},
    ]}})
    assert stream.trade_repo.save_balance_history.call_args.kwargs["balance"] == 1100


def test_unknown_closed_position_does_not_clear_prior_unrealized_pnl():
    stream = BinanceUserDataStream(api_key="test", db=MagicMock())
    stream.trade_repo = MagicMock()
    stream._last_unrealized_pnl = -300
    stream._handle_account_update({"a": {
        "B": [{"a": "USDT", "wb": "1000", "cw": "1000"}],
        "P": [{"s": "BTCUSDT", "pa": "0", "up": "0", "ps": "BOTH"}],
    }})
    assert stream.trade_repo.save_balance_history.call_args.kwargs["balance"] == 700
