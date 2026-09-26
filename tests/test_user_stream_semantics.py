import json
from unittest.mock import MagicMock

from app.user_stream import BinanceUserDataStream


def test_user_stream_account_update_with_positions_unrealized_pnl():
    """Verify that user_stream accounts for unrealized PnL from positions rather than mistaking cw for margin balance."""
    mock_db = MagicMock()
    stream = BinanceUserDataStream(api_key="test_key", db=mock_db)
    stream.trade_repo = MagicMock()

    # Payload with wallet balance 15000 and position with unrealized loss -500
    msg = json.dumps({
        "e": "ACCOUNT_UPDATE",
        "E": 1780000000000,
        "a": {
            "m": "ORDER",
            "B": [
                {"a": "USDT", "wb": "15000.00", "cw": "15000.00"}
            ],
            "P": [
                {"s": "BTCUSDT", "pa": "1.0", "up": "-500.00", "ma": "USDT"}
            ]
        }
    })

    stream._on_message(None, msg)

    # balance should be 15000 + (-500) = 14500, wallet_balance should be 15000
    stream.trade_repo.save_balance_history.assert_called_once_with(
        balance=14500.0, wallet_balance=15000.0
    )


def test_user_stream_preserves_prior_unrealized_pnl_when_positions_not_in_event():
    """When ACCOUNT_UPDATE has balance change only (e.g. deposit/fee), preserve known unrealized PnL."""
    mock_db = MagicMock()
    stream = BinanceUserDataStream(api_key="test_key", db=mock_db)
    stream.trade_repo = MagicMock()
    stream._last_unrealized_pnl = -300.0  # prior known unrealized PnL

    msg = json.dumps({
        "e": "ACCOUNT_UPDATE",
        "E": 1780000001000,
        "a": {
            "m": "DEPOSIT",
            "B": [
                {"a": "USDT", "wb": "16000.00", "cw": "16000.00"}
            ]
        }
    })

    stream._on_message(None, msg)

    # balance should be 16000 + (-300) = 15700
    stream.trade_repo.save_balance_history.assert_called_once_with(
        balance=15700.0, wallet_balance=16000.0
    )
