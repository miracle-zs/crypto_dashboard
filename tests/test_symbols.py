from app.core.symbols import normalize_futures_symbol


def test_normalize_futures_symbol_appends_usdt_for_plain_symbol():
    assert normalize_futures_symbol("btc") == "BTCUSDT"


def test_normalize_futures_symbol_keeps_existing_usdt_symbol():
    assert normalize_futures_symbol("ethusdt") == "ETHUSDT"


def test_normalize_futures_symbol_returns_empty_for_blank_input():
    assert normalize_futures_symbol("") == ""
    assert normalize_futures_symbol(None) == ""


def test_normalize_futures_symbol_optionally_preserves_busd():
    assert normalize_futures_symbol("bnbbusd", preserve_busd=True) == "BNBBUSD"
    assert normalize_futures_symbol("bnbbusd", preserve_busd=False) == "BNBBUSDUSDT"
