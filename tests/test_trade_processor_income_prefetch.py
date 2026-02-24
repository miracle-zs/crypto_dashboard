from app.trade_processor import TradeDataProcessor


def test_analyze_orders_prefetches_income_once_when_symbols_not_provided():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    processor.max_etl_workers = 1
    processor.max_price_workers = 1

    calls = {"income": 0}

    def fake_fetch_income_history(since, until, client=None, income_type=None):
        calls["income"] += 1
        return [
            {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-1.5"},
            {"symbol": "BTCUSDT", "incomeType": "REALIZED_PNL", "income": "3.2"},
        ]

    processor._fetch_income_history = fake_fetch_income_history
    processor._resolve_workers = lambda task_count, max_workers=None: 1
    processor._extract_symbol_closed_positions = (
        lambda symbol, since, until, use_time_filter, fee_totals_by_symbol: ([], 0.0)
    )

    df = processor.analyze_orders(
        since=1,
        until=2,
        traded_symbols=None,
        use_time_filter=True,
    )

    assert df.empty
    assert calls["income"] == 1


def test_analyze_orders_return_symbol_status_with_no_symbols_returns_triplet():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    processor._fetch_income_history = lambda since, until, client=None, income_type=None: []

    result = processor.analyze_orders(
        since=1,
        until=2,
        traded_symbols=None,
        use_time_filter=True,
        return_symbol_status=True,
    )

    assert isinstance(result, tuple)
    assert len(result) == 3
    df, success_symbols, failure_symbols = result
    assert df.empty
    assert success_symbols == []
    assert failure_symbols == {}
