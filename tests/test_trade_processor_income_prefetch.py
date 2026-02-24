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


def test_summarize_income_records_tracks_extra_loss_income_types():
    records = [
        {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-1.0"},
        {"symbol": "BTCUSDT", "incomeType": "INSURANCE_CLEAR", "income": "-9.5"},
        {"symbol": "BTCUSDT", "incomeType": "REALIZED_PNL", "income": "-30.0"},
        {"symbol": "ETHUSDT", "incomeType": "FUNDING_FEE", "income": "-0.2"},
    ]

    symbols, totals = TradeDataProcessor._summarize_income_records(
        records,
        extra_loss_income_types={"INSURANCE_CLEAR"},
    )

    assert set(symbols) == {"BTCUSDT", "ETHUSDT"}
    assert totals["BTCUSDT"] == -10.5
    assert totals["ETHUSDT"] == -0.2


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


def test_analyze_orders_uses_prefetched_fee_totals_without_second_income_fetch():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    processor.max_etl_workers = 1
    processor.max_price_workers = 1
    processor.max_open_positions_workers = 1
    processor._resolve_workers = lambda task_count, max_workers=None: 1
    processor._extract_symbol_closed_positions = (
        lambda symbol, since, until, use_time_filter, fee_totals_by_symbol: ([], 0.0)
    )

    called = {"fee_totals": 0}

    def fail_if_called(*args, **kwargs):
        called["fee_totals"] += 1
        raise AssertionError("get_fee_totals_by_symbol should not be called when prefetched_fee_totals is provided")

    processor.get_fee_totals_by_symbol = fail_if_called

    result = processor.analyze_orders(
        since=1,
        until=2,
        traded_symbols=["BTCUSDT"],
        use_time_filter=True,
        prefetched_fee_totals={"BTCUSDT": -1.5},
        return_symbol_status=True,
    )

    assert isinstance(result, tuple)
    df, success_symbols, failure_symbols = result
    assert df.empty
    assert success_symbols == ["BTCUSDT"]
    assert failure_symbols == {}
    assert called["fee_totals"] == 0
