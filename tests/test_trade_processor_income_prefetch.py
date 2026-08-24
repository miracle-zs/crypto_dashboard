from app.trade_processor import TradeDataProcessor
from app.services.trade_etl_service import _prefetch_open_prices, extract_symbol_closed_positions


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


def test_incremental_symbol_etl_queries_orders_and_user_trades_once():
    class FakeProcessor:
        def __init__(self):
            self.calls = []

        def _create_worker_client(self):
            return object()

        def get_all_orders(self, symbol, **kwargs):
            self.calls.append(("orders", symbol, kwargs["start_time"], kwargs["end_time"]))
            return [
                {
                    "orderId": 12,
                    "updateTime": 950_000,
                    "executedQty": "1",
                }
            ]

        def get_user_trades(self, symbol, **kwargs):
            self.calls.append(("trades", symbol, kwargs["start_time"], kwargs["end_time"]))
            return [{"id": 13, "orderId": 12, "time": 960_000}]

        def match_orders_to_positions(self, orders, symbol, fees_map, presorted=False):
            return []

    processor = FakeProcessor()
    positions, _elapsed, cursors = extract_symbol_closed_positions(
        processor,
        symbol="BTCUSDT",
        since=800_000,
        until=1_000_000,
        fee_totals_by_symbol={"BTCUSDT": -1.0},
        order_cursor={"last_id": 10, "last_time_ms": 900_000},
        trade_cursor={"last_id": 11, "last_time_ms": 910_000},
        cursor_overlap_minutes=10,
        return_cursor_update=True,
    )

    assert positions == []
    assert [call[0] for call in processor.calls] == ["orders", "trades"]
    assert cursors == {
        "orders": {"last_id": 12, "last_time_ms": 950_000},
        "trades": {"last_id": 13, "last_time_ms": 960_000},
    }


def test_trade_open_prices_prefer_local_daily_kline_cache():
    day_start = 1_700_000_000_000

    class FakeRepo:
        def load(self, symbols, since_open_time=None):
            assert symbols == ["BTCUSDT"]
            return {
                "BTCUSDT": [
                    {"symbol": "BTCUSDT", "open_time": day_start, "open": 123.0}
                ]
            }

    class FakeProcessor:
        daily_kline_repo = FakeRepo()
        max_price_workers = 1

        def get_utc_day_start(self, timestamp):
            return day_start

        def _resolve_workers(self, task_count, max_workers=None):
            return 1

        def _fetch_open_price_for_key(self, *args, **kwargs):
            raise AssertionError("local cache hit must not emit a kline request")

    prices, _elapsed = _prefetch_open_prices(
        FakeProcessor(),
        all_positions=[{"symbol": "BTCUSDT", "entry_time": day_start + 1000}],
    )

    assert prices == {("BTCUSDT", day_start): 123.0}
