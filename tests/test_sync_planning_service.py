from app.services.sync_planning_service import build_symbol_since_map


def test_build_symbol_since_map_handles_warm_and_cold_symbols():
    symbols = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
    watermarks = {"BTCUSDT": 1_000_000, "ETHUSDT": None}

    symbol_since_map, warmed = build_symbol_since_map(
        traded_symbols=symbols,
        watermarks=watermarks,
        since=800_000,
        overlap_minutes=10,
    )

    assert warmed == 1
    assert symbol_since_map["BTCUSDT"] == 800_000  # 1_000_000 - 600_000 = 400_000, clipped by since
    assert symbol_since_map["ETHUSDT"] == 800_000
    assert symbol_since_map["XRPUSDT"] == 800_000


def test_build_symbol_since_map_returns_empty_for_no_symbols():
    symbol_since_map, warmed = build_symbol_since_map(
        traded_symbols=[],
        watermarks={},
        since=123,
        overlap_minutes=5,
    )
    assert symbol_since_map == {}
    assert warmed == 0
