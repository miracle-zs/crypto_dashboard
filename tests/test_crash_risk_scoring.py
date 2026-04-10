def test_fetch_symbol_inputs_uses_1h_market_data():
    from app.services.crash_risk_service import CrashRiskService

    calls = []

    class FakeClient:
        def get_klines_1h(self, symbol, limit=24):
            calls.append(("klines", symbol, limit))
            return [
                [0, "100", "101", "99", "100", "10"],
                [1, "101", "106", "100", "105", "12"],
                [2, "105", "108", "104", "107", "14"],
            ]

        def get_open_interest_history_1h(self, symbol, limit=24):
            calls.append(("open_interest", symbol, limit))
            return [
                {"sumOpenInterest": "1000"},
                {"sumOpenInterest": "980"},
                {"sumOpenInterest": "960"},
            ]

    service = CrashRiskService(client=FakeClient())
    inputs = service.fetch_symbol_inputs("BTCUSDT")

    assert calls == [
        ("klines", "BTCUSDT", 24),
        ("open_interest", "BTCUSDT", 24),
    ]
    assert inputs == {
        "closes": [100.0, 105.0, 107.0],
        "highs": [101.0, 106.0, 108.0],
        "lows": [99.0, 100.0, 104.0],
        "volumes": [10.0, 12.0, 14.0],
        "open_interests": [1000.0, 980.0, 960.0],
    }


def test_score_symbol_returns_explainable_breakdown():
    from app.services.crash_risk_service import CrashRiskService

    market_series = {
        "symbol": "BTCUSDT",
        "closes": [100.0, 104.0, 108.0, 112.0, 115.0, 111.0],
        "highs": [101.0, 105.0, 109.0, 113.0, 116.0, 112.0],
        "lows": [99.0, 103.0, 107.0, 111.0, 114.0, 110.0],
        "open_interests": [1000.0, 990.0, 970.0, 940.0, 920.0, 900.0],
        "volumes": [120.0, 125.0, 110.0, 90.0, 70.0, 50.0],
    }

    result = CrashRiskService.score_symbol(market_series)

    assert 0 <= result["risk_score"] <= 100
    assert result["stage"] == "高危"
    assert result["drivers"] == [
        "OI divergence is widening",
        "Price is pressing into recent highs",
        "Momentum is fading on thinner volume",
        "Support is weakening",
        "Breakdown trigger is confirming",
    ]
    assert set(result["component_scores"]) == {
        "oi_divergence",
        "price_extension",
        "momentum_fade",
        "support_fragility",
        "trigger_confirmation",
    }


def test_score_symbol_rewards_windowed_price_extension():
    from app.services.crash_risk_service import CrashRiskService

    base_highs = [101.0] * 18
    base_lows = [99.0] * 18
    volumes = [100.0] * 24
    open_interests = [1000.0] * 24

    stronger_recent_extension = {
        "closes": [
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 115.0, 118.0,
            119.0, 120.0,
        ],
        "highs": base_highs + [101.0, 101.0, 116.0, 119.0, 120.0, 121.0],
        "lows": base_lows + [99.0, 99.0, 114.0, 117.0, 118.0, 119.0],
        "open_interests": open_interests,
        "volumes": volumes,
    }
    earlier_extension = {
        "closes": [
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            110.0, 112.0, 115.0, 118.0,
            119.0, 120.0,
        ],
        "highs": base_highs + [101.0, 101.0, 116.0, 119.0, 120.0, 121.0],
        "lows": base_lows + [99.0, 99.0, 114.0, 117.0, 118.0, 119.0],
        "open_interests": open_interests,
        "volumes": volumes,
    }

    strong = CrashRiskService.score_symbol(stronger_recent_extension)
    weak = CrashRiskService.score_symbol(earlier_extension)

    assert strong["component_scores"]["price_extension"] > weak["component_scores"]["price_extension"]
    assert strong["risk_score"] > weak["risk_score"]


def test_score_symbol_rewards_consecutive_oi_decline_streak():
    from app.services.crash_risk_service import CrashRiskService

    closes = [100.0, 101.0, 103.0, 104.0, 106.0, 108.0, 110.0, 112.0]
    highs = [101.0, 102.0, 104.0, 105.0, 107.0, 109.0, 111.0, 113.0]
    lows = [99.0, 100.0, 102.0, 103.0, 105.0, 107.0, 109.0, 111.0]
    volumes = [100.0, 105.0, 103.0, 101.0, 99.0, 97.0, 96.0, 95.0]

    long_decline_streak = {
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "open_interests": [1000.0, 995.0, 990.0, 985.0, 980.0, 975.0, 970.0, 965.0],
        "volumes": volumes,
    }
    short_decline_streak = {
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "open_interests": [1000.0, 990.0, 995.0, 985.0, 990.0, 980.0, 985.0, 965.0],
        "volumes": volumes,
    }

    strong = CrashRiskService.score_symbol(long_decline_streak)
    weak = CrashRiskService.score_symbol(short_decline_streak)

    assert strong["component_scores"]["oi_divergence"] > weak["component_scores"]["oi_divergence"]
    assert strong["risk_score"] > weak["risk_score"]


def test_score_symbol_stays_low_for_flat_structure():
    from app.services.crash_risk_service import CrashRiskService

    market_series = {
        "closes": [100.0, 100.0, 100.0, 100.0, 100.0],
        "highs": [100.5, 100.5, 100.5, 100.5, 100.5],
        "lows": [99.5, 99.5, 99.5, 99.5, 99.5],
        "open_interests": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
        "volumes": [100.0, 100.0, 100.0, 100.0, 100.0],
    }

    result = CrashRiskService.score_symbol(market_series)

    assert result["risk_score"] == 0
    assert result["stage"] == "观察"
    assert result["drivers"] == []
    assert all(score == 0 for score in result["component_scores"].values())
