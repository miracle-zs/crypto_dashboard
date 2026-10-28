from app.services.trade_api_gateway import fetch_real_positions


def test_successful_empty_position_response_clears_previous_risk_cache():
    class FakeClient:
        _latest_position_risk = [{"symbol": "BTCUSDT", "positionAmt": "1"}]
        _latest_position_risk_map = {"BTCUSDT": _latest_position_risk[0]}

        def signed_get(self, endpoint):
            assert endpoint == "/fapi/v3/positionRisk"
            return []

    client = FakeClient()

    assert fetch_real_positions(client=client) == {}
    assert client._latest_position_risk == []
    assert client._latest_position_risk_map == {}


def test_failed_position_request_does_not_clear_previous_risk_cache():
    previous = [{"symbol": "BTCUSDT", "positionAmt": "1"}]

    class FakeClient:
        _latest_position_risk = previous
        _latest_position_risk_map = {"BTCUSDT": previous[0]}

        def signed_get(self, endpoint):
            assert endpoint == "/fapi/v3/positionRisk"
            return None

    client = FakeClient()

    assert fetch_real_positions(client=client) is None
    assert client._latest_position_risk == previous
    assert client._latest_position_risk_map == {"BTCUSDT": previous[0]}
