def _assert_snapshot_payload_shape(body, *, kind: str):
    assert isinstance(body, dict)
    assert "ok" in body
    assert isinstance(body["ok"], bool)
    if body["ok"]:
        assert "rows" in body
        assert isinstance(body["rows"], list)
        if kind == "rebound":
            assert "top_count" in body
            assert isinstance(body["top_count"], int)
        elif kind == "leaderboard":
            assert "gainers_top_count" in body
            assert "losers_top_count" in body
    else:
        assert "reason" in body
        assert "message" in body


def test_leaderboard_dates_contract(client):
    r = client.get("/api/leaderboard/dates?limit=30")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, dict)
    assert "dates" in body
    assert isinstance(body["dates"], list)


def test_leaderboard_metrics_history_contract(client):
    r = client.get("/api/leaderboard/metrics-history?limit=30")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, dict)
    assert "rows" in body
    assert isinstance(body["rows"], list)


def test_leaderboard_snapshot_contract(client):
    r = client.get("/api/leaderboard")
    assert r.status_code == 200
    _assert_snapshot_payload_shape(r.json(), kind="leaderboard")


def test_rebound_snapshot_contracts(client):
    endpoints = ["/api/rebound-7d", "/api/rebound-30d", "/api/rebound-60d"]
    for endpoint in endpoints:
        r = client.get(endpoint)
        assert r.status_code == 200
        _assert_snapshot_payload_shape(r.json(), kind="rebound")


def test_rebound_dates_contracts(client):
    endpoints = ["/api/rebound-7d/dates", "/api/rebound-30d/dates", "/api/rebound-60d/dates"]
    for endpoint in endpoints:
        r = client.get(f"{endpoint}?limit=30")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, dict)
        assert "dates" in body
        assert isinstance(body["dates"], list)
