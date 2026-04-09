def test_core_pages_available(client):
    assert client.get("/").status_code == 200
    assert client.get("/leaderboard").status_code == 200


def test_leaderboard_static_assets_have_cache_busting_version(client):
    response = client.get("/leaderboard")
    assert response.status_code == 200
    text = response.text
    assert "/static/dark-unified.css?v=" in text
    assert "/static/js/leaderboard.js?v=" in text
    assert "Drawdown 7D High" in text
    assert "Drawdown Window High" in text


def test_core_pages_static_assets_have_cache_busting_version(client):
    checks = {
        "/": ["/static/dark-unified.css?v=", "/static/js/index-dashboard.js?v="],
        "/live-monitor": ["/static/dark-unified.css?v=", "/static/js/live-monitor.js?v="],
        "/metrics": ["/static/dark-unified.css?v=", "/static/js/metrics.js?v="],
        "/logs": ["/static/dark-unified.css?v="],
    }
    for path, patterns in checks.items():
        response = client.get(path)
        assert response.status_code == 200
        for pattern in patterns:
            assert pattern in response.text


def test_core_api_available(client):
    assert client.get("/api/status").status_code == 200


def test_route_set_still_available(client):
    assert client.get("/api/status").status_code == 200
    assert client.get("/api/trades").status_code == 200
