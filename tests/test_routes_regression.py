def test_core_pages_available(client):
    assert client.get("/").status_code == 200
    assert client.get("/leaderboard").status_code == 200


def test_core_api_available(client):
    assert client.get("/api/status").status_code == 200


def test_route_set_still_available(client):
    assert client.get("/api/status").status_code == 200
    assert client.get("/api/trades").status_code == 200
