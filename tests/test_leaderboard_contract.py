def test_leaderboard_service_exists():
    from app.services.leaderboard_service import LeaderboardService

    assert hasattr(LeaderboardService, "build_snapshot_response")


def test_leaderboard_contract_shape(client):
    r = client.get("/api/leaderboard")
    assert r.status_code == 200
    body = r.json()
    assert "ok" in body
