def test_manual_sync_requires_token(client, monkeypatch):
    monkeypatch.setenv("DASHBOARD_ADMIN_TOKEN", "secret")

    r = client.post("/api/sync/manual")

    assert r.status_code in (401, 403)


def test_monthly_target_requires_token(client, monkeypatch):
    monkeypatch.setenv("DASHBOARD_ADMIN_TOKEN", "secret")

    r = client.post("/api/monthly-target?target=100")

    assert r.status_code in (401, 403)
