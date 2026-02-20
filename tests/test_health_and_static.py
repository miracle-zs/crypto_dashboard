def test_status_endpoint(client):
    r = client.get("/api/status")
    assert r.status_code == 200
    assert r.json()["status"] == "online"


def test_static_dark_css_served(client):
    r = client.get("/static/dark-unified.css")
    assert r.status_code == 200


def test_pytest_runtime_ready():
    import pytest  # noqa: F401

    assert True
