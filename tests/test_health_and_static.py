def test_status_endpoint(client):
    r = client.get("/api/status")
    assert r.status_code == 200
    assert r.json()["status"] == "online"


def test_static_dark_css_served(client):
    r = client.get("/static/dark-unified.css")
    assert r.status_code == 200


def test_crash_risk_page_renders_with_page_assets(client):
    r = client.get("/crash-risk")
    assert r.status_code == 200
    text = r.text
    assert "/static/dark-unified.css?v=" in text
    assert "/static/js/crash-risk.js?v=" in text
    assert "结构性崩盘预警" in text
    assert "手动刷新" in text
    assert "方法说明" in text


def test_pytest_runtime_ready():
    import pytest  # noqa: F401

    assert True
