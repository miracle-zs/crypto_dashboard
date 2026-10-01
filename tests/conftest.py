import pytest
from fastapi.testclient import TestClient
from app.main import app


def _client():
    return TestClient(app)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("ENABLE_USER_STREAM", "0")
    monkeypatch.setattr("app.main.should_start_scheduler", lambda: (False, "testing"))
    with _client() as c:
        yield c
