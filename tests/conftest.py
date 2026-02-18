from fastapi.testclient import TestClient
from app.main import app


def _client():
    return TestClient(app)


import pytest


@pytest.fixture
def client():
    with _client() as c:
        yield c
