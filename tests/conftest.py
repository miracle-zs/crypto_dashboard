import logging
import os
import socket

import pytest
from fastapi.testclient import TestClient

# Keep the app import from loading live credentials or opening the project log.
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
for name in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "SERVERCHAN_SENDKEY", "DASHBOARD_ADMIN_TOKEN"):
    os.environ[name] = ""
logging.getLogger("crypto_dashboard").handlers = [logging.NullHandler()]

from app.database import Database
from app.main import app


@pytest.fixture(autouse=True)
def isolate_test_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("BINANCE_API_KEY", "")
    monkeypatch.setenv("BINANCE_API_SECRET", "")
    monkeypatch.setenv("ENABLE_USER_STREAM", "0")
    monkeypatch.setattr("app.notifier.SERVERCHAN_SENDKEY", None)
    monkeypatch.setattr("app.main.should_start_scheduler", lambda: (False, "testing"))
    monkeypatch.setattr("app.main.scheduler", None)
    monkeypatch.setattr("app.main.user_stream", None)
    monkeypatch.setattr("app.logger.LOG_FILE", tmp_path / "app.log")
    database_init = Database.__init__

    def temporary_database_init(self, db_path=None):
        database_init(self, str(tmp_path / "app.db") if db_path is None else db_path)

    monkeypatch.setattr(Database, "__init__", temporary_database_init)

    def block_network(_socket, _address):
        raise RuntimeError("network disabled during tests")

    monkeypatch.setattr(socket.socket, "connect", block_network)
    monkeypatch.setattr(socket.socket, "connect_ex", block_network)


def _client():
    return TestClient(app)


@pytest.fixture
def client():
    with _client() as c:
        yield c
