import socket

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.logger import get_log_file_path
from app.database import Database


def test_direct_client_uses_temporary_database_and_never_starts_scheduler(tmp_path, monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", "test-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-secret")
    monkeypatch.setattr("app.main.get_scheduler", lambda **kwargs: pytest.fail("scheduler must not start"))
    with TestClient(app):
        assert app.state.scheduler is None
        assert str(tmp_path) in app.state.context.db.db_path
        assert str(tmp_path) in get_log_file_path()


def test_real_network_connections_are_blocked():
    with socket.socket() as sock:
        with pytest.raises(RuntimeError, match="network disabled during tests"):
            sock.connect(("127.0.0.1", 1))


def test_default_database_is_temporary(tmp_path):
    db = Database()
    assert str(tmp_path) in db.db_path
