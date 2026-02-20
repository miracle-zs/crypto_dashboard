from pathlib import Path


def test_rebound_api_has_no_run_in_executor_logic():
    text = Path("app/api/rebound_api.py").read_text(encoding="utf-8")
    assert "run_in_executor" not in text


def test_leaderboard_api_has_no_run_in_executor_logic():
    text = Path("app/api/leaderboard_api.py").read_text(encoding="utf-8")
    assert "run_in_executor" not in text


def test_trades_api_has_no_run_in_executor_logic():
    text = Path("app/api/trades_api.py").read_text(encoding="utf-8")
    assert "run_in_executor" not in text


def test_watchnotes_api_has_no_run_in_executor_logic():
    text = Path("app/api/watchnotes_api.py").read_text(encoding="utf-8")
    assert "run_in_executor" not in text


def test_system_api_has_no_run_in_executor_logic():
    text = Path("app/api/system_api.py").read_text(encoding="utf-8")
    assert "run_in_executor" not in text


def test_positions_api_has_no_run_in_executor_logic():
    text = Path("app/api/positions_api.py").read_text(encoding="utf-8")
    assert "run_in_executor" not in text
