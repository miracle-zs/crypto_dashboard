from pathlib import Path


def test_rebound_api_uses_repository_instead_of_db_snapshot_calls():
    text = Path("app/api/rebound_api.py").read_text(encoding="utf-8")
    forbidden = [
        "db.get_rebound_7d_snapshot_by_date",
        "db.get_latest_rebound_7d_snapshot",
        "db.list_rebound_7d_snapshot_dates",
        "db.get_rebound_30d_snapshot_by_date",
        "db.get_latest_rebound_30d_snapshot",
        "db.list_rebound_30d_snapshot_dates",
        "db.get_rebound_60d_snapshot_by_date",
        "db.get_latest_rebound_60d_snapshot",
        "db.list_rebound_60d_snapshot_dates",
        "db.get_open_positions",
    ]
    for pattern in forbidden:
        assert pattern not in text


def test_balance_service_uses_repository_for_balance_queries():
    text = Path("app/services/balance_service.py").read_text(encoding="utf-8")
    forbidden = [
        "db.get_balance_history",
        "db.get_transfers",
    ]
    for pattern in forbidden:
        assert pattern not in text
