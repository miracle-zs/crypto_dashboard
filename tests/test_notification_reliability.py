from unittest.mock import MagicMock
from app.jobs.alert_jobs import run_profit_alert_check, run_reentry_alert_check


def test_profit_alerts_not_marked_if_notification_fails(monkeypatch):
    scheduler = MagicMock()
    # Mock positions with profit > threshold
    scheduler.sync_repo.get_open_positions.return_value = [{
        "symbol": "BTC",
        "order_id": 1001,
        "side": "LONG",
        "entry_price": 50000.0,
        "profit_alerted": 0,
        "entry_time": "2026-09-26 10:00:00",
    }]
    # Mark price 65000 (+30% profit)
    monkeypatch.setattr(
        "app.services.market_price_service.MarketPriceService.get_mark_price_map",
        lambda symbols, client: {"BTCUSDT": 65000.0},
    )
    # Notification fails
    monkeypatch.setattr(
        "app.jobs.alert_jobs.send_server_chan_notification",
        lambda title, content: False,
    )

    run_profit_alert_check(scheduler, threshold_pct=20.0)

    # set_positions_profit_alerted_batch MUST NOT be called because notification failed!
    scheduler.risk_repo.set_positions_profit_alerted_batch.assert_not_called()


def test_reentry_alerts_not_marked_if_notification_fails(monkeypatch):
    scheduler = MagicMock()
    # Mock positions with re-entry on same day
    scheduler.sync_repo.get_open_positions.return_value = [
        {"symbol": "BTC", "order_id": 1001, "side": "LONG", "entry_time": "2026-09-26 01:00:00", "reentry_alerted": 0},
        {"symbol": "BTC", "order_id": 1002, "side": "LONG", "entry_time": "2026-09-26 03:00:00", "reentry_alerted": 0},
    ]
    # Notification fails
    monkeypatch.setattr(
        "app.jobs.alert_jobs.send_server_chan_notification",
        lambda title, content: False,
    )

    run_reentry_alert_check(scheduler)

    # set_positions_reentry_alerted_batch MUST NOT be called because notification failed!
    scheduler.risk_repo.set_positions_reentry_alerted_batch.assert_not_called()
