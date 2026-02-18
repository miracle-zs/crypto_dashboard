from app import scheduler


def test_scheduler_not_started_without_api_keys(monkeypatch):
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    monkeypatch.setenv("WEB_CONCURRENCY", "1")

    should_start, reason = scheduler.should_start_scheduler()

    assert should_start is False
    assert reason == "missing_api_keys"


def test_scheduler_blocked_on_multi_worker_without_override(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setenv("WEB_CONCURRENCY", "2")
    monkeypatch.delenv("SCHEDULER_ALLOW_MULTI_WORKER", raising=False)

    should_start, reason = scheduler.should_start_scheduler()

    assert should_start is False
    assert reason == "multi_worker_unsupported"
