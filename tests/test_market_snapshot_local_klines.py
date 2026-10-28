from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.core.binance_request_budget import BulkRequestAborted
from app.services import market_snapshot_service


DAY_MS = 86_400_000


def _reset_exchange_cache():
    market_snapshot_service._EXCHANGE_SYMBOLS_CACHE["symbols"] = None
    market_snapshot_service._EXCHANGE_SYMBOLS_CACHE["expires_at"] = 0.0


def test_daily_kline_update_backfills_once_then_requests_only_five_rows(monkeypatch):
    _reset_exchange_cache()
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "300")
    latest_btc = 2 * DAY_MS

    class FakeClient:
        def __init__(self):
            self.calls = []

        def public_get(self, path, params=None):
            self.calls.append((path, params))
            if path == "/fapi/v1/exchangeInfo":
                return {
                    "symbols": [
                        {
                            "symbol": symbol,
                            "contractType": "PERPETUAL",
                            "quoteAsset": "USDT",
                            "status": "TRADING",
                        }
                        for symbol in ("BTCUSDT", "ETHUSDT")
                    ]
                }
            if path == "/fapi/v1/klines":
                return [
                    [
                        int((params or {}).get("startTime", DAY_MS)),
                        "10",
                        "12",
                        "8",
                        "11",
                        "0",
                        10 * DAY_MS,
                    ]
                ]
            raise AssertionError(path)

    class FakeProcessor:
        def __init__(self, client):
            self.client = client

        def get_exchange_info(self, client=None):
            return (client or self.client).public_get("/fapi/v1/exchangeInfo")

    class FakeRepo:
        def __init__(self):
            self.saved = []

        def latest_open_times(self, symbols):
            return {"BTCUSDT": latest_btc, "ETHUSDT": None}

        def upsert(self, rows):
            self.saved.extend(rows)
            return len(rows)

    client = FakeClient()
    repo = FakeRepo()
    scheduler = SimpleNamespace(
        processor=FakeProcessor(client),
        daily_kline_repo=repo,
    )

    result = market_snapshot_service.update_daily_kline_cache(
        scheduler,
        now_ms=20 * DAY_MS,
        max_weight_per_60s=200,
    )

    kline_calls = [params for path, params in client.calls if path == "/fapi/v1/klines"]
    assert kline_calls == [
        {"symbol": "BTCUSDT", "interval": "1d", "startTime": latest_btc, "limit": 5},
        {"symbol": "ETHUSDT", "interval": "1d", "limit": 365},
    ]
    assert result["request_weight"] == 4
    assert result["peak_limit_per_60s"] == 200
    assert len(repo.saved) == 2


def test_all_market_snapshots_use_one_ticker_and_zero_kline_requests():
    now_utc = datetime(2026, 8, 24, 0, 30, tzinfo=timezone.utc)
    midnight_ms = int(now_utc.replace(hour=0, minute=0).timestamp() * 1000)
    daily_view = {
        "BTCUSDT": [
            {
                "symbol": "BTCUSDT",
                "open_time": midnight_ms - DAY_MS,
                "open": 80.0,
                "high": 100.0,
                "low": 70.0,
                "close": 90.0,
                "is_closed": True,
            },
            {
                "symbol": "BTCUSDT",
                "open_time": midnight_ms,
                "open": 100.0,
                "high": 125.0,
                "low": 95.0,
                "close": 120.0,
                "is_closed": False,
            },
        ]
    }

    class FakeRepo:
        def load(self, symbols=None, since_open_time=None):
            return daily_view

    class FakeClient:
        def __init__(self):
            self.calls = []

        def public_get(self, path, params=None):
            self.calls.append((path, params))
            assert path == "/fapi/v1/ticker/24hr"
            return [
                {
                    "symbol": "BTCUSDT",
                    "lastPrice": "120",
                    "quoteVolume": "100000000",
                }
            ]

    client = FakeClient()
    scheduler = SimpleNamespace(
        processor=SimpleNamespace(client=client),
        daily_kline_repo=FakeRepo(),
        leaderboard_min_quote_volume=50_000_000,
        leaderboard_max_symbols=120,
        leaderboard_top_n=10,
        rebound_7d_top_n=10,
        rebound_30d_top_n=10,
        rebound_60d_top_n=10,
        rebound_365d_top_n=10,
    )

    result = market_snapshot_service.build_all_market_snapshots(
        scheduler,
        timezone.utc,
        now_utc=now_utc,
    )

    assert client.calls == [("/fapi/v1/ticker/24hr", None)]
    assert result["request_weight"] == 40
    assert result["leaderboard"]["rows"][0]["change"] == pytest.approx(20.0)
    assert set(result["rebounds"]) == {14, 30, 60, 365}
    assert all(snapshot["rows"][0]["symbol"] == "BTCUSDT" for snapshot in result["rebounds"].values())


def test_daily_kline_batch_stops_immediately_after_429_cooldown(monkeypatch):
    _reset_exchange_cache()
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "0")

    class FakeClient:
        def __init__(self):
            self.calls = []
            self.cooldown = False

        def public_get(self, path, params=None):
            self.calls.append((path, params))
            if path == "/fapi/v1/klines":
                self.cooldown = True
                return None
            raise AssertionError(path)

        def is_global_cooldown_active(self):
            return self.cooldown

    class FakeProcessor:
        def __init__(self, client):
            self.client = client

        def get_exchange_info(self, client=None):
            return {
                "symbols": [
                    {
                        "symbol": symbol,
                        "contractType": "PERPETUAL",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    }
                    for symbol in ("BTCUSDT", "ETHUSDT")
                ]
            }

    class FakeRepo:
        def latest_open_times(self, symbols):
            return {symbol: None for symbol in symbols}

        def upsert(self, rows):
            raise AssertionError("429 response must not be persisted")

    client = FakeClient()
    scheduler = SimpleNamespace(
        processor=FakeProcessor(client),
        daily_kline_repo=FakeRepo(),
    )

    with pytest.raises(BulkRequestAborted):
        market_snapshot_service.update_daily_kline_cache(
            scheduler,
            max_weight_per_60s=200,
        )

    assert [path for path, _params in client.calls] == ["/fapi/v1/klines"]


def test_market_snapshot_job_saves_only_enabled_rebound_windows(monkeypatch):
    from app.jobs import market_snapshot_jobs

    saved = []

    class SnapshotRepo:
        def save_leaderboard_snapshot(self, snapshot):
            saved.append(("leaderboard", snapshot))

        def save_rebound_7d_snapshot(self, snapshot):
            saved.append(("7d", snapshot))

        def save_rebound_30d_snapshot(self, snapshot):
            saved.append(("30d", snapshot))

        def save_rebound_60d_snapshot(self, snapshot):
            saved.append(("60d", snapshot))

        def save_rebound_365d_snapshot(self, snapshot):
            saved.append(("365d", snapshot))

        def upsert_leaderboard_daily_metrics_for_date(self, snapshot_date):
            saved.append(("metrics", snapshot_date))

    class Scheduler:
        processor = object()
        snapshot_repo = SnapshotRepo()
        enable_rebound_7d_snapshot = False
        enable_rebound_30d_snapshot = True
        enable_rebound_60d_snapshot = False
        enable_rebound_365d_snapshot = True

        def _is_api_cooldown_active(self, source):
            return False

        def _release_api_job_slot(self):
            pass

    monkeypatch.setattr(market_snapshot_jobs, "try_enter_slot", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        market_snapshot_jobs,
        "build_all_market_snapshots",
        lambda *_args: {
            "leaderboard": {"snapshot_date": "2026-10-28", "rows": []},
            "rebounds": {14: "7d", 30: "30d", 60: "60d", 365: "365d"},
            "request_weight": 40,
        },
    )

    result = market_snapshot_jobs.build_and_save_all_market_snapshots_job(Scheduler(), timezone.utc)

    assert result["ok"] is True
    assert saved == [
        ("leaderboard", {"snapshot_date": "2026-10-28", "rows": []}),
        ("30d", "30d"),
        ("365d", "365d"),
        ("metrics", "2026-10-28"),
    ]


def test_morning_notification_reads_saved_snapshot_without_binance(monkeypatch):
    from app.jobs.market_snapshot_jobs import send_morning_top_gainers_job

    today = datetime.now(timezone.utc).date()
    sent = []
    monkeypatch.setattr(
        "app.jobs.market_snapshot_jobs.send_server_chan_notification",
        lambda title, content: sent.append((title, content)),
    )

    class SnapshotRepo:
        def get_latest_leaderboard_snapshot(self):
            return {
                "snapshot_date": today.isoformat(),
                "snapshot_time": f"{today.isoformat()} 07:00:00",
                "window_start_utc": f"{(today - timedelta(days=1)).isoformat()} 00:00:00",
                "candidates": 1,
                "effective": 1,
                "top": 1,
                "rows": [
                    {"symbol": "BTCUSDT", "change": 2.0, "volume": 100_000_000}
                ],
                "losers_rows": [],
            }

    scheduler = SimpleNamespace(snapshot_repo=SnapshotRepo())
    send_morning_top_gainers_job(
        scheduler,
        source="test",
        schedule_hour=7,
        schedule_minute=30,
        utc8=timezone.utc,
    )

    assert len(sent) == 1


def test_morning_notification_skips_stale_snapshot_when_refresh_is_unavailable(monkeypatch):
    from app.jobs.market_snapshot_jobs import send_morning_top_gainers_job

    sent = []
    monkeypatch.setattr(
        "app.jobs.market_snapshot_jobs.send_server_chan_notification",
        lambda title, content: sent.append((title, content)),
    )

    class SnapshotRepo:
        def get_latest_leaderboard_snapshot(self):
            yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
            return {
                "snapshot_date": yesterday.isoformat(),
                "snapshot_time": f"{yesterday.isoformat()} 07:00:00",
                "window_start_utc": f"{(yesterday - timedelta(days=1)).isoformat()} 00:00:00",
                "candidates": 1,
                "effective": 1,
                "top": 1,
                "rows": [{"symbol": "BTCUSDT", "change": 2.0, "volume": 100_000_000}],
                "losers_rows": [],
            }

    scheduler = SimpleNamespace(processor=None, snapshot_repo=SnapshotRepo())
    result = send_morning_top_gainers_job(
        scheduler,
        source="test",
        schedule_hour=7,
        schedule_minute=30,
        utc8=timezone.utc,
    )

    assert result is None
    assert sent == []
