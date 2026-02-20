from pathlib import Path


def test_main_does_not_define_business_api_routes_directly():
    main_text = Path("app/main.py").read_text(encoding="utf-8")
    forbidden = [
        '@app.get("/api/rebound-7d")',
        '@app.get("/api/rebound-30d")',
        '@app.get("/api/rebound-60d")',
        '@app.get("/api/leaderboard/dates")',
        '@app.get("/api/leaderboard/metrics-history")',
        '@app.get("/api/balance-history"',
    ]
    for pattern in forbidden:
        assert pattern not in main_text
