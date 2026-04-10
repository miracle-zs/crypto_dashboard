from pathlib import Path


def test_main_does_not_define_business_api_routes_directly():
    main_path = Path(__file__).resolve().parents[1] / "app" / "main.py"
    main_text = main_path.read_text(encoding="utf-8")
    forbidden = [
        '@app.get("/api/rebound-7d")',
        '@app.get("/api/rebound-30d")',
        '@app.get("/api/rebound-60d")',
        '@app.get("/api/rebound-365d")',
        '@app.get("/api/crash-risk")',
        '@app.get("/api/leaderboard/dates")',
        '@app.get("/api/leaderboard/metrics-history")',
        '@app.get("/api/balance-history"',
    ]
    for pattern in forbidden:
        assert pattern not in main_text
