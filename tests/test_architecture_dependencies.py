from pathlib import Path


def test_background_jobs_depend_on_context_not_scheduler_implementation():
    for path in Path("app/jobs").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "from app.scheduler import" not in text
        assert "TradeDataScheduler" not in text


def test_page_routes_do_not_serve_json_api_or_access_persistence():
    for path in Path("app/routes").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert '"/api/' not in text
        assert "Depends(get_db)" not in text
        assert "Repository(" not in text


def test_removed_hollow_services_have_no_remaining_source_references():
    source_files = list(Path("app").rglob("*.py"))
    for name in ["TradesApiService", "SystemApiService"]:
        assert not any(name in path.read_text(encoding="utf-8") for path in source_files)
