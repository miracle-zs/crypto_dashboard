from pathlib import Path


def test_main_uses_lifespan_instead_of_on_event_hooks():
    text = Path("app/main.py").read_text(encoding="utf-8")
    assert "@app.on_event(" not in text
    assert "FastAPI(title=\"Zero Gravity Dashboard\", lifespan=lifespan)" in text
