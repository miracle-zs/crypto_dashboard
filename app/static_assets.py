from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def static_asset_url(path: str) -> str:
    normalized = path if path.startswith("/") else f"/{path}"
    try:
        file_path = PROJECT_ROOT / normalized.lstrip("/")
        version = int(file_path.stat().st_mtime)
    except OSError:
        version = 0
    return f"{normalized}?v={version}"
