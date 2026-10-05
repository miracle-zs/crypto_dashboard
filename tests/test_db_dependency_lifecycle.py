import inspect
from types import SimpleNamespace

from starlette.requests import Request


def test_main_does_not_define_local_get_db():
    from app import main

    assert not hasattr(main, "get_db")


def test_core_get_db_is_generator():
    from app.core import deps

    assert inspect.isgeneratorfunction(deps.get_db)


def test_db_dependency_is_context_managed(client):
    r = client.get("/api/database/stats")
    assert r.status_code == 200


def test_db_dependency_reads_the_application_context():
    from app.core.deps import get_db

    expected_db = object()
    app = SimpleNamespace(state=SimpleNamespace(context=SimpleNamespace(db=expected_db)))
    request = Request({"type": "http", "app": app})

    assert next(get_db(request)) is expected_db


def test_application_callers_do_not_create_fallback_databases():
    for path in ["app/services/trade_query_service.py", "app/user_stream.py"]:
        text = open(path, encoding="utf-8").read()
        assert "or Database()" not in text
