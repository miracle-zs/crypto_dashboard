import inspect


def test_main_does_not_define_local_get_db():
    from app import main

    assert not hasattr(main, "get_db")


def test_core_get_db_is_generator():
    from app.core import deps

    assert inspect.isgeneratorfunction(deps.get_db)


def test_db_dependency_is_context_managed(client):
    r = client.get("/api/database/stats")
    assert r.status_code == 200
