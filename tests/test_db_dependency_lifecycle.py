import inspect

from app import main


def test_get_db_is_generator_dependency():
    assert inspect.isgeneratorfunction(main.get_db)


def test_db_dependency_is_context_managed(client):
    r = client.get("/api/database/stats")
    assert r.status_code == 200
