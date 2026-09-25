import asyncio

from app.core.read_model import OperationalReadModel, get_read_model, reset_read_model


def test_lazy_load_runs_loader_once_until_invalidate():
    model = OperationalReadModel()
    calls = {"n": 0}

    def loader():
        calls["n"] += 1
        return {"v": calls["n"]}

    first = model.get_or_load("sec", loader)
    second = model.get_or_load("sec", loader)
    assert first == {"v": 1}
    assert second == {"v": 1}
    assert calls["n"] == 1

    model.invalidate("sec")
    third = model.get_or_load("sec", loader)
    assert third == {"v": 2}
    assert calls["n"] == 2


def test_publish_invalidation_and_version():
    model = OperationalReadModel()
    assert model.get("x") is None
    v1 = model.publish("x", {"a": 1})
    assert model.get("x") == {"a": 1}
    assert model.version("x") == v1

    model.invalidate(prefix="x")
    assert model.get("x") is None
    v2 = model.publish("x", {"a": 2})
    assert v2 == v1 + 1


def test_max_age_forces_reload():
    model = OperationalReadModel()
    calls = {"n": 0}

    def loader():
        calls["n"] += 1
        return calls["n"]

    assert model.get_or_load("k", loader, max_age_seconds=60) == 1
    assert model.get_or_load("k", loader, max_age_seconds=60) == 1
    # 强制过期
    model._section("k").loaded_at = 0
    assert model.get_or_load("k", loader, max_age_seconds=60) == 2


def test_get_or_load_async_lazy():
    model = OperationalReadModel()
    calls = {"n": 0}

    async def loader():
        calls["n"] += 1
        return {"ok": True, "n": calls["n"]}

    async def run():
        a = await model.get_or_load_async("async-sec", loader)
        b = await model.get_or_load_async("async-sec", loader)
        return a, b

    a, b = asyncio.run(run())
    assert a == {"ok": True, "n": 1}
    assert b == {"ok": True, "n": 1}
    assert calls["n"] == 1


def test_get_read_model_singleton_and_reset():
    reset_read_model()
    a = get_read_model()
    b = get_read_model()
    assert a is b
    c = reset_read_model()
    assert c is not a


def test_invalidate_prefix_clears_only_matching():
    model = OperationalReadModel()
    model.publish("positions:open:2026-01-01", {"p": 1})
    model.publish("leaderboard:snapshot:latest", {"l": 1})
    model.invalidate(prefix="positions:open:")
    assert model.get("positions:open:2026-01-01") is None
    assert model.get("leaderboard:snapshot:latest") == {"l": 1}
