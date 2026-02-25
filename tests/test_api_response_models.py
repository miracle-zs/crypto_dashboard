from app.api.system_api import router as system_router
from app.api.leaderboard_api import router as leaderboard_router
from app.api.rebound_api import router as rebound_router
from app.api.trades_api import router as trades_router


def _response_model_by_path(router):
    return {
        route.path: getattr(route, "response_model", None)
        for route in router.routes
    }


def test_trades_aggregates_endpoint_has_response_model():
    models = _response_model_by_path(trades_router)
    assert models.get("/api/trades-aggregates") is not None


def test_system_endpoints_have_response_models():
    models = _response_model_by_path(system_router)
    assert models.get("/api/logs") is not None
    assert models.get("/api/noon-loss-review-history") is not None
    assert models.get("/api/sync-runs") is not None


def test_leaderboard_endpoints_have_response_models():
    models = _response_model_by_path(leaderboard_router)
    assert models.get("/api/leaderboard") is not None
    assert models.get("/api/leaderboard/dates") is not None
    assert models.get("/api/leaderboard/metrics-history") is not None


def test_rebound_endpoints_have_response_models():
    models = _response_model_by_path(rebound_router)
    assert models.get("/api/rebound-7d") is not None
    assert models.get("/api/rebound-7d/dates") is not None
    assert models.get("/api/rebound-30d") is not None
    assert models.get("/api/rebound-30d/dates") is not None
    assert models.get("/api/rebound-60d") is not None
    assert models.get("/api/rebound-60d/dates") is not None
