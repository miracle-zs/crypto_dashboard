from app.api.system_api import router as system_router
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
