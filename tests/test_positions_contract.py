def test_positions_service_exists():
    from app.services.positions_service import PositionsService

    assert hasattr(PositionsService, "build_open_positions_response")


def test_open_positions_contract_shape(client):
    r = client.get("/api/open-positions")
    assert r.status_code == 200
    body = r.json()
    assert "as_of" in body
    assert "positions" in body
    assert "summary" in body


def test_open_positions_page_has_no_binance_dependency():
    from app.main import app

    route = next(route for route in app.routes if getattr(route, "path", None) == "/api/open-positions")
    dependency_names = {
        getattr(dependency.call, "__name__", "")
        for dependency in route.dependant.dependencies
    }

    assert "get_public_rest" not in dependency_names
