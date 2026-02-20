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
