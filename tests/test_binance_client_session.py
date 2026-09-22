import requests
from unittest.mock import Mock, patch
from app.binance_client import BinanceFuturesRestClient


def test_binance_client_uses_persistent_session():
    client = BinanceFuturesRestClient(api_key="key", api_secret="secret")
    assert hasattr(client, "_session")
    assert isinstance(client._session, requests.Session)


def test_binance_client_retries_transient_connection_errors_on_get():
    client = BinanceFuturesRestClient(api_key="key", api_secret="secret", min_request_interval=0.0)

    # First attempt raises ConnectionError, second succeeds
    mock_resp = Mock()
    mock_resp.json.return_value = {"status": "ok"}
    mock_resp.raise_for_status.return_value = None

    side_effects = [
        requests.exceptions.ConnectionError("Temporary network blip"),
        mock_resp,
    ]

    with patch.object(client._session, "request", side_effect=side_effects) as mock_req:
        result = client.public_get("/fapi/v1/ping")
        assert result == {"status": "ok"}
        assert mock_req.call_count == 2
