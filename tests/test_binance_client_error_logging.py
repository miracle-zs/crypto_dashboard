import requests

from app.binance_client import BinanceFuturesRestClient


def test_signed_http_error_does_not_log_request_signature(monkeypatch):
    messages = []

    class FakeResponse:
        status_code = 400
        text = '{"code": -4166, "msg": "Search window is restricted"}'

        def json(self):
            return {"code": -4166, "msg": "Search window is restricted"}

        def raise_for_status(self):
            error = requests.HTTPError(
                "400 Client Error: Bad Request for url: https://example.test/orders?signature=exposed"
            )
            error.response = self
            raise error

    client = BinanceFuturesRestClient(api_key="key", api_secret="secret", min_request_interval=0)
    monkeypatch.setattr(client._session, "request", lambda **kwargs: FakeResponse())
    monkeypatch.setattr("app.binance_client.logger.error", messages.append)

    assert client.signed_get("/fapi/v1/allOrders", {"symbol": "BTCUSDT"}) is None

    assert all("signature=" not in message for message in messages)
    assert all("secret" not in message for message in messages)
