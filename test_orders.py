import json
from unittest.mock import Mock

import pytest
import requests

import config
from api_client import APIClient
from models import OrderRequest, PriceTick, StageStatus, StockInfo


def _http_response(status_code: int, payload: object | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://gateway.test/api/v1"
    if payload is not None:
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(payload).encode("utf-8")
    return response


def _client(
    response: requests.Response,
    *,
    token: str = "test-token",
) -> tuple[APIClient, Mock]:
    session = Mock(spec=requests.Session)
    session.headers = {}
    session.request.return_value = response
    return APIClient(
        base_url="https://gateway.test/api/v1/",
        token=token,
        session=session,
        timeout=3.5,
    ), session


def test_get_stage_uses_public_endpoint_and_parses_nested_response() -> None:
    client, session = _client(
        _http_response(
            200,
            {
                "data": {
                    "stage": "simulation",
                    "status": "RUNNING",
                    "simulation_id": "sim-123",
                    "days_completed": 4,
                }
            },
        )
    )

    stage = client.get_stage()

    assert stage == StageStatus(
        stage="simulation",
        status="RUNNING",
        simulation_id="sim-123",
        days_completed=4,
    )
    session.request.assert_called_once_with(
        "GET",
        "https://gateway.test/api/v1/stage",
        headers={},
        timeout=3.5,
    )


def test_post_heartbeat_sends_sequence_with_authentication() -> None:
    client, session = _client(_http_response(200, {"ok": True}))

    result = client.post_heartbeat(17)

    assert result == {"ok": True}
    session.request.assert_called_once_with(
        "POST",
        "https://gateway.test/api/v1/bot/heartbeat",
        headers=config.get_auth_headers("test-token"),
        timeout=3.5,
        json={"seq": 17},
    )


def test_get_stocks_parses_wrapped_stock_records() -> None:
    payload =     {
        "stocks": [
            {
                "ticker": "ALPHA",
                "symbol": "ALP",
                "name": "Alpha Corp",
                "description": "Alpha company",
            },
            {
                "ticker": "BETA",
                "symbol": "BET",
                "name": "Beta Ltd",
                "description": "Beta company",
            },
        ]
    }
    client, session = _client(_http_response(200, payload))

    stocks = client.get_stocks()

    assert all(isinstance(stock, StockInfo) for stock in stocks)
    assert [stock.ticker for stock in stocks] == ["ALPHA", "BETA"]
    session.request.assert_called_once_with(
        "GET",
        "https://gateway.test/api/v1/stocks",
        headers=config.get_auth_headers("test-token"),
        timeout=3.5,
    )


def test_get_stock_history_normalizes_and_escapes_ticker() -> None:
    client, session = _client(
        _http_response(
            200,
            {
                "data": [
                    {
                        "ticker": "BRK/B",
                        "open": 10,
                        "high": 12,
                        "low": 9,
                        "close": 11,
                        "volume": 500,
                        "date": "2026-10-28",
                    }
                ]
            },
        )
    )

    history = client.get_stock_history(" brk/b ", days=30)

    assert history == [
        PriceTick(
            ticker="BRK/B",
            open=10.0,
            high=12.0,
            low=9.0,
            close=11.0,
            volume=500,
            simulated_date="2026-10-28",
        )
    ]
    session.request.assert_called_once_with(
        "GET",
        "https://gateway.test/api/v1/stocks/BRK%2FB/history",
        headers=config.get_auth_headers("test-token"),
        timeout=3.5,
        params={"days": 30},
    )


@pytest.mark.parametrize(
    ("method_name", "path", "payload"),
    [
        ("get_portfolio", "/portfolio", {"cash": 125_000}),
        ("get_cash", "/portfolio/cash", {"cash": 125_000, "buying_power": 90_000}),
        ("get_orders", "/orders", {"orders": []}),
        ("get_simulation_clock", "/simulation/clock", {"current_date": "2026-10-28"}),
    ],
)
def test_read_endpoints_return_api_payload(
    method_name: str,
    path: str,
    payload: dict[str, object],
) -> None:
    client, session = _client(_http_response(200, payload))

    result = getattr(client, method_name)()

    assert result == payload
    session.request.assert_called_once_with(
        "GET",
        f"https://gateway.test/api/v1{path}",
        headers=config.get_auth_headers("test-token"),
        timeout=3.5,
    )


def test_post_order_sends_normalized_order_fields() -> None:
    client, session = _client(
        _http_response(201, {"order_id": "order-456", "status": "accepted"})
    )
    order = OrderRequest(
        ticker=" alpha ",
        action=" buy ",
        quantity=25,
        date="2026-10-28",
    )

    result = client.post_order(order)

    assert result == {"order_id": "order-456", "status": "accepted"}
    session.request.assert_called_once_with(
        "POST",
        "https://gateway.test/api/v1/orders",
        headers=config.get_auth_headers("test-token"),
        timeout=3.5,
        json={
            "ticker": "ALPHA",
            "action": "BUY",
            "quantity": 25,
            "date": "2026-10-28",
        },
    )


def test_order_request_omits_empty_date() -> None:
    order = OrderRequest(ticker="beta", action="short", quantity=3)

    assert order.to_dict() == {
        "ticker": "BETA",
        "action": "SHORT",
        "quantity": 3,
    }


@pytest.mark.parametrize(
    ("ticker", "action", "quantity"),
    [
        ("ALPHA", "HOLD", 1),
        ("ALPHA", "BUY", 0),
        ("ALPHA", "BUY", 1_000_001),
    ],
)
def test_order_request_rejects_invalid_fields(
    ticker: str,
    action: str,
    quantity: int,
) -> None:
    with pytest.raises(ValueError):
        OrderRequest(ticker=ticker, action=action, quantity=quantity)


def test_authenticated_endpoint_requires_nonempty_token() -> None:
    client, session = _client(_http_response(200, {}), token="  ")

    with pytest.raises(ValueError, match="access token is required"):
        client.get_portfolio()

    session.request.assert_not_called()


@pytest.mark.parametrize("days", [0, -1, True, 1.5])
def test_stock_history_rejects_invalid_day_count(days: object) -> None:
    client, session = _client(_http_response(200, []))

    with pytest.raises(ValueError, match="positive integer"):
        client.get_stock_history("ALPHA", days=days)  # type: ignore[arg-type]

    session.request.assert_not_called()


def test_stock_history_rejects_blank_ticker() -> None:
    client, session = _client(_http_response(200, []))

    with pytest.raises(ValueError, match="Ticker must not be empty"):
        client.get_stock_history("  ")

    session.request.assert_not_called()


def test_no_content_response_returns_none() -> None:
    client, _ = _client(_http_response(204))

    assert client.post_heartbeat(1) is None


def test_http_error_is_propagated() -> None:
    client, _ = _client(_http_response(401, {"error": "unauthorized"}))

    with pytest.raises(requests.HTTPError):
        client.get_portfolio()


def test_context_manager_closes_http_session() -> None:
    client, session = _client(_http_response(200, {}))

    with client as entered_client:
        assert entered_client is client

    session.close.assert_called_once_with()