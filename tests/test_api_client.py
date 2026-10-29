import unittest
from typing import Any

import config
from api_client import APIClient


class FakeResponse:
    status_code = 200

    def __init__(self, payload: Any) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self.payload


class FakeSession:
    def __init__(self, payload: Any) -> None:
        self.headers: dict[str, str] = {}
        self.payload = payload
        self.request_args: tuple[Any, ...] | None = None
        self.request_kwargs: dict[str, Any] | None = None

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.request_args = (method, url)
        self.request_kwargs = kwargs
        return FakeResponse(self.payload)

    def close(self) -> None:
        return None


class APIClientStockDiscoveryTests(unittest.TestCase):
    def test_get_stocks_requests_authenticated_endpoint_and_parses_records(self) -> None:
        session = FakeSession(
            {
                "stocks": [
                    {
                        "ticker": " abc ",
                        "symbol": " abc ",
                        "name": "Alpha Beta Corp",
                        "description": "A listed company",
                    }
                ]
            }
        )
        client = APIClient(
            base_url="https://gateway.example/api/v1/",
            token="test-token",
            session=session,  # type: ignore[arg-type]
        )

        stocks = client.get_stocks()

        self.assertEqual(session.request_args, ("GET", "https://gateway.example/api/v1/stocks"))
        self.assertEqual(
            session.request_kwargs,
            {
                "headers": config.get_auth_headers("test-token"),
                "timeout": 10.0,
            },
        )
        self.assertEqual(len(stocks), 1)
        self.assertEqual(stocks[0].ticker, "ABC")
        self.assertEqual(stocks[0].symbol, "ABC")
        self.assertEqual(stocks[0].name, "Alpha Beta Corp")

    def test_get_stocks_accepts_nested_data_envelope(self) -> None:
        session = FakeSession(
            {"data": {"stocks": [{"ticker": "xyz", "name": "Xyz Inc"}]}}
        )
        client = APIClient(token="test-token", session=session)  # type: ignore[arg-type]

        self.assertEqual([stock.ticker for stock in client.get_stocks()], ["XYZ"])

    def test_get_stocks_rejects_malformed_records(self) -> None:
        session = FakeSession({"stocks": [{"name": "Missing ticker"}]})
        client = APIClient(token="test-token", session=session)  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "missing a ticker"):
            client.get_stocks()

    def test_get_stocks_rejects_unexpected_response_shape(self) -> None:
        session = FakeSession({"stocks": "not-a-list"})
        client = APIClient(token="test-token", session=session)  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "Expected a list of stocks"):
            client.get_stocks()


class APIClientStockHistoryTests(unittest.TestCase):
    def test_get_stock_history_requests_90_days_and_parses_prices_envelope(self) -> None:
        session = FakeSession(
            {
                "prices": [
                    {
                        "date": "2026-01-02",
                        "open": 10,
                        "high": 12,
                        "low": 9,
                        "close": 11,
                        "volume": 100,
                    }
                ]
            }
        )
        client = APIClient(
            base_url="https://gateway.example/api/v1",
            token="test-token",
            session=session,  # type: ignore[arg-type]
        )

        history = client.get_stock_history("abc", days=90)

        self.assertEqual(
            session.request_args,
            ("GET", "https://gateway.example/api/v1/stocks/ABC/history"),
        )
        self.assertEqual(session.request_kwargs["params"], {"days": 90})
        self.assertEqual(session.request_kwargs["headers"], config.get_auth_headers("test-token"))
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].ticker, "ABC")
        self.assertEqual(history[0].simulated_date, "2026-01-02")
        self.assertEqual(history[0].close, 11.0)

    def test_get_stock_history_validates_ticker_and_days(self) -> None:
        client = APIClient(token="test-token", session=FakeSession([]))  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "Ticker must not be empty"):
            client.get_stock_history(" ")
        with self.assertRaisesRegex(ValueError, "positive integer"):
            client.get_stock_history("ABC", days=0)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            client.get_stock_history("ABC", days=True)


class APIClientHeartbeatTests(unittest.TestCase):
    def test_post_heartbeat_sends_sequence_to_authenticated_endpoint(self) -> None:
        session = FakeSession({"status": "Ready"})
        client = APIClient(
            base_url="https://gateway.example/api/v1",
            token="test-token",
            session=session,  # type: ignore[arg-type]
        )

        response = client.post_heartbeat(17)

        self.assertEqual(
            session.request_args,
            ("POST", "https://gateway.example/api/v1/bot/heartbeat"),
        )
        self.assertEqual(session.request_kwargs["json"], {"seq": 17})
        self.assertEqual(
            session.request_kwargs["headers"],
            config.get_auth_headers("test-token"),
        )
        self.assertEqual(response, {"status": "Ready"})


if __name__ == "__main__":
    unittest.main()
