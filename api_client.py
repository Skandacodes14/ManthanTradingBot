"""Authenticated REST client for the Manthan trading gateway."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import requests

import config
from models import OrderRequest, PriceTick, StageStatus, StockInfo


class APIClient:
    """HTTP client for the REST endpoints documented in section 5.3."""

    def __init__(
        self,
        base_url: str = config.BASE_URL,
        token: str | None = None,
        session: requests.Session | None = None,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = config.BOT_ACCESS_TOKEN if token is None else token
        self.timeout = timeout
        self.session = session if session is not None else requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _request(
        self,
        method: str,
        path: str,
        *,
        authenticated: bool = True,
        **kwargs: Any,
    ) -> Any:
        headers: dict[str, str] = {}
        if authenticated:
            token = self.token.strip()
            if not token:
                raise ValueError("An access token is required for this API endpoint.")
            headers = config.get_auth_headers(token)

        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            headers=headers,
            timeout=self.timeout,
            **kwargs,
        )
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return response.json()

    @staticmethod
    def _list_payload(payload: Any, resource: str) -> list[dict[str, Any]]:
        while isinstance(payload, dict):
            if resource in payload:
                payload = payload[resource]
            elif resource == "history" and "prices" in payload:
                payload = payload["prices"]
            elif "data" in payload:
                payload = payload["data"]
            else:
                break
        if not isinstance(payload, list) or not all(
            isinstance(item, dict) for item in payload
        ):
            raise ValueError(f"Expected a list of {resource} records in API response.")
        return payload

    def get_stage(self) -> StageStatus:
        """Return the public lifecycle stage and status."""
        payload = self._request("GET", "/stage", authenticated=False)
        if not isinstance(payload, dict):
            raise ValueError("Expected an object in stage API response.")
        if isinstance(payload.get("data"), dict):
            payload = payload["data"]
        return StageStatus.from_dict(payload)

    def post_heartbeat(self, seq: int) -> Any:
        """Acknowledge the latest preflight heartbeat sequence."""
        return self._request("POST", "/bot/heartbeat", json={"seq": seq})

    def get_stocks(self) -> list[StockInfo]:
        """Return the stocks available for trading."""
        payload = self._request("GET", "/stocks")
        return [
            StockInfo.from_dict(item)
            for item in self._list_payload(payload, "stocks")
        ]

    def get_stock_history(self, ticker: str, days: int = 90) -> list[PriceTick]:
        """Return historical candles for a stock."""
        normalized_ticker = ticker.strip().upper()
        if not normalized_ticker:
            raise ValueError("Ticker must not be empty.")
        if isinstance(days, bool) or not isinstance(days, int) or days <= 0:
            raise ValueError("History days must be a positive integer.")

        path_ticker = quote(normalized_ticker, safe="")
        payload = self._request(
            "GET",
            f"/stocks/{path_ticker}/history",
            params={"days": days},
        )
        history: list[PriceTick] = []
        for item in self._list_payload(payload, "history"):
            record = dict(item)
            record.setdefault("ticker", normalized_ticker)
            history.append(PriceTick.from_dict(record))
        return history

    def get_portfolio(self) -> Any:
        """Return the current portfolio response."""
        return self._request("GET", "/portfolio")

    def get_cash(self) -> Any:
        """Return the current cash and buying-power response."""
        return self._request("GET", "/portfolio/cash")

    def get_orders(self) -> Any:
        """Return the order history response."""
        return self._request("GET", "/orders")

    def get_simulation_clock(self) -> Any:
        """Return the current simulation clock response."""
        return self._request("GET", "/simulation/clock")

    def post_order(self, order: OrderRequest) -> Any:
        """Submit a market order to the gateway."""
        return self._request("POST", "/orders", json=order.to_dict())

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()

    def __enter__(self) -> APIClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()