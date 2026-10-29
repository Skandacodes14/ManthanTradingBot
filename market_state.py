"""Historical market-data warm-start for discovered stocks."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from threading import RLock
from typing import Protocol

import config
from models import PriceTick


class HistoricalDataClient(Protocol):
    """The API client operation required to load historical candles."""

    def get_stock_history(
        self,
        ticker: str,
        days: int = config.HISTORY_WARMUP_DAYS,
    ) -> list[PriceTick]: ...


class MarketState:
    """Keep recent historical candles available as indicator warm-up data."""

    def __init__(
        self,
        *,
        history_days: int = config.HISTORY_WARMUP_DAYS,
        max_history: int = config.SLIDING_WINDOW_MAXLEN,
    ) -> None:
        if isinstance(history_days, bool) or not isinstance(history_days, int) or history_days <= 0:
            raise ValueError("history_days must be a positive integer.")
        if isinstance(max_history, bool) or not isinstance(max_history, int) or max_history <= 0:
            raise ValueError("max_history must be a positive integer.")

        self.history_days = history_days
        self.max_history = max_history
        self.price_history: dict[str, deque[PriceTick]] = {}
        self._lock = RLock()

    def bootstrap_history(
        self,
        api_client: HistoricalDataClient,
        tickers: Iterable[str],
    ) -> None:
        """Fetch, validate, and cache history for each ticker, atomically."""
        normalized_tickers: list[str] = []
        seen: set[str] = set()
        for ticker in tickers:
            if not isinstance(ticker, str) or not ticker.strip():
                raise ValueError("Tickers must be non-empty strings.")
            normalized = ticker.strip().upper()
            if normalized not in seen:
                normalized_tickers.append(normalized)
                seen.add(normalized)

        fetched: dict[str, deque[PriceTick]] = {}
        for ticker in normalized_tickers:
            candles = api_client.get_stock_history(ticker, days=self.history_days)
            for candle in candles:
                if candle.ticker and candle.ticker != ticker:
                    raise ValueError(
                        f"History for {ticker} contains candle for {candle.ticker}."
                    )
                if not candle.simulated_date:
                    raise ValueError(f"History candle for {ticker} is missing a date.")

            ordered_candles = sorted(candles, key=lambda candle: candle.simulated_date)
            fetched[ticker] = deque(ordered_candles, maxlen=self.max_history)

        with self._lock:
            self.price_history.update(fetched)