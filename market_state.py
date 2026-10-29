"""Historical market-data warm-start for discovered stocks."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import replace
import math
from threading import RLock
from typing import Protocol

import config
from models import NewsFlash, PriceTick


class HistoricalDataClient(Protocol):
    """The API client operation required to load historical candles."""

    def get_stock_history(
        self,
        ticker: str,
        days: int = config.HISTORY_WARMUP_DAYS,
    ) -> list[PriceTick]: ...


class MarketState:
    """Keep recent market candles and news available to decision-making code."""

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
        self.news_history: dict[str, deque[NewsFlash]] = {}
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

            normalized_candles = [
                replace(candle, ticker=ticker)
                for candle in candles
            ]
            ordered_candles = sorted(
                normalized_candles,
                key=lambda candle: candle.simulated_date,
            )
            fetched[ticker] = deque(ordered_candles, maxlen=self.max_history)

        with self._lock:
            self.price_history.update(fetched)

    def on_tick(self, tick: PriceTick) -> None:
        """Append a live candle, replacing the same day's candle or resetting on rewind."""
        normalized = self._normalize_tick(tick)
        with self._lock:
            history = self.price_history.setdefault(
                normalized.ticker,
                deque(maxlen=self.max_history),
            )
            if history and normalized.simulated_date < history[-1].simulated_date:
                history.clear()
                self.news_history.pop(normalized.ticker, None)

            if history and normalized.simulated_date == history[-1].simulated_date:
                history[-1] = normalized
            else:
                history.append(normalized)

    def on_news(self, news: NewsFlash) -> None:
        """Cache a news flash by ticker so consumers can use current-day sentiment."""
        if not isinstance(news, NewsFlash):
            raise TypeError("news must be a NewsFlash.")
        ticker = news.ticker.strip().upper()
        if not ticker:
            raise ValueError("News ticker must not be empty.")

        normalized = replace(news, ticker=ticker)
        with self._lock:
            history = self.news_history.setdefault(
                ticker,
                deque(maxlen=self.max_history),
            )
            history.append(normalized)

    def get_closing_prices(self, ticker: str) -> tuple[float, ...]:
        """Return a stable snapshot of closing prices in chronological order."""
        with self._lock:
            history = self.price_history.get(ticker.strip().upper())
            if history is None:
                return ()
            return tuple(tick.close for tick in history)

    def get_latest_news(
        self,
        ticker: str,
        *,
        simulated_date: str | None = None,
    ) -> NewsFlash | None:
        """Return the newest cached news, optionally restricted to one simulation date."""
        with self._lock:
            history = self.news_history.get(ticker.strip().upper())
            if history is None:
                return None
            for news in reversed(history):
                if simulated_date is None or news.date == simulated_date:
                    return news
            return None

    @staticmethod
    def _normalize_tick(tick: PriceTick) -> PriceTick:
        if not isinstance(tick, PriceTick):
            raise TypeError("tick must be a PriceTick.")

        ticker = tick.ticker.strip().upper()
        if not ticker:
            raise ValueError("Tick ticker must not be empty.")
        if not tick.simulated_date.strip():
            raise ValueError("Tick simulated_date must not be empty.")

        prices = (tick.open, tick.high, tick.low, tick.close)
        if any(
            isinstance(price, bool)
            or not isinstance(price, (int, float))
            or not math.isfinite(price)
            or price <= 0
            for price in prices
        ):
            raise ValueError("Tick OHLC prices must be finite and positive.")
        if tick.high < max(tick.open, tick.close) or tick.low > min(tick.open, tick.close):
            raise ValueError("Tick high and low must contain the open and close.")
        if isinstance(tick.volume, bool) or not isinstance(tick.volume, int) or tick.volume < 0:
            raise ValueError("Tick volume must be a non-negative integer.")

        return replace(
            tick,
            ticker=ticker,
            simulated_date=tick.simulated_date.strip(),
        )