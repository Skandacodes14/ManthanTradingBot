"""Trend-following, dip-buying, and news-sentiment trading signals."""

from collections.abc import Sequence
import math


_TREND_SHORT_WINDOW = 5
_TREND_LONG_WINDOW = 20
_RSI_PERIOD = 14
_DIP_LOOKBACK_DAYS = 3
_DIP_THRESHOLD = 0.035
_TREND_RSI_THRESHOLD = 50.0
_DIP_RSI_THRESHOLD = 32.0
_BULLISH_NEWS_THRESHOLD = 2.0
_BEARISH_NEWS_THRESHOLD = -2.0


def _calculate_rsi(prices: Sequence[float], period: int = _RSI_PERIOD) -> float | None:
    if len(prices) < period + 1:
        return None

    changes = [
        prices[index] - prices[index - 1]
        for index in range(len(prices) - period, len(prices))
    ]
    average_gain = sum(max(change, 0.0) for change in changes) / period
    average_loss = sum(max(-change, 0.0) for change in changes) / period

    if average_loss == 0:
        return 50.0 if average_gain == 0 else 100.0
    if average_gain == 0:
        return 0.0

    relative_strength = average_gain / average_loss
    return 100.0 - 100.0 / (1.0 + relative_strength)


def _is_bullish_trend_crossover(prices: Sequence[float], rsi: float) -> bool:
    required_history = _TREND_LONG_WINDOW + 1
    if len(prices) < required_history or rsi <= _TREND_RSI_THRESHOLD:
        return False

    previous_short_sma = sum(prices[-6:-1]) / _TREND_SHORT_WINDOW
    previous_long_sma = sum(prices[-required_history:-1]) / _TREND_LONG_WINDOW
    current_short_sma = sum(prices[-_TREND_SHORT_WINDOW:]) / _TREND_SHORT_WINDOW
    current_long_sma = sum(prices[-_TREND_LONG_WINDOW:]) / _TREND_LONG_WINDOW

    return (
        previous_short_sma <= previous_long_sma
        and current_short_sma > current_long_sma
    )


def _is_dip_buy(prices: Sequence[float], rsi: float, negative_news: bool) -> bool:
    if negative_news or rsi >= _DIP_RSI_THRESHOLD:
        return False
    if len(prices) < _DIP_LOOKBACK_DAYS + 1:
        return False

    reference_price = prices[-(_DIP_LOOKBACK_DAYS + 1)]
    return prices[-1] < reference_price * (1.0 - _DIP_THRESHOLD)


def _is_below_long_sma(prices: Sequence[float]) -> bool:
    if len(prices) < _TREND_LONG_WINDOW:
        return False
    long_sma = sum(prices[-_TREND_LONG_WINDOW:]) / _TREND_LONG_WINDOW
    return prices[-1] < long_sma


def generate_signal(
    prices: Sequence[float],
    *,
    negative_news: bool = False,
    news_score: float = 0.0,
) -> str | None:
    """Return a trading action based on technical and sentiment signals.

    ``prices`` must contain positive closing prices in chronological order.
    Insufficient history suppresses technical signals and short entries.
    Strong bearish sentiment returns ``SHORT`` only below the 20-day SMA;
    otherwise it returns ``SELL``. Negative news also suppresses dip buys,
    but does not affect a bullish trend crossover.
    """
    if any(not math.isfinite(price) or price <= 0 for price in prices):
        raise ValueError("Prices must be finite positive closing prices.")
    if not math.isfinite(news_score):
        raise ValueError("News score must be finite.")

    if news_score >= _BULLISH_NEWS_THRESHOLD:
        return "BUY"
    if news_score <= _BEARISH_NEWS_THRESHOLD:
        return "SHORT" if _is_below_long_sma(prices) else "SELL"

    rsi = _calculate_rsi(prices)
    if rsi is None:
        return None

    if _is_bullish_trend_crossover(prices, rsi):
        return "BUY"
    if _is_dip_buy(prices, rsi, negative_news):
        return "BUY"
    return None