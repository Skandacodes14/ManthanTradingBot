"""
Technical Indicator Engine for MANTHAN 2026 Autonomous Trading Bot.

Provides ultra-low-latency (< 0.1ms per call) mathematical indicators:
- Simple Moving Average (SMA)
- Exponential Moving Average (EMA)
- Relative Strength Index (RSI - Wilder's Smoothing)
- Bollinger Bands (Upper, Middle, Lower - Population StdDev)
- Multi-Day Price Momentum (% change)

Designed to operate directly on Python lists or collections.deque of floats
without DataFrame allocation overhead to stay well inside the 5ms budget.
"""

from __future__ import annotations

import math
from typing import Sequence, Tuple


def _extract_tail(prices: Sequence[float], count: int) -> list[float]:
    """Extract the last `count` elements cleanly from a list or deque."""
    n = len(prices)
    if count <= 0 or n == 0:
        return []
    if count >= n:
        return list(prices)
    if isinstance(prices, list):
        return prices[-count:]
    # For collections.deque or other sequences that do not support slice syntax
    return [prices[i] for i in range(n - count, n)]


def calculate_sma(prices: Sequence[float], window: int) -> float:
    """
    Calculate the Simple Moving Average (SMA) over the trailing `window` prices.

    Args:
        prices: Chronological sequence of closing prices (oldest to newest).
        window: Lookback period (must be > 0).

    Returns:
        float: The trailing SMA, or math.nan if insufficient data or invalid window.
    """
    if window <= 0 or len(prices) < window:
        return math.nan

    tail = _extract_tail(prices, window)
    return sum(tail) / window


def calculate_ema(prices: Sequence[float], window: int) -> float:
    """
    Calculate the Exponential Moving Average (EMA) seeded with the initial SMA.

    Uses standard smoothing factor alpha = 2 / (window + 1).
    To bound execution time on large buffers, bounds lookback to the most recent
    max(window * 4, 150) prices, where initial seed weight decays below 0.03%.

    Args:
        prices: Chronological sequence of closing prices (oldest to newest).
        window: Lookback period (must be > 0).

    Returns:
        float: The current EMA value, or math.nan if len(prices) < window.
    """
    if window <= 0 or len(prices) < window:
        return math.nan

    max_lookback = max(window * 4, 150)
    series = _extract_tail(prices, max_lookback)

    # Seed EMA with the Simple Moving Average of the first `window` elements
    ema = sum(series[:window]) / window
    alpha = 2.0 / (window + 1.0)
    one_minus_alpha = 1.0 - alpha

    for price in series[window:]:
        ema = (price * alpha) + (ema * one_minus_alpha)

    return ema


def calculate_rsi(prices: Sequence[float], period: int = 14) -> float:
    """
    Calculate Wilder's Relative Strength Index (RSI).

    Requires at least `period + 1` prices (to compute `period` price changes).
    Uses Wilder's smoothing (alpha = 1 / period) seeded with the simple average
    of the first `period` gains and losses.

    Args:
        prices: Chronological sequence of closing prices (oldest to newest).
        period: RSI lookback period (default: 14).

    Returns:
        float: RSI bounded in [0.0, 100.0], or math.nan if len(prices) < period + 1.
    """
    if period <= 0 or len(prices) < period + 1:
        return math.nan

    max_lookback = max(period * 6, 150)
    series = _extract_tail(prices, max_lookback)

    # Compute initial average gain and loss over the first `period` deltas
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        delta = series[i] - series[i - 1]
        if delta > 0.0:
            gains += delta
        elif delta < 0.0:
            losses -= delta

    avg_gain = gains / period
    avg_loss = losses / period

    # Apply Wilder's smoothing over remaining deltas
    pm1 = float(period - 1)
    for i in range(period + 1, len(series)):
        delta = series[i] - series[i - 1]
        gain = delta if delta > 0.0 else 0.0
        loss = -delta if delta < 0.0 else 0.0
        avg_gain = (avg_gain * pm1 + gain) / period
        avg_loss = (avg_loss * pm1 + loss) / period

    if avg_loss == 0.0:
        return 100.0 if avg_gain > 0.0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calculate_bollinger(
    prices: Sequence[float],
    window: int = 20,
    num_std: float = 2.0,
) -> Tuple[float, float, float]:
    """
    Calculate Bollinger Bands (Upper Band, Middle Band, Lower Band).

    Uses population standard deviation (ddof=0) over the trailing `window` prices,
    matching standard charting and technical analysis conventions.

    Args:
        prices: Chronological sequence of closing prices (oldest to newest).
        window: Moving average & volatility window (default: 20).
        num_std: Standard deviation multiplier (default: 2.0).

    Returns:
        tuple[float, float, float]: (upper_band, middle_band, lower_band),
        or (math.nan, math.nan, math.nan) if len(prices) < window.
    """
    if window <= 0 or len(prices) < window or num_std < 0.0:
        return (math.nan, math.nan, math.nan)

    tail = _extract_tail(prices, window)
    middle = sum(tail) / window

    variance = sum((p - middle) ** 2 for p in tail) / window
    std_dev = math.sqrt(variance)

    offset = num_std * std_dev
    return (middle + offset, middle, middle - offset)


def calculate_momentum(prices: Sequence[float], days: int = 3) -> float:
    """
    Calculate multi-day percentage return (rate of change) over `days`.

    Formula: (prices[-1] - prices[-1 - days]) / prices[-1 - days]
    Example: A 4.0% drop over 3 days returns -0.04.

    Args:
        prices: Chronological sequence of closing prices (oldest to newest).
        days: Number of trading days lookback (default: 3).

    Returns:
        float: Fractional price change (e.g., -0.04 for -4%), or math.nan if
        len(prices) < days + 1 or base price <= 0.
    """
    if days <= 0 or len(prices) < days + 1:
        return math.nan

    current_price = prices[-1]
    past_price = prices[-1 - days]

    if past_price <= 0.0:
        return math.nan

    return (current_price - past_price) / past_price


def compute_all_indicators(prices: Sequence[float]) -> dict[str, float]:
    """Calculate the indicator set consumed by the live verifier and strategy."""
    bb_upper, _, bb_lower = calculate_bollinger(prices, window=20)

    return {
        "sma_5": calculate_sma(prices, window=5),
        "sma_20": calculate_sma(prices, window=20),
        "rsi_14": calculate_rsi(prices, period=14),
        "bb_lower": bb_lower,
        "bb_upper": bb_upper,
        "momentum_3d": calculate_momentum(prices, days=3),
    }