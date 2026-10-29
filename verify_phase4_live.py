"""
Live Competition Server Smoke Test for Phase 4 (`indicators.py` & `news_sentiment.py`).

Pre-configured with Team ZentroGen (MN2663) competition coordinates:
- GATEWAY:  http://192.168.65.157:3002
- BROKER:   192.168.65.157:9094
- USERNAME: MN2663 (group-MN2663)

What it does:
1. Checks current platform stage via `GET /api/v1/stage`.
2. Discovers active tradable tickers via `GET /api/v1/stocks`.
3. Warm-starts 90-day historical closing prices via `GET /api/v1/stocks/:ticker/history?days=90`
   and prints initial SMA(5), SMA(20), RSI(14), Bollinger Bands, and 3-Day Momentum with latency.
4. Connects to Kafka (`market.ticks`, `market.news`) exactly like the official starter template:
   - Handles `preflight_tick` events and sends `POST /api/v1/bot/heartbeat`.
   - Detects sandbox loop date resets (`simulated_date < last_date`) and re-bootstraps history.
   - Updates `indicators.py` on every `market.ticks` candle and prints live indicator values + ms latency.
   - Scores every `market.news` headline/description with `news_sentiment.py` and prints the signal.

Usage:
    python3 verify_phase4_live.py               # Run REST warm-start + continuous Kafka listener
    python3 verify_phase4_live.py --rest-only   # Run only the REST warm-start table and exit
"""

from __future__ import annotations

import argparse
from collections import deque
import json
import os
import sys
import time
import requests

from indicators import compute_all_indicators
from news_sentiment import analyze_news

# ---------------------------------------------------------------------------
# 1. Competition Coordinates & Persistent Bearer Token (from Starter Template)
# ---------------------------------------------------------------------------
GATEWAY = os.environ.get("GATEWAY", "http://192.168.65.157:3002")
BROKER = os.environ.get("BROKER", "192.168.65.157:9094")
USERNAME = os.environ.get("TEAM_USERNAME", "MN2663")

TOKEN = os.environ.get(
    "BOT_ACCESS_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJ0ZWFtX2lkIjoidGVhbV9pYTRtOHR2IiwidXNlcm5hbWUiOiJtbjI2NjMiLCJ0ZWFtX25hbWUiOiJaZW50cm9HZW4iLCJpc192ZXJpZmllZCI6dHJ1ZSwiaWF0IjoxNzkzMjU3NDAzLCJleHAiOjE3OTM0MzAyMDN9."
    "Zfl69E9IvNIDWJm1X9090JNB00W8VaJ_Ow2INEFHFFQ",
)

session = requests.Session()
session.headers.update(
    {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }
)


def bootstrap_universe_and_history() -> tuple[set[str], dict[str, deque[float]], dict[str, str]]:
    """
    Fetch active stock universe and warm-start 90-day closing price buffers.

    Returns:
        (tradable_tickers, price_buffers, last_seen_dates)
    """
    print(f"=== [1/3] Checking Competition Stage ({GATEWAY}/api/v1/stage) ===")
    stage_resp = session.get(f"{GATEWAY}/api/v1/stage", timeout=5)
    stage_resp.raise_for_status()
    stage_info = stage_resp.json()
    print(
        f"Stage: {stage_info.get('stage')} | Status: {stage_info.get('status')} | "
        f"Sim Date: {stage_info.get('current_date')} | "
        f"Days: {stage_info.get('days_completed')}/{stage_info.get('total_days')}\n"
    )

    print(f"=== [2/3] Discovering Stock Universe ({GATEWAY}/api/v1/stocks) ===")
    stocks_resp = session.get(f"{GATEWAY}/api/v1/stocks", timeout=5)
    stocks_resp.raise_for_status()
    stocks_list = stocks_resp.json().get("stocks", [])
    tradable_tickers = {stock["ticker"] for stock in stocks_list}
    print("Simulation tickers:", sorted(tradable_tickers), "\n")

    print("=== [3/3] Warm-Starting 90-Day History & Benchmarking Phase 4 Indicators ===")
    header = (
        f"{'TICKER':<12} | {'BARS':>4} | {'CLOSE':>9} | {'SMA(5)':>9} | "
        f"{'SMA(20)':>9} | {'RSI(14)':>7} | {'BB_LOW':>9} | {'BB_UP':>9} | "
        f"{'MOM(3D)':>8} | {'LATENCY':>9}"
    )
    print(header)
    print("-" * len(header))

    price_buffers: dict[str, deque[float]] = {}
    last_dates: dict[str, str] = {}

    for ticker in sorted(tradable_tickers):
        hist_resp = session.get(
            f"{GATEWAY}/api/v1/stocks/{ticker}/history",
            params={"days": 90},
            timeout=5,
        )
        hist_resp.raise_for_status()
        bars = hist_resp.json().get("prices", [])

        closes: deque[float] = deque(
            (float(bar["close"]) for bar in bars if "close" in bar),
            maxlen=150,
        )
        price_buffers[ticker] = closes
        if bars and "date" in bars[-1]:
            last_dates[ticker] = str(bars[-1]["date"])

        t0 = time.perf_counter_ns()
        ind = compute_all_indicators(closes)
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000.0

        last_close = closes[-1] if closes else float("nan")
        mom = ind["momentum_3d"]
        mom_pct = mom * 100.0 if mom == mom else float("nan")

        print(
            f"{ticker:<12} | {len(closes):>4} | {last_close:>9.2f} | "
            f"{ind['sma_5']:>9.2f} | {ind['sma_20']:>9.2f} | "
            f"{ind['rsi_14']:>7.2f} | {ind['bb_lower']:>9.2f} | "
            f"{ind['bb_upper']:>9.2f} | {mom_pct:>+7.2f}% | {elapsed_ms:>7.4f}ms"
        )

    return tradable_tickers, price_buffers, last_dates


def stream_live_kafka(
    tradable_tickers: set[str],
    price_buffers: dict[str, deque[float]],
    last_dates: dict[str, str],
) -> None:
    """Connect to Kafka and run Phase 4 indicators + sentiment on every live event."""
    from kafka import KafkaConsumer  # type: ignore

    consumer = KafkaConsumer(
        "market.ticks",
        "market.news",
        bootstrap_servers=[BROKER],
        group_id=f"group-{USERNAME}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda msg: json.loads(msg.decode("utf-8")),
    )
    print(f"\nConnected to Kafka broker at {BROKER} (group-{USERNAME}). Listening for events...")

    for message in consumer:
        event = message.value
        event_type = event.get("event_type") or event.get("event")

        if event_type == "preflight_tick":
            # Ensure stock universe is loaded during pre-flight
            if not tradable_tickers:
                tradable_tickers, price_buffers, last_dates = bootstrap_universe_and_history()

            seq = event.get("seq") or (event.get("data") or {}).get("seq")
            if seq is not None:
                session.post(
                    f"{GATEWAY}/api/v1/bot/heartbeat",
                    json={"seq": seq},
                    timeout=2,
                ).raise_for_status()
                print(f"[PREFLIGHT] Heartbeat ACK sent for seq={seq}")

        elif message.topic == "market.ticks":
            data = event.get("data") or {}
            ticker = data.get("ticker")
            if not ticker or (tradable_tickers and ticker not in tradable_tickers):
                continue

            sim_date = str(event.get("simulated_date") or "")
            close_price = float(data.get("close", 0.0))

            # Detect sandbox continuous loop reset (date rewinds back to Day 1)
            prev_date = last_dates.get(ticker)
            if prev_date and sim_date and sim_date < prev_date:
                print(
                    f"[SANDBOX LOOP DETECTED] {ticker} date rewound ({prev_date} -> {sim_date}). "
                    "Resetting indicator buffer."
                )
                price_buffers[ticker].clear()

            last_dates[ticker] = sim_date
            buf = price_buffers.setdefault(ticker, deque(maxlen=150))
            buf.append(close_price)

            t0 = time.perf_counter_ns()
            ind = compute_all_indicators(buf)
            latency_ms = (time.perf_counter_ns() - t0) / 1_000_000.0

            mom = ind["momentum_3d"]
            mom_str = f"{mom * 100.0:+.2f}%" if mom == mom else "   NaN"

            print(
                f"[TICK {sim_date}] {ticker:<12} Close={close_price:>8.2f} | "
                f"SMA5={ind['sma_5']:>8.2f} SMA20={ind['sma_20']:>8.2f} "
                f"RSI14={ind['rsi_14']:>5.1f} BB=[{ind['bb_lower']:>7.2f}, {ind['bb_upper']:>7.2f}] "
                f"Mom3D={mom_str} ({latency_ms:.4f}ms)"
            )

        elif message.topic == "market.news" or event_type == "news_flash":
            data = event.get("data") or {}
            ticker = data.get("ticker", "UNKNOWN")
            headline = data.get("headline", "")
            description = data.get("description", "")

            t0 = time.perf_counter_ns()
            sentiment = analyze_news(headline, description)
            latency_ms = (time.perf_counter_ns() - t0) / 1_000_000.0

            print(
                f"[NEWS {event.get('simulated_date', '')}] {ticker:<12} | "
                f"Signal={sentiment['signal']:<7} Score={sentiment['score']:>+5.2f} "
                f"(Raw={sentiment['raw_score']:+d}, Bull={sentiment['bullish_hits']}, "
                f"Bear={sentiment['bearish_hits']}) ({latency_ms:.4f}ms) | {headline}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live Phase 4 Tester on Competition Server")
    parser.add_argument(
        "--rest-only",
        action="store_true",
        help="Only run the REST API 90-day history warm-start and indicator table, then exit.",
    )
    args = parser.parse_args()

    try:
        tickers, buffers, dates = bootstrap_universe_and_history()
        if not args.rest_only:
            stream_live_kafka(tickers, buffers, dates)
    except requests.exceptions.RequestException as exc:
        print(f"\n[SERVER CONNECTION ERROR] {exc}")
        print(f"Verify your laptop is on the venue Wi-Fi and can reach {GATEWAY}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nStopped live listener cleanly.")