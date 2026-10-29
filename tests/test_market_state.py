from collections import deque
import unittest

from market_state import MarketState
from models import PriceTick


def candle(ticker: str, date: str, close: float) -> PriceTick:
    return PriceTick(
        ticker=ticker,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=100,
        simulated_date=date,
    )


class FakeHistoryClient:
    def __init__(self, histories: dict[str, list[PriceTick]]) -> None:
        self.histories = histories
        self.calls: list[tuple[str, int]] = []

    def get_stock_history(self, ticker: str, days: int = 90) -> list[PriceTick]:
        self.calls.append((ticker, days))
        return self.histories[ticker]


class MarketStateHistoryBootstrapTests(unittest.TestCase):
    def test_bootstrap_fetches_each_ticker_and_keeps_recent_chronological_history(self) -> None:
        client = FakeHistoryClient(
            {
                "ABC": [
                    candle("ABC", "2026-01-03", 3.0),
                    candle("ABC", "2026-01-01", 1.0),
                    candle("ABC", "2026-01-02", 2.0),
                ],
                "XYZ": [candle("XYZ", "2026-01-02", 20.0)],
            }
        )
        state = MarketState(history_days=90, max_history=2)

        state.bootstrap_history(client, [" abc ", "ABC", "xyz"])

        self.assertEqual(client.calls, [("ABC", 90), ("XYZ", 90)])
        self.assertEqual(
            [tick.close for tick in state.price_history["ABC"]],
            [2.0, 3.0],
        )
        self.assertEqual(state.price_history["ABC"].maxlen, 2)
        self.assertEqual(state.price_history["XYZ"][0].close, 20.0)

    def test_bootstrap_does_not_publish_partial_results_when_fetch_fails(self) -> None:
        class FailingClient:
            def get_stock_history(self, ticker: str, days: int = 90) -> list[PriceTick]:
                if days <= 0:
                    raise ValueError("days must be positive")
                if ticker == "XYZ":
                    raise OSError("history endpoint unavailable")
                return [candle(ticker, "2026-01-01", 1.0)]

        state = MarketState()
        state.price_history["OLD"] = deque(
            [candle("OLD", "2025-01-01", 5.0)],
            maxlen=150,
        )

        with self.assertRaisesRegex(OSError, "history endpoint unavailable"):
            state.bootstrap_history(FailingClient(), ["ABC", "XYZ"])

        self.assertEqual(list(state.price_history), ["OLD"])

    def test_bootstrap_rejects_mismatched_ticker_or_missing_date(self) -> None:
        state = MarketState()

        with self.assertRaisesRegex(ValueError, "contains candle for XYZ"):
            state.bootstrap_history(
                FakeHistoryClient({"ABC": [candle("XYZ", "2026-01-01", 1.0)]}),
                ["ABC"],
            )
        with self.assertRaisesRegex(ValueError, "missing a date"):
            state.bootstrap_history(
                FakeHistoryClient({"ABC": [candle("ABC", "", 1.0)]}),
                ["ABC"],
            )

    def test_bootstrap_rejects_empty_tickers(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-empty strings"):
            MarketState().bootstrap_history(FakeHistoryClient({}), [""])

    def test_constructor_rejects_non_positive_limits(self) -> None:
        with self.assertRaises(ValueError):
            MarketState(history_days=0)
        with self.assertRaises(ValueError):
            MarketState(max_history=0)


if __name__ == "__main__":
    unittest.main()
