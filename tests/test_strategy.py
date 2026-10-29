import unittest

from strategy import generate_signal


class GenerateSignalTests(unittest.TestCase):
    def test_buys_on_bullish_sma_crossover_with_positive_rsi(self) -> None:
        prices = [100.0] * 20 + [101.0]

        self.assertEqual(generate_signal(prices), "BUY")

    def test_does_not_buy_when_short_sma_is_already_above_long_sma(self) -> None:
        prices = [float(price) for price in range(1, 22)]

        self.assertIsNone(generate_signal(prices))

    def test_buys_three_day_dip_when_rsi_is_oversold(self) -> None:
        prices = [100.0] * 14 + [96.0]

        self.assertEqual(generate_signal(prices), "BUY")

    def test_negative_news_suppresses_dip_buy(self) -> None:
        prices = [100.0] * 14 + [96.0]

        self.assertIsNone(generate_signal(prices, negative_news=True))

    def test_strong_positive_news_produces_buy(self) -> None:
        self.assertEqual(generate_signal([100.0], news_score=2.0), "BUY")

    def test_strong_negative_news_produces_short_only_below_long_sma(self) -> None:
        prices_below_average = [110.0] * 19 + [90.0]
        prices_above_average = [90.0] * 19 + [110.0]

        self.assertEqual(
            generate_signal(prices_below_average, news_score=-2.0),
            "SHORT",
        )
        self.assertEqual(
            generate_signal(prices_above_average, news_score=-2.0),
            "SELL",
        )

    def test_strong_negative_news_without_long_sma_history_does_not_short(self) -> None:
        self.assertEqual(generate_signal([100.0] * 19, news_score=-2.0), "SELL")

    def test_dip_threshold_is_strict(self) -> None:
        prices = [100.0] * 14 + [96.5]

        self.assertIsNone(generate_signal(prices))

    def test_insufficient_history_produces_no_signal(self) -> None:
        self.assertIsNone(generate_signal([100.0] * 14))

    def test_non_positive_or_non_finite_price_is_rejected(self) -> None:
        for prices in ([100.0, 0.0], [100.0, float("nan")], [100.0, float("inf")]):
            with self.subTest(prices=prices):
                with self.assertRaises(ValueError):
                    generate_signal(prices)

    def test_non_finite_news_score_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            generate_signal([100.0], news_score=float("nan"))


if __name__ == "__main__":
    unittest.main()