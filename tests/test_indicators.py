from collections import deque
import math
import unittest

from indicators import (
    calculate_bollinger,
    calculate_ema,
    calculate_momentum,
    calculate_rsi,
    calculate_sma,
    compute_all_indicators,
)


class SimpleMovingAverageTests(unittest.TestCase):
    def test_uses_trailing_window(self) -> None:
        self.assertEqual(calculate_sma([1.0, 2.0, 3.0, 8.0], 2), 5.5)

    def test_accepts_deque_input(self) -> None:
        self.assertEqual(calculate_sma(deque([1.0, 2.0, 3.0]), 3), 2.0)

    def test_returns_nan_for_insufficient_data_or_invalid_window(self) -> None:
        self.assertTrue(math.isnan(calculate_sma([1.0, 2.0], 3)))
        self.assertTrue(math.isnan(calculate_sma([1.0, 2.0], 0)))


class ExponentialMovingAverageTests(unittest.TestCase):
    def test_seeds_with_sma_and_applies_standard_smoothing(self) -> None:
        self.assertEqual(calculate_ema([1.0, 2.0, 3.0, 4.0], 3), 3.0)

    def test_accepts_deque_and_uses_trailing_data(self) -> None:
        self.assertEqual(calculate_ema(deque([1.0, 2.0, 3.0]), 2), 2.5)

    def test_returns_nan_for_insufficient_data_or_invalid_window(self) -> None:
        self.assertTrue(math.isnan(calculate_ema([1.0, 2.0], 3)))
        self.assertTrue(math.isnan(calculate_ema([1.0, 2.0], 0)))


class RelativeStrengthIndexTests(unittest.TestCase):
    def test_uses_wilder_smoothing(self) -> None:
        self.assertAlmostEqual(calculate_rsi([10.0, 11.0, 10.0, 12.0], 3), 75.0)

    def test_returns_100_for_gains_without_losses(self) -> None:
        self.assertEqual(calculate_rsi([1.0, 2.0, 3.0], 2), 100.0)

    def test_returns_50_for_flat_prices(self) -> None:
        self.assertEqual(calculate_rsi([5.0, 5.0, 5.0], 2), 50.0)

    def test_returns_nan_for_insufficient_data_or_invalid_period(self) -> None:
        self.assertTrue(math.isnan(calculate_rsi([1.0, 2.0], 2)))
        self.assertTrue(math.isnan(calculate_rsi([1.0, 2.0], 0)))


class BollingerBandTests(unittest.TestCase):
    def test_returns_upper_middle_and_lower_using_population_deviation(self) -> None:
        upper, middle, lower = calculate_bollinger([1.0, 2.0, 3.0], window=3)

        self.assertAlmostEqual(middle, 2.0)
        self.assertAlmostEqual(upper, 2.0 + 2.0 * math.sqrt(2.0 / 3.0))
        self.assertAlmostEqual(lower, 2.0 - 2.0 * math.sqrt(2.0 / 3.0))

    def test_zero_multiplier_returns_equal_bands(self) -> None:
        self.assertEqual(calculate_bollinger([1.0, 2.0], window=2, num_std=0.0), (1.5, 1.5, 1.5))

    def test_returns_nan_bands_for_invalid_inputs(self) -> None:
        for result in (
            calculate_bollinger([1.0, 2.0], window=3),
            calculate_bollinger([1.0, 2.0], window=0),
            calculate_bollinger([1.0, 2.0], window=2, num_std=-1.0),
        ):
            self.assertTrue(all(math.isnan(value) for value in result))


class MomentumTests(unittest.TestCase):
    def test_calculates_fractional_return_from_requested_lookback(self) -> None:
        self.assertAlmostEqual(calculate_momentum([100.0, 90.0, 95.0, 96.0], 3), -0.04)

    def test_accepts_deque_input(self) -> None:
        self.assertAlmostEqual(calculate_momentum(deque([100.0, 105.0]), 1), 0.05)

    def test_returns_nan_for_insufficient_data_invalid_days_or_nonpositive_base(self) -> None:
        self.assertTrue(math.isnan(calculate_momentum([1.0, 2.0], 2)))
        self.assertTrue(math.isnan(calculate_momentum([1.0, 2.0], 0)))
        self.assertTrue(math.isnan(calculate_momentum([0.0, 2.0], 1)))


class ComputeAllIndicatorsTests(unittest.TestCase):
    def test_returns_expected_indicator_set_and_matches_individual_functions(self) -> None:
        prices = [float(value) for value in range(1, 21)]
        result = compute_all_indicators(prices)
        upper, _, lower = calculate_bollinger(prices, window=20)

        self.assertEqual(
            set(result),
            {"sma_5", "sma_20", "rsi_14", "bb_lower", "bb_upper", "momentum_3d"},
        )
        self.assertEqual(result["sma_5"], calculate_sma(prices, 5))
        self.assertEqual(result["sma_20"], calculate_sma(prices, 20))
        self.assertEqual(result["rsi_14"], calculate_rsi(prices, 14))
        self.assertEqual(result["bb_lower"], lower)
        self.assertEqual(result["bb_upper"], upper)
        self.assertEqual(result["momentum_3d"], calculate_momentum(prices, 3))

    def test_short_history_reports_nan_for_unavailable_indicators(self) -> None:
        result = compute_all_indicators([1.0, 2.0, 3.0])

        self.assertTrue(all(math.isnan(value) for value in result.values()))


if __name__ == "__main__":
    unittest.main()