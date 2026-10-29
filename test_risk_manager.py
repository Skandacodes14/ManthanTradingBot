import unittest

from models import OrderAction, Position
from risk_manager import RiskManager


class RiskManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.risk_manager = RiskManager(starting_capital=1_000_000.0)

    def test_buy_respects_cash_reserve_and_allows_exact_reserve(self) -> None:
        approved = self.risk_manager.check_buy(
            quantity=100,
            price=1_000.0,
            cash_remaining=300_000.0,
            total_wealth=1_000_000.0,
        )
        self.assertTrue(approved.approved)

        rejected = self.risk_manager.check_buy(
            quantity=101,
            price=1_000.0,
            cash_remaining=300_000.0,
            total_wealth=1_000_000.0,
        )
        self.assertFalse(rejected.approved)
        self.assertIn("Cash reserve violation", rejected.reason)

    def test_buy_respects_single_stock_weight_including_existing_position(self) -> None:
        approved = self.risk_manager.check_buy(
            quantity=100,
            price=1_000.0,
            cash_remaining=500_000.0,
            total_wealth=1_000_000.0,
            current_position_value=150_000.0,
        )
        self.assertTrue(approved.approved)

        rejected = self.risk_manager.check_buy(
            quantity=101,
            price=1_000.0,
            cash_remaining=500_000.0,
            total_wealth=1_000_000.0,
            current_position_value=150_000.0,
        )
        self.assertFalse(rejected.approved)
        self.assertIn("Position limit violation", rejected.reason)

    def test_short_requires_full_margin_and_respects_position_limit(self) -> None:
        self.assertTrue(
            self.risk_manager.check_short(
                quantity=100,
                price=1_000.0,
                margin_available=100_000.0,
                total_wealth=1_000_000.0,
            ).approved
        )
        low_margin = self.risk_manager.check_short(
            quantity=100,
            price=1_000.0,
            margin_available=99_999.0,
            total_wealth=1_000_000.0,
        )
        self.assertFalse(low_margin.approved)
        self.assertIn("Insufficient margin", low_margin.reason)

        oversized = self.risk_manager.check_short(
            quantity=101,
            price=1_000.0,
            margin_available=200_000.0,
            total_wealth=1_000_000.0,
            current_position_value=150_000.0,
        )
        self.assertFalse(oversized.approved)
        self.assertIn("Position limit violation", oversized.reason)

    def test_long_stop_loss_and_take_profit_return_sell(self) -> None:
        position = Position(ticker="ABC", quantity=10, avg_price=100.0)

        self.assertEqual(
            self.risk_manager.monitor_position(position, 96.49),
            OrderAction.SELL,
        )
        self.assertEqual(
            self.risk_manager.monitor_position(position, 106.01),
            OrderAction.SELL,
        )
        self.assertIsNone(self.risk_manager.monitor_position(position, 96.5))
        self.assertIsNone(self.risk_manager.monitor_position(position, 106.0))
        self.assertIsNone(self.risk_manager.monitor_position(position, 100.0))

    def test_short_is_covered_on_day_seven(self) -> None:
        before_deadline = Position(
            ticker="ABC", quantity=10, avg_price=100.0, is_short=True, days_held=6
        )
        at_deadline = Position(
            ticker="ABC", quantity=10, avg_price=100.0, is_short=True, days_held=7
        )

        self.assertIsNone(self.risk_manager.monitor_position(before_deadline, 100.0))
        self.assertEqual(
            self.risk_manager.monitor_position(at_deadline, 100.0),
            OrderAction.COVER,
        )

    def test_invalid_trade_input_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.risk_manager.check_buy(
                quantity=0,
                price=100.0,
                cash_remaining=500_000.0,
                total_wealth=1_000_000.0,
            )
        with self.assertRaises(ValueError):
            self.risk_manager.check_short(
                quantity=10,
                price=float("nan"),
                margin_available=10_000.0,
                total_wealth=1_000_000.0,
            )
        with self.assertRaises(ValueError):
            self.risk_manager.monitor_position(
                Position(ticker="ABC", quantity=10, avg_price=100.0),
                0.0,
            )


if __name__ == "__main__":
    unittest.main()