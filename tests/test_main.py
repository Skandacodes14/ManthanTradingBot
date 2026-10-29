import unittest

from main import TradingBot, load_account_snapshot
from market_state import MarketState
from models import NewsFlash, Position, PriceTick, StageStatus
from stage_monitor import StageMonitor


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


class FakeAPIClient:
    def __init__(
        self,
        *,
        positions: list[dict[str, object]] | None = None,
        cash: float = 1_000_000.0,
        margin: float = 1_000_000.0,
        total_wealth: float = 1_000_000.0,
    ) -> None:
        self.positions = positions if positions is not None else []
        self.cash = cash
        self.margin = margin
        self.total_wealth = total_wealth
        self.orders: list[object] = []
        self.closed = False

    def get_stage(self) -> StageStatus:
        return StageStatus(stage="simulation", status="RUNNING")

    def get_stocks(self) -> list[object]:
        return [type("Stock", (), {"ticker": "ABC"})()]

    def get_stock_history(self, ticker: str, days: int = 90) -> list[PriceTick]:
        return []

    def get_portfolio(self) -> dict[str, object]:
        return {"total_wealth": self.total_wealth, "positions": self.positions}

    def get_cash(self) -> dict[str, float]:
        return {"cash": self.cash, "available_margin": self.margin}

    def post_order(self, order: object) -> dict[str, str]:
        self.orders.append(order)
        return {"status": "accepted"}

    def close(self) -> None:
        self.closed = True


class AccountSnapshotTests(unittest.TestCase):
    def test_loads_nested_account_responses_and_short_positions(self) -> None:
        class AccountAPI(FakeAPIClient):
            def get_portfolio(self) -> dict[str, object]:
                return {
                    "data": {
                        "portfolio": {
                            "equity": 500_000,
                            "holdings": [
                                {
                                    "ticker": "abc",
                                    "quantity": 10,
                                    "avg_price": 100,
                                    "side": "SHORT",
                                    "days_held": 2,
                                }
                            ],
                        }
                    }
                }

            def get_cash(self) -> dict[str, object]:
                return {"data": {"available_cash": 200_000, "buying_power": 300_000}}

        snapshot = load_account_snapshot(AccountAPI())

        self.assertEqual(snapshot.cash, 200_000)
        self.assertEqual(snapshot.available_margin, 300_000)
        self.assertEqual(snapshot.total_wealth, 500_000)
        self.assertEqual(
            snapshot.positions["ABC"],
            Position("ABC", 10, 100.0, is_short=True, days_held=2),
        )

    def test_rejects_missing_risk_inputs(self) -> None:
        class IncompleteAPI(FakeAPIClient):
            def get_portfolio(self) -> dict[str, object]:
                return {"positions": []}

        with self.assertRaisesRegex(ValueError, "total_wealth"):
            load_account_snapshot(IncompleteAPI())

    def test_rejects_fractional_position_quantity(self) -> None:
        class InvalidPositionAPI(FakeAPIClient):
            def get_portfolio(self) -> dict[str, object]:
                return {
                    "total_wealth": 500_000,
                    "positions": [
                        {"ticker": "ABC", "quantity": 1.5, "avg_price": 100}
                    ],
                }

        with self.assertRaisesRegex(ValueError, "positive quantity"):
            load_account_snapshot(InvalidPositionAPI())


class TradingBotTests(unittest.TestCase):
    def create_bot(self, api: FakeAPIClient) -> TradingBot:
        state = MarketState()
        bot = TradingBot(
            api,
            market_state=state,
            stage_monitor=StageMonitor(api),
        )
        bot.stage_monitor.poll_once()
        return bot

    def seed_prices(self, bot: TradingBot, closes: list[float]) -> None:
        history = [
            candle("ABC", f"2026-01-{index + 1:02d}", close)
            for index, close in enumerate(closes)
        ]

        class HistoryAPI:
            def get_stock_history(
                self,
                ticker: str,
                days: int = 90,
            ) -> list[PriceTick]:
                return history

        bot.market_state.bootstrap_history(HistoryAPI(), ["ABC"])

    def test_dispatches_risk_sized_buy_during_running_simulation(self) -> None:
        api = FakeAPIClient()
        bot = self.create_bot(api)
        self.seed_prices(bot, [100.0] * 20)

        bot.on_tick(candle("ABC", "2026-01-21", 101.0))

        self.assertEqual(len(api.orders), 1)
        order = api.orders[0]
        self.assertEqual(order.action, "BUY")
        self.assertEqual(order.ticker, "ABC")
        self.assertEqual(order.date, "2026-01-21")
        self.assertLessEqual(order.quantity * 101.0, 250_000.0)

    def test_skips_signal_when_simulation_is_not_running(self) -> None:
        api = FakeAPIClient()
        bot = self.create_bot(api)
        self.seed_prices(bot, [100.0] * 20)
        bot.stage_monitor.dispatch_enabled_event.clear()

        bot.on_tick(candle("ABC", "2026-01-21", 101.0))

        self.assertEqual(api.orders, [])
        self.assertIsNone(bot._account)

    def test_kafka_consumer_routes_events_through_decision_loop(self) -> None:
        api = FakeAPIClient()
        bot = self.create_bot(api)

        self.assertIs(bot.consumer.market_state, bot)

    def test_respects_cash_reserve_and_does_not_submit_undersized_order(self) -> None:
        api = FakeAPIClient(cash=200_000.0)
        bot = self.create_bot(api)
        self.seed_prices(bot, [100.0] * 20)

        bot.on_tick(candle("ABC", "2026-01-21", 101.0))

        self.assertEqual(api.orders, [])

    def test_negative_news_can_open_risk_limited_short(self) -> None:
        api = FakeAPIClient(margin=300_000.0)
        bot = self.create_bot(api)
        self.seed_prices(bot, [110.0] * 18 + [100.0, 100.0])
        bot.on_news(NewsFlash("ABC", "Loss and fraud reported", "", "2026-01-21"))

        bot.on_tick(candle("ABC", "2026-01-21", 90.0))

        self.assertEqual(len(api.orders), 1)
        self.assertEqual(api.orders[0].action, "SHORT")
        self.assertLessEqual(api.orders[0].quantity * 90.0, 250_000.0)

    def test_stop_loss_closes_existing_long_position(self) -> None:
        api = FakeAPIClient(
            positions=[
                {
                    "ticker": "ABC",
                    "quantity": 10,
                    "avg_price": 100.0,
                    "is_short": False,
                }
            ]
        )
        bot = self.create_bot(api)
        self.seed_prices(bot, [100.0] * 20)

        bot.on_tick(candle("ABC", "2026-01-21", 96.0))

        self.assertEqual(len(api.orders), 1)
        self.assertEqual(api.orders[0].action, "SELL")
        self.assertEqual(api.orders[0].quantity, 10)

    def test_does_not_submit_more_than_one_order_for_ticker_per_day(self) -> None:
        api = FakeAPIClient()
        bot = self.create_bot(api)
        self.seed_prices(bot, [100.0] * 20)
        tick = candle("ABC", "2026-01-21", 101.0)

        bot.on_tick(tick)
        bot.on_tick(tick)

        self.assertEqual(len(api.orders), 1)


if __name__ == "__main__":
    unittest.main()
