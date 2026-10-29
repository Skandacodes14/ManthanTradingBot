import io
import logging
import unittest

from models import OrderRequest
from order_manager import (
    DispatcherPausedError,
    OrderManager,
    OrderRejectedError,
    TokenBucketRateLimiter,
)
from telemetry import TradeTelemetry


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class TokenBucketRateLimiterTests(unittest.TestCase):
    def test_paces_requests_at_configured_rate(self) -> None:
        clock = FakeClock()
        limiter = TokenBucketRateLimiter(
            rate=1.5,
            clock=clock.monotonic,
            sleep=clock.sleep,
        )

        limiter.acquire()
        limiter.acquire()

        self.assertAlmostEqual(clock.sleeps[0], 2 / 3)

    def test_order_manager_dispatches_after_rate_limit(self) -> None:
        clock = FakeClock()
        limiter = TokenBucketRateLimiter(
            rate=1.5,
            clock=clock.monotonic,
            sleep=clock.sleep,
        )
        order = OrderRequest(ticker="abc", action="buy", quantity=5)
        sent: list[OrderRequest] = []
        manager = OrderManager(lambda value: sent.append(value) or "accepted", rate_limiter=limiter)

        result = manager.dispatch(order)

        self.assertEqual(result, "accepted")
        self.assertEqual(sent, [order])

    def test_rejects_invalid_rate_or_capacity(self) -> None:
        with self.assertRaises(ValueError):
            TokenBucketRateLimiter(rate=0)
        with self.assertRaises(ValueError):
            TokenBucketRateLimiter(capacity=0.5)


class OrderManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.order = OrderRequest(
            ticker="abc",
            action="buy",
            quantity=5,
            date="2021-04-15",
        )
        self.limiter = TokenBucketRateLimiter(
            rate=1.5,
            clock=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

    def test_dispatch_preserves_date_and_records_structured_telemetry(self) -> None:
        self.clock.now = 8.0
        response = {
            "fill_price": 123.5,
            "brokerage_charged": 20.0,
            "tax_deducted": 1.25,
        }
        sent: list[OrderRequest] = []
        telemetry = TradeTelemetry(wall_clock=lambda: 0.0)
        manager = OrderManager(
            lambda order: sent.append(order) or response,
            rate_limiter=self.limiter,
            telemetry=telemetry,
            clock=self.clock.monotonic,
        )

        result = manager.dispatch(self.order, tick_received_at=5.0)

        self.assertIs(result, response)
        self.assertEqual(sent[0].to_dict(), {
            "ticker": "ABC",
            "action": "BUY",
            "quantity": 5,
            "date": "2021-04-15",
        })
        event = telemetry.events[0]
        self.assertEqual(event.status, "submitted")
        self.assertEqual(event.decision_latency_seconds, 3.0)
        self.assertEqual(event.fill_price, 123.5)
        self.assertEqual(event.brokerage_charged, 20.0)
        self.assertEqual(event.tax_deducted, 1.25)

    def test_thread_safe_queue_dispatches_queued_order(self) -> None:
        sent: list[OrderRequest] = []
        manager = OrderManager(
            lambda order: sent.append(order) or "accepted",
            rate_limiter=self.limiter,
            clock=self.clock.monotonic,
            sleep=self.clock.sleep,
        )
        manager.enqueue(self.order, tick_received_at=0.0)

        self.assertEqual(manager.pending_count, 1)
        self.assertEqual(manager.dispatch_next(timeout=0), "accepted")
        self.assertEqual(manager.pending_count, 0)
        self.assertEqual(sent, [self.order])
        self.assertEqual(manager.telemetry.events[0].decision_latency_seconds, 0.0)

    def test_insufficient_capital_refreshes_cash_and_surfaces_rejection(self) -> None:
        refreshes: list[bool] = []
        manager = OrderManager(
            lambda _: {"error": {"code": "INSUFFICIENT_CAPITAL"}},
            rate_limiter=self.limiter,
            refresh_cash_cache=lambda: refreshes.append(True),
            clock=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        with self.assertRaisesRegex(OrderRejectedError, "INSUFFICIENT_CAPITAL"):
            manager.dispatch(self.order)

        self.assertEqual(refreshes, [True])
        self.assertEqual(manager.telemetry.events[0].error_code, "INSUFFICIENT_CAPITAL")

    def test_no_short_position_rejection_is_recorded_and_propagated(self) -> None:
        manager = OrderManager(
            lambda _: {"error_code": "NO_SHORT_POSITION"},
            rate_limiter=self.limiter,
            clock=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        with self.assertRaisesRegex(OrderRejectedError, "NO_SHORT_POSITION"):
            manager.dispatch(self.order)

        self.assertEqual(manager.telemetry.events[0].status, "rejected")

    def test_rate_limited_rejection_applies_penalty_before_next_dispatch(self) -> None:
        responses = iter([{"code": "RATE_LIMITED"}, {"status": "accepted"}])
        manager = OrderManager(
            lambda _: next(responses),
            rate_limiter=self.limiter,
            penalty_box_seconds=30,
            clock=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        with self.assertRaisesRegex(OrderRejectedError, "RATE_LIMITED"):
            manager.dispatch(self.order)
        self.assertEqual(manager.dispatch(self.order), {"status": "accepted"})
        self.assertGreaterEqual(self.clock.now, 30)

    def test_simulation_not_running_pauses_until_resumed(self) -> None:
        manager = OrderManager(
            lambda _: {"code": "SIMULATION_NOT_RUNNING"},
            rate_limiter=self.limiter,
            clock=self.clock.monotonic,
            sleep=self.clock.sleep,
        )

        with self.assertRaisesRegex(OrderRejectedError, "SIMULATION_NOT_RUNNING"):
            manager.dispatch(self.order)
        self.assertTrue(manager.paused)
        with self.assertRaises(DispatcherPausedError):
            manager.dispatch(self.order)

        manager.resume()
        self.assertFalse(manager.paused)

    def test_telemetry_formats_readable_terminal_audit(self) -> None:
        output = io.StringIO()
        logger = logging.getLogger(f"test.telemetry.{id(output)}")
        telemetry = TradeTelemetry(
            logger=logger,
            wall_clock=lambda: 0.0,
            stream=output,
        )

        telemetry.record_order(
            self.order,
            status="submitted",
            tick_received_at=1.0,
            sent_at=1.125,
            response={
                "fill_price": 123.5,
                "brokerage_charged": 20.0,
                "tax_deducted": 1.25,
            },
        )

        terminal_line = output.getvalue()
        self.assertIn("[SUBMITTED]", terminal_line)
        self.assertIn("BUY 5 ABC", terminal_line)
        self.assertIn("date=2021-04-15", terminal_line)
        self.assertIn("decision_latency=0.125s", terminal_line)
        self.assertIn("fill=123.5", terminal_line)
        self.assertIn("brokerage=20.0", terminal_line)
        self.assertIn("tax=1.25", terminal_line)


if __name__ == "__main__":
    unittest.main()
