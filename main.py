"""Application entry point and live trading decision loop."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import logging
import math
from threading import Event
import time
from typing import Any, Protocol

import config
from api_client import APIClient
from kafka_consumer import KafkaMarketDataConsumer
from market_state import MarketState
from models import NewsFlash, OrderRequest, Position, PriceTick
from news_sentiment import analyze_news
from order_manager import OrderManager, OrderRejectedError
from risk_manager import RiskManager
from stage_monitor import StageMonitor
from strategy import generate_signal

logger = logging.getLogger(__name__)


class APIClientProtocol(Protocol):
    def get_stage(self) -> Any: ...

    def post_heartbeat(self, seq: int) -> Any: ...

    def get_stocks(self) -> list[Any]: ...

    def get_stock_history(self, ticker: str, days: int = 90) -> list[PriceTick]: ...

    def get_portfolio(self) -> Any: ...

    def get_cash(self) -> Any: ...

    def post_order(self, order: OrderRequest) -> Any: ...

    def close(self) -> None: ...


class ConsumerProtocol(Protocol):
    def run(self, stop_event: Event | None = None) -> None: ...


@dataclass(frozen=True)
class AccountSnapshot:
    """Validated account data required for position monitoring and sizing."""

    cash: float
    available_margin: float
    total_wealth: float
    positions: dict[str, Position]


def _mapping_payload(payload: Any, resource: str) -> Mapping[str, Any]:
    """Unwrap common API response envelopes and require an object payload."""
    while isinstance(payload, Mapping):
        if resource == "portfolio" and isinstance(payload.get("portfolio"), Mapping):
            payload = payload["portfolio"]
        elif isinstance(payload.get("data"), Mapping):
            payload = payload["data"]
        else:
            return payload
    raise ValueError(f"Expected an object in {resource} API response.")


def _number(payload: Mapping[str, Any], names: tuple[str, ...], resource: str) -> float:
    for name in names:
        if name in payload:
            value = payload[name]
            if isinstance(value, bool):
                break
            try:
                number = float(value)
            except (TypeError, ValueError):
                break
            if math.isfinite(number):
                return number
            break
    raise ValueError(
        f"{resource} API response must contain a finite numeric "
        f"{' or '.join(names)} value."
    )


def _coerce_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{field} must be numeric.")
    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be numeric.") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite.")
    return number


def _positions(payload: Mapping[str, Any]) -> dict[str, Position]:
    raw_positions = payload.get("positions", payload.get("holdings"))
    if not isinstance(raw_positions, list):
        raise ValueError("Portfolio response must contain a positions or holdings list.")

    positions: dict[str, Position] = {}
    for raw in raw_positions:
        if not isinstance(raw, Mapping):
            raise ValueError("Portfolio positions must be JSON objects.")
        ticker = str(raw.get("ticker", "")).strip().upper()
        if not ticker:
            raise ValueError("Portfolio position is missing its ticker.")

        quantity_value = raw.get("quantity")
        quantity_number = _coerce_number(
            quantity_value,
            f"Portfolio quantity for {ticker}",
        )
        quantity = int(quantity_number)
        avg_price_value = raw.get(
            "avg_price",
            raw.get("average_price", raw.get("avg_cost")),
        )
        avg_price = _coerce_number(
            avg_price_value,
            f"Portfolio average price for {ticker}",
        )
        days_held_value = raw.get("days_held", 0)
        days_held_number = _coerce_number(
            days_held_value,
            f"Portfolio holding period for {ticker}",
        )
        days_held = int(days_held_number)
        if (
            isinstance(quantity_value, bool)
            or not math.isfinite(quantity_number)
            or quantity_number != quantity
            or quantity <= 0
        ):
            raise ValueError(f"Portfolio position for {ticker} must have positive quantity.")
        if (
            isinstance(days_held_value, bool)
            or not math.isfinite(days_held_number)
            or days_held_number != days_held
            or not math.isfinite(avg_price)
            or avg_price <= 0
            or days_held < 0
        ):
            raise ValueError(f"Portfolio position for {ticker} has invalid values.")

        side = str(raw.get("side", "")).strip().upper()
        is_short_value = raw.get("is_short", side in {"SHORT", "SELL"})
        if not isinstance(is_short_value, bool):
            raise ValueError(f"Portfolio position for {ticker} has invalid side.")
        is_short = is_short_value
        positions[ticker] = Position(
            ticker=ticker,
            quantity=quantity,
            avg_price=avg_price,
            is_short=is_short,
            entry_date=str(raw.get("entry_date", "")),
            days_held=days_held,
        )
    return positions


def load_account_snapshot(api_client: APIClientProtocol) -> AccountSnapshot:
    """Fetch and validate portfolio and cash responses; never invent risk inputs."""
    portfolio = _mapping_payload(api_client.get_portfolio(), "portfolio")
    cash_response = _mapping_payload(api_client.get_cash(), "cash")
    cash = _number(
        cash_response,
        ("available_cash", "cash", "cash_balance", "balance"),
        "Cash",
    )
    margin_names = ("available_margin", "margin_available", "buying_power")
    if any(name in cash_response for name in margin_names):
        margin = _number(cash_response, margin_names, "Cash")
    else:
        margin = cash
    wealth = _number(
        portfolio,
        ("total_wealth", "total_equity", "net_worth", "equity", "total_value"),
        "Portfolio",
    )
    if cash < 0 or margin < 0 or wealth < 0:
        raise ValueError("Cash, available margin, and total wealth cannot be negative.")
    return AccountSnapshot(
        cash=cash,
        available_margin=margin,
        total_wealth=wealth,
        positions=_positions(portfolio),
    )


class TradingBot:
    """Coordinate market events, strategy signals, risk checks, and order dispatch."""

    def __init__(
        self,
        api_client: APIClientProtocol | None = None,
        *,
        market_state: MarketState | None = None,
        risk_manager: RiskManager | None = None,
        stage_monitor: StageMonitor | None = None,
        order_manager: OrderManager[Any] | None = None,
        consumer: ConsumerProtocol | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.api_client = api_client if api_client is not None else APIClient()
        self.market_state = market_state if market_state is not None else MarketState()
        self.risk_manager = risk_manager if risk_manager is not None else RiskManager()
        self.stage_monitor = (
            stage_monitor if stage_monitor is not None else StageMonitor(self.api_client)
        )
        self.order_manager = (
            order_manager
            if order_manager is not None
            else OrderManager(
                self.api_client.post_order,
                refresh_cash_cache=self._refresh_account_snapshot,
            )
        )
        self.consumer = (
            consumer
            if consumer is not None
            else KafkaMarketDataConsumer(self.api_client, self)
        )
        self._monotonic = monotonic
        self._account: AccountSnapshot | None = None
        self._last_order_date: dict[str, str] = {}
        self._last_tick_date: dict[str, str] = {}

    def run(self) -> None:
        """Validate setup, warm market history, then consume until stopped."""
        errors = config.config.validate()
        if errors:
            raise RuntimeError("Invalid bot configuration: " + "; ".join(errors))

        try:
            status = self.stage_monitor.poll_once()
            if status.is_completed:
                logger.info("Simulation is already complete; nothing to run.")
                return

            stocks = self.api_client.get_stocks()
            if not stocks:
                raise RuntimeError("The gateway returned no tradable stocks.")
            tickers = [stock.ticker for stock in stocks]
            self.market_state.bootstrap_history(self.api_client, tickers)
            logger.info("Loaded history for %d stocks.", len(tickers))

            self.stage_monitor.start()
            logger.info(
                "Entering Kafka consume loop (stage=%s, status=%s). "
                "Waiting for market events...",
                status.stage,
                status.status,
            )
            self.consumer.run(stop_event=self.stage_monitor.completed_event)
        finally:
            self.stage_monitor.stop(timeout=5.0)
            self.api_client.close()

    def on_news(self, news: NewsFlash) -> None:
        """Cache a news event for use with its ticker's next price tick."""
        logger.debug("Received news for %s: %s", news.ticker, news.headline[:80])
        self.market_state.on_news(news)

    def on_tick(self, tick: PriceTick) -> None:
        """Update market state and place at most one risk-approved order per ticker/day."""
        received_at = self._monotonic()
        self.market_state.on_tick(tick)
        ticker = tick.ticker.strip().upper()
        date = tick.simulated_date.strip()
        logger.debug("Tick received: %s @ %.2f on %s", ticker, tick.close, date)
        self._advance_position_age(ticker, date)

        if not self.stage_monitor.dispatch_enabled_event.is_set():
            logger.debug(
                "Dispatch not enabled (stage not in running simulation); "
                "tick for %s on %s cached but no order attempted.",
                ticker,
                date,
            )
            return
        if self._last_order_date.get(ticker) == date:
            return

        try:
            account = self._account_snapshot()
        except Exception:
            logger.exception("Failed to load account snapshot; skipping tick for %s.", ticker)
            return

        position = account.positions.get(ticker)
        if position is not None:
            exit_action = self.risk_manager.monitor_position(position, tick.close)
            if exit_action is not None:
                self._submit(
                    ticker,
                    exit_action.value,
                    position.quantity,
                    date,
                    received_at,
                )
                return

        prices = self.market_state.get_closing_prices(ticker)
        news = self.market_state.get_latest_news(ticker, simulated_date=date)
        sentiment = (
            analyze_news(news.headline, news.description)
            if news is not None
            else {"raw_score": 0}
        )
        raw_news_score = int(sentiment["raw_score"])
        signal = generate_signal(
            prices,
            negative_news=raw_news_score < 0,
            news_score=float(raw_news_score),
        )
        if signal is None:
            return

        action, quantity = self._prepare_order(
            signal,
            ticker,
            tick.close,
            account,
        )
        if action is not None and quantity > 0:
            self._submit(ticker, action, quantity, date, received_at)

    def _account_snapshot(self) -> AccountSnapshot:
        if self._account is None:
            self._account = load_account_snapshot(self.api_client)
        return self._account

    def _refresh_account_snapshot(self) -> AccountSnapshot:
        self._account = load_account_snapshot(self.api_client)
        return self._account

    def _advance_position_age(self, ticker: str, date: str) -> None:
        previous_date = self._last_tick_date.get(ticker)
        self._last_tick_date[ticker] = date
        if previous_date is None or date <= previous_date or self._account is None:
            return
        position = self._account.positions.get(ticker)
        if position is not None:
            position.days_held += 1

    def _prepare_order(
        self,
        signal: str,
        ticker: str,
        price: float,
        account: AccountSnapshot,
    ) -> tuple[str | None, int]:
        position = account.positions.get(ticker)
        if signal == "BUY":
            if position is not None:
                return ("COVER", position.quantity) if position.is_short else (None, 0)
            action = "BUY"
            budget = min(
                account.cash - self.risk_manager.starting_capital
                * self.risk_manager.cash_buffer_pct,
                account.total_wealth * self.risk_manager.max_stock_weight,
            )
        elif signal == "SELL":
            return ("SELL", position.quantity) if position and not position.is_short else (None, 0)
        elif signal == "SHORT":
            if position is not None:
                return ("SELL", position.quantity) if not position.is_short else (None, 0)
            action = "SHORT"
            budget = min(
                account.available_margin,
                account.total_wealth * self.risk_manager.max_stock_weight,
            )
        else:
            logger.warning("Ignoring unsupported strategy signal %r.", signal)
            return None, 0

        quantity = int(budget // price)
        if quantity <= 0 or quantity * price < config.MIN_ORDER_VALUE:
            logger.info("Skipping %s %s: order is below the minimum trade value.", action, ticker)
            return None, 0

        if action == "BUY":
            check = self.risk_manager.check_buy(
                quantity,
                price,
                account.cash,
                account.total_wealth,
            )
        else:
            check = self.risk_manager.check_short(
                quantity,
                price,
                account.available_margin,
                account.total_wealth,
            )
        if not check.approved:
            logger.info("Skipping %s %s: %s", action, ticker, check.reason)
            return None, 0
        return action, quantity

    def _submit(
        self,
        ticker: str,
        action: str,
        quantity: int,
        date: str,
        received_at: float,
    ) -> None:
        order = OrderRequest(
            ticker=ticker,
            action=action,
            quantity=quantity,
            date=date,
        )
        try:
            self.order_manager.dispatch(order, tick_received_at=received_at)
        except OrderRejectedError as exc:
            self._last_order_date[ticker] = date
            logger.warning("Gateway rejected %s %s: %s", action, ticker, exc.error_code)
            return

        self._last_order_date[ticker] = date
        self._refresh_account_snapshot()


def main() -> int:
    """Run the trading bot and return a process exit code."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    bot = TradingBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    except Exception:
        logger.exception("Trading bot stopped because of an error.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
