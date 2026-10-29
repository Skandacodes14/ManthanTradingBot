"""Pre-trade risk checks and position exit monitoring."""

from dataclasses import dataclass
import math

from config import (
    INITIAL_CAPITAL,
    MAX_PORTFOLIO_STOCK_WEIGHT,
    MAX_SHORT_DAYS,
    MIN_CASH_BUFFER_PCT,
    SHORT_MARGIN_PCT,
    STOP_LOSS_PCT,
    TAKE_PROFIT_PCT,
)
from models import OrderAction, Position


@dataclass(frozen=True)
class RiskCheck:
    """Outcome of a pre-trade risk check."""

    approved: bool
    reason: str


class RiskManager:
    """Enforces portfolio limits before orders and monitors exit conditions."""

    def __init__(
        self,
        starting_capital: float = INITIAL_CAPITAL,
        cash_buffer_pct: float = MIN_CASH_BUFFER_PCT,
        max_stock_weight: float = MAX_PORTFOLIO_STOCK_WEIGHT,
        short_margin_pct: float = SHORT_MARGIN_PCT,
        stop_loss_pct: float = STOP_LOSS_PCT,
        take_profit_pct: float = TAKE_PROFIT_PCT,
        max_short_days: int = MAX_SHORT_DAYS,
    ) -> None:
        values = (
            starting_capital,
            cash_buffer_pct,
            max_stock_weight,
            short_margin_pct,
            stop_loss_pct,
            take_profit_pct,
        )
        if any(not math.isfinite(value) for value in values):
            raise ValueError("Risk limits and starting capital must be finite.")
        if starting_capital <= 0:
            raise ValueError("Starting capital must be positive.")
        if not 0 <= cash_buffer_pct <= 1:
            raise ValueError("Cash buffer percentage must be between 0 and 1.")
        if not 0 < max_stock_weight <= 1:
            raise ValueError("Maximum stock weight must be greater than 0 and at most 1.")
        if short_margin_pct < 0:
            raise ValueError("Short margin percentage cannot be negative.")
        if not 0 <= stop_loss_pct < 1:
            raise ValueError("Stop-loss percentage must be between 0 and 1.")
        if take_profit_pct < 0:
            raise ValueError("Take-profit percentage cannot be negative.")
        if max_short_days <= 0:
            raise ValueError("Maximum short holding period must be positive.")

        self.starting_capital = starting_capital
        self.cash_buffer_pct = cash_buffer_pct
        self.max_stock_weight = max_stock_weight
        self.short_margin_pct = short_margin_pct
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.max_short_days = max_short_days

    @staticmethod
    def _validate_trade(
        quantity: int,
        price: float,
        total_wealth: float,
        current_position_value: float,
    ) -> None:
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
            raise ValueError("Quantity must be a positive integer.")
        values = (price, total_wealth, current_position_value)
        if any(not math.isfinite(value) for value in values):
            raise ValueError("Price and portfolio values must be finite.")
        if price <= 0:
            raise ValueError("Price must be positive.")
        if total_wealth < 0 or current_position_value < 0:
            raise ValueError("Portfolio values cannot be negative.")

    def check_buy(
        self,
        quantity: int,
        price: float,
        cash_remaining: float,
        total_wealth: float,
        current_position_value: float = 0.0,
    ) -> RiskCheck:
        """Check reserve and per-stock allocation before a BUY order."""
        self._validate_trade(quantity, price, total_wealth, current_position_value)
        if not math.isfinite(cash_remaining) or cash_remaining < 0:
            raise ValueError("Cash remaining must be finite and non-negative.")

        order_value = quantity * price
        cash_after_order = cash_remaining - order_value
        cash_reserve = self.starting_capital * self.cash_buffer_pct
        if cash_after_order < cash_reserve:
            return RiskCheck(
                False,
                f"Cash reserve violation: post-trade cash {cash_after_order:.2f} "
                f"is below required reserve {cash_reserve:.2f}.",
            )

        position_limit = total_wealth * self.max_stock_weight
        position_after_order = current_position_value + order_value
        if position_after_order > position_limit:
            return RiskCheck(
                False,
                f"Position limit violation: allocation {position_after_order:.2f} "
                f"exceeds per-stock limit {position_limit:.2f}.",
            )
        return RiskCheck(True, "BUY approved.")

    def check_short(
        self,
        quantity: int,
        price: float,
        margin_available: float,
        total_wealth: float,
        current_position_value: float = 0.0,
    ) -> RiskCheck:
        """Check 100% margin availability and per-stock allocation before SHORT."""
        self._validate_trade(quantity, price, total_wealth, current_position_value)
        if not math.isfinite(margin_available) or margin_available < 0:
            raise ValueError("Available margin must be finite and non-negative.")

        order_value = quantity * price
        required_margin = order_value * self.short_margin_pct
        if margin_available < required_margin:
            return RiskCheck(
                False,
                f"Insufficient margin: {margin_available:.2f} available, "
                f"{required_margin:.2f} required.",
            )

        position_limit = total_wealth * self.max_stock_weight
        position_after_order = current_position_value + order_value
        if position_after_order > position_limit:
            return RiskCheck(
                False,
                f"Position limit violation: allocation {position_after_order:.2f} "
                f"exceeds per-stock limit {position_limit:.2f}.",
            )
        return RiskCheck(True, "SHORT approved.")

    def monitor_position(
        self,
        position: Position,
        current_price: float,
    ) -> OrderAction | None:
        """Return the exit action required by stop-loss, take-profit, or short age."""
        if not math.isfinite(current_price) or current_price <= 0:
            raise ValueError("Current price must be finite and positive.")
        if position.quantity <= 0:
            raise ValueError("Position quantity must be positive.")
        if not math.isfinite(position.avg_price) or position.avg_price <= 0:
            raise ValueError("Position average price must be finite and positive.")

        if position.is_short:
            if position.days_held >= self.max_short_days:
                return OrderAction.COVER
            return None

        return_pct = position.unrealized_pnl_pct(current_price)
        if return_pct < -self.stop_loss_pct or return_pct > self.take_profit_pct:
            return OrderAction.SELL
        return None