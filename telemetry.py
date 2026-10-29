"""Structured order and trade execution telemetry."""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import TextIO
from threading import Lock

from models import OrderRequest


class TradeTelemetryFormatter(logging.Formatter):
    """Render order audit events as concise, presentation-friendly terminal lines."""

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "trade_telemetry", None)
        if not isinstance(event, Mapping):
            return super().format(record)

        parts = [
            f"[{event['timestamp']}]",
            f"[{event['status'].upper()}]",
            f"{event['action']} {event['quantity']} {event['ticker']}",
        ]
        if event.get("date"):
            parts.append(f"date={event['date']}")
        latency = event.get("decision_latency_seconds")
        if latency is not None:
            parts.append(f"decision_latency={latency:.3f}s")
        for field, label in (
            ("fill_price", "fill"),
            ("brokerage_charged", "brokerage"),
            ("tax_deducted", "tax"),
        ):
            value = event.get(field)
            if value is not None:
                parts.append(f"{label}={value}")
        if event.get("error_code"):
            parts.append(f"error={event['error_code']}")
        return " | ".join(parts)


def configure_terminal_logging(
    logger: logging.Logger | None = None,
    *,
    stream: TextIO = sys.stderr,
) -> logging.Logger:
    """Attach the clean trade formatter to a logger, without duplicate handlers."""
    target = logger or logging.getLogger("trading.telemetry")
    if not any(getattr(handler, "_trade_telemetry_handler", False) for handler in target.handlers):
        handler = logging.StreamHandler(stream)
        handler.setFormatter(TradeTelemetryFormatter())
        setattr(handler, "_trade_telemetry_handler", True)
        target.addHandler(handler)
    target.setLevel(min(target.level or logging.INFO, logging.INFO))
    target.propagate = False
    return target


@dataclass(frozen=True)
class TradeTelemetryEvent:
    """Audit record for a submitted, rejected, or failed order."""

    timestamp: str
    status: str
    ticker: str
    action: str
    quantity: int
    date: str | None
    decision_latency_seconds: float | None
    fill_price: object = None
    brokerage_charged: object = None
    tax_deducted: object = None
    error_code: str | None = None


class TradeTelemetry:
    """Collect and emit structured order audit records."""

    def __init__(
        self,
        logger: logging.Logger | None = None,
        *,
        wall_clock: Callable[[], float] = time.time,
        stream: TextIO = sys.stderr,
    ) -> None:
        self._logger = configure_terminal_logging(logger, stream=stream)
        self._wall_clock = wall_clock
        self._events: list[TradeTelemetryEvent] = []
        self._lock = Lock()

    @property
    def events(self) -> tuple[TradeTelemetryEvent, ...]:
        with self._lock:
            return tuple(self._events)

    def record_order(
        self,
        order: OrderRequest,
        *,
        status: str,
        tick_received_at: float | None = None,
        sent_at: float | None = None,
        error_code: str | None = None,
        response: object = None,
    ) -> TradeTelemetryEvent:
        """Record decision latency and available execution-cost details."""
        latency = None
        if tick_received_at is not None and sent_at is not None:
            latency = max(0.0, sent_at - tick_received_at)

        details = self._trade_details(response)
        event = TradeTelemetryEvent(
            timestamp=datetime.fromtimestamp(
                self._wall_clock(), tz=timezone.utc
            ).isoformat(),
            status=status,
            ticker=order.ticker,
            action=order.action,
            quantity=order.quantity,
            date=order.date,
            decision_latency_seconds=latency,
            fill_price=details.get("fill_price"),
            brokerage_charged=details.get("brokerage_charged"),
            tax_deducted=details.get("tax_deducted"),
            error_code=error_code,
        )
        with self._lock:
            self._events.append(event)

        self._logger.info(
            "trade_order",
            extra={"trade_telemetry": asdict(event)},
        )
        return event

    @staticmethod
    def _trade_details(response: object) -> Mapping[str, object]:
        if not isinstance(response, Mapping):
            return {}
        data = response.get("data")
        if isinstance(data, Mapping):
            return data
        return response
