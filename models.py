"""
Data models and schemas for MANTHAN 2026 Autonomous Trading Bot.
Technical Specification: Section 5.2 (models.py)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


# ===========================================================================
# Enums for Constrained State Values
# ===========================================================================
class OrderAction(str, Enum):
    """Supported order actions for exchange order gateway."""
    BUY = "BUY"
    SELL = "SELL"
    SHORT = "SHORT"
    COVER = "COVER"


class StageType(str, Enum):
    """Platform lifecycle stages."""
    DEVELOPMENT = "development"
    READY = "ready"
    PREFLIGHT = "preflight"
    SIMULATION = "simulation"


class StageRunStatus(str, Enum):
    """Execution status within a lifecycle stage."""
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"


# ===========================================================================
# 1. PriceTick (market.ticks)
# ===========================================================================
@dataclass
class PriceTick:
    """
    Daily OHLCV candlestick record received from Kafka 'market.ticks'
    or historical warm-start APIs.
    """
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    simulated_date: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PriceTick":
        """
        Parses a dictionary, supporting both direct flat records and Kafka event wrappers:
        e.g. {'event_type': 'price_tick', 'simulated_date': '...', 'data': {'ticker': ...}}
        or {'date': '...', 'ticker': '...', 'open': 100, ...}
        """
        if "data" in data and isinstance(data["data"], dict):
            inner = data["data"]
            date_val = str(data.get("simulated_date") or inner.get("date") or inner.get("simulated_date", ""))
            return cls(
                ticker=str(inner.get("ticker", "")).strip().upper(),
                open=float(inner.get("open", 0.0)),
                high=float(inner.get("high", 0.0)),
                low=float(inner.get("low", 0.0)),
                close=float(inner.get("close", 0.0)),
                volume=int(inner.get("volume", 0)),
                simulated_date=date_val,
            )

        # Flat structure
        date_val = str(data.get("simulated_date") or data.get("date", ""))
        return cls(
            ticker=str(data.get("ticker", "")).strip().upper(),
            open=float(data.get("open", 0.0)),
            high=float(data.get("high", 0.0)),
            low=float(data.get("low", 0.0)),
            close=float(data.get("close", 0.0)),
            volume=int(data.get("volume", 0)),
            simulated_date=date_val,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serializes tick to dictionary."""
        return asdict(self)


# ===========================================================================
# 2. NewsFlash (market.news)
# ===========================================================================
@dataclass
class NewsFlash:
    """
    Corporate news announcement received from Kafka 'market.news' at T+150ms.
    """
    ticker: str
    headline: str
    description: str
    date: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NewsFlash":
        """
        Parses a news record from Kafka event envelopes or flat dictionaries:
        e.g. {'event_type': 'news_flash', 'simulated_date': '...', 'data': {'ticker': ...}}
        """
        if "data" in data and isinstance(data["data"], dict):
            inner = data["data"]
            date_val = str(inner.get("date") or data.get("simulated_date", ""))
            return cls(
                ticker=str(inner.get("ticker", "")).strip().upper(),
                headline=str(inner.get("headline", "")).strip(),
                description=str(inner.get("description", "")).strip(),
                date=date_val,
            )

        return cls(
            ticker=str(data.get("ticker", "")).strip().upper(),
            headline=str(data.get("headline", "")).strip(),
            description=str(data.get("description", "")).strip(),
            date=str(data.get("date") or data.get("simulated_date", "")),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serializes news flash to dictionary."""
        return asdict(self)


# ===========================================================================
# 3. OrderRequest (POST /api/v1/orders)
# ===========================================================================
@dataclass
class OrderRequest:
    """
    Order payload for submitting market orders to http://<SERVER_IP>:3002/api/v1/orders.
    Supported actions: BUY, SELL, SHORT, COVER.
    """
    ticker: str
    action: str  # BUY | SELL | SHORT | COVER
    quantity: int
    date: Optional[str] = None  # Current simulated date for latency scoring

    def __post_init__(self) -> None:
        # Normalize and validate action
        normalized_action = self.action.upper().strip()
        valid_actions = {a.value for a in OrderAction}
        if normalized_action not in valid_actions:
            raise ValueError(
                f"Invalid order action '{self.action}'. Must be one of: {valid_actions}"
            )
        self.action = normalized_action

        # Clean ticker
        self.ticker = self.ticker.strip().upper()

        # Validate quantity
        if self.quantity <= 0:
            raise ValueError(f"Quantity must be a positive integer, got {self.quantity}")
        if self.quantity > 1_000_000:
            raise ValueError(f"Quantity exceeds max permitted size (1,000,000), got {self.quantity}")

    def to_dict(self) -> Dict[str, Any]:
        """Serializes payload for JSON request body."""
        payload: Dict[str, Any] = {
            "ticker": self.ticker,
            "action": self.action,
            "quantity": int(self.quantity),
        }
        if self.date:
            payload["date"] = self.date
        return payload

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderRequest":
        return cls(
            ticker=str(data.get("ticker", "")),
            action=str(data.get("action", "")),
            quantity=int(data.get("quantity", 0)),
            date=data.get("date"),
        )


# ===========================================================================
# 4. Position (Local Portfolio & Holding Tracking)
# ===========================================================================
@dataclass
class Position:
    """
    Tracks an open long or short position held in the portfolio.
    """
    ticker: str
    quantity: int
    avg_price: float
    is_short: bool = False
    entry_date: str = ""
    days_held: int = 0

    def __post_init__(self) -> None:
        self.ticker = self.ticker.strip().upper()

    @property
    def cost_basis(self) -> float:
        """Total capital committed to this position."""
        return self.quantity * self.avg_price

    def market_value(self, current_price: float) -> float:
        """Current market valuation of the position."""
        return self.quantity * current_price

    def unrealized_pnl(self, current_price: float) -> float:
        """
        Unrealized profit or loss:
        - Long:  (current_price - avg_price) * quantity
        - Short: (avg_price - current_price) * quantity
        """
        if self.is_short:
            return (self.avg_price - current_price) * self.quantity
        return (current_price - self.avg_price) * self.quantity

    def unrealized_pnl_pct(self, current_price: float) -> float:
        """Percentage return on position."""
        if self.avg_price <= 0:
            return 0.0
        if self.is_short:
            return (self.avg_price - current_price) / self.avg_price
        return (current_price - self.avg_price) / self.avg_price

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Position":
        return cls(
            ticker=str(data.get("ticker", "")),
            quantity=int(data.get("quantity", 0)),
            avg_price=float(data.get("avg_price", 0.0)),
            is_short=bool(data.get("is_short", False)),
            entry_date=str(data.get("entry_date", "")),
            days_held=int(data.get("days_held", 0)),
        )


# ===========================================================================
# 5. StageStatus (GET /api/v1/stage)
# ===========================================================================
@dataclass
class StageStatus:
    """
    Represents the platform lifecycle stage response from GET /api/v1/stage.
    Stage: development | ready | preflight | simulation
    Status: IDLE | RUNNING | PAUSED | COMPLETED
    """
    stage: str    # development | ready | preflight | simulation
    status: str   # IDLE | RUNNING | PAUSED | COMPLETED
    has_run: bool = False
    simulation_id: Optional[str] = None
    interval_seconds: int = 4
    total_days: int = 0
    days_completed: int = 0
    current_date: Optional[str] = None

    def __post_init__(self) -> None:
        self.stage = self.stage.strip().lower()
        self.status = self.status.strip().upper()

    @property
    def is_simulation_running(self) -> bool:
        """Returns True if the live simulation is currently RUNNING."""
        return self.stage == StageType.SIMULATION.value and self.status == StageRunStatus.RUNNING.value

    @property
    def is_preflight(self) -> bool:
        """Returns True if the platform is in preflight heartbeat readiness stage."""
        return self.stage == StageType.PREFLIGHT.value

    @property
    def is_completed(self) -> bool:
        """Returns True if the simulation has completed."""
        return self.status == StageRunStatus.COMPLETED.value

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StageStatus":
        return cls(
            stage=str(data.get("stage", "development")),
            status=str(data.get("status", "IDLE")),
            has_run=bool(data.get("has_run", False)),
            simulation_id=data.get("simulation_id"),
            interval_seconds=int(data.get("interval_seconds", 4)),
            total_days=int(data.get("total_days", 0)),
            days_completed=int(data.get("days_completed", 0)),
            current_date=data.get("current_date"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ===========================================================================
# 6. Supplementary Competition Models
# ===========================================================================
@dataclass
class PreflightTick:
    """Standby ping broadcast on Kafka 'market.ticks' during preflight."""
    seq: int
    status: str = "STANDBY"
    timestamp: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PreflightTick":
        seq_val = data.get("seq")
        if seq_val is None and "data" in data and isinstance(data["data"], dict):
            seq_val = data["data"].get("seq", 0)
        status_val = "STANDBY"
        if "data" in data and isinstance(data["data"], dict):
            status_val = data["data"].get("status", "STANDBY")
        return cls(
            seq=int(seq_val or 0),
            status=str(status_val),
            timestamp=str(data.get("timestamp", "")),
        )


@dataclass
class StockInfo:
    """Stock metadata returned from GET /api/v1/stocks."""
    ticker: str
    symbol: str
    name: str
    description: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StockInfo":
        ticker = str(data.get("ticker") or data.get("symbol") or "").strip().upper()
        symbol = str(data.get("symbol") or data.get("ticker") or "").strip().upper()
        if not ticker:
            raise ValueError("Stock record is missing a ticker.")
        return cls(
            ticker=ticker,
            symbol=symbol,
            name=str(data.get("name", "")).strip(),
            description=str(data.get("description", "")).strip(),
        )
