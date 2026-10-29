"""
Central configuration module for MANTHAN 2026 Autonomous Trading Bot.
Technical Specification: Section 5.1 (config.py)
Configured for Team ZentroGen (MN2663).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(filepath: Path | str = ".env") -> None:
    """
    Loads environment variables from a .env file.
    Prefers python-dotenv if installed, otherwise uses a zero-dependency fallback.
    """
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(filepath)
        return
    except ImportError:
        pass

    env_path = Path(filepath)
    if not env_path.is_file():
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" in stripped:
                key, val = stripped.split("=", 1)
                key = key.strip()
                val = val.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = val


# Automatically load .env located in the same directory as this file
_BASE_DIR = Path(__file__).resolve().parent
_load_env_file(_BASE_DIR / ".env")


# ===========================================================================
# 1. Environment Variables (Section 5.1 & Competition Coordinates)
# ===========================================================================
TEAM_NAME: str = os.getenv("TEAM_NAME", "ZentroGen")
TEAM_USERNAME: str = os.getenv("TEAM_USERNAME", "MN2663")
SERVER_IP: str = os.getenv("SERVER_IP", "192.168.65.157")

# Accept either BOT_ACCESS_TOKEN or ACCESS_TOKEN
BOT_ACCESS_TOKEN: str = os.getenv("BOT_ACCESS_TOKEN") or os.getenv("ACCESS_TOKEN", "")
ACCESS_TOKEN: str = BOT_ACCESS_TOKEN  # Alias for backward/spec compatibility

GATEWAY_PORT: int = int(os.getenv("GATEWAY_PORT", "3002"))
KAFKA_PORT: int = int(os.getenv("KAFKA_PORT", "9094"))
PORTAL_PORT: int = int(os.getenv("PORTAL_PORT", "3003"))


# ===========================================================================
# 2. Derived URLs & Stream Coordinates (Section 5.1)
# ===========================================================================
BASE_URL: str = f"http://{SERVER_IP}:{GATEWAY_PORT}/api/v1"
KAFKA_BROKER: str = f"{SERVER_IP}:{KAFKA_PORT}"
PORTAL_URL: str = f"http://{SERVER_IP}:{PORTAL_PORT}"

# Kafka Topics & Consumer Group
KAFKA_TOPIC_TICKS: str = "market.ticks"
KAFKA_TOPIC_NEWS: str = "market.news"
KAFKA_CONSUMER_GROUP: str = f"group-{TEAM_USERNAME}"


# ===========================================================================
# 3. HTTP Header Helper
# ===========================================================================
def get_auth_headers(token: str | None = None) -> dict[str, str]:
    """
    Returns standard HTTP headers for authenticated API requests:
    Authorization: Bearer <BOT_ACCESS_TOKEN>
    Content-Type: application/json
    """
    tok = token if token is not None else BOT_ACCESS_TOKEN
    return {
        "Authorization": f"Bearer {tok}",
        "Content-Type": "application/json",
    }


# ===========================================================================
# 4. Operational Constants & Risk Guardrails (Section 5.1)
# ===========================================================================
MAX_REQUESTS_PER_MIN: int = 100          # Server limit is 120 req/min; capped at 100 for safety
MIN_CASH_BUFFER_PCT: float = 0.20        # Retain at least 20% starting capital in reserve
MAX_PORTFOLIO_STOCK_WEIGHT: float = 0.25 # Max 25% portfolio weight per individual stock
STOP_LOSS_PCT: float = 0.035             # 3.5% stop loss from position entry price
TAKE_PROFIT_PCT: float = 0.06            # 6.0% take profit target
MAX_SHORT_DAYS: int = 7                  # Cover by Day 7 before 10-day server auto-square-off


# ===========================================================================
# 5. Competition Engine & Financial Mechanics Constants
# ===========================================================================
INITIAL_CAPITAL: float = 1_000_000.00    # Virtual starting capital: ₹10,00,000.00
MAX_REQUESTS_PER_SEC: float = 1.5        # Paced rate limiter dispatch rate (90-100 req/min)
PENALTY_BOX_SECONDS: float = 30.0        # Server 429 penalty timeout duration
SHORT_MARGIN_PCT: float = 1.0            # 100% margin locked when entering short

# Market Frictions
BROKERAGE_PER_ORDER: float = 20.00       # Flat ₹20 per executed order
DP_CHARGE_PER_SELL: float = 15.34        # Flat ₹15.34 DP charge on delivery SELL
MIN_ORDER_VALUE: float = 15_000.00       # Batch sizing threshold to keep friction < 0.25%

# History Warm-up & Indicator Buffers
HISTORY_WARMUP_DAYS: int = 90            # Days of history fetched via /stocks/:ticker/history
SLIDING_WINDOW_MAXLEN: int = 150         # Deque buffer size for price ticks
PREFLIGHT_HEARTBEAT_TIMEOUT_SEC: int = 15


# ===========================================================================
# 6. Dataclass Config Wrapper (Optional OOP Access & Validation)
# ===========================================================================
@dataclass(frozen=True)
class Config:
    """Read-only container holding all bot configurations and risk boundaries."""
    team_name: str = TEAM_NAME
    team_username: str = TEAM_USERNAME
    server_ip: str = SERVER_IP
    bot_access_token: str = BOT_ACCESS_TOKEN
    access_token: str = ACCESS_TOKEN
    gateway_port: int = GATEWAY_PORT
    kafka_port: int = KAFKA_PORT
    portal_port: int = PORTAL_PORT

    base_url: str = BASE_URL
    kafka_broker: str = KAFKA_BROKER
    portal_url: str = PORTAL_URL

    kafka_topic_ticks: str = KAFKA_TOPIC_TICKS
    kafka_topic_news: str = KAFKA_TOPIC_NEWS
    kafka_consumer_group: str = KAFKA_CONSUMER_GROUP

    max_requests_per_min: int = MAX_REQUESTS_PER_MIN
    min_cash_buffer_pct: float = MIN_CASH_BUFFER_PCT
    max_portfolio_stock_weight: float = MAX_PORTFOLIO_STOCK_WEIGHT
    stop_loss_pct: float = STOP_LOSS_PCT
    take_profit_pct: float = TAKE_PROFIT_PCT
    max_short_days: int = MAX_SHORT_DAYS

    initial_capital: float = INITIAL_CAPITAL
    max_requests_per_sec: float = MAX_REQUESTS_PER_SEC
    penalty_box_seconds: float = PENALTY_BOX_SECONDS
    short_margin_pct: float = SHORT_MARGIN_PCT
    brokerage_per_order: float = BROKERAGE_PER_ORDER
    dp_charge_per_sell: float = DP_CHARGE_PER_SELL
    min_order_value: float = MIN_ORDER_VALUE
    history_warmup_days: int = HISTORY_WARMUP_DAYS
    sliding_window_maxlen: int = SLIDING_WINDOW_MAXLEN

    def get_headers(self) -> dict[str, str]:
        return get_auth_headers(self.bot_access_token)

    def validate(self) -> list[str]:
        """
        Validates configuration sanity before bot initialization.
        Returns a list of error or warning strings.
        """
        errors: list[str] = []
        if not self.server_ip or self.server_ip.strip() == "":
            errors.append("SERVER_IP is required.")
        if not self.bot_access_token or self.bot_access_token.strip() == "":
            errors.append("BOT_ACCESS_TOKEN is not set. Add token to .env before trading.")
        if not self.team_username or self.team_username.strip() == "":
            errors.append("TEAM_USERNAME is required for Kafka consumer group identification.")
        if not (0.0 < self.min_cash_buffer_pct < 1.0):
            errors.append("MIN_CASH_BUFFER_PCT must be between 0.0 and 1.0.")
        if not (0.0 < self.max_portfolio_stock_weight <= 1.0):
            errors.append("MAX_PORTFOLIO_STOCK_WEIGHT must be between 0.0 and 1.0.")
        if self.max_short_days >= 10:
            errors.append("MAX_SHORT_DAYS must be strictly less than 10 to avoid server auto-settlement.")
        if self.max_requests_per_min > 120:
            errors.append("MAX_REQUESTS_PER_MIN cannot exceed server limit of 120.")
        return errors


# Default singleton instance
config = Config()
