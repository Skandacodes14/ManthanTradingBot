# MANTHAN 2026: Autonomous Trading Bot Implementation Plan

## 1. Executive Summary & Competition Specs

This document defines the complete technical architecture and step-by-step implementation blueprint for the **Autonomous Trading Bot** competing in **MANTHAN 2026 (24-Hour National Hackathon)**.

### Key Competition Constraints & Specifications

| Parameter | Specification | Notes |
|---|---|---|
| **Simulation Speed** | 1 simulated trading day = 3 to 4 seconds real time | Decision latency window: ~2–3 seconds per day |
| **Initial Capital** | ₹10,00,000.00 (Ten Lakhs INR) | Tracked on server |
| **Market Data Ingestion** | Apache Kafka at `<SERVER_IP>:9094` (PLAINTEXT) | Topics: `market.ticks` (T+0s), `market.news` (T+150ms) |
| **Order Gateway** | REST API at `http://<SERVER_IP>:3002/api/v1` | Auth: `Authorization: Bearer <TOKEN>` |
| **Participant Portal** | `http://<SERVER_IP>:3003` | Live dashboard, interactive API docs, token retrieval |
| **Supported Order Actions**| `BUY`, `SELL`, `SHORT`, `COVER` | All executed at current day's **Close Price** |
| **API Rate Limit** | Max 120 requests/minute | HTTP 429 triggers 30-second penalty box |
| **Short Selling Constraints** | 100% margin locked; 10-day auto-square-off | Short positions automatically settled on alternate Fridays |
| **Market Frictions** | Flat ₹20/order brokerage; Flat ₹15.34 DP charge on SELL | Avoid high-frequency micro-trading |
| **Taxes (Income-tax Act)**| STCG: 15% (<=365 days); LTCG: 10% (>365 days) | First ₹1,00,000 LTCG is exempt; loss offsets supported |
| **Evaluation Criteria** | Balanced Scorecard (Wealth, Latency, Error Rate, Tax Efficiency) + Jury Defense | Not purely highest gross profit |

---

## 2. System Architecture & Environment

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                               COMPETITION SERVER                                 │
│                                                                                  │
│  [Port 9094 - Kafka Broker]                [Port 3002 - Order Gateway Engine]    │
│  - Streams 'market.ticks' (OHLCV at T+0)    - /api/v1/orders (BUY/SELL/SHORT/COV)│
│  - Streams 'market.news'  (News at T+150ms) - /api/v1/portfolio & /portfolio/cash │
│  - Broadcasts 'preflight_tick'              - /api/v1/stocks & /stocks/:id/hist   │
│                                            - 10-day Auto-settlement for SHORTs   │
│                                            - Deducts Brokerage, DP & Tax         │
│  [Port 3003 - Participant Portal & Docs]                                         │
│  - Live P&L, Total Wealth, Token Retrieval, Reset Cash Button                    │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │ Local Venue Wi-Fi Subnet
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             LOCAL TRADING BOT                                    │
│                                                                                  │
│  Kafka Consumer (group-<team>) ──► In-Memory Market State ──► Strategy Engine    │
│                                            ▲                        │            │
│                                            │ (Warm-start history)   ▼            │
│  Stage & Preflight Heartbeat ──────────────┴───────────────► Risk Manager        │
│                                                                     │            │
│  Telemetry & Audit Trail ◄──────────────────────────────── Order Manager         │
│                                                                     │            │
│                                                                     ▼ POST /orders
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Repository Structure & Module Breakdown

```
manthan-trading-bot/
├── .env.example              # Template for server IP, token, credentials
├── config.py                 # Central configurations & parameters
├── models.py                 # Dataclasses/Pydantic schemas for data integrity
├── api_client.py             # HTTP REST wrapper with auth headers & retry handling
├── stage_monitor.py          # Lifecycle state poller & pre-flight heartbeat responder
├── kafka_consumer.py         # Multi-topic Kafka consumer (market.ticks, market.news)
├── market_state.py           # In-memory sliding window, history warm-start & reset logic
├── indicators.py             # Technical indicators (SMA, EMA, RSI, Bollinger Bands)
├── news_sentiment.py         # Keyword matching & sentiment scoring (<5ms)
├── strategy.py               # Alpha signal generator (Trend Following, Dip Buy, News)
├── risk_manager.py           # Position sizing, stop-loss, margin check, capital buffer
├── order_manager.py          # Rate-limited order dispatcher with execution tracking
├── telemetry.py              # Latency tracker, structured logs, jury defense audit
├── main.py                   # Master orchestration loop
├── requirements.txt          # Python dependencies
└── tests/
    ├── test_indicators.py    # Math & indicator correctness tests
    ├── test_strategy.py      # Strategy signal unit tests
    ├── test_risk_manager.py  # Margin, sizing & stop-loss tests
    └── test_orders.py        # Mocked API client & order tests
```

---

## 4. Gap Mitigations & Critical Design Decisions

| Risk / Gap | Root Cause | Engineering Solution |
|---|---|---|
| **Sandbox Loop Discontinuity** | During sandbox practice (Hours 0.5–20), prices loop back to Day 1 when dataset ends. | In `market_state.py`, compare incoming `simulated_date < last_date`. If date resets, wipe price buffers and trigger warm-start. |
| **Pre-Flight Disqualification** | In pre-flight, server broadcasts `preflight_tick` requiring `POST /api/v1/bot/heartbeat` within 15 seconds. | Immediate event handling in `kafka_consumer.py` / `stage_monitor.py` to continuously ACK sequence numbers. |
| **Rate Limit Violations (HTTP 429)**| Server enforces max 120 req/min with a 30s penalty box. | `order_manager.py` implements a token-bucket rate limiter capped at 1.5 req/sec (90 req/min). |
| **Cold-Start Indicator Blind Spot** | 20-day SMA or 14-day RSI requires historical candles before producing signals. | On boot, fetch 90-day history via `GET /api/v1/stocks/:ticker/history?days=90` for all discovered stocks. |
| **Excess Friction Drag** | ₹20 flat brokerage per order + ₹15.34 DP charge on SELL degrades returns on micro-orders. | Minimum trade batch size (e.g. ₹15,000–₹25,000 order value) to keep friction under 0.25% per trade. |
| **Short Squeeze / 10-Day Auto-Close**| Short positions auto-liquidate after 10 trading days; margin lock equals 100%. | Short positions are actively tracked by holding duration and covered within 6–7 trading days. |

---

## 5. Module-by-Module Technical Specification

### 5.1 `config.py`
* **Responsibilities**:
  * Loads `.env` variables: `SERVER_IP`, `ACCESS_TOKEN`, `TEAM_USERNAME`.
  * Computes derived URLs: `BASE_URL = http://{SERVER_IP}:3002/api/v1`, `KAFKA_BROKER = {SERVER_IP}:9094`.
  * Defines operational constants:
    * `MAX_REQUESTS_PER_MIN = 100`
    * `MIN_CASH_BUFFER_PCT = 0.20`
    * `MAX_PORTFOLIO_STOCK_WEIGHT = 0.25`
    * `STOP_LOSS_PCT = 0.035` (3.5% stop loss)
    * `TAKE_PROFIT_PCT = 0.06` (6% take profit)
    * `MAX_SHORT_DAYS = 7` (Cover before 10-day auto-square-off)

### 5.2 `models.py`
* **Responsibilities**:
  * Dataclasses for clear serialization:
    * `PriceTick`: `ticker`, `open`, `high`, `low`, `close`, `volume`, `simulated_date`.
    * `NewsFlash`: `ticker`, `headline`, `description`, `date`.
    * `OrderRequest`: `ticker`, `action` (`BUY`|`SELL`|`SHORT`|`COVER`), `quantity`, `date`.
    * `Position`: `ticker`, `quantity`, `avg_price`, `is_short`, `entry_date`, `days_held`.
    * `StageStatus`: `stage` (`development`|`ready`|`preflight`|`simulation`), `status` (`IDLE`|`RUNNING`|`PAUSED`|`COMPLETED`).

### 5.3 `api_client.py`
* **Responsibilities**:
  * Persistent `requests.Session` with `Authorization: Bearer <TOKEN>`.
  * Implements all server endpoints:
    * `get_stage()` -> `GET /api/v1/stage` (Public)
    * `post_heartbeat(seq: int)` -> `POST /api/v1/bot/heartbeat`
    * `get_stocks()` -> `GET /api/v1/stocks`
    * `get_stock_history(ticker: str, days: int = 90)` -> `GET /api/v1/stocks/{ticker}/history?days={days}`
    * `get_portfolio()` -> `GET /api/v1/portfolio`
    * `get_cash()` -> `GET /api/v1/portfolio/cash`
    * `post_order(order: OrderRequest)` -> `POST /api/v1/orders`

### 5.4 `market_state.py`
* **Responsibilities**:
  * In-memory cache for all discovered stocks:
    * `price_history: Dict[str, deque[PriceTick]]` (maxlen = 150).
    * `portfolio_cache`: Local copy of holdings, cash, and locked margin.
  * `bootstrap_history(api_client, tickers)`: Pre-populates historical candles at startup.
  * `on_tick(tick: PriceTick)`: Appends tick; detects date rewind for sandbox resets.
  * `on_fill(ticker, action, qty, price, date)`: Updates local position tracking.

### 5.5 `indicators.py`
* **Responsibilities**:
  * Fast vectorized mathematical indicators:
    * `calculate_sma(prices: list[float], window: int) -> float`
    * `calculate_ema(prices: list[float], window: int) -> float`
    * `calculate_rsi(prices: list[float], period: int = 14) -> float`
    * `calculate_bollinger(prices: list[float], window: int = 20, num_std: float = 2.0) -> tuple[float, float, float]`
    * `calculate_momentum(prices: list[float], days: int = 3) -> float`

### 5.6 `news_sentiment.py`
* **Responsibilities**:
  * Low-latency keyword matching dictionary:
    * Bullish: `PROFIT`, `SURGE`, `GROWTH`, `RECORD`, `BEAT`, `EXPANSION`, `DIVIDEND`, `CONTRACT`, `PARTNERSHIP`.
    * Bearish: `LOSS`, `PLUNGE`, `DROP`, `FRAUD`, `SCRUTINY`, `RESIGNATION`, `PROBE`, `DOWNGRADE`, `DEFAULT`.
  * `score_headline(text: str) -> float`: Returns score in `[-1.0, 1.0]`.

### 5.7 `strategy.py`
* **Responsibilities**:
  * Combines three complementary quantitative signals:
    1. **Trend-Following**: 5-day SMA crosses above 20-day SMA + RSI > 50 -> `BUY`.
    2. **Mean-Reversion**: 3-day drop > 3.5%, RSI < 32, no negative news -> `BUY` (Dip).
    3. **Sentiment Momentum**: Strong positive news score >= 2 -> `BUY`; strong negative news score <= -2 -> `SELL` / `SHORT`.
  * **Short Rules**: Only short if strong bearish news AND price below 20-day SMA.

### 5.8 `risk_manager.py`
* **Responsibilities**:
  * Pre-execution sanity checks:
    1. **Cash Reserve Guard**: Ensures `cash_remaining >= 20%` of starting capital.
    2. **Max Position Size**: Single stock allocation capped at 25% of total wealth.
    3. **Margin Validation**: Confirms 100% margin available before submitting `SHORT`.
    4. **Stop-Loss Monitor**: Triggers immediate `SELL` if price drops > 3.5% from entry.
    5. **Take-Profit Monitor**: Triggers `SELL` if profit reaches > 6.0%.
    6. **Short Duration Guard**: Enforces `COVER` on Day 7 to avoid server-side 10-day auto-liquidation.

### 5.9 `order_manager.py`
* **Responsibilities**:
  * Thread-safe order queue.
  * Rate-limit pacing (token bucket) enforcing <= 1.5 orders/sec.
  * Dispatches `POST /api/v1/orders` with exact required fields:
    ```json
    {
      "ticker": "ALPHACORP",
      "action": "BUY",
      "quantity": 100,
      "date": "2021-04-15"
    }
    ```
  * Error code handler:
    * `INSUFFICIENT_CAPITAL` / `INSUFFICIENT_MARGIN` -> Refreshes cash cache and throttles.
    * `SIMULATION_NOT_RUNNING` -> Pauses order dispatcher.
    * `RATE_LIMITED` -> Suspends requests for 30s penalty window.

### 5.10 `stage_monitor.py`
* **Responsibilities**:
  * Background thread polling `GET /api/v1/stage` every 2 seconds.
  * Detects transitions: `development` -> `preflight` -> `simulation` -> `COMPLETED`.
  * Sets internal threading event flags to safely pause or resume order dispatching.

### 5.11 `kafka_consumer.py`
* **Responsibilities**:
  * Connects to Kafka at `<SERVER_IP>:9094`.
  * Subscribes to `market.ticks` and `market.news`.
  * Consumer Group: `group-<team_username>`.
  * `auto_offset_reset='latest'`, `enable_auto_commit=False`.
  * Dispatches incoming messages:
    * `preflight_tick`: Immediately forwards sequence ID to `api_client.post_heartbeat(seq)`.
    * `price_tick`: Routes to `market_state.on_tick()`.
    * `news_flash`: Routes to `market_state.on_news()`.

### 5.12 `telemetry.py`
* **Responsibilities**:
  * Measures decision latency: `(T_order_sent - T_tick_received)`.
  * Logs order execution audit trail (fill price, brokerage charged, tax deducted).
  * Formats clean terminal logs for jury presentation and live debugging.

### 5.13 `main.py`
* **Responsibilities**:
  * Master application entrypoint.
  * Initializes configuration, API client, state, risk manager, and consumer.
  * Runs the main event cycle:
    1. Check stage status.
    2. Run pre-flight heartbeat loop.
    3. Warm-start historical indicators.
    4. Enter real-time simulation processing loop.
    5. Clean up and log closing statistics upon `COMPLETED`.

---

## 6. Phased Implementation Roadmap

### Phase 1: Environment & REST Client (Hours 0.5 – 3)
- [x] Configure `.env.example` and `config.py`.
- [ ] Implement `models.py` dataclasses.
- [ ] Implement `api_client.py` covering all 8 REST endpoints with token authentication.
- [ ] Write unit tests in `tests/test_orders.py` using mock HTTP responses.
- [ ] Verify connectivity to Participant Portal (`:3003`) and Gateway (`:3002`).

### Phase 2: Pre-Flight, Discovery & Historical Bootstrap (Hours 3 – 6)
- [ ] Implement `stage_monitor.py` to poll `/stage`.
- [ ] Implement stock discovery (`GET /api/v1/stocks`).
- [ ] Implement historical data warm-start (`GET /api/v1/stocks/:ticker/history?days=90`).
- [ ] Verify Pre-Flight Heartbeat: send `seq` ACK to `POST /api/v1/bot/heartbeat` and confirm "Ready" status on dashboard.

### Phase 3: Kafka Ingestion & Sliding State (Hours 6 – 9)
- [ ] Implement `kafka_consumer.py` for topics `market.ticks` and `market.news`.
- [ ] Implement `market_state.py` sliding window buffers.
- [ ] Implement sandbox loop reset handling (`current_date < previous_date`).
- [ ] Measure ingestion latency to ensure <100ms processing time per candle.

### Phase 4: Indicator Engine & News Sentiment (Hours 9 – 12)
- [ ] Implement SMA, EMA, RSI, and Bollinger Bands in `indicators.py`.
- [ ] Implement keyword scoring in `news_sentiment.py`.
- [ ] Write unit tests in `tests/test_indicators.py` verifying indicator accuracy.
- [ ] Benchmark execution time (target < 5ms per stock calculation).

### Phase 5: Risk Manager & Strategy Synthesis (Hours 12 – 15)
- [ ] Implement Trend-Following + Dip-Buying logic in `strategy.py`.
- [ ] Implement `risk_manager.py`:
  - 20% cash reserve enforcement.
  - 25% single-stock wealth limit.
  - 3.5% stop-loss and 6.0% take-profit.
  - 7-day max holding time for short positions.
- [ ] Write unit tests in `tests/test_risk_manager.py` and `tests/test_strategy.py`.

### Phase 6: Order Dispatcher, Rate Limiting & Telemetry (Hours 15 – 18)
- [ ] Implement `order_manager.py` with 1.5 req/sec token bucket rate limiter.
- [ ] Wire date parameter passing into orders for server decision latency scoring.
- [ ] Implement structured trade telemetry in `telemetry.py`.
- [ ] Test rejection handling (`INSUFFICIENT_CAPITAL`, `NO_SHORT_POSITION`, `RATE_LIMITED`).

### Phase 7: Sandbox End-to-End Testing & Jury Defense Preparation (Hours 18 – 20)
- [ ] Run full autonomous cycle in Development sandbox.
- [ ] Test dashboard 'Reset Cash' integration.
- [ ] Verify scorecard metrics: zero 429 errors, low decision latency, and disciplined risk controls.
- [ ] Prepare code walkthrough and architecture diagrams for final jury defense.

---

## 7. Pre-Competition Verification Checklist

- [ ] **Wi-Fi Connectivity**: Verified connection to official venue Wi-Fi (no external hotspots).
- [ ] **Desk Verification**: Completed in-person team verification at registration desk.
- [ ] **Token Configuration**: Copied persistent token from Portal into `.env` file.
- [ ] **Consumer Group Prefix**: Confirmed group ID matches `group-<your_username>`.
- [ ] **Pre-Flight ACK**: Verified team status shows green **Ready** on Portal dashboard.
- [ ] **Order Mechanics**: Verified orders execute with exact capital actions (`BUY`, `SELL`, `SHORT`, `COVER`).
- [ ] **Rate Limits**: Verified order throttle stays safely below 120 req/minute.
- [ ] **Friction Protection**: Confirmed minimum trade sizing prevents flat fee erosion.
