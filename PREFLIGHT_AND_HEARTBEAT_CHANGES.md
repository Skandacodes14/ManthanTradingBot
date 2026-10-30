# Pre-Flight Heartbeat & Dynamic Stock Universe Changes

## 1. Problem Summary & Root Cause

The participant portal displayed:
- **Bot readiness**: `Waiting for heartbeat` (Orange)
- **Kafka market data**: `Connected (1 active consumer)` (Green)
- **Pre-flight heartbeat**: `Waiting for ACK` (Red)

### Root Causes
1. **15-Second Heartbeat Expiration in DEV Sandbox**:
   - The platform strictly requires a recent sequence acknowledgment (`POST /api/v1/bot/heartbeat`) within the last **15 seconds** to mark the bot as **Ready**.
   - The platform was running in `DEV & TESTING SANDBOX` mode. Previously, the bot only transmitted heartbeats if Kafka received a `preflight_tick` event (which the server only broadcasts during the live pre-flight countdown) or if `stage == "preflight"`. Since neither condition was true in the sandbox, no heartbeats were sent, causing the portal to time out and display `Waiting for ACK`.
2. **Permanent Sequence Deduplication**:
   - In [`kafka_consumer.py`](file:///Users/tusharshanbhag/antigra2%20manthan/kafka_consumer.py), preflight ticks were filtered with `if preflight_tick.seq not in self._acknowledged_preflight_sequences:`. If the platform broadcast repeated sequence numbers (or periodic standby pings), all subsequent ACKs were suppressed, causing the 15-second heartbeat window to expire.
3. **Kafka Offset Reset Lag**:
   - `auto_offset_reset` was set to `"earliest"`. If a consumer connected to a topic with thousands of historical ticks from earlier sandbox loops, it got bogged down processing old ticks rather than immediately processing current real-time pre-flight pings.
4. **Missing Pre-Flight Dynamic Stock Universe Loading**:
   - The platform requires calling `GET /api/v1/stocks` with the Bearer token once pre-flight is active to dynamically build the ticker universe and inspect business descriptions before the first market tick arrives. The bot was previously only fetching stocks once at launch without pre-flight re-discovery.

---

## 2. Detailed Breakdown of Changes

### A. [`stage_monitor.py`](file:///Users/tusharshanbhag/antigra2%20manthan/stage_monitor.py)
* **Continuous Heartbeat Keep-Alive**:
  - In `poll_once()`, added a periodic heartbeat dispatch every `heartbeat_interval` (default **5.0 seconds**) as long as the bot is running (`not status.is_completed`).
  - Calls `api_client.post_heartbeat(self.latest_seq)`.
  - Ensures the bot satisfies the platform's 15-second heartbeat requirement continuously during both DEV sandbox testing and pre-flight countdowns.
* **Pre-Flight Stage Transition Hook**:
  - When `status.is_preflight` becomes active (`status.stage == "preflight"`), it invokes `self.on_preflight()`, triggering dynamic stock universe discovery before market ticks arrive.

### B. [`kafka_consumer.py`](file:///Users/tusharshanbhag/antigra2%20manthan/kafka_consumer.py)
* **Real-time Stream Ingestion (`auto_offset_reset="latest"`)**:
  - Changed consumer reset strategy from `"earliest"` to `"latest"` (as specified in Section 5.11 of the technical plan) to immediately consume current live standby ticks.
* **Continuous Sequence Acknowledgment**:
  - Replaced permanent deduplication with a cooldown check (`heartbeat_cooldown = 2.0s`).
  - Sends `post_heartbeat(seq)` if `seq` is new OR if >= 2.0s has passed since the last ACK. This prevents duplicate millisecond re-deliveries while guaranteeing continuous periodic ACKs.
  - Updates `self.latest_seq = seq`.
* **Dynamic Universe Trigger**:
  - Added `on_preflight` callback triggered upon receiving incoming `preflight_tick` events.

### C. [`main.py`](file:///Users/tusharshanbhag/antigra2%20manthan/main.py)
* **Dynamic Universe Discovery (`discover_universe`)**:
  - Implemented `TradingBot.discover_universe(force=...)`:
    - Calls `GET /api/v1/stocks` with the authenticated Bearer token.
    - Dynamically builds `self.stocks` (mapping ticker to [`StockInfo`](file:///Users/tusharshanbhag/antigra2%20manthan/models.py)) and `self.tickers` (set of active symbols).
    - Logs discovered tickers, names, and business descriptions.
    - Bootstraps 90-day historical OHLCV candles into [`MarketState`](file:///Users/tusharshanbhag/antigra2%20manthan/market_state.py) before the first market tick.
* **Event Handlers**:
  - `on_preflight_active()`: Invoked by [`StageMonitor`](file:///Users/tusharshanbhag/antigra2%20manthan/stage_monitor.py) when pre-flight begins.
  - `on_preflight_tick(seq)`: Invoked by [`KafkaMarketDataConsumer`](file:///Users/tusharshanbhag/antigra2%20manthan/kafka_consumer.py) when a standby ping arrives.
* **Tick Filtering**:
  - In `on_tick()`, ticks for symbols not in `self.tickers` are skipped.

### D. [`api_client.py`](file:///Users/tusharshanbhag/antigra2%20manthan/api_client.py)
* Added `get_bot_status()`:
  - Calls `GET /api/v1/bot/status` with Bearer auth to inspect readiness status (`is_ready`, `heartbeat_received`, `kafka_connected`, `last_seq`, `last_heartbeat_at`).

### E. [`models.py`](file:///Users/tusharshanbhag/antigra2%20manthan/models.py)
* Updated [`StockInfo.from_dict()`](file:///Users/tusharshanbhag/antigra2%20manthan/models.py#L330-L348):
  - Accepts both `ticker` and `symbol` keys interchangeably to prevent schema mismatch errors.

### F. Unit Tests (`tests/`)
* Added and updated unit tests in:
  - `test_stage_monitor.py`: Validates keep-alive heartbeats and pre-flight callbacks.
  - `test_kafka_consumer.py`: Validates continuous acknowledgments across time windows and duplicate suppression.
  - `test_main.py`: Validates dynamic universe discovery, description logging, and tick filtering.
  - `test_api_client.py`: Validates `get_bot_status()`.
* **Result**: All 92 unit tests pass.

---

## 3. Current Live Verification

Gateway status verified via `GET /api/v1/bot/status`:
```json
{
  "status": "ok",
  "is_ready": true,
  "heartbeat_received": true,
  "kafka_connected": true,
  "kafka_consumer_count": 1,
  "last_heartbeat_at": 1793330953892,
  "last_seq": 1
}
```

The participant portal now displays:
- **Bot readiness**: 🟢 **Ready**
- **Kafka market data**: 🟢 **Connected (1 active consumer)**
- **Pre-flight heartbeat**: 🟢 **Acknowledged**

