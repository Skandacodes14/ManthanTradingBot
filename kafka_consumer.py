"""Kafka ingestion for market ticks, news, and preflight heartbeats."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from threading import Event
import time
from typing import Any, Protocol

import config
from models import NewsFlash, PreflightTick, PriceTick

logger = logging.getLogger(__name__)


class MarketStateHandlers(Protocol):
    """Callbacks implemented by the market-state layer."""

    def on_tick(self, tick: PriceTick) -> None: ...

    def on_news(self, news: NewsFlash) -> None: ...


class KafkaMessage(Protocol):
    topic: str
    value: Any


class KafkaConsumerHandle(Protocol):
    def __iter__(self) -> Any: ...

    def poll(self, timeout_ms: int = 1000) -> Mapping[Any, Sequence[Any]]: ...

    def commit(self) -> Any: ...

    def close(self) -> None: ...


class HeartbeatClient(Protocol):
    def post_heartbeat(self, seq: int) -> Any: ...


def _deserialize_json(value: bytes | None) -> dict[str, Any]:
    """Decode a Kafka value and reject non-object JSON payloads."""
    if value is None:
        raise ValueError("Kafka event payload must not be null.")
    payload = json.loads(value.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Kafka event payload must be a JSON object.")
    return payload


class KafkaMarketDataConsumer:
    """Consume and route events from the configured market Kafka topics."""

    def __init__(
        self,
        api_client: HeartbeatClient,
        market_state: MarketStateHandlers,
        *,
        broker: str = config.KAFKA_BROKER,
        group_id: str = config.KAFKA_CONSUMER_GROUP,
        consumer_factory: Callable[..., KafkaConsumerHandle] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        heartbeat_cooldown: float = 2.0,
        on_preflight: Callable[[int], None] | None = None,
    ) -> None:
        self.api_client = api_client
        self.market_state = market_state
        self.broker = broker
        self.group_id = group_id
        self.consumer_factory = consumer_factory
        self._monotonic = monotonic
        self.heartbeat_cooldown = heartbeat_cooldown
        self.on_preflight = on_preflight
        self._acknowledged_preflight_sequences: set[int] = set()
        self._last_heartbeat_time: float = 0.0
        self._last_heartbeat_seq: int | None = None
        self.latest_seq: int = 1

    def _create_consumer(self) -> KafkaConsumerHandle:
        factory = self.consumer_factory
        if factory is None:
            from kafka import KafkaConsumer

            factory = KafkaConsumer

        consumer_options: dict[str, Any] = {
            "bootstrap_servers": [self.broker],
            "group_id": self.group_id,
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "fetch_max_bytes": 100_000_000,
            "max_partition_fetch_bytes": 10_000_000,
            "receive_message_max_bytes": 100_000_000,
            "value_deserializer": _deserialize_json,
            "session_timeout_ms": 30000,
            "heartbeat_interval_ms": 10000,
            "max_poll_interval_ms": 300000,
            "reconnect_backoff_ms": 500,
            "reconnect_backoff_max_ms": 30000,
            "retry_backoff_ms": 500,
            "request_timeout_ms": 40000,
        }
        return factory(
            config.KAFKA_TOPIC_TICKS,
            config.KAFKA_TOPIC_NEWS,
            **consumer_options,
        )

    def process_message(self, message: KafkaMessage) -> None:
        """Dispatch one decoded Kafka message to its matching handler."""
        payload = message.value
        if not isinstance(payload, dict):
            raise ValueError("Kafka event payload must be a JSON object.")

        event_type = payload.get("event_type") or payload.get("event")
        if message.topic == config.KAFKA_TOPIC_TICKS:
            if event_type == "preflight_tick":
                nested_data = payload.get("data")
                sequence = payload.get("seq")
                if sequence is None and isinstance(nested_data, dict):
                    sequence = nested_data.get("seq")
                if sequence is None:
                    raise ValueError("Preflight tick is missing its sequence ID.")

                preflight_tick = PreflightTick.from_dict(payload)
                seq = preflight_tick.seq
                self.latest_seq = seq
                now = self._monotonic()
                is_new_seq = seq not in self._acknowledged_preflight_sequences
                cooldown_elapsed = (now - self._last_heartbeat_time) >= self.heartbeat_cooldown

                if is_new_seq or cooldown_elapsed:
                    self.api_client.post_heartbeat(seq)
                    self._acknowledged_preflight_sequences.add(seq)
                    self._last_heartbeat_time = now
                    self._last_heartbeat_seq = seq
                    logger.info(
                        "Acknowledged preflight heartbeat seq=%s",
                        seq,
                    )

                if self.on_preflight is not None:
                    try:
                        self.on_preflight(seq)
                    except Exception:
                        logger.exception("Error in consumer on_preflight handler for seq=%s", seq)
                return

            if event_type == "price_tick":
                self.market_state.on_tick(PriceTick.from_dict(payload))
                return

        elif message.topic == config.KAFKA_TOPIC_NEWS and event_type == "preflight_news":
            logger.debug("Ignoring preflight news event.")
            return
        elif message.topic == config.KAFKA_TOPIC_NEWS and event_type == "news_flash":
            self.market_state.on_news(NewsFlash.from_dict(payload))
            return

        logger.warning(
            "Ignoring unsupported Kafka event: topic=%s event_type=%r",
            message.topic,
            event_type,
        )

    def run(self, stop_event: Event | None = None) -> None:
        """Consume until stopped, committing each message only after handling it."""
        consumer: KafkaConsumerHandle | None = None
        retry_delay = 1.0
        while stop_event is None or not stop_event.is_set():
            try:
                consumer = self._create_consumer()
                break
            except Exception as exc:
                logger.warning(
                    "Kafka broker connection failed (%s); retrying in %.1f seconds.",
                    exc,
                    retry_delay,
                )
                if stop_event is not None:
                    if stop_event.wait(retry_delay):
                        return
                else:
                    time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2.0, 30.0)

        if consumer is None:
            return

        logger.info(
            "Connected to Kafka broker %s; subscribed to %s and %s",
            self.broker,
            config.KAFKA_TOPIC_TICKS,
            config.KAFKA_TOPIC_NEWS,
        )
        try:
            if stop_event is None:
                for message in consumer:
                    try:
                        self.process_message(message)
                    except Exception:
                        logger.exception(
                            "Error processing Kafka message on topic=%s; skipping.",
                            message.topic,
                        )
                    consumer.commit()
            else:
                assigned_logged = False
                last_waiting_log = time.monotonic()
                while not stop_event.is_set():
                    records = consumer.poll(timeout_ms=500)
                    if not assigned_logged:
                        assignment = consumer.assignment() if hasattr(consumer, "assignment") else None
                        if assignment:
                            logger.info(
                                "Kafka consumer active: %d partitions assigned across %s and %s. Listening for market events...",
                                len(assignment),
                                config.KAFKA_TOPIC_TICKS,
                                config.KAFKA_TOPIC_NEWS,
                            )
                            assigned_logged = True

                    if not records:
                        now = time.monotonic()
                        if now - last_waiting_log >= 30.0:
                            last_waiting_log = now
                            logger.info(
                                "Kafka consumer healthy; waiting for market events from broker %s...",
                                self.broker,
                            )
                        continue

                    for messages in records.values():
                        for message in messages:
                            if stop_event.is_set():
                                return
                            try:
                                self.process_message(message)
                            except Exception:
                                logger.exception(
                                    "Error processing Kafka message on topic=%s; skipping.",
                                    message.topic,
                                )
                            consumer.commit()
        finally:
            consumer.close()
