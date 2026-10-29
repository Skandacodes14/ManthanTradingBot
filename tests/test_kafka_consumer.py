import unittest
from threading import Event
from typing import Any
from unittest.mock import patch

import config
from kafka_consumer import KafkaMarketDataConsumer


class FakeAPIClient:
    def __init__(self) -> None:
        self.acknowledged_sequences: list[int] = []

    def post_heartbeat(self, seq: int) -> Any:
        self.acknowledged_sequences.append(seq)
        return {"status": "Ready"}


class FakeMarketState:
    def on_tick(self, _tick: Any) -> None:
        raise AssertionError(f"Unexpected price tick: {_tick!r}")

    def on_news(self, _news: Any) -> None:
        raise AssertionError(f"Unexpected news event: {_news!r}")


class FakeMessage:
    def __init__(
        self,
        value: dict[str, Any],
        topic: str = config.KAFKA_TOPIC_TICKS,
    ) -> None:
        self.topic = topic
        self.value = value


class KafkaPreflightHeartbeatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.api_client = FakeAPIClient()
        self.consumer = KafkaMarketDataConsumer(
            self.api_client,
            FakeMarketState(),
        )

    def test_preflight_event_acknowledges_top_level_sequence(self) -> None:
        self.consumer.process_message(
            FakeMessage({"event_type": "preflight_tick", "seq": 17})
        )

        self.assertEqual(self.api_client.acknowledged_sequences, [17])

    def test_preflight_event_acknowledges_nested_sequence(self) -> None:
        self.consumer.process_message(
            FakeMessage(
                {
                    "event": "preflight_tick",
                    "data": {"seq": 23, "status": "STANDBY"},
                }
            )
        )

        self.assertEqual(self.api_client.acknowledged_sequences, [23])

    def test_duplicate_preflight_sequence_is_acknowledged_only_once(self) -> None:
        message = FakeMessage({"event_type": "preflight_tick", "seq": 17})

        self.consumer.process_message(message)
        self.consumer.process_message(message)

        self.assertEqual(self.api_client.acknowledged_sequences, [17])

    def test_preflight_sequence_is_retried_if_acknowledgement_fails(self) -> None:
        class FailingOnceAPIClient(FakeAPIClient):
            def __init__(self) -> None:
                super().__init__()
                self.fail = True

            def post_heartbeat(self, seq: int) -> Any:
                if self.fail:
                    self.fail = False
                    raise OSError("heartbeat request failed")
                return super().post_heartbeat(seq)

        api_client = FailingOnceAPIClient()
        consumer = KafkaMarketDataConsumer(api_client, FakeMarketState())
        message = FakeMessage({"event_type": "preflight_tick", "seq": 17})

        with self.assertRaisesRegex(OSError, "heartbeat request failed"):
            consumer.process_message(message)
        consumer.process_message(message)
        consumer.process_message(message)

        self.assertEqual(api_client.acknowledged_sequences, [17])

    def test_preflight_news_is_ignored_without_warning(self) -> None:
        with self.assertNoLogs("kafka_consumer", level="WARNING"):
            self.consumer.process_message(
                FakeMessage(
                    {"event_type": "preflight_news"},
                    topic=config.KAFKA_TOPIC_NEWS,
                )
            )

    def test_preflight_event_without_sequence_fails_without_ack(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing its sequence ID"):
            self.consumer.process_message(
                FakeMessage({"event_type": "preflight_tick", "data": {}})
            )

        self.assertEqual(self.api_client.acknowledged_sequences, [])

    def test_run_polls_until_stop_event_and_closes_consumer(self) -> None:
        stop_event = Event()

        class PollingConsumer:
            def __init__(self) -> None:
                self.poll_count = 0
                self.commits = 0
                self.closed = False

            def poll(self, timeout_ms: int = 1000) -> dict[str, list[FakeMessage]]:
                self.poll_count += 1
                if self.poll_count == 1:
                    return {
                        "ticks": [
                            FakeMessage(
                                {"event_type": "preflight_tick", "seq": 31}
                            )
                        ]
                    }
                stop_event.set()
                return {}

            def commit(self) -> None:
                self.commits += 1

            def close(self) -> None:
                self.closed = True

        polling_consumer = PollingConsumer()
        consumer = KafkaMarketDataConsumer(
            self.api_client,
            FakeMarketState(),
            consumer_factory=lambda *_args, **_kwargs: polling_consumer,
        )

        consumer.run(stop_event=stop_event)

        self.assertEqual(self.api_client.acknowledged_sequences, [31])
        self.assertEqual(polling_consumer.commits, 1)
        self.assertTrue(polling_consumer.closed)

    def test_run_retries_broker_connection_until_consumer_is_created(self) -> None:
        stop_event = Event()

        class PollingConsumer:
            closed = False

            def __iter__(self) -> Any:
                stop_event.set()
                return iter(())

            def commit(self) -> None:
                raise AssertionError("No message should be committed.")

            def close(self) -> None:
                self.closed = True

        polling_consumer = PollingConsumer()
        attempts = 0

        def consumer_factory(*_args: Any, **_kwargs: Any) -> Any:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("broker unavailable")
            return polling_consumer

        consumer = KafkaMarketDataConsumer(
            self.api_client,
            FakeMarketState(),
            consumer_factory=consumer_factory,
        )

        with patch("kafka_consumer.time.sleep") as sleep:
            consumer.run()

        self.assertEqual(attempts, 2)
        sleep.assert_called_once_with(1.0)
        self.assertTrue(polling_consumer.closed)


if __name__ == "__main__":
    unittest.main()
