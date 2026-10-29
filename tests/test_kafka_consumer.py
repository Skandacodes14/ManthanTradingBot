import unittest
from typing import Any

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
    def __init__(self, value: dict[str, Any]) -> None:
        self.topic = config.KAFKA_TOPIC_TICKS
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

    def test_preflight_event_without_sequence_fails_without_ack(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing its sequence ID"):
            self.consumer.process_message(
                FakeMessage({"event_type": "preflight_tick", "data": {}})
            )

        self.assertEqual(self.api_client.acknowledged_sequences, [])


if __name__ == "__main__":
    unittest.main()
