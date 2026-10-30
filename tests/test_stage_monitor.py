import threading
import unittest

from models import StageStatus
from stage_monitor import StageMonitor


class FakeAPIClient:
    def __init__(self, *responses: StageStatus | Exception) -> None:
        self.responses = list(responses)

    def get_stage(self) -> StageStatus:
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class StageMonitorTests(unittest.TestCase):
    def test_stage_transitions_update_dispatch_flags(self) -> None:
        api_client = FakeAPIClient(
            StageStatus(stage="development", status="IDLE"),
            StageStatus(stage="preflight", status="IDLE"),
            StageStatus(stage="simulation", status="RUNNING"),
            StageStatus(stage="simulation", status="COMPLETED"),
        )
        monitor = StageMonitor(api_client)

        monitor.poll_once()
        self.assertFalse(monitor.dispatch_enabled_event.is_set())
        self.assertFalse(monitor.preflight_event.is_set())

        monitor.poll_once()
        self.assertTrue(monitor.preflight_event.is_set())
        self.assertFalse(monitor.dispatch_enabled_event.is_set())

        monitor.poll_once()
        self.assertFalse(monitor.preflight_event.is_set())
        self.assertTrue(monitor.simulation_running_event.is_set())
        self.assertTrue(monitor.dispatch_enabled_event.is_set())

        monitor.poll_once()
        self.assertFalse(monitor.simulation_running_event.is_set())
        self.assertFalse(monitor.dispatch_enabled_event.is_set())
        self.assertTrue(monitor.completed_event.is_set())

    def test_preflight_transition_invokes_on_preflight_and_heartbeat(self) -> None:
        class HeartbeatAPIClient(FakeAPIClient):
            def __init__(self, *responses: StageStatus | Exception) -> None:
                super().__init__(*responses)
                self.heartbeats: list[int] = []

            def post_heartbeat(self, seq: int) -> None:
                self.heartbeats.append(seq)

        preflight_triggered: list[bool] = []
        api = HeartbeatAPIClient(
            StageStatus(stage="development", status="IDLE"),
            StageStatus(stage="preflight", status="RUNNING"),
        )
        monitor = StageMonitor(
            api,
            on_preflight=lambda: preflight_triggered.append(True),
            heartbeat_interval=0.0,
        )
        monitor.poll_once()
        self.assertEqual(preflight_triggered, [])
        self.assertEqual(api.heartbeats, [1])

        monitor.poll_once()
        self.assertEqual(preflight_triggered, [True])
        self.assertEqual(api.heartbeats, [1, 1])

    def test_poll_failure_is_recorded_and_fails_closed_for_dispatch(self) -> None:
        expected_error = OSError("gateway unavailable")
        monitor = StageMonitor(
            FakeAPIClient(
                StageStatus(stage="simulation", status="RUNNING"),
                expected_error,
            )
        )

        monitor.poll_once()
        with self.assertRaisesRegex(OSError, "gateway unavailable"):
            monitor.poll_once()

        self.assertIs(monitor.last_error, expected_error)
        self.assertFalse(monitor.dispatch_enabled_event.is_set())
        self.assertFalse(monitor.simulation_running_event.is_set())
        self.assertEqual(monitor.status.stage, "simulation")

    def test_start_polls_in_background_and_stop_joins_thread(self) -> None:
        polled = threading.Event()

        class SignalingAPIClient:
            def get_stage(self) -> StageStatus:
                polled.set()
                return StageStatus(stage="simulation", status="RUNNING")

        monitor = StageMonitor(SignalingAPIClient(), poll_interval=60)
        monitor.start()

        self.assertTrue(polled.wait(timeout=1))
        monitor.stop(timeout=1)

        self.assertFalse(monitor.is_running)
        self.assertFalse(monitor.dispatch_enabled_event.is_set())

    def test_rejects_non_positive_poll_interval(self) -> None:
        with self.assertRaises(ValueError):
            StageMonitor(FakeAPIClient(), poll_interval=0)


if __name__ == "__main__":
    unittest.main()
