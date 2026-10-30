"""Poll the trading gateway lifecycle and gate order dispatch accordingly."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Event, Lock, Thread, current_thread
from typing import Protocol

from models import StageStatus

logger = logging.getLogger(__name__)


class StageAPIClient(Protocol):
    """The API client operations required by the stage monitor."""

    def get_stage(self) -> StageStatus: ...


class StageMonitor:
    """Poll stage status in the background and publish thread-safe lifecycle flags."""

    def __init__(
        self,
        api_client: StageAPIClient,
        *,
        poll_interval: float = 2.0,
        on_preflight: Callable[[], None] | None = None,
        heartbeat_interval: float = 5.0,
    ) -> None:
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError("poll_interval must be finite and positive")

        self.api_client = api_client
        self.poll_interval = poll_interval
        self.on_preflight = on_preflight
        self.heartbeat_interval = heartbeat_interval
        self.preflight_event = Event()
        self.simulation_running_event = Event()
        self.dispatch_enabled_event = Event()
        self.completed_event = Event()
        self.stop_event = Event()
        self._state_lock = Lock()
        self._poll_lock = Lock()
        self._thread_lock = Lock()
        self._status: StageStatus | None = None
        self._last_error: Exception | None = None
        self._thread: Thread | None = None
        self._last_heartbeat_time: float = 0.0
        self.latest_seq: int = 1

    @property
    def status(self) -> StageStatus | None:
        """Return the most recently received stage status."""
        with self._state_lock:
            return self._status

    @property
    def last_error(self) -> Exception | None:
        """Return the most recent polling error, if any."""
        with self._state_lock:
            return self._last_error

    @property
    def is_running(self) -> bool:
        """Whether the background polling thread is alive."""
        with self._thread_lock:
            return self._thread is not None and self._thread.is_alive()

    def poll_once(self) -> StageStatus:
        """Fetch and publish one lifecycle status, propagating API failures."""
        with self._poll_lock:
            try:
                status = self.api_client.get_stage()
            except Exception as exc:
                with self._state_lock:
                    self._last_error = exc
                    self.dispatch_enabled_event.clear()
                    self.simulation_running_event.clear()
                raise

            with self._state_lock:
                previous = self._status
                self._status = status
                self._last_error = None
                self._set_flag(self.preflight_event, status.is_preflight)
                simulation_running = (
                    status.is_simulation_running and not self.stop_event.is_set()
                )
                self._set_flag(
                    self.simulation_running_event,
                    simulation_running,
                )
                self._set_flag(self.dispatch_enabled_event, simulation_running)
                self._set_flag(self.completed_event, status.is_completed)

            if previous is None:
                logger.info("Platform stage: %s (%s)", status.stage, status.status)
            elif (previous.stage, previous.status) != (status.stage, status.status):
                logger.info(
                    "Platform stage changed: %s (%s) -> %s (%s)",
                    previous.stage,
                    previous.status,
                    status.stage,
                    status.status,
                )

            # Trigger dynamic universe discovery when preflight becomes active
            if status.is_preflight:
                if (previous is None or not previous.is_preflight) and self.on_preflight is not None:
                    try:
                        self.on_preflight()
                    except Exception:
                        logger.exception("Failed during stage monitor on_preflight callback.")

            # Keep-alive heartbeat: send continuous ACK while bot is running to pass pre-flight readiness check
            now = time.monotonic()
            if (
                not status.is_completed
                and hasattr(self.api_client, "post_heartbeat")
                and (now - self._last_heartbeat_time >= self.heartbeat_interval)
            ):
                try:
                    self.api_client.post_heartbeat(self.latest_seq)
                    self._last_heartbeat_time = now
                    logger.info(
                        "Continuous pre-flight heartbeat ACK sent (seq=%s, stage=%s)",
                        self.latest_seq,
                        status.stage,
                    )
                except Exception as exc:
                    logger.warning(
                        "Continuous pre-flight heartbeat failed (%s)",
                        exc,
                    )
        return status

    @staticmethod
    def _set_flag(flag: Event, enabled: bool) -> None:
        if enabled:
            flag.set()
        else:
            flag.clear()

    def start(self) -> None:
        """Start the daemon polling thread; repeated calls are harmless."""
        with self._thread_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self.stop_event.clear()
            self._thread = Thread(
                target=self._run,
                name="stage-monitor",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        """Request shutdown and optionally wait for the polling thread."""
        with self._thread_lock:
            self.stop_event.set()
            with self._state_lock:
                self.dispatch_enabled_event.clear()
                self.simulation_running_event.clear()
            thread = self._thread
        if thread is not None and thread is not current_thread():
            thread.join(timeout)

    def _run(self) -> None:
        consecutive_failures = 0
        while not self.stop_event.is_set():
            try:
                self.poll_once()
                consecutive_failures = 0
                delay = self.poll_interval
            except Exception as exc:
                consecutive_failures += 1
                backoff = min(
                    60.0,
                    self.poll_interval * (2 ** min(consecutive_failures, 3)),
                )
                retry_after = self._retry_after_seconds(exc)
                delay = max(backoff, retry_after or 0.0)
                logger.warning(
                    "Failed to poll platform stage (%s); dispatch remains paused. "
                    "Retrying in %.1f seconds.",
                    exc,
                    delay,
                )
            self.stop_event.wait(delay)

    @staticmethod
    def _retry_after_seconds(error: Exception) -> float | None:
        response = getattr(error, "response", None)
        headers = getattr(response, "headers", None)
        if not headers:
            return None

        value = headers.get("Retry-After")
        if value is None:
            return None
        try:
            delay = float(value)
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(str(value))
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return None
        if not math.isfinite(delay):
            return None
        return max(0.0, delay)