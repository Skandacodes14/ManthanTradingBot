"""Rate-limited, thread-safe order dispatch."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from queue import Queue
from threading import Lock
from typing import Generic, TypeVar

from config import MAX_REQUESTS_PER_SEC, PENALTY_BOX_SECONDS
from models import OrderRequest
from telemetry import TradeTelemetry

T = TypeVar("T")


class OrderRejectedError(RuntimeError):
    """Raised when the order endpoint explicitly rejects an order."""

    def __init__(self, error_code: str, response: object = None) -> None:
        self.error_code = error_code
        self.response = response
        super().__init__(f"Order rejected: {error_code}")


class DispatcherPausedError(RuntimeError):
    """Raised when dispatching is paused by the order endpoint."""


class TokenBucketRateLimiter:
    """Thread-safe token bucket that paces requests at a fixed rate."""

    def __init__(
        self,
        rate: float = MAX_REQUESTS_PER_SEC,
        capacity: float = 1.0,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("rate must be a finite positive number")
        if not math.isfinite(capacity) or capacity < 1:
            raise ValueError("capacity must be finite and at least one request")

        self.rate = rate
        self.capacity = capacity
        self._clock = clock
        self._sleep = sleep
        self._tokens = capacity
        self._updated_at = clock()
        self._lock = Lock()

    def acquire(self) -> None:
        """Block until one request token is available, then consume it."""
        while True:
            with self._lock:
                now = self._clock()
                elapsed = max(0.0, now - self._updated_at)
                self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                self._updated_at = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                delay = (1.0 - self._tokens) / self.rate

            self._sleep(delay)


class OrderManager(Generic[T]):
    """Queue and dispatch orders through an injected API submission function."""

    def __init__(
        self,
        submit_order: Callable[[OrderRequest], T],
        *,
        rate_limiter: TokenBucketRateLimiter | None = None,
        telemetry: TradeTelemetry | None = None,
        refresh_cash_cache: Callable[[], object] | None = None,
        penalty_box_seconds: float = PENALTY_BOX_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not math.isfinite(penalty_box_seconds) or penalty_box_seconds < 0:
            raise ValueError("penalty_box_seconds must be finite and non-negative")

        self._submit_order = submit_order
        self.rate_limiter = rate_limiter or TokenBucketRateLimiter(clock=clock, sleep=sleep)
        self.telemetry = telemetry or TradeTelemetry()
        self._refresh_cash_cache = refresh_cash_cache
        self._penalty_box_seconds = penalty_box_seconds
        self._clock = clock
        self._sleep = sleep
        self._queue: Queue[tuple[OrderRequest, float | None]] = Queue()
        self._state_lock = Lock()
        self._paused_reason: str | None = None
        self._penalty_until = 0.0

    @property
    def paused(self) -> bool:
        with self._state_lock:
            return self._paused_reason is not None

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    def enqueue(self, order: OrderRequest, *, tick_received_at: float | None = None) -> None:
        """Add an order to the thread-safe dispatch queue."""
        self._queue.put((order, tick_received_at))

    def dispatch_next(self, timeout: float | None = None) -> T:
        """Dispatch the next queued order, raising queue.Empty on timeout."""
        order, tick_received_at = self._queue.get(timeout=timeout)
        try:
            return self.dispatch(order, tick_received_at=tick_received_at)
        finally:
            self._queue.task_done()

    def resume(self) -> None:
        """Resume dispatch after an explicit simulation-not-running rejection."""
        with self._state_lock:
            self._paused_reason = None

    def dispatch(
        self,
        order: OrderRequest,
        *,
        tick_received_at: float | None = None,
    ) -> T:
        """Rate-limit and submit an order, returning the sender's result."""
        self._wait_until_dispatchable()
        self.rate_limiter.acquire()
        sent_at = self._clock()

        try:
            result = self._submit_order(order)
        except OrderRejectedError as exc:
            self._handle_rejection(exc.error_code)
            self.telemetry.record_order(
                order,
                status="rejected",
                tick_received_at=tick_received_at,
                sent_at=sent_at,
                error_code=exc.error_code,
                response=exc.response,
            )
            raise
        except Exception:
            self.telemetry.record_order(
                order,
                status="error",
                tick_received_at=tick_received_at,
                sent_at=sent_at,
            )
            raise

        error_code = self._response_error_code(result)
        if error_code:
            self._handle_rejection(error_code)
            self.telemetry.record_order(
                order,
                status="rejected",
                tick_received_at=tick_received_at,
                sent_at=sent_at,
                error_code=error_code,
                response=result,
            )
            raise OrderRejectedError(error_code, result)

        self.telemetry.record_order(
            order,
            status="submitted",
            tick_received_at=tick_received_at,
            sent_at=sent_at,
            response=result,
        )
        return result

    def _wait_until_dispatchable(self) -> None:
        while True:
            with self._state_lock:
                if self._paused_reason is not None:
                    raise DispatcherPausedError(
                        f"Order dispatcher paused: {self._paused_reason}"
                    )
                delay = self._penalty_until - self._clock()

            if delay <= 0:
                return
            self._sleep(delay)

    def _handle_rejection(self, error_code: str) -> None:
        if error_code in {"INSUFFICIENT_CAPITAL", "INSUFFICIENT_MARGIN"}:
            if self._refresh_cash_cache is not None:
                self._refresh_cash_cache()
        elif error_code == "SIMULATION_NOT_RUNNING":
            with self._state_lock:
                self._paused_reason = error_code
        elif error_code == "RATE_LIMITED":
            with self._state_lock:
                self._penalty_until = max(
                    self._penalty_until,
                    self._clock() + self._penalty_box_seconds,
                )

    @staticmethod
    def _response_error_code(response: object) -> str | None:
        if not isinstance(response, Mapping):
            return None

        error = response.get("error")
        code = response.get("error_code") or response.get("code")
        if isinstance(error, Mapping):
            code = code or error.get("error_code") or error.get("code")
        elif isinstance(error, str):
            code = code or error

        if code:
            return str(code)

        data = response.get("data")
        if isinstance(data, Mapping):
            return OrderManager._response_error_code(data)
        return None
