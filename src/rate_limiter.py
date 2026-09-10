"""Asynchronous Token Bucket rate limiter for batch stream throttling."""

import asyncio
import time


class AsyncTokenBucketRateLimiter:
    """Thread-safe and coroutine-safe Token Bucket rate limiter.

    Controls processing cadence to prevent overwhelming downstream APIs and databases.

    Attributes:
        rate: Refill rate in tokens per second.
        capacity: Maximum burst capacity of the token bucket.
    """

    def __init__(self, rate: float, capacity: float | None = None) -> None:
        """Initializes rate limiter with refill rate and maximum capacity.

        Args:
            rate: Target throughput rate in tokens per second. Must be > 0.
            capacity: Maximum burst capacity. If None, defaults to rate.
        """
        if rate <= 0:
            raise ValueError("Rate must be greater than 0")

        self.rate: float = float(rate)
        self.capacity: float = float(capacity) if capacity is not None else float(rate)
        self._tokens: float = self.capacity
        self._last_refill_time: float = time.monotonic()
        self._lock: asyncio.Lock = asyncio.Lock()

    def _refill(self) -> None:
        """Adds accrued tokens based on elapsed monotonic time."""
        now = time.monotonic()
        elapsed = now - self._last_refill_time
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill_time = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Asynchronously waits until the requested token quota is available and consumes it.

        Args:
            tokens: Number of tokens required for the operation.
        """
        if tokens <= 0:
            return

        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return

                # Calculate required sleep duration to accumulate needed tokens
                deficit = tokens - self._tokens
                sleep_seconds = deficit / self.rate

            await asyncio.sleep(sleep_seconds)

    async def try_acquire(self, tokens: float = 1.0) -> bool:
        """Attempts to acquire tokens immediately without blocking.

        Args:
            tokens: Number of tokens requested.

        Returns:
            True if tokens were available and consumed, False otherwise.
        """
        if tokens <= 0:
            return True

        async with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False
