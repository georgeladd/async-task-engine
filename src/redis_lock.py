"""Distributed lock implementation using Redis with Lua script safety."""

import asyncio
import logging
from types import TracebackType
from typing import Any, Self
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# Lua script to release lock only if the token matches, ensuring safe atomic release
RELEASE_LOCK_LUA_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class LockAcquisitionError(Exception):
    """Raised when a distributed lock cannot be acquired within the timeout."""


class DistributedLock:
    """Redis-backed distributed lock with safe token verification and TTL auto-expiry.

    Prevents concurrent task execution on identical resources across multiple worker nodes.

    Attributes:
        redis: Active asynchronous Redis connection client.
        lock_key: Formatted lock resource key string.
        ttl_seconds: Expiration timeout in seconds to prevent deadlocks.
        lock_token: Unique random token verifying lock ownership.
        is_acquired: Boolean flag indicating current lock status.
    """

    def __init__(
        self,
        redis_client: Redis,
        resource_key: str,
        ttl_seconds: int = 300,
    ) -> None:
        """Initializes the distributed lock instance.

        Args:
            redis_client: Asynchronous Redis client.
            resource_key: The unique resource key to protect against concurrent writes.
            ttl_seconds: Lock auto-expiry in seconds. Defaults to 300.
        """
        self.redis: Redis = redis_client
        self.lock_key: str = f"lock:resource:{resource_key}"
        self.ttl_seconds: int = ttl_seconds
        self.lock_token: str = str(uuid4())
        self.is_acquired: bool = False

    async def acquire(self, timeout_seconds: float = 0.0) -> bool:
        """Attempts to acquire the distributed lock.

        Args:
            timeout_seconds: Maximum duration in seconds to retry acquiring the lock.

        Returns:
            True if the lock was acquired, False otherwise.
        """
        success: bool = False
        start_time: float = asyncio.get_event_loop().time()

        while True:
            # Set key only if it does not exist (NX) with expiration in seconds (EX)
            acquired: Any = await self.redis.set(
                self.lock_key,
                self.lock_token,
                nx=True,
                ex=self.ttl_seconds,
            )
            if acquired:
                self.is_acquired = True
                success = True
                break

            elapsed: float = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout_seconds:
                break

            await asyncio.sleep(0.1)

        return success

    async def release(self) -> bool:
        """Safely releases the distributed lock using atomic Lua evaluation.

        Returns:
            True if lock was released, False if lock was already expired or belonged to another owner.
        """
        released: bool = False
        if not self.is_acquired:
            return released

        try:
            result: Any = await self.redis.eval(
                RELEASE_LOCK_LUA_SCRIPT,
                1,
                self.lock_key,
                self.lock_token,
            )
            if result == 1:
                released = True
        except (RedisError, OSError) as err:
            logger.error(f"Error releasing distributed lock for {self.lock_key}: {err}")
        finally:
            self.is_acquired = False

        return released

    async def __aenter__(self) -> Self:
        """Context manager entry point."""
        acquired: bool = await self.acquire(timeout_seconds=5.0)
        if not acquired:
            raise LockAcquisitionError(
                f"Could not acquire distributed lock for resource: {self.lock_key} within timeout"
            )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit point."""
        await self.release()

