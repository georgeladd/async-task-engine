"""Support and Operations CLI utility for Async Task Engine management."""

import argparse
import asyncio
import json
import sys
from typing import Any
from uuid import UUID

import httpx
import redis.asyncio as aioredis
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.config import settings


async def check_api_health(api_url: str) -> int:
    """Checks the health endpoint of the running API service.

    Args:
        api_url: Base URL of the API.

    Returns:
        0 on success, 1 on connection failure.
    """
    exit_code: int = 0
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{api_url}/health")
            if not response.is_success:
                print(f"Health check failed with status: {response.status_code}")
                exit_code = 1
            else:
                data = response.json()
                print(f"Service status: {data.get('status')} | Service name: {data.get('service')}")
    except (httpx.HTTPError, OSError) as err:
        print(f"Failed to connect to API service at {api_url}: {err}")
        exit_code = 1

    return exit_code


async def submit_operational_task(
    api_url: str,
    task_type: str,
    resource_id: str,
    priority: str,
    items_count: int,
) -> int:
    """Submits a new task for asynchronous processing via the API.

    Args:
        api_url: Base URL of the API service.
        task_type: Identifier of the business logic handler.
        resource_id: Resource identifier key for distributed locking.
        priority: Priority string ('low', 'normal', 'high', 'critical').
        items_count: Number of sample records to simulate inside task payload.

    Returns:
        0 on successful enqueuing, 1 on rejection or error.
    """
    exit_code: int = 0
    payload: dict[str, Any] = {
        "task_type": task_type,
        "resource_id": resource_id,
        "priority": priority,
        "payload": {
            "items": [{"id": idx, "data": f"record_{idx}"} for idx in range(items_count)],
            "parameters": {"submitted_via": "ops-cli"},
        },
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(f"{api_url}/api/v1/tasks", json=payload)
            if not response.is_success:
                print(f"Task submission rejected: HTTP {response.status_code} - {response.text}")
                exit_code = 1
            else:
                result = response.json()
                print("Task successfully submitted:")
                print(f"  Task UUID: {result.get('task_id')}")
                print(f"  Initial Status: {result.get('status')}")
                print(f"  Message: {result.get('message')}")
    except (httpx.HTTPError, OSError) as err:
        print(f"Failed to submit task: {err}")
        exit_code = 1

    return exit_code


async def get_task_status(api_url: str, task_id: str) -> int:
    """Queries current status and execution metrics for a specific task UUID.

    Args:
        api_url: Base URL of the API service.
        task_id: UUID of the target task.

    Returns:
        0 on success, 1 on failure.
    """
    exit_code: int = 0
    try:
        # Validate UUID structure
        UUID(task_id)
    except ValueError:
        print(f"Invalid UUID string format: {task_id}")
        return 1

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{api_url}/api/v1/tasks/{task_id}")
            if not response.is_success:
                print(f"Task {task_id} not found or query error: HTTP {response.status_code}")
                exit_code = 1
            else:
                data = response.json()
                print(f"Task {task_id} Status Details:")
                print(f"  Current Status: {data.get('status')}")
                raw_result = data.get("result")
                if raw_result:
                    parsed_result = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
                    print(f"  Processed Count: {parsed_result.get('processed_count')}")
                    print(f"  Total Chunks: {parsed_result.get('chunk_count')}")
                    print(f"  Execution Time: {parsed_result.get('execution_time_seconds')}s")
                    if parsed_result.get("error_message"):
                        print(f"  Error Details: {parsed_result.get('error_message')}")
    except (httpx.HTTPError, OSError) as err:
        print(f"Failed to query task status: {err}")
        exit_code = 1

    return exit_code


async def unlock_stale_resource(resource_id: str) -> int:
    """Manually releases an orphaned distributed lock key in Redis.

    Args:
        resource_id: Resource identifier to clear from lock state.

    Returns:
        0 on success, 1 on error.
    """
    exit_code: int = 0
    lock_key: str = f"lock:resource:{resource_id}"
    redis: Redis = aioredis.from_url(settings.redis_uri, encoding="utf-8", decode_responses=True)

    try:
        exists = await redis.exists(lock_key)
        if not exists:
            print(f"Resource '{resource_id}' is not currently locked (key {lock_key} not found)")
        else:
            ttl = await redis.ttl(lock_key)
            await redis.delete(lock_key)
            print(f"Successfully released lock for '{resource_id}' (previous TTL was {ttl}s)")
    except (RedisError, OSError) as err:
        print(f"Failed to connect to Redis or release lock: {err}")
        exit_code = 1
    finally:
        await redis.close()

    return exit_code


async def list_active_locks() -> int:
    """Lists all active distributed resource locks currently registered in Redis.

    Returns:
        0 on success, 1 on error.
    """
    exit_code: int = 0
    redis: Redis = aioredis.from_url(settings.redis_uri, encoding="utf-8", decode_responses=True)

    try:
        keys = await redis.keys("lock:resource:*")
        if not keys:
            print("No active resource locks found in Redis")
        else:
            print(f"Found {len(keys)} active distributed lock(s):")
            for key in keys:
                ttl = await redis.ttl(key)
                token = await redis.get(key)
                resource_name = key.replace("lock:resource:", "")
                print(f"  - Resource: '{resource_name}' | TTL remaining: {ttl}s | Owner token: {token}")
    except (RedisError, OSError) as err:
        print(f"Failed to inspect active locks: {err}")
        exit_code = 1
    finally:
        await redis.close()

    return exit_code


def build_parser() -> argparse.ArgumentParser:
    """Constructs command line argument parser for support operations.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="ops-cli",
        description="Support & Operations CLI for Async Task Engine",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Target API base URL (default: http://localhost:8000)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Health command
    subparsers.add_parser("health", help="Check health status of API service")

    # Submit task command
    submit_parser = subparsers.add_parser("submit", help="Submit a new operational task")
    submit_parser.add_argument("--type", default="data_maintenance", help="Task type category")
    submit_parser.add_argument("--resource", required=True, help="Resource ID for distributed lock")
    submit_parser.add_argument(
        "--priority",
        choices=["low", "normal", "high", "critical"],
        default="normal",
        help="Task priority",
    )
    submit_parser.add_argument(
        "--items",
        type=int,
        default=50,
        help="Simulated payload items count (default: 50)",
    )

    # Status command
    status_parser = subparsers.add_parser("status", help="Query task execution status by UUID")
    status_parser.add_argument("task_id", help="Task UUID to query")

    # Unlock command
    unlock_parser = subparsers.add_parser("unlock", help="Manually release stuck distributed lock")
    unlock_parser.add_argument("resource", help="Target resource ID to unlock")

    # List locks command
    subparsers.add_parser("locks", help="List all active distributed resource locks")

    return parser


async def main() -> int:
    """CLI entry point executing mapped subcommands.

    Returns:
        Process exit code.
    """
    parser = build_parser()
    args = parser.parse_args()

    exit_code: int = 0
    if args.command == "health":
        exit_code = await check_api_health(args.api_url)
    elif args.command == "submit":
        exit_code = await submit_operational_task(
            api_url=args.api_url,
            task_type=args.type,
            resource_id=args.resource,
            priority=args.priority,
            items_count=args.items,
        )
    elif args.command == "status":
        exit_code = await get_task_status(args.api_url, args.task_id)
    elif args.command == "unlock":
        exit_code = await unlock_stale_resource(args.resource)
    elif args.command == "locks":
        exit_code = await list_active_locks()

    return exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
