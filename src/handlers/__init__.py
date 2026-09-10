"""Business logic handlers registry and built-in task executors."""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from src.handlers.db_bulk import handle_db_bulk
from src.handlers.http_batch import handle_http_batch

TaskHandler = Callable[[list[dict[str, Any]], dict[str, Any]], Coroutine[Any, Any, None]]

HANDLERS: dict[str, TaskHandler] = {}


async def _handle_demo_simulation(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Default fallback simulation handler for lightweight testing and demo runs.

    Args:
        chunk: Sliced list of data items.
        parameters: Execution parameters.
    """
    delay = float(parameters.get("simulated_delay_seconds", 0.01))
    await asyncio.sleep(delay)


def task_handler(task_type: str) -> Callable[[TaskHandler], TaskHandler]:
    """Decorator to register a custom task handler function.

    Args:
        task_type: Unique task type string identifier.

    Returns:
        Decorator function.
    """
    def decorator(func: TaskHandler) -> TaskHandler:
        HANDLERS[task_type] = func
        return func

    return decorator


def register_handler(task_type: str, handler: TaskHandler) -> None:
    """Registers a handler directly into the central registry.

    Args:
        task_type: Unique task type identifier.
        handler: Async callable executing a batch chunk.
    """
    HANDLERS[task_type] = handler


def get_handler(task_type: str) -> TaskHandler:
    """Retrieves a registered handler or falls back to demo simulation.

    Args:
        task_type: Requested task type identifier.

    Returns:
        Callable task handler.
    """
    result = HANDLERS.get(task_type)
    if result is None:
        result = _handle_demo_simulation
    return result


# Pre-register built-in enterprise executors
register_handler("demo", _handle_demo_simulation)
register_handler("simulate", _handle_demo_simulation)
register_handler("http_batch", handle_http_batch)
register_handler("db_bulk", handle_db_bulk)

__all__ = [
    "HANDLERS",
    "TaskHandler",
    "get_handler",
    "register_handler",
    "task_handler",
]
