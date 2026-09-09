"""Memory-safe chunked batch streaming utilities."""

from collections.abc import AsyncIterator, Iterable, Iterator
from typing import TypeVar

T = TypeVar("T")


def chunk_iterator(items: Iterable[T], chunk_size: int) -> Iterator[list[T]]:
    """Splits an iterable into fixed-size chunks without loading entire datasets into memory.

    Args:
        items: Source collection or generator of data elements.
        chunk_size: Maximum quantity of items inside each chunk. Must be positive.

    Yields:
        Lists of elements with length up to chunk_size.

    Raises:
        ValueError: If chunk_size is less than 1.
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be greater than 0, got {chunk_size}")

    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


async def async_chunk_stream(
    stream: AsyncIterator[T],
    chunk_size: int,
) -> AsyncIterator[list[T]]:
    """Asynchronously groups incoming stream items into fixed-size batch chunks.

    Args:
        stream: Asynchronous iterator of incoming data items.
        chunk_size: Target size for grouped batches.

    Yields:
        Asynchronous lists of batch items.

    Raises:
        ValueError: If chunk_size is less than 1.
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be greater than 0, got {chunk_size}")

    chunk: list[T] = []
    async for item in stream:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def compute_chunk_metrics(total_items: int, chunk_size: int) -> dict[str, int]:
    """Calculates expected chunk count and item distribution.

    Args:
        total_items: Total quantity of items to process.
        chunk_size: Batch size per iteration.

    Returns:
        Dictionary with total items, chunk size, and expected batch count.
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")

    if total_items < 0:
        raise ValueError(f"total_items cannot be negative, got {total_items}")

    chunk_count: int = (total_items + chunk_size - 1) // chunk_size if total_items > 0 else 0
    result: dict[str, int] = {
        "total_items": total_items,
        "chunk_size": chunk_size,
        "chunk_count": chunk_count,
    }
    return result
