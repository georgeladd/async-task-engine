"""Tests for memory-safe chunking utilities."""

import pytest

from src.chunker import async_chunk_stream, chunk_iterator, compute_chunk_metrics


def test_chunk_iterator_exact_division() -> None:
    """Tests chunking when items divide evenly by chunk size."""
    data = list(range(10))
    chunks = list(chunk_iterator(data, chunk_size=5))
    assert len(chunks) == 2
    assert chunks[0] == [0, 1, 2, 3, 4]
    assert chunks[1] == [5, 6, 7, 8, 9]


def test_chunk_iterator_uneven_division() -> None:
    """Tests chunking when final batch has fewer elements."""
    data = list(range(7))
    chunks = list(chunk_iterator(data, chunk_size=3))
    assert len(chunks) == 3
    assert chunks[0] == [0, 1, 2]
    assert chunks[1] == [3, 4, 5]
    assert chunks[2] == [6]


def test_chunk_iterator_empty_collection() -> None:
    """Tests chunking an empty collection returns empty generator."""
    chunks = list(chunk_iterator([], chunk_size=10))
    assert chunks == []


def test_chunk_iterator_invalid_size() -> None:
    """Verifies that non-positive chunk sizes raise ValueError."""
    with pytest.raises(ValueError, match="chunk_size must be greater than 0"):
        list(chunk_iterator([1, 2, 3], chunk_size=0))


@pytest.mark.asyncio
async def test_async_chunk_stream() -> None:
    """Tests asynchronous generator batch streaming."""
    async def item_generator():
        for i in range(5):
            yield i

    chunks = []
    async for batch in async_chunk_stream(item_generator(), chunk_size=2):
        chunks.append(batch)

    assert len(chunks) == 3
    assert chunks[0] == [0, 1]
    assert chunks[1] == [2, 3]
    assert chunks[2] == [4]


def test_compute_chunk_metrics() -> None:
    """Tests chunk metrics calculations."""
    metrics = compute_chunk_metrics(total_items=250, chunk_size=100)
    assert metrics["total_items"] == 250
    assert metrics["chunk_size"] == 100
    assert metrics["chunk_count"] == 3

    zero_metrics = compute_chunk_metrics(total_items=0, chunk_size=50)
    assert zero_metrics["chunk_count"] == 0
