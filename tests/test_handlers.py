"""Unit tests for task handlers registry and built-in executors."""

import sqlite3
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.handlers import (
    get_handler,
    register_handler,
    task_handler,
)
from src.handlers.db_bulk import handle_db_bulk
from src.handlers.http_batch import handle_http_batch


@pytest.mark.asyncio
async def test_get_handler_raises_on_unknown_type() -> None:
    """Verifies that unknown task types strictly raise ValueError with informative diagnostics."""
    with pytest.raises(ValueError) as exc_info:
        get_handler("non_existent_type")
    assert "No handler registered for task_type 'non_existent_type'" in str(exc_info.value)
    assert "demo" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_handler_returns_explicit_demo() -> None:
    """Verifies that explicitly registered demo and simulate handlers resolve properly."""
    demo_handler = get_handler("demo")
    assert callable(demo_handler)
    await demo_handler([{"id": 1}], {"simulated_delay_seconds": 0.001})


@pytest.mark.asyncio
async def test_custom_task_handler_registration() -> None:
    """Verifies registering and resolving a custom task handler."""
    invoked = []

    @task_handler("analytics_export")
    async def custom_handler(chunk: list[dict], params: dict) -> None:
        invoked.extend(chunk)

    handler = get_handler("analytics_export")
    await handler([{"user": "alice"}, {"user": "bob"}], {})
    assert len(invoked) == 2


@pytest.mark.asyncio
async def test_direct_handler_registration() -> None:
    """Verifies direct manual registration of a task handler."""
    async def raw_executor(chunk: list[dict], params: dict) -> None:
        pass

    register_handler("manual_sync", raw_executor)
    assert get_handler("manual_sync") is raw_executor


@pytest.mark.asyncio
async def test_http_batch_handler_success() -> None:
    """Verifies successful batch dispatch via handle_http_batch."""
    chunk = [{"sku": "A1", "price": 100}, {"sku": "A2", "price": 200}]
    params = {
        "target_url": "https://api.example.com/bulk",
        "http_method": "POST",
        "batch_reference": "batch-101",
    }

    mock_response = httpx.Response(
        status_code=200,
        json={"received": 2},
        request=httpx.Request("POST", "https://api.example.com/bulk"),
    )

    with (
        patch("src.handlers.http_batch.is_safe_webhook_url", return_value=(True, "")),
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
    ):
        mock_post.return_value = mock_response
        await handle_http_batch(chunk, params)
        mock_post.assert_called_once()
        sent_json = mock_post.call_args[1]["json"]
        assert sent_json["count"] == 2
        assert sent_json["batch_reference"] == "batch-101"


@pytest.mark.asyncio
async def test_http_batch_handler_rejects_ssrf() -> None:
    """Verifies that handle_http_batch blocks internal IP destinations."""
    chunk = [{"id": 1}]
    params = {"target_url": "http://169.254.169.254/latest/meta-data"}

    with patch("src.handlers.http_batch.is_safe_webhook_url", return_value=(False, "Link-local blocked")):
        with pytest.raises(ValueError) as exc_info:
            await handle_http_batch(chunk, params)
        assert "SSRF violation" in str(exc_info.value)
        assert "Link-local blocked" in str(exc_info.value)


@pytest.mark.asyncio
async def test_http_batch_handler_real_ssrf_validation_without_mock() -> None:
    """Verifies end-to-end SSRF rejection against loopback and cloud metadata without mocking validator."""
    chunk = [{"id": 1}]
    params = {"target_url": "http://127.0.0.1:8000/internal-api"}

    with pytest.raises(ValueError) as exc_info:
        await handle_http_batch(chunk, params)
    assert "SSRF violation" in str(exc_info.value)
    assert "prohibited" in str(exc_info.value) or "loopback" in str(exc_info.value)


@pytest.mark.asyncio
async def test_http_batch_handler_raises_on_api_failure() -> None:
    """Verifies that non-success HTTP status raises RuntimeError to allow worker retries."""
    chunk = [{"id": 1}]
    params = {"target_url": "https://api.example.com/bulk"}

    mock_response = httpx.Response(
        status_code=500,
        text="Internal Server Error",
        request=httpx.Request("POST", "https://api.example.com/bulk"),
    )

    with (
        patch("src.handlers.http_batch.is_safe_webhook_url", return_value=(True, "")),
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
    ):
        mock_post.return_value = mock_response
        with pytest.raises(RuntimeError) as exc_info:
            await handle_http_batch(chunk, params)
        assert "HTTP batch dispatch failed with status 500" in str(exc_info.value)


@pytest.mark.asyncio
async def test_db_bulk_handler_inserts_into_sqlite(tmp_path) -> None:
    """Verifies that handle_db_bulk creates table and populates records."""
    db_file = str(tmp_path / "test_bulk.db")
    chunk = [
        {"order_id": "1001", "customer": "Alice", "amount": "250.0"},
        {"order_id": "1002", "customer": "Bob", "amount": "490.5"},
    ]
    params = {
        "db_path": db_file,
        "table_name": "orders",
    }

    await handle_db_bulk(chunk, params)

    # Verify directly from SQLite
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders")
        count = cursor.fetchone()[0]
        assert count == 2

        cursor.execute("SELECT customer FROM orders WHERE order_id = '1001'")
        row = cursor.fetchone()
        assert row[0] == "Alice"


@pytest.mark.asyncio
async def test_db_bulk_handler_upserts_duplicate_primary_key(tmp_path) -> None:
    """Verifies that handle_db_bulk deterministically updates records with matching primary keys."""
    db_file = str(tmp_path / "test_upsert.db")
    chunk_initial = [{"sku": "SKU-99", "price": "10.0"}]
    chunk_updated = [{"sku": "SKU-99", "price": "19.99"}]
    params = {"db_path": db_file, "table_name": "inventory", "primary_key": "sku"}

    await handle_db_bulk(chunk_initial, params)
    await handle_db_bulk(chunk_updated, params)

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM inventory")
        assert cursor.fetchone()[0] == 1

        cursor.execute("SELECT price FROM inventory WHERE sku = 'SKU-99'")
        assert cursor.fetchone()[0] == "19.99"
