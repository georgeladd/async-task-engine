"""Unit tests for the Operations CLI utility."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.cli import (
    build_parser,
    check_api_health,
    get_task_status,
    submit_operational_task,
    unlock_stale_resource,
)


def test_cli_parser_build() -> None:
    """Verifies that argument parser configures all operational subcommands."""
    parser = build_parser()

    # Test submit subcommand
    submit_args = parser.parse_args(["submit", "--resource", "tenant_123", "--items", "10"])
    assert submit_args.command == "submit"
    assert submit_args.resource == "tenant_123"
    assert submit_args.items == 10

    # Test status subcommand
    random_uuid = str(uuid4())
    status_args = parser.parse_args(["status", random_uuid])
    assert status_args.command == "status"
    assert status_args.task_id == random_uuid

    # Test unlock subcommand
    unlock_args = parser.parse_args(["unlock", "tenant_123"])
    assert unlock_args.command == "unlock"
    assert unlock_args.resource == "tenant_123"


@pytest.mark.asyncio
async def test_cli_check_api_health_success() -> None:
    """Tests health check subcommand when API is available."""
    mock_response = MagicMock()
    mock_response.is_success = True
    mock_response.json.return_value = {"status": "ok", "service": "async-task-engine"}

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response):
        exit_code = await check_api_health("http://testserver")
        assert exit_code == 0


@pytest.mark.asyncio
async def test_cli_submit_task_success() -> None:
    """Tests task submission subcommand success path."""
    mock_response = MagicMock()
    mock_response.is_success = True
    mock_response.json.return_value = {
        "task_id": str(uuid4()),
        "status": "pending",
        "message": "Enqueued",
    }

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response):
        exit_code = await submit_operational_task(
            api_url="http://testserver",
            task_type="test_sync",
            resource_id="cluster_1",
            priority="normal",
            items_count=5,
        )
        assert exit_code == 0


@pytest.mark.asyncio
async def test_cli_get_status_invalid_uuid() -> None:
    """Tests get_task_status rejection when invalid UUID format is supplied."""
    exit_code = await get_task_status("http://testserver", "not-a-valid-uuid")
    assert exit_code == 1


@pytest.mark.asyncio
async def test_cli_unlock_resource(mock_redis: AsyncMock) -> None:
    """Tests unlock subcommand with mock Redis client."""
    with patch("redis.asyncio.from_url", return_value=mock_redis):
        # Resource does not exist
        mock_redis.exists = AsyncMock(return_value=False)
        code_not_found = await unlock_stale_resource("test_res")
        assert code_not_found == 0

        # Resource exists and is deleted
        mock_redis.exists = AsyncMock(return_value=True)
        mock_redis.ttl = AsyncMock(return_value=120)
        mock_redis.delete = AsyncMock(return_value=1)
        code_deleted = await unlock_stale_resource("test_res")
        assert code_deleted == 0
