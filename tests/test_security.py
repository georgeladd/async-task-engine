"""Unit tests for security utilities, SSRF validation, and webhook HMAC signatures."""

import socket
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from src.schemas import TaskMessage, TaskPayload, TaskPriority, TaskStatus
from src.security import generate_hmac_signature, is_safe_webhook_url
from src.worker import TaskWorker


def test_is_safe_webhook_url_rejects_internal_and_private_hosts() -> None:
    """Verifies that is_safe_webhook_url blocks SSRF attempts on internal networks."""
    # 1. Loopback addresses
    assert is_safe_webhook_url("http://127.0.0.1/hook")[0] is False
    assert is_safe_webhook_url("http://localhost:8000/hook")[0] is False
    assert is_safe_webhook_url("http://[::1]/hook")[0] is False

    # 2. Cloud metadata link-local address
    assert is_safe_webhook_url("http://169.254.169.254/latest/meta-data/")[0] is False
    assert is_safe_webhook_url("http://metadata.google.internal/computeMetadata/v1/")[0] is False

    # 3. Invalid schemes
    assert is_safe_webhook_url("ftp://example.com/file")[0] is False
    assert is_safe_webhook_url("gopher://example.com")[0] is False

    # 4. Private IPv4 subnets
    assert is_safe_webhook_url("https://10.1.2.3/notify")[0] is False
    assert is_safe_webhook_url("https://192.168.1.100/notify")[0] is False
    assert is_safe_webhook_url("https://172.16.5.10/notify")[0] is False


def test_is_safe_webhook_url_allows_local_when_flag_set() -> None:
    """Verifies that local URLs are permitted when allow_local is True."""
    is_safe, _ = is_safe_webhook_url("http://127.0.0.1:8000/hook", allow_local=True)
    assert is_safe is True

    is_safe, _ = is_safe_webhook_url("http://localhost:3000/callback", allow_local=True)
    assert is_safe is True


def test_is_safe_webhook_url_allows_public_domain() -> None:
    """Verifies that public domains resolving to external IPs are permitted."""
    mock_addrinfo = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
    ]
    with patch("socket.getaddrinfo", return_value=mock_addrinfo):
        is_safe, reason = is_safe_webhook_url("https://api.partner.com/webhook")
        assert is_safe is True
        assert reason == ""


def test_generate_hmac_signature_correctness() -> None:
    """Verifies SHA256 HMAC signature calculation."""
    payload = b'{"status":"completed"}'
    secret = "test-secret-key"
    sig1 = generate_hmac_signature(payload, secret)
    sig2 = generate_hmac_signature(payload, secret)

    assert sig1.startswith("sha256=")
    assert sig1 == sig2
    assert len(sig1) == 7 + 64

    # Different secret yields different signature
    different_sig = generate_hmac_signature(payload, "other-secret")
    assert sig1 != different_sig


@pytest.mark.asyncio
async def test_worker_dispatch_webhook_rejects_ssrf() -> None:
    """Verifies that worker blocks webhook calls to private or cloud metadata IPs."""
    worker = TaskWorker()
    task = TaskMessage(
        task_id=uuid4(),
        task_type="sync_task",
        resource_id="res_1",
        priority=TaskPriority.NORMAL,
        payload=TaskPayload(items=[]),
    )

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        # Private cloud metadata IP
        await worker.dispatch_webhook(
            callback_url="http://169.254.169.254/latest/meta-data/",
            event="task.completed",
            task=task,
            status=TaskStatus.COMPLETED,
        )
        mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_worker_dispatch_webhook_attaches_hmac_header() -> None:
    """Verifies that worker attaches X-Hub-Signature-256 header for safe destinations."""
    worker = TaskWorker()
    task = TaskMessage(
        task_id=uuid4(),
        task_type="sync_task",
        resource_id="res_1",
        priority=TaskPriority.NORMAL,
        payload=TaskPayload(items=[]),
    )

    mock_response = AsyncMock()
    mock_response.is_success = True
    mock_response.status_code = 200

    mock_addrinfo = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
    ]

    with (
        patch("socket.getaddrinfo", return_value=mock_addrinfo),
        patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response) as mock_post,
    ):
        await worker.dispatch_webhook(
            callback_url="https://api.external.com/webhook",
            event="task.completed",
            task=task,
            status=TaskStatus.COMPLETED,
        )
        mock_post.assert_called_once()
        headers = mock_post.call_args.kwargs["headers"]
        assert "X-Hub-Signature-256" in headers
        assert headers["X-Hub-Signature-256"].startswith("sha256=")
