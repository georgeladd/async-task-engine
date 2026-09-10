"""Tests for fail-fast production security settings validation."""

import pytest
from pydantic import ValidationError

from src.config import Settings


def test_config_allows_defaults_in_development_mode() -> None:
    """Verifies that development and local modes permit default secrets."""
    settings = Settings(
        environment="local",
        ops_api_key="ops-dev-secret",
        webhook_signing_secret="webhook-dev-secret",
        allow_local_webhooks=True,
    )
    assert settings.environment == "local"
    assert settings.ops_api_key == "ops-dev-secret"


def test_config_rejects_weak_ops_api_key_in_production() -> None:
    """Verifies that weak or default OPS_API_KEY is rejected in production."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            environment="production",
            ops_api_key="ops-dev-secret",
            webhook_signing_secret="super-strong-webhook-signing-secret-32-chars",
            allow_local_webhooks=False,
        )
    assert "Production security violation: OPS_API_KEY" in str(exc_info.value)


def test_config_rejects_weak_webhook_secret_in_production() -> None:
    """Verifies that weak or default WEBHOOK_SIGNING_SECRET is rejected in production."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            environment="production",
            ops_api_key="super-strong-ops-api-secret-key-32-chars",
            webhook_signing_secret="short",
            allow_local_webhooks=False,
        )
    assert "Production security violation: WEBHOOK_SIGNING_SECRET" in str(exc_info.value)


def test_config_rejects_local_webhooks_in_production() -> None:
    """Verifies that ALLOW_LOCAL_WEBHOOKS cannot be enabled in production."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            environment="production",
            ops_api_key="super-strong-ops-api-secret-key-32-chars",
            webhook_signing_secret="super-strong-webhook-signing-secret-32-chars",
            allow_local_webhooks=True,
        )
    assert "ALLOW_LOCAL_WEBHOOKS cannot be enabled in production" in str(exc_info.value)


def test_config_accepts_strong_secrets_in_production() -> None:
    """Verifies that production initialization succeeds with hardened credentials."""
    settings = Settings(
        environment="production",
        ops_api_key="super-strong-ops-api-secret-key-32-chars",
        webhook_signing_secret="super-strong-webhook-signing-secret-32-chars",
        allow_local_webhooks=False,
    )
    assert settings.environment == "production"
    assert len(settings.ops_api_key) >= 16
