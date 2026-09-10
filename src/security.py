"""Security utilities for webhook validation, SSRF protection, and cryptographic signing."""

import hashlib
import hmac
import ipaddress
import logging
import socket
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def is_safe_webhook_url(url: str, allow_local: bool = False) -> tuple[bool, str]:
    """Validates target URL against SSRF attacks and internal network reconnaissance.

    Checks scheme, resolves domain names, and ensures IP addresses are not loopback,
    private, link-local (cloud metadata), or reserved subnets.

    Args:
        url: The destination webhook URL to check.
        allow_local: If True, bypasses loopback/private checks for local development/testing.

    Returns:
        Tuple of (is_safe, error_reason).
    """
    is_safe: bool = True
    error_reason: str = ""

    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            is_safe = False
            error_reason = f"Unsupported URL scheme: '{parsed.scheme}'. Only http/https are allowed"
        elif not parsed.hostname:
            is_safe = False
            error_reason = "Missing hostname in destination webhook URL"
        elif not allow_local:
            hostname: str = parsed.hostname

            # Explicit check for known internal aliases
            if hostname.lower() in ("localhost", "127.0.0.1", "::1", "metadata.google.internal"):
                is_safe = False
                error_reason = f"Access to internal host '{hostname}' is prohibited"
            else:
                port = parsed.port or (443 if parsed.scheme == "https" else 80)
                addr_info = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)

                for _family, _socktype, _proto, _canonname, sockaddr in addr_info:
                    ip_str = sockaddr[0]
                    ip_obj = ipaddress.ip_address(ip_str)

                    if ip_obj.is_loopback:
                        is_safe = False
                        error_reason = f"Resolved IP '{ip_str}' is a loopback address"
                        break
                    if ip_obj.is_private:
                        is_safe = False
                        error_reason = f"Resolved IP '{ip_str}' is within a private subnet"
                        break
                    if ip_obj.is_link_local:
                        is_safe = False
                        error_reason = f"Resolved IP '{ip_str}' is a link-local/cloud metadata address"
                        break
                    if ip_obj.is_reserved or ip_obj.is_multicast:
                        is_safe = False
                        error_reason = f"Resolved IP '{ip_str}' is reserved or multicast"
                        break
    except (socket.gaierror, ValueError) as err:
        is_safe = False
        error_reason = f"Could not validate destination host: {err}"

    return is_safe, error_reason


def generate_hmac_signature(payload_bytes: bytes, secret: str) -> str:
    """Calculates SHA256 HMAC signature for webhook payload authentication.

    Args:
        payload_bytes: Raw JSON bytes of the request body.
        secret: Pre-shared signing key.

    Returns:
        Formatted signature string 'sha256=<hex_digest>'.
    """
    signature = hmac.new(
        key=secret.encode("utf-8"),
        msg=payload_bytes,
        digestmod=hashlib.sha256,
    ).hexdigest()
    return f"sha256={signature}"
