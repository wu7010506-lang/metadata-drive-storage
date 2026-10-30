"""Bounded exponential backoff for Google API requests."""

from __future__ import annotations

import json
import logging
import random
import socket
import ssl
import time
from collections.abc import Callable
from typing import Any, TypeVar

from googleapiclient.errors import HttpError

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")

TRANSIENT_STATUSES = frozenset({429, 500, 502, 503, 504})
RATE_LIMIT_REASONS = frozenset(
    {"rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded"}
)


def execute_with_backoff(
    request_factory: Callable[[], Any],
    *,
    max_attempts: int,
    initial_delay: float,
    max_delay: float,
    on_attempt: Callable[[], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    random_value: Callable[[], float] = random.random,
) -> T:
    """Execute an API request with capped exponential backoff and jitter."""
    for attempt in range(1, max_attempts + 1):
        if on_attempt:
            on_attempt()
        try:
            return request_factory().execute()
        except (HttpError, TimeoutError, socket.timeout, OSError) as exc:
            if not is_retryable_error(exc) or attempt == max_attempts:
                raise
            base_delay = min(max_delay, initial_delay * (2 ** (attempt - 1)))
            delay = min(max_delay, base_delay + random_value())
            status = int(getattr(getattr(exc, "resp", None), "status", 0))
            LOGGER.warning(
                "Drive API request failed transiently (%s, HTTP %s, attempt %d/%d); retrying in %.2fs",
                type(exc).__name__,
                status or "transport",
                attempt,
                max_attempts,
                delay,
                extra={"api_status": status},
            )
            sleep(delay)
    raise RuntimeError("retry loop terminated unexpectedly")


def is_retryable_http_error(error: HttpError) -> bool:
    """Return whether an HTTP error is transient or rate-limit related."""
    status = int(getattr(error.resp, "status", 0))
    if status in TRANSIENT_STATUSES:
        return True
    if status != 403:
        return False
    return bool(_error_reasons(error) & RATE_LIMIT_REASONS)


def is_retryable_error(error: BaseException) -> bool:
    """Return whether an API failure is safe to retry."""
    if isinstance(error, HttpError):
        return is_retryable_http_error(error)
    if isinstance(error, (TimeoutError, socket.timeout, ssl.SSLEOFError)):
        return True
    # Windows reports socket timeouts as WSAETIMEDOUT (10060); do not retry
    # unrelated OSErrors such as errno 28 (disk full).
    return isinstance(error, OSError) and getattr(error, "errno", None) in {110, 10060}


def _error_reasons(error: HttpError) -> set[str]:
    try:
        document = json.loads(error.content.decode("utf-8"))
        errors = document.get("error", {}).get("errors", [])
        return {item.get("reason", "") for item in errors if isinstance(item, dict)}
    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
        return set()
