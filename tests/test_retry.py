import json
import ssl

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from src.retry import execute_with_backoff, is_retryable_http_error, is_retryable_error


class Request:
    def __init__(self, outcome: object) -> None:
        self.outcome = outcome

    def execute(self) -> object:
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def http_error(status: int, reason: str) -> HttpError:
    content = json.dumps({"error": {"errors": [{"reason": reason}]}}).encode()
    return HttpError(Response({"status": status}), content)


def test_only_rate_limit_403_is_retryable() -> None:
    assert is_retryable_http_error(http_error(403, "rateLimitExceeded"))
    assert not is_retryable_http_error(http_error(403, "storageQuotaExceeded"))


def test_retry_is_bounded_and_uses_backoff() -> None:
    outcomes = iter([http_error(429, "rateLimitExceeded"), {"ok": True}])
    sleeps: list[float] = []
    attempts = 0

    def count() -> None:
        nonlocal attempts
        attempts += 1

    result = execute_with_backoff(
        lambda: Request(next(outcomes)),
        max_attempts=3,
        initial_delay=1,
        max_delay=16,
        on_attempt=count,
        sleep=sleeps.append,
        random_value=lambda: 0.25,
    )
    assert result == {"ok": True}
    assert attempts == 2
    assert sleeps == [1.25]


def test_non_retryable_error_is_immediate() -> None:
    error = http_error(403, "insufficientFilePermissions")
    with pytest.raises(HttpError):
        execute_with_backoff(
            lambda: Request(error),
            max_attempts=5,
            initial_delay=1,
            max_delay=16,
            sleep=lambda _: None,
        )


def test_tls_eof_recovers_with_bounded_backoff():
    error = ssl.SSLEOFError(8, 'EOF occurred in violation of protocol (_ssl.c:2427)')
    outcomes = iter([error, {'ok': True}])
    sleeps = []
    assert execute_with_backoff(lambda: Request(next(outcomes)), max_attempts=3,
        initial_delay=1, max_delay=8, sleep=sleeps.append,
        random_value=lambda: 0) == {'ok': True}
    assert sleeps == [1]


def test_persistent_tls_eof_stops_at_attempt_limit():
    calls = []
    error = ssl.SSLEOFError(8, 'EOF occurred in violation of protocol (_ssl.c:2427)')
    def factory():
        calls.append(1)
        return Request(error)
    sleeps = []
    with pytest.raises(ssl.SSLEOFError):
        execute_with_backoff(factory, max_attempts=3, initial_delay=1,
            max_delay=8, sleep=sleeps.append, random_value=lambda: 0)
    assert len(calls) == 3
    assert sleeps == [1, 2]


@pytest.mark.parametrize('error', [
    ssl.SSLCertVerificationError(1, 'certificate verify failed'),
    ssl.SSLError(1, 'wrong version number'),
    OSError(28, 'No space left on device'),
])
def test_tls_recovery_does_not_retry_security_or_disk_errors(error):
    assert not is_retryable_error(error)

