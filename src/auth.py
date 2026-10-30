"""Google Drive Service Account authentication."""

from __future__ import annotations

import logging
import gzip
import threading
import ssl
from urllib.parse import urlsplit
import httplib2
from google_auth_httplib2 import Request, AuthorizedHttp
from pathlib import Path
from typing import Any

from google.auth.exceptions import GoogleAuthError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import Error as GoogleAPIClientError
from googleapiclient.http import build_http

from src.errors import AuthenticationError
from src.config import Settings
from src.retry import execute_with_backoff

LOGGER = logging.getLogger(__name__)
DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"


class _RecoverableAuthorizedHttp(AuthorizedHttp):
    """Discard a broken TLS connection; callers own bounded/idempotent retries."""

    batch_response_gzip = False  # Benchmark opt-in: explicit negotiation showed no gain.
    batch_request_gzip = False  # Benchmark opt-in; endpoint support is tested separately.

    def request(self, uri, method='GET', body=None, headers=None, redirections=5,
                connection_type=None, **kwargs):
        endpoint = urlsplit(uri) if self.batch_response_gzip or self.batch_request_gzip else None
        if (endpoint is not None and endpoint.scheme == 'https'
                and endpoint.netloc == 'www.googleapis.com'
                and endpoint.path == '/batch/drive/v3'):
            headers = dict(headers or {})
            keys = {key.lower(): key for key in headers}
            if self.batch_response_gzip:
                headers.setdefault(keys.get('accept-encoding', 'accept-encoding'), 'gzip')
                ua_key = keys.get('user-agent', 'user-agent')
                ua = headers.get(ua_key, 'metadata-drive-storage')
                if 'gzip' not in ua.lower():
                    headers[ua_key] = ua + ' (gzip)'
            if (self.batch_request_gzip and method.upper() == 'POST'
                    and isinstance(body, (str, bytes)) and body and 'content-encoding' not in keys):
                raw = body.encode('utf-8') if isinstance(body, str) else body
                body = gzip.compress(raw, compresslevel=1, mtime=0)
                headers['content-encoding'] = 'gzip'
                headers[keys.get('content-length', 'content-length')] = str(len(body))
                self.last_batch_request_body_bytes = len(body)
        try:
            return super().request(uri, method=method, body=body, headers=headers,
                redirections=redirections, connection_type=connection_type, **kwargs)
        except ssl.SSLEOFError:
            # This HTTP instance belongs to one worker. close() clears its
            # connection cache, preserving credentials, TLS checks and timeout.
            # Never replay here: a create may already have reached Drive.
            self.http.close()
            raise


def _authorized_http(credentials, timeout):
    # Preserve the SDK's redirect handling (notably HTTP 308) without changing
    # global socket settings. No connection exists yet when timeout is set.
    http = build_http()
    http.timeout = timeout
    return _RecoverableAuthorizedHttp(credentials, http=http)


class DriveServiceFactory:
    """Load the key once and reuse the initial token, with independent credentials/HTTP.

    Existing clients refresh their own tokens after expiry; a new client gets a
    cached valid token (or refreshes the factory's cache under the lock).
    """

    def __init__(self, credentials_path: Path, *, http_timeout_seconds: int = 60) -> None:
        if not 5 <= http_timeout_seconds <= 120:
            raise ValueError("HTTP timeout must be between 5 and 120 seconds")
        self.http_timeout_seconds = http_timeout_seconds
        if not credentials_path.is_file():
            raise AuthenticationError(f"credential file not found: {credentials_path}")
        try:
            self._credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path), scopes=[DRIVE_SCOPE],
            )
        except (GoogleAuthError, OSError, ValueError) as exc:
            raise AuthenticationError("failed to load Drive credentials") from exc
        self._lock = threading.Lock()

    def __call__(self) -> Any:
        try:
            with self._lock:
                if not self._credentials.valid:
                    http = httplib2.Http(timeout=60)
                    try:
                        self._credentials.refresh(Request(http))
                    finally:
                        http.close()
                credentials = self._credentials.with_scopes([DRIVE_SCOPE])
                credentials.token = self._credentials.token
                credentials.expiry = self._credentials.expiry
            # Neither HTTP connections nor mutable token state are shared by workers.
            transport = _authorized_http(credentials, self.http_timeout_seconds)
            try:
                return build("drive", "v3", http=transport, cache_discovery=False)
            except Exception:
                transport.close()
                raise
        except (GoogleAuthError, GoogleAPIClientError, OSError, ValueError) as exc:
            raise AuthenticationError("failed to initialize cached Drive authentication") from exc


def build_drive_service(credentials_path: Path, *, http_timeout_seconds: int = 60) -> Any:
    """Build an authorized Drive API v3 service from a Service Account file."""
    if not 5 <= http_timeout_seconds <= 120:
        raise ValueError("HTTP timeout must be between 5 and 120 seconds")
    if not credentials_path.is_file():
        raise AuthenticationError(f"credential file not found: {credentials_path}")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path), scopes=[DRIVE_SCOPE]
        )
        transport = _authorized_http(credentials, http_timeout_seconds)
        try:
            return build("drive", "v3", http=transport, cache_discovery=False)
        except Exception:
            transport.close()
            raise
    except (GoogleAuthError, GoogleAPIClientError, OSError, ValueError) as exc:
        LOGGER.error("Unable to initialize Google Drive authentication: %s", exc)
        raise AuthenticationError("failed to initialize Google Drive client") from exc


def verify_target_folder(service: Any, settings: Settings) -> dict[str, Any]:
    """Verify credentials, target visibility, and permission to add children."""
    try:
        folder = execute_with_backoff(
            lambda: service.files().get(
                fileId=settings.drive_folder_id,
                fields="id,name,mimeType,capabilities(canAddChildren)",
                supportsAllDrives=True,
            ),
            max_attempts=settings.max_attempts,
            initial_delay=settings.initial_delay_seconds,
            max_delay=settings.max_delay_seconds,
        )
    except (GoogleAuthError, GoogleAPIClientError, OSError) as exc:
        LOGGER.error("Google Drive connection check failed: %s", exc)
        raise AuthenticationError("could not access the configured Drive folder") from exc
    if folder.get("mimeType") != "application/vnd.google-apps.folder":
        raise AuthenticationError("GOOGLE_DRIVE_FOLDER_ID does not identify a folder")
    if not folder.get("capabilities", {}).get("canAddChildren", False):
        raise AuthenticationError("Service Account cannot add files to the target folder")
    return folder
