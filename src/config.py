"""Environment-based configuration for the prototype."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

from dotenv import load_dotenv

from src.errors import ConfigurationError

MAX_UPLOAD_MIB = 10 * 1024


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    credentials_path: Path
    drive_folder_id: str
    metadata_backend: str = "appProperties"
    probe_initial_bytes: int = 100
    probe_max_bytes: int = 1_048_576
    safe_margin_percent: float = 15.0
    name_safe_payload_bytes: int = 0
    description_safe_payload_bytes: int = 0
    upload_backend: str = "name"
    storage_encoding: str = "base64url"
    max_attempts: int = 5
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 16.0
    api_min_interval_seconds: float = 0.1
    upload_workers: int = 1
    upload_batch_size: int = 1
    download_workers: int = 1
    max_upload_mb: int = MAX_UPLOAD_MIB
    upload_temp_dir: Path | None = None
    sqlite_path: Path = Path("data/metadata-drive.db")
    log_level: str = "INFO"
    api_http_timeout_seconds: int = 60
    upload_target_requests_per_minute: int = 0  # Experimental, disabled by default.

    @classmethod
    def from_env(cls, env_file: Path | None = None) -> "Settings":
        """Load settings from a .env file and the process environment."""
        load_dotenv(dotenv_path=env_file)
        raw_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        if not raw_credentials:
            raise ConfigurationError("GOOGLE_APPLICATION_CREDENTIALS is required")
        if not folder_id:
            raise ConfigurationError("GOOGLE_DRIVE_FOLDER_ID is required")

        settings = cls(
            credentials_path=Path(raw_credentials).expanduser(),
            drive_folder_id=folder_id,
            metadata_backend=os.getenv("METADATA_BACKEND", "appProperties"),
            probe_initial_bytes=_env_int("PROBE_INITIAL_BYTES", 100),
            probe_max_bytes=_env_int("PROBE_MAX_BYTES", 1_048_576),
            safe_margin_percent=_env_float("PROBE_SAFE_MARGIN_PERCENT", 15.0),
            name_safe_payload_bytes=_env_int("NAME_SAFE_PAYLOAD_BYTES", 0),
            description_safe_payload_bytes=_env_int(
                "DESCRIPTION_SAFE_PAYLOAD_BYTES", 0
            ),
            upload_backend=os.getenv("UPLOAD_BACKEND", "name"),
            storage_encoding=os.getenv("STORAGE_ENCODING", "base64url"),
            max_attempts=_env_int("GOOGLE_API_MAX_ATTEMPTS", 5),
            api_http_timeout_seconds=_env_int("GOOGLE_API_HTTP_TIMEOUT_SECONDS", 60),
            initial_delay_seconds=_env_float("GOOGLE_API_INITIAL_DELAY_SECONDS", 1.0),
            max_delay_seconds=_env_float("GOOGLE_API_MAX_DELAY_SECONDS", 16.0),
            api_min_interval_seconds=_env_float(
                "GOOGLE_API_MIN_INTERVAL_SECONDS", 0.1
            ),
            sqlite_path=Path(os.getenv("SQLITE_PATH", "data/metadata-drive.db")),
            upload_workers=_env_int("UPLOAD_WORKERS", 1),
            upload_batch_size=_env_int("UPLOAD_BATCH_SIZE", 1),
            upload_target_requests_per_minute=_env_int("UPLOAD_TARGET_REQUESTS_PER_MINUTE", 0),
            download_workers=_env_int("DOWNLOAD_WORKERS", 1),
            max_upload_mb=_env_int("UI_MAX_UPLOAD_MB", MAX_UPLOAD_MIB),
            upload_temp_dir=Path(os.environ["UPLOAD_TEMP_DIR"]) if os.getenv("UPLOAD_TEMP_DIR") else None,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )
        settings.validate()
        return settings

    def with_overrides(self, **changes: object) -> "Settings":
        """Return a validated copy containing CLI overrides."""
        result = replace(self, **changes)
        result.validate()
        return result

    def validate(self) -> None:
        """Validate numeric bounds before making any API requests."""
        if self.probe_initial_bytes < 1:
            raise ConfigurationError("PROBE_INITIAL_BYTES must be positive")
        if self.probe_max_bytes < self.probe_initial_bytes:
            raise ConfigurationError("PROBE_MAX_BYTES must be >= PROBE_INITIAL_BYTES")
        if not 0 < self.safe_margin_percent < 100:
            raise ConfigurationError("PROBE_SAFE_MARGIN_PERCENT must be between 0 and 100")
        if self.name_safe_payload_bytes < 0:
            raise ConfigurationError("NAME_SAFE_PAYLOAD_BYTES cannot be negative")
        if self.description_safe_payload_bytes < 0:
            raise ConfigurationError(
                "DESCRIPTION_SAFE_PAYLOAD_BYTES cannot be negative"
            )
        if self.upload_backend not in {"name", "description", "combined"}:
            raise ConfigurationError("UPLOAD_BACKEND must be name, description or combined")
        if self.storage_encoding not in {"base64url", "base85"}:
            raise ConfigurationError("STORAGE_ENCODING must be base64url or base85")
        if self.max_attempts < 1:
            raise ConfigurationError("GOOGLE_API_MAX_ATTEMPTS must be positive")
        if not 5 <= self.api_http_timeout_seconds <= 120:
            raise ConfigurationError("GOOGLE_API_HTTP_TIMEOUT_SECONDS must be between 5 and 120")
        if not 1 <= self.upload_workers <= 128:
            raise ConfigurationError("UPLOAD_WORKERS must be between 1 and 128")
        if not 1 <= self.upload_batch_size <= 100:
            raise ConfigurationError("UPLOAD_BATCH_SIZE must be between 1 and 100")
        if not 0 <= self.upload_target_requests_per_minute <= 1_000_000:
            raise ConfigurationError("UPLOAD_TARGET_REQUESTS_PER_MINUTE must be between 0 and 1000000")
        if not 1 <= self.download_workers <= 16:
            raise ConfigurationError("DOWNLOAD_WORKERS must be between 1 and 16")
        if not 1 <= self.max_upload_mb <= MAX_UPLOAD_MIB:
            raise ConfigurationError(f"UI_MAX_UPLOAD_MB must be between 1 and {MAX_UPLOAD_MIB} MiB")
        if self.initial_delay_seconds <= 0 or self.max_delay_seconds <= 0:
            raise ConfigurationError("retry delays must be positive")
        if self.initial_delay_seconds > self.max_delay_seconds:
            raise ConfigurationError("initial retry delay cannot exceed maximum delay")
        if self.api_min_interval_seconds < 0:
            raise ConfigurationError("GOOGLE_API_MIN_INTERVAL_SECONDS cannot be negative")

    def safe_payload_for(self, backend: str) -> int:
        """Return the empirically measured safe payload for an upload backend."""
        if backend == "name":
            return self.name_safe_payload_bytes
        if backend == "description":
            return self.description_safe_payload_bytes
        if backend == "combined":
            if self.name_safe_payload_bytes < 1 or self.description_safe_payload_bytes < 1:
                return 0
            return self.name_safe_payload_bytes + self.description_safe_payload_bytes
        raise ConfigurationError(f"unsupported upload backend: {backend}")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be numeric") from exc
