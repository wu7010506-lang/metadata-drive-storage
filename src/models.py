"""Typed data models shared by the probe and future milestones."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    """Local canonical manifest for a stored dataset."""

    dataset_id: str
    filename: str
    original_size: int
    compressed_size: int
    compression: str
    sha256: str
    chunk_count: int
    storage_backend: str = "name"
    chunk_size: int = 0
    version: int = 1
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    encoding: str = "base64url"


@dataclass(frozen=True, slots=True)
class ProbeAttempt:
    """Result of one write/read/compare cycle."""

    size_bytes: int
    succeeded: bool
    write_latency_seconds: float = 0.0
    read_latency_seconds: float = 0.0
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """Capacity probe summary."""

    backend: str
    field_tested: str
    maximum_successful_payload: int
    safe_payload: int
    average_write_latency_seconds: float
    average_read_latency_seconds: float
    api_requests: int
    errors: tuple[str, ...]
    attempts: tuple[ProbeAttempt, ...]
    capped: bool = False
