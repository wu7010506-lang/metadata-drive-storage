"""Metadata capacity backends and bounded capacity probe."""

from __future__ import annotations

import logging
import statistics
import time
from abc import ABC, abstractmethod
from dataclasses import replace
from typing import Any
from uuid import uuid4

from googleapiclient.errors import HttpError

from src.config import Settings
from src.errors import DriveAPIError, MetadataCapacityError
from src.models import ProbeAttempt, ProbeResult
from src.retry import execute_with_backoff

LOGGER = logging.getLogger(__name__)
TEST_FILE_PREFIX = "metadata-drive-probe"
SUPPORTED_BACKENDS = ("appProperties", "properties", "description", "name")


class MetadataBackend(ABC):
    """Storage field adapter for a single Drive file metadata field."""

    field_name: str
    documented_max_payload_bytes: int | None = None

    def __init__(self, service: Any, settings: Settings) -> None:
        self.service = service
        self.settings = settings
        self.api_requests = 0

    def _execute(self, request_factory: Any) -> dict[str, Any]:
        return execute_with_backoff(
            request_factory,
            max_attempts=self.settings.max_attempts,
            initial_delay=self.settings.initial_delay_seconds,
            max_delay=self.settings.max_delay_seconds,
            on_attempt=self._count_request,
        )

    def _count_request(self) -> None:
        self.api_requests += 1

    def create_object(self) -> str:
        """Create one metadata-only temporary Drive file and return its ID."""
        generated = self._execute(
            lambda: self.service.files().generateIds(count=1, space="drive", type="files")
        )
        file_id = str(generated["ids"][0])
        body: dict[str, Any] = {
            "id": file_id,
            "name": f"{TEST_FILE_PREFIX}-{uuid4()}",
            "mimeType": "application/octet-stream",
            "parents": [self.settings.drive_folder_id],
        }
        try:
            result = self._execute(
                lambda: self.service.files().create(
                    body=body, fields="id", supportsAllDrives=True
                )
            )
            return str(result["id"])
        except HttpError as exc:
            # An indeterminate first create may have succeeded; retrying the same
            # pre-generated ID yields 409. Confirm that object instead of duplicating it.
            if int(getattr(exc.resp, "status", 0)) != 409:
                raise
            result = self._execute(
                lambda: self.service.files().get(
                    fileId=file_id, fields="id", supportsAllDrives=True
                )
            )
            return str(result["id"])

    @abstractmethod
    def write_payload(self, file_id: str, payload: str) -> None:
        """Replace the selected metadata field with payload."""

    @abstractmethod
    def read_payload(self, file_id: str) -> str:
        """Read payload from the selected metadata field."""

    def delete_object(self, file_id: str) -> None:
        """Permanently delete the temporary probe object."""
        self._execute(
            lambda: self.service.files().delete(
                fileId=file_id, supportsAllDrives=True
            )
        )

    def probe_capacity(self) -> ProbeResult:
        """Probe this backend using exponential growth followed by binary search."""
        return CapacityProbe(self, self.settings).run()


class MappingMetadataBackend(MetadataBackend):
    """Base adapter for properties and appProperties maps."""

    property_count = 30
    property_keys = tuple(f"p{index:02d}" for index in range(property_count))
    value_bytes_per_property = 124 - len(property_keys[0].encode("utf-8"))
    documented_max_payload_bytes = property_count * value_bytes_per_property

    def write_payload(self, file_id: str, payload: str) -> None:
        if len(payload.encode("utf-8")) > self.documented_max_payload_bytes:
            raise ValueError("payload exceeds the documented custom-property limit")
        values: dict[str, str | None] = {}
        for index, key in enumerate(self.property_keys):
            start = index * self.value_bytes_per_property
            piece = payload[start : start + self.value_bytes_per_property]
            values[key] = piece or None
        self._execute(
            lambda: self.service.files().update(
                fileId=file_id,
                body={self.field_name: values},
                fields="id",
                supportsAllDrives=True,
            )
        )

    def read_payload(self, file_id: str) -> str:
        result = self._execute(
            lambda: self.service.files().get(
                fileId=file_id,
                fields=self.field_name,
                supportsAllDrives=True,
            )
        )
        values = result.get(self.field_name, {})
        return "".join(str(values.get(key, "")) for key in self.property_keys)


class AppPropertiesBackend(MappingMetadataBackend):
    """Store payload in the private appProperties map."""

    field_name = "appProperties"


class PropertiesBackend(MappingMetadataBackend):
    """Store payload in the visible properties map."""

    field_name = "properties"


class ScalarMetadataBackend(MetadataBackend):
    """Base adapter for scalar string metadata fields."""

    def write_payload(self, file_id: str, payload: str) -> None:
        self._execute(
            lambda: self.service.files().update(
                fileId=file_id,
                body={self.field_name: payload},
                fields="id",
                supportsAllDrives=True,
            )
        )

    def read_payload(self, file_id: str) -> str:
        result = self._execute(
            lambda: self.service.files().get(
                fileId=file_id,
                fields=self.field_name,
                supportsAllDrives=True,
            )
        )
        return str(result.get(self.field_name, ""))


class DescriptionBackend(ScalarMetadataBackend):
    """Store payload in the description field."""

    field_name = "description"


class NameBackend(ScalarMetadataBackend):
    """Store payload in the file name field."""

    field_name = "name"


BACKENDS: dict[str, type[MetadataBackend]] = {
    "appProperties": AppPropertiesBackend,
    "properties": PropertiesBackend,
    "description": DescriptionBackend,
    "name": NameBackend,
}


def create_backend(name: str, service: Any, settings: Settings) -> MetadataBackend:
    """Construct a named metadata backend."""
    try:
        return BACKENDS[name](service, settings)
    except KeyError as exc:
        raise ValueError(f"unsupported backend {name!r}; choose from {SUPPORTED_BACKENDS}") from exc


class CapacityProbe:
    """Measure a backend with one object and always attempt cleanup."""

    def __init__(self, backend: MetadataBackend, settings: Settings) -> None:
        self.backend = backend
        self.settings = settings
        self.attempts: list[ProbeAttempt] = []
        self.errors: list[str] = []

    def run(self) -> ProbeResult:
        """Run exponential growth, then byte-precision binary search."""
        file_id: str | None = None
        result: ProbeResult | None = None
        try:
            file_id = self.backend.create_object()
            LOGGER.info("Created temporary metadata-only object %s", file_id)
            low, high, capped = self._find_bounds(file_id)
            if low == 0:
                raise MetadataCapacityError(
                    "the smallest configured payload could not be stored reliably"
                )
            maximum = low if capped else self._binary_search(file_id, low, high)
            safe = max(1, int(maximum * (1 - self.settings.safe_margin_percent / 100)))
            successful = [attempt for attempt in self.attempts if attempt.succeeded]
            result = ProbeResult(
                backend=self.settings.metadata_backend,
                field_tested=self.backend.field_name,
                maximum_successful_payload=maximum,
                safe_payload=safe,
                average_write_latency_seconds=statistics.fmean(
                    item.write_latency_seconds for item in successful
                ),
                average_read_latency_seconds=statistics.fmean(
                    item.read_latency_seconds for item in successful
                ),
                api_requests=0,
                errors=tuple(self.errors),
                attempts=tuple(self.attempts),
                capped=capped,
            )
        except HttpError as exc:
            raise DriveAPIError(f"Drive API probe failed: {exc}") from exc
        finally:
            if file_id:
                try:
                    self.backend.delete_object(file_id)
                    LOGGER.info("Deleted temporary probe object %s", file_id)
                except HttpError as exc:
                    LOGGER.error("Could not delete temporary object %s: %s", file_id, exc)
                    raise DriveAPIError(
                        f"probe object cleanup failed; delete Drive file {file_id} manually"
                    ) from exc
        if result is None:
            raise RuntimeError("probe completed without a result")
        return replace(result, api_requests=self.backend.api_requests)

    def _find_bounds(self, file_id: str) -> tuple[int, int, bool]:
        low = 0
        effective_max = self.settings.probe_max_bytes
        if self.backend.documented_max_payload_bytes is not None:
            effective_max = min(effective_max, self.backend.documented_max_payload_bytes)
            LOGGER.info(
                "Respecting documented %s payload ceiling of %d bytes",
                self.backend.field_name,
                effective_max,
            )
        size = min(self.settings.probe_initial_bytes, effective_max)
        while size <= effective_max:
            if self._attempt(file_id, size).succeeded:
                low = size
                if size == effective_max:
                    return low, low, True
                size = min(size * 2, effective_max)
            else:
                return low, size, False
        return low, low, True

    def _binary_search(self, file_id: str, low: int, high: int) -> int:
        LOGGER.info("Running binary search between %d and %d bytes", low, high)
        while low + 1 < high:
            midpoint = (low + high) // 2
            if self._attempt(file_id, midpoint).succeeded:
                low = midpoint
            else:
                high = midpoint
        return low

    def _attempt(self, file_id: str, size: int) -> ProbeAttempt:
        payload = "A" * size
        write_started = time.perf_counter()
        try:
            self.backend.write_payload(file_id, payload)
            write_latency = time.perf_counter() - write_started
            read_started = time.perf_counter()
            actual = self.backend.read_payload(file_id)
            read_latency = time.perf_counter() - read_started
            if actual != payload:
                error = f"{size} bytes: read-back mismatch"
                result = ProbeAttempt(size, False, write_latency, read_latency, error)
                self.errors.append(error)
            else:
                result = ProbeAttempt(size, True, write_latency, read_latency)
        except HttpError as exc:
            if int(getattr(exc.resp, "status", 0)) not in {400, 413}:
                raise
            error = f"{size} bytes: {exc}"
            result = ProbeAttempt(size, False, error=error)
            self.errors.append(error)
        self.attempts.append(result)
        LOGGER.info("%d bytes\t%s", size, "PASS" if result.succeeded else "FAIL")
        return result
