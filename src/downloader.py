"""Verified reconstruction of metadata-backed datasets."""

from __future__ import annotations

import logging
import os
import tempfile
import time
import threading
import io
from contextlib import nullcontext
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait
from collections.abc import Callable
from pathlib import Path
from typing import Any

from googleapiclient.errors import HttpError

from src.config import Settings
from src.database import Database
from src.streaming import restore_stream
from src.transfer_pool import TransferPool
from src.payload import BACKENDS, extract_payload, payload_fields
from src.errors import (
    ChunkCorruptedError,
    ChunkMissingError,
    DatasetNotFoundError,
    DriveAPIError,
    IntegrityError,
)
from src.integrity import sha256_text
from src.retry import execute_with_backoff

LOGGER = logging.getLogger(__name__)


class VerifiedDownloader:
    """Read Drive metadata chunks and release bytes only after full validation."""

    def __init__(
        self, service: Any, settings: Settings, database: Database, *,
        service_factory: Callable[[], Any] | None = None,
        transfer_pool: TransferPool | None = None,
    ) -> None:
        if settings.download_workers > 1 and service_factory is None and transfer_pool is None:
            raise ValueError("Parallel download requires an independent service_factory")
        self.service = service
        self.settings = settings
        self.database = database
        self.service_factory = service_factory
        self.transfer_pool = transfer_pool
        self._rate_lock = transfer_pool.rate_lock if transfer_pool else threading.Lock()
        self._rate_state = transfer_pool.rate_state if transfer_pool else [0.0]

    def download_bytes(
        self,
        dataset_id: str,
        *,
        progress: Callable[[int, int], None] | None = None,
    ) -> tuple[bytes, dict[str, object]]:
        """Return verified original bytes and their manifest."""
        buffer = io.BytesIO()
        manifest = self._restore(dataset_id, buffer, progress)
        return buffer.getvalue(), manifest

    def _restore(self, dataset_id, output, progress):
        self.database.initialize()
        manifest = self.database.get_dataset(dataset_id)
        if manifest is None:
            raise DatasetNotFoundError(f"dataset not found: {dataset_id}")

        expected_count = int(manifest["chunk_count"])
        actual_count = self.database.count_complete_chunks(dataset_id)
        if actual_count != expected_count or self.database.count_chunks(dataset_id) != expected_count:
            raise ChunkMissingError(
                f"expected {expected_count} completed chunks, found {actual_count}"
            )

        backend = str(manifest["storage_backend"])
        if backend not in BACKENDS:
            raise IntegrityError(f"unsupported storage backend: {backend}")

        def chunks():
            found = 0
            for expected_index, chunk in enumerate(self.database.iter_chunks(dataset_id, expected_count)):
                if int(chunk["chunk_index"]) != expected_index or chunk["status"] != "complete":
                    raise ChunkMissingError(f"chunk {expected_index} is missing or incomplete")
                found += 1
                yield chunk
            if found != expected_count:
                raise ChunkMissingError("chunk index is not contiguous")

        def payloads():
            for index, payload in enumerate(self._ordered_payloads(chunks(), manifest, backend)):
                yield payload
                if progress:
                    progress(index + 1, expected_count)

        restore_stream(payloads(), manifest, output)
        return manifest

    def _ordered_payloads(self, chunks, manifest, backend):
        """Bound in-flight reads and yield verified payloads in manifest order."""
        local = threading.local()
        workers = self.settings.download_workers

        def read(chunk):
            if workers == 1 and self.transfer_pool is None:
                service = self.service
            else:
                if not hasattr(local, "service"):
                    local.service = self.transfer_pool.service() if self.transfer_pool else self.service_factory()
                service = local.service
            expected_index = int(chunk["chunk_index"])
            remote = self._get_remote(str(chunk["drive_file_id"]), backend, service=service)
            properties = remote.get("appProperties", {})
            if (
                remote.get("id") != chunk["drive_file_id"]
                or properties.get("dataset_id") != manifest["dataset_id"]
                or properties.get("chunk_index") != str(expected_index)
                or properties.get("chunk_hash") != chunk["chunk_hash"]
                or properties.get("version") != str(manifest["version"])
                or (int(manifest["version"]) > 1 and properties.get("encoding") != manifest["encoding"])
            ):
                raise ChunkCorruptedError(
                    f"chunk {expected_index} metadata does not match the local index"
                )
            try:
                payload = extract_payload(remote, backend)
            except ValueError as exc:
                raise ChunkCorruptedError(f"chunk {expected_index} invalid metadata payload") from exc
            if sha256_text(payload) != chunk["chunk_hash"]:
                raise ChunkCorruptedError(f"chunk {expected_index} hash mismatch")
            return payload

        if workers == 1 and self.transfer_pool is None:
            for chunk in chunks:
                yield read(chunk)
            return
        remaining = iter(chunks)
        pool_context = (nullcontext(self.transfer_pool.executor) if self.transfer_pool else
                        ThreadPoolExecutor(max_workers=workers, thread_name_prefix="drive-read"))
        with pool_context as pool:
            pending = deque()
            try:
                for _ in range(workers * 2):
                    chunk = next(remaining, None)
                    if chunk is None:
                        break
                    pending.append(pool.submit(read, chunk))
                while pending:
                    payload = pending.popleft().result()
                    chunk = next(remaining, None)
                    if chunk is not None:
                        pending.append(pool.submit(read, chunk))
                    yield payload
            finally:
                for future in pending:
                    future.cancel()
                if pending:
                    wait(pending)

    def download_file(
        self,
        dataset_id: str,
        output_path: Path,
        *,
        progress: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Atomically write a verified dataset to the requested path."""
        target = output_path.resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            prefix=f".{target.name}.", dir=target.parent, delete=False
        )
        temporary = Path(handle.name)
        try:
            with handle:
                self._restore(dataset_id, handle, progress)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return target

    def _get_remote(self, file_id: str, backend: str, *, service: Any = None) -> dict[str, Any]:
        fields = f"id,{payload_fields(backend)},appProperties"
        service = self.service if service is None else service
        try:
            return self._execute(
                lambda: service.files().get(
                    fileId=file_id,
                    fields=fields,
                    supportsAllDrives=True,
                )
            )
        except HttpError as exc:
            status = int(getattr(exc.resp, "status", 0))
            if status == 404:
                raise ChunkMissingError(f"Drive object missing: {file_id}") from exc
            raise DriveAPIError(f"failed to read Drive object {file_id}: {exc}") from exc

    def _execute(self, request_factory: Callable[[], Any]) -> dict[str, Any]:
        def throttled_request_factory() -> Any:
            with self._rate_lock:
                elapsed = time.monotonic() - self._rate_state[0]
                remaining = self.settings.api_min_interval_seconds - elapsed
                if remaining > 0:
                    time.sleep(remaining)
                self._rate_state[0] = time.monotonic()
            return request_factory()

        return execute_with_backoff(
            throttled_request_factory,
            max_attempts=self.settings.max_attempts,
            initial_delay=self.settings.initial_delay_seconds,
            max_delay=self.settings.max_delay_seconds,
        )
