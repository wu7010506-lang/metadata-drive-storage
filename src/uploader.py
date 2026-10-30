"""Resumable, metadata-only uploader with verified encoded chunks."""

from __future__ import annotations

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from collections.abc import Callable
from typing import Any
from uuid import uuid4
from contextlib import nullcontext

from googleapiclient.errors import HttpError

from src.config import Settings
from src.database import Database
from src.streaming import prepare_upload
from src.transfer_pool import TransferPool
from src.errors import DriveAPIError
from src.integrity import sha256_text
from src.models import DatasetManifest
from src.payload import BACKENDS, extract_payload, pack_payload
from src.retry import execute_with_backoff, is_retryable_error
from src.upload_pacing import UploadRequestPacer

LOGGER = logging.getLogger(__name__)


class NameChunkUploader:
    """Upload encoded chunks into the selected Drive metadata fields."""

    def __init__(
        self,
        service: Any,
        settings: Settings,
        database: Database,
        chunk_size: int,
        payload_backend: str = "name",
        *,
        service_factory: Callable[[], Any] | None = None,
        transfer_pool: TransferPool | None = None,
        request_pacer: UploadRequestPacer | None = None,
        confirm_ambiguous_writes: bool = True,
    ) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must come from a successful backend probe")
        if payload_backend not in BACKENDS:
            raise ValueError("payload_backend must be name, description or combined")
        self.service = service
        self.settings = settings
        self.database = database
        self.chunk_size = chunk_size
        self.payload_backend = payload_backend
        self.encoding = settings.storage_encoding
        self.version = 2 if self.encoding != "base64url" or payload_backend == "combined" else 1
        self.service_factory = service_factory
        self.transfer_pool = transfer_pool
        self.confirm_ambiguous_writes = confirm_ambiguous_writes
        self._rate_lock = transfer_pool.rate_lock if transfer_pool else threading.Lock()
        self._rate_state = transfer_pool.rate_state if transfer_pool else [0.0]
        self._request_pacer = (request_pacer if request_pacer is not None else
            transfer_pool.request_pacer if transfer_pool else
            UploadRequestPacer(settings.upload_target_requests_per_minute)
            if settings.upload_target_requests_per_minute else None)
        self._id_pool: list[str] = []
        self._id_batch_size = 100

    def upload_file(
        self,
        path: Path,
        *,
        filename: str | None = None,
        progress: Callable[[int, int], None] | None = None,
        phase: Callable[[str], None] | None = None,
    ) -> DatasetManifest:
        """Compress, encode, split, and resumably create metadata-only objects."""
        source = path.resolve()
        display_name = filename or source.name
        if phase:
            phase("preparing")
        with prepare_upload(source, max_bytes=self.settings.max_upload_mb * 1024**2,
                            spool_dir=self.settings.upload_temp_dir) as prepared:
            if phase:
                phase("indexing")
            return self._upload_prepared(prepared, display_name, progress, phase=phase)

    def _upload_prepared(self, prepared, display_name, progress, *, phase=None):
        algorithm = prepared.compression
        original_hash = prepared.sha256

        self.database.initialize()
        # Keep the original partition when resuming after a chunk-size change.
        with self.database.connect() as connection:
            previous = connection.execute(
                """SELECT dataset_id, chunk_size FROM datasets
                WHERE filename = ? AND original_size = ? AND sha256 = ?
                  AND compression = ? AND storage_backend = ? AND chunk_size > 0 AND encoding = ?
                ORDER BY created_at DESC LIMIT 1""",
                (display_name, prepared.original_size, original_hash, algorithm, self.payload_backend, self.encoding),
            ).fetchone()
        chunk_size = int(previous["chunk_size"]) if previous else self.chunk_size
        chunk_count = (prepared.encoded_size_for(self.encoding) + chunk_size - 1) // chunk_size
        dataset_id = self.database.find_matching_dataset(
            filename=display_name,
            original_size=prepared.original_size,
            sha256=original_hash,
            compression=algorithm,
            chunk_count=chunk_count,
            storage_backend=self.payload_backend,
            chunk_size=chunk_size,
            encoding=self.encoding,
        ) or str(uuid4())
        manifest = DatasetManifest(
            dataset_id=dataset_id,
            filename=display_name,
            original_size=prepared.original_size,
            compressed_size=prepared.compressed_size,
            compression=algorithm,
            sha256=original_hash,
            chunk_count=chunk_count,
            storage_backend=self.payload_backend,
            chunk_size=chunk_size,
            encoding=self.encoding,
            version=self.version,
        )
        self.database.save_dataset(manifest)

        completed = 0
        if previous:
            # Validate ALL reserved hashes before any new remote writes. This also
            # protects resumes across compressor/library changes, without a full index list.
            records = iter(self.database.iter_chunks(dataset_id, chunk_count))
            record = next(records, None)
            payloads = prepared.chunks(chunk_size, self.encoding) if record is not None else ()
            for index, payload in enumerate(payloads):
                if record is not None and int(record["chunk_index"]) == index:
                    if record["chunk_hash"] != sha256_text(payload):
                        raise DriveAPIError(f"local chunk {index} hash conflicts with resume state")
                    if record["status"] == "complete":
                        completed += 1
                        if progress:
                            progress(completed, chunk_count)
                    record = next(records, None)
                    if record is None:
                        break  # No reserved tail remains to validate.

        if completed == chunk_count:
            return manifest  # Every existing hash was checked; no second encoding pass.

        if phase:
            phase("transferring")
        workers = self.settings.upload_workers
        self._id_batch_size = min(1000, max(100, chunk_count - completed))
        batch_size = self.settings.upload_batch_size
        if workers > 1 and self.service_factory is None and self.transfer_pool is None:
            raise ValueError("Parallel upload requires an independent service_factory")
        local = threading.local()

        def upload_group(group: list[tuple[str, int, str, str]]) -> tuple[list[int], Exception | None]:
            if workers == 1 and self.transfer_pool is None:
                uploader = self
            else:
                if not hasattr(local, "uploader"):
                    local.uploader = NameChunkUploader(
                        self.transfer_pool.service() if self.transfer_pool else self.service_factory(),
                        self.settings, self.database,
                        chunk_size, self.payload_backend,
                        transfer_pool=self.transfer_pool,
                        request_pacer=self._request_pacer,
                        confirm_ambiguous_writes=self.confirm_ambiguous_writes,
                    )
                    local.uploader._rate_lock = self._rate_lock
                    local.uploader._rate_state = self._rate_state
                uploader = local.uploader
            return uploader._upload_group(dataset_id, group)

        def report_finished(futures: set) -> Exception | None:
            nonlocal completed
            verified = []
            failure = None
            for future in futures:
                try:
                    indices, error = future.result()
                    verified.extend(indices)
                    failure = failure or error
                except Exception as exc:
                    failure = failure or exc
            # Only the coordinator writes SQLite. Progress never precedes commit.
            self.database.mark_chunks_complete(dataset_id, verified)
            for _ in verified:
                completed += 1
                if progress:
                    progress(completed, chunk_count)
            return failure

        def reserved_groups():
            # Reserve several groups in a single transaction before submitting
            # any remote creates. Keep reservations bounded independently of size.
            window = max(64, workers * batch_size)
            for start in range(0, chunk_count, window):
                stop = min(chunk_count, start + window)
                records = {int(row["chunk_index"]): row for row in
                           self.database.get_chunk_window(dataset_id, start, stop - start)}
                needed = [index for index in range(start, stop)
                          if index not in records or records[index]["status"] != "complete"]
                if not needed:
                    continue  # Seek over completed windows without reading/encoding them.
                first, last = needed[0], needed[-1] + 1
                payloads = prepared.chunks(chunk_size, self.encoding,
                                           start_index=first, stop_index=last)
                reserved = []
                new_records = []
                for index, payload in enumerate(payloads, first):
                    record = records.get(index)
                    if record and record["status"] == "complete":
                        continue
                    expected_hash = sha256_text(payload)
                    file_id = str(record["drive_file_id"]) if record else self._generate_id()
                    if record and record["chunk_hash"] != expected_hash:
                        raise DriveAPIError(f"local chunk {index} hash conflicts with resume state")
                    if not record:
                        new_records.append((index, file_id, expected_hash))
                    reserved.append((file_id, index, payload, expected_hash))
                self.database.save_pending_chunks(dataset_id, new_records)
                for start in range(0, len(reserved), batch_size):
                    yield reserved[start:start + batch_size]

        groups = iter(reserved_groups())
        failure = None
        exhausted = False
        pool_context = (nullcontext(self.transfer_pool.executor) if self.transfer_pool else
                        ThreadPoolExecutor(max_workers=workers, thread_name_prefix="drive-chunk"))
        with pool_context as pool:
            pending = set()
            while pending or not exhausted:
                while not failure and not exhausted and len(pending) < workers:
                    try:
                        group = next(groups)
                        pending.add(pool.submit(upload_group, group))
                    except StopIteration:
                        exhausted = True
                    except Exception as exc:
                        failure = exc
                if not pending:
                    break
                finished, pending = wait(pending, return_when=FIRST_COMPLETED)
                error = report_finished(finished)
                failure = failure or error
                if failure:
                    # Drain and commit other in-flight successes even if one
                    # group fails. Unconfirmed preallocated IDs remain resumable.
                    exhausted = True
        if failure:
            raise failure
        return manifest

    def _upload_group(
        self, dataset_id: str, group: list[tuple[str, int, str, str]]
    ) -> tuple[list[int], Exception | None]:
        """Verify each subresponse; recover failed items without repeating successes."""
        if len(group) == 1:
            file_id, index, payload, chunk_hash = group[0]
            try:
                self._create_or_confirm(file_id, dataset_id, index, chunk_hash, payload)
                return [index], None
            except Exception as exc:
                return [], exc

        return self._upload_batch(dataset_id, group)

    def _upload_batch(self, dataset_id, group):
        """Verify an explicitly batched group, including its ambiguous-write recovery."""

        bodies = {
            file_id: self._chunk_body(file_id, dataset_id, index, chunk_hash, payload)
            for file_id, index, payload, chunk_hash in group
        }
        confirmed: set[str] = set()
        errors: dict[str, Exception] = {}
        uploader = self

        def should_send(file_id):
            error = errors.get(file_id)
            return file_id not in confirmed and (error is None or is_retryable_error(error))

        def callback(request_id, response, exception):
            if exception is not None:
                errors[request_id] = exception
            elif self._matches(response, bodies[request_id], extract_payload(bodies[request_id], self.payload_backend)):
                confirmed.add(request_id)
                errors.pop(request_id, None)
            else:
                errors[request_id] = DriveAPIError("batch create response conflicts with chunk")

        def request_factory():
            batch = self.service.new_batch_http_request(callback=callback)
            for file_id, body in bodies.items():
                if should_send(file_id):
                    batch.add(self.service.files().create(
                        body=body, fields="id,name,description,appProperties",
                        supportsAllDrives=True,
                    ), request_id=file_id)

            class BatchRound:
                def execute(self):
                    try:
                        batch.execute()
                    except OSError as error:
                        if not uploader.confirm_ambiguous_writes or not is_retryable_error(error):
                            raise
                        missing = False
                        for file_id, body in bodies.items():
                            if not should_send(file_id):
                                continue
                            actual = uploader._confirm_uncertain_write(file_id, body,
                                extract_payload(body, uploader.payload_backend))
                            if actual is None:
                                missing = True  # Only a definite 404 permits another create.
                            else:
                                confirmed.add(file_id)
                                errors.pop(file_id, None)
                        if missing:
                            raise
                    # A batch HTTP 200 may contain 429/5xx subresponses. Feed
                    # these through the same bounded backoff as outer failures;
                    # only unconfirmed transient items enter the next round.
                    for error in errors.values():
                        if is_retryable_error(error):
                            raise error

            return BatchRound()

        outer_error = None
        try:
            self._execute(request_factory, request_cost=lambda: sum(should_send(ident) for ident in bodies))
        except Exception as exc:
            outer_error = exc

        failure = None
        for file_id, index, payload, chunk_hash in group:
            if file_id in confirmed:
                continue
            error = errors.get(file_id) or outer_error or DriveAPIError("missing batch subresponse")
            try:
                if isinstance(error, HttpError) and int(error.resp.status) == 409:
                    actual = self._read_chunk_metadata(file_id)
                    if not self._matches(actual, bodies[file_id], payload):
                        raise DriveAPIError(f"existing Drive object conflicts with chunk {index}")
                else:
                    raise error
                confirmed.add(file_id)
            except Exception as exc:
                failure = failure or exc
        return [index for file_id, index, _, _ in group if file_id in confirmed], failure

    def _execute(self, request_factory: Any, *, request_cost=1) -> dict[str, Any]:
        def throttled_request_factory() -> Any:
            with self._rate_lock:
                elapsed = time.monotonic() - self._rate_state[0]
                remaining = self.settings.api_min_interval_seconds - elapsed
                if remaining > 0:
                    time.sleep(remaining)
                self._rate_state[0] = time.monotonic()
            request_round = request_factory()
            pacer = self._request_pacer
            if pacer is None:
                return request_round

            class PacedRound:
                def execute(self):
                    cost = request_cost() if callable(request_cost) else request_cost
                    pacer.acquire(cost)
                    try:
                        result = request_round.execute()
                    except Exception as error:
                        if is_retryable_error(error):
                            pacer.congested()
                        raise
                    else:
                        pacer.succeeded()
                        return result

            return PacedRound()

        return execute_with_backoff(
            throttled_request_factory,
            max_attempts=self.settings.max_attempts,
            initial_delay=self.settings.initial_delay_seconds,
            max_delay=self.settings.max_delay_seconds,
        )

    def _generate_id(self) -> str:
        if self.transfer_pool:
            return self.transfer_pool.generate_id(lambda: self._fetch_ids())
        if self._id_pool:
            return self._id_pool.pop()
        self._id_pool = self._fetch_ids()
        return self._id_pool.pop()

    def _fetch_ids(self) -> list[str]:
        try:
            result = self._execute(
                lambda: self.service.files().generateIds(count=self._id_batch_size, space="drive", type="files")
            )
        except HttpError as exc:
            raise DriveAPIError(f"failed to generate a Drive file ID: {exc}") from exc
        return [str(file_id) for file_id in result["ids"]]

    def _create_or_confirm(
        self,
        file_id: str,
        dataset_id: str,
        index: int,
        chunk_hash: str,
        payload: str,
    ) -> None:
        body = self._chunk_body(file_id, dataset_id, index, chunk_hash, payload)
        uploader = self

        def request_factory():
            request = uploader.service.files().create(body=body,
                fields="id,name,description,appProperties", supportsAllDrives=True)

            class CreateRound:
                def execute(self):
                    try:
                        return request.execute()
                    except OSError as error:
                        if not uploader.confirm_ambiguous_writes or not is_retryable_error(error):
                            raise
                        actual = uploader._confirm_uncertain_write(file_id, body, payload)
                        if actual is None:
                            raise
                        return actual

            return CreateRound()

        try:
            actual = self._execute(request_factory)
            if not self._matches(actual, body, payload):
                raise DriveAPIError(f"Drive create response conflicts with chunk {index}")
        except HttpError as exc:
            if int(getattr(exc.resp, "status", 0)) != 409:
                raise DriveAPIError(f"failed to upload chunk {index}: {exc}") from exc
            actual = self._read_chunk_metadata(file_id)
            if not self._matches(actual, body, payload):
                raise DriveAPIError(f"existing Drive object conflicts with chunk {index}")

    def _confirm_uncertain_write(self, file_id, body, payload):
        """Return verified metadata or definite absence; never guess after a failed read."""
        try:
            actual = self._read_chunk_metadata(file_id)
        except HttpError as error:
            if int(error.resp.status) == 404:
                return None
            raise DriveAPIError("unable to confirm uncertain write safely") from error
        except Exception as error:
            raise DriveAPIError("unable to confirm uncertain write safely") from error
        if not self._matches(actual, body, payload):
            raise DriveAPIError("uncertain Drive write conflicts with reserved chunk")
        LOGGER.warning("Recovered an uncertain chunk write by verified read-back; skipped create replay",
                       extra={"ambiguous_chunks_confirmed": 1})
        return actual

    def _chunk_body(
        self, file_id: str, dataset_id: str, index: int, chunk_hash: str, payload: str
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "id": file_id,
            "name": payload if self.payload_backend == "name" else (
                f"metadata-chunk-{dataset_id[:8]}-{index:08d}"
            ),
            "mimeType": "application/octet-stream",
            "parents": [self.settings.drive_folder_id],
            "appProperties": {
                "dataset_id": dataset_id,
                "chunk_index": str(index),
                "chunk_hash": chunk_hash,
                "version": str(self.version),
            },
        }
        body.update(pack_payload(payload, self.payload_backend, self.settings.name_safe_payload_bytes,
                                 self.settings.description_safe_payload_bytes))
        if self.version > 1:
            body["appProperties"]["encoding"] = self.encoding
        return body

    def _matches(
        self, actual: dict[str, Any], expected: dict[str, Any], payload: str
    ) -> bool:
        try:
            return (
                actual.get("id") == expected["id"]
                and extract_payload(actual, self.payload_backend) == payload
                and actual.get("appProperties") == expected["appProperties"]
            )
        except (AttributeError, ValueError):
            return False

    def _read_chunk_metadata(self, file_id: str) -> dict[str, Any]:
        return self._execute(
            lambda: self.service.files().get(
                fileId=file_id,
                fields="id,name,description,appProperties",
                supportsAllDrives=True,
            )
        )
