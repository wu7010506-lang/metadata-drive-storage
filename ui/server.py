"""Local-only Flask UI for testing metadata-name uploads."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from uuid import uuid4

from flask import Flask, Request, jsonify, request, send_file, send_from_directory
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

from src.auth import build_drive_service, verify_target_folder
from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.errors import MetadataDriveError
from src.uploader import NameChunkUploader
from src.upload_timings import UploadTimings
from src.upload_pacing import UploadRequestPacer
from src.transfer_pool import TransferPool
from src.payload import BACKENDS
from src.encoder import ENCODINGS

LOGGER = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).resolve().parent / "static"


class _LimitedUploadStream:
    """Limit actual file bytes independently of multipart headers/content length."""

    def __init__(self, handle, limit):
        self.handle = handle
        self.limit = limit
        self.size = 0

    def write(self, data):
        if self.size + len(data) > self.limit:
            raise RequestEntityTooLarge()
        count = self.handle.write(data)
        self.size += count
        return count

    def __getattr__(self, name):
        return getattr(self.handle, name)


def create_app(settings: Settings, *, service_factory=None) -> Flask:
    """Create a local UI app bound to the configured Drive target."""
    app = Flask(__name__, static_folder=None)
    max_file_bytes = settings.max_upload_mb * 1024**2
    # The file limit excludes the bounded multipart envelope.
    app.config["MAX_CONTENT_LENGTH"] = max_file_bytes + 1024**2
    app.config["MAX_FORM_MEMORY_SIZE"] = 128 * 1024
    app.config["MAX_FORM_PARTS"] = 4
    upload_temp_dir = settings.upload_temp_dir or Path(tempfile.gettempdir())

    class StagedUploadRequest(Request):
        def _get_file_stream(self, total_content_length, content_type, filename=None, content_length=None):
            upload_temp_dir.mkdir(parents=True, exist_ok=True)
            handle = tempfile.NamedTemporaryFile(prefix="metadata-drive-ui-", dir=upload_temp_dir, delete=False)
            stream = _LimitedUploadStream(handle, max_file_bytes)
            if not hasattr(self, "staged_streams"):
                self.staged_streams = []
                self.claimed_paths = set()
            self.staged_streams.append(stream)
            return stream

        def close(self):
            try:
                super().close()
            finally:
                # Parsing failures can occur before Request.files exists.
                for stream in getattr(self, "staged_streams", ()):
                    stream.close()
                    if stream.name not in self.claimed_paths:
                        Path(stream.name).unlink(missing_ok=True)

    app.request_class = StagedUploadRequest
    configured_download_output = os.getenv(
        "DOWNLOAD_OUTPUT_DIR", os.getenv("DOWNLOAD_TEMP_DIR", "")
    ).strip()
    download_output_dir = (
        Path(configured_download_output).expanduser()
        if configured_download_output
        else Path(tempfile.gettempdir())
    )

    @app.errorhandler(413)
    def request_too_large(_error: Any) -> Any:
        return jsonify({"error": "檔案超過本機 UI 上限，請選擇較小的檔案"}), 413

    @app.errorhandler(OSError)
    def storage_error(error: OSError) -> Any:
        if getattr(error, "errno", None) == 28:
            LOGGER.error("Temporary storage is full while handling upload")
            return jsonify({"error": "本機暫存磁碟空間不足，請先清理磁碟後再試"}), 507
        LOGGER.error("Operating system error in UI server", exc_info=True)
        return jsonify({"error": "作業系統 I/O 錯誤，請查看終端機紀錄"}), 500

    @app.errorhandler(404)
    def not_found(_error: Any) -> Any:
        return jsonify({"error": "找不到 API 路徑"}), 404

    @app.errorhandler(500)
    def internal_error(_error: Any) -> Any:
        LOGGER.error("Unhandled UI server error", exc_info=True)
        return jsonify({"error": "伺服器內部錯誤，請查看終端機紀錄"}), 500
    database = Database(settings.sqlite_path)
    database.initialize()
    jobs: dict[str, dict[str, Any]] = {}
    jobs_lock = threading.Lock()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="drive-upload")
    download_jobs: dict[str, dict[str, Any]] = {}
    download_jobs_lock = threading.Lock()
    download_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="drive-download")

    factory = service_factory or (lambda: build_drive_service(settings.credentials_path))
    service = factory()
    folder = verify_target_folder(service, settings)
    transfer_pool = TransferPool(factory,
        max(settings.upload_workers, settings.download_workers),
        request_pacer=(UploadRequestPacer(settings.upload_target_requests_per_minute)
                       if settings.upload_target_requests_per_minute else None))

    def close_transfers():
        # Coordinators finish before the network executor/HTTP clients are closed.
        executor.shutdown(wait=True)
        download_executor.shutdown(wait=True)
        transfer_pool.close()
        close = getattr(service, "close", None)
        if close:
            close()

    app.extensions["close_transfers"] = close_transfers

    @app.before_request
    def begin_upload_timing():
        if request.method == "POST" and request.path == "/api/uploads":
            # Starts before Werkzeug reads/parses the multipart body. Does not
            # include browser file selection or time before this request arrives.
            request.upload_timings = UploadTimings()

    @app.get("/")
    def index() -> Any:
        return send_from_directory(STATIC_DIR, "index.html")

    @app.get("/static/<path:name>")
    def static_file(name: str) -> Any:
        return send_from_directory(STATIC_DIR, name)

    @app.get("/api/status")
    def status() -> Any:
        return jsonify(
            {
                "connected": True,
                "folderName": folder["name"],
                "uploadBackend": settings.upload_backend,
                "storageEncoding": settings.storage_encoding,
                "uploadWorkers": settings.upload_workers,
                "uploadBatchSize": settings.upload_batch_size,
                "uploadTargetRequestsPerMinute": settings.upload_target_requests_per_minute,
                "uploadPacing": transfer_pool.request_pacer.snapshot() if transfer_pool.request_pacer else None,
                "downloadWorkers": settings.download_workers,
                "nameSafePayloadBytes": settings.name_safe_payload_bytes,
                "descriptionSafePayloadBytes": settings.description_safe_payload_bytes,
                "combinedSafePayloadBytes": settings.safe_payload_for("combined"),
                "maxUploadMb": settings.max_upload_mb,
            }
        )

    @app.get("/api/datasets")
    def datasets() -> Any:
        return jsonify(database.list_datasets())

    @app.get("/api/datasets/<dataset_id>/download")
    def download_dataset(dataset_id: str) -> Any:
        manifest = database.get_dataset(dataset_id)
        if manifest is None:
            return jsonify({"error": "找不到資料集"}), 404
        download_output_dir.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(prefix="metadata-drive-download-", dir=download_output_dir, delete=False)
        output_path = Path(handle.name)
        handle.close()
        try:
            download_executor.submit(restore_to_file, dataset_id, output_path).result()
            response = send_file(
                output_path,
                as_attachment=True,
                download_name=Path(str(manifest["filename"])).name,
                mimetype="application/octet-stream",
                max_age=0,
            )
            response.headers["Cache-Control"] = "no-store"
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.call_on_close(lambda: output_path.unlink(missing_ok=True))
            # Ensure the response close callback executes for this disposable legacy route.
            response.direct_passthrough = False
            return response
        except MetadataDriveError as exc:
            output_path.unlink(missing_ok=True)
            LOGGER.warning("Verified download failed for %s: %s", dataset_id, exc)
            return jsonify({"error": str(exc)}), 409
        except BaseException:
            output_path.unlink(missing_ok=True)
            raise

    @app.post("/api/datasets/<dataset_id>/downloads")
    def start_download(dataset_id: str) -> Any:
        manifest = database.get_dataset(dataset_id)
        if manifest is None:
            return jsonify({"error": "找不到資料集"}), 404
        download_output_dir.mkdir(parents=True, exist_ok=True)
        required_bytes = int(manifest["original_size"])
        free_bytes = shutil.disk_usage(download_output_dir).free
        if free_bytes < required_bytes:
            return jsonify(
                {
                    "error": (
                        f"下載暫存空間不足：需要至少 {required_bytes} bytes，"
                        f"目前只有 {free_bytes} bytes"
                    )
                }
            ), 507
        job_id = str(uuid4())
        original_name = Path(str(manifest["filename"])).name
        safe_name = secure_filename(original_name) or f"{dataset_id}.bin"
        safe_path = Path(safe_name)
        output_path = download_output_dir / (
            f"{safe_path.stem}-{job_id[:8]}{safe_path.suffix}"
        )
        with download_jobs_lock:
            download_jobs[job_id] = {
                "id": job_id,
                "datasetId": dataset_id,
                "filename": original_name,
                "status": "queued",
                "completedChunks": 0,
                "totalChunks": int(manifest["chunk_count"]),
                "outputPath": str(output_path),
            }
        download_executor.submit(run_download, job_id, dataset_id, output_path)
        return jsonify({"jobId": job_id}), 202

    @app.get("/api/downloads/<job_id>")
    def download_status(job_id: str) -> Any:
        with download_jobs_lock:
            job = download_jobs.get(job_id)
            if job is None:
                return jsonify({"error": "找不到這次下載工作"}), 404
            public_job = {key: value for key, value in job.items() if key != "outputPath"}
            return jsonify(public_job)

    @app.get("/api/downloads/<job_id>/file")
    def download_file(job_id: str) -> Any:
        with download_jobs_lock:
            job = download_jobs.get(job_id)
            if job is None:
                return jsonify({"error": "找不到這次下載工作"}), 404
            if job["status"] != "complete":
                return jsonify({"error": "下載檔案尚未準備完成"}), 409
            output_path = Path(str(job["outputPath"]))
            filename = str(job["filename"])
        if not output_path.is_file():
            return jsonify({"error": "已驗證的暫存檔不存在"}), 410
        response = send_file(
            output_path,
            as_attachment=True,
            download_name=filename,
            mimetype="application/octet-stream",
            max_age=0,
            conditional=True,
        )
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        # Keep the job's verified final file for local use and subsequent Range requests.
        return response

    @app.post("/api/uploads")
    def start_upload() -> Any:
        uploaded = request.files.get("file")
        if uploaded is None or not uploaded.filename:
            return jsonify({"error": "請先選擇檔案"}), 400
        backend = request.form.get("backend", settings.upload_backend)
        if backend not in BACKENDS:
            return jsonify({"error": "不支援的 metadata 欄位"}), 400
        encoding = request.form.get("encoding", settings.storage_encoding)
        if encoding not in ENCODINGS:
            return jsonify({"error": "不支援的資料編碼"}), 400
        safe_payload = settings.safe_payload_for(backend)
        if safe_payload < 1:
            return jsonify({"error": f"尚未設定 {backend.upper()}_SAFE_PAYLOAD_BYTES"}), 503

        original_name = Path(uploaded.filename).name
        # Werkzeug already wrote into the job's staged file: no second full-file copy.
        uploaded.stream.flush()
        temp_path = Path(uploaded.stream.name)
        uploaded.close()

        job_id = str(uuid4())
        with jobs_lock:
            timings = request.upload_timings
            timings.transition("queued")
            jobs[job_id] = {
                "id": job_id,
                "status": "queued",
                "filename": original_name,
                "completedChunks": 0,
                "totalChunks": 0,
                "backend": backend,
                "encoding": encoding,
                "_timings": timings,
            }
        request.claimed_paths.add(str(temp_path))
        try:
            executor.submit(run_upload, job_id, temp_path, original_name, backend, safe_payload, encoding)
        except BaseException:
            request.claimed_paths.discard(str(temp_path))
            with jobs_lock:
                jobs.pop(job_id, None)
            raise
        return jsonify({"jobId": job_id}), 202

    @app.get("/api/uploads/<job_id>")
    def upload_status(job_id: str) -> Any:
        with jobs_lock:
            job = jobs.get(job_id)
            if job is None:
                return jsonify({"error": "找不到這次上傳工作"}), 404
            public_job = {key: value for key, value in job.items() if key != "_timings"}
            public_job["timings"] = job["_timings"].snapshot()
            return jsonify(public_job)

    def run_upload(
        job_id: str,
        temp_path: Path,
        original_name: str,
        backend: str,
        safe_payload: int,
        encoding: str,
    ) -> None:
        def update_phase(phase: str) -> None:
            with jobs_lock:
                jobs[job_id]["_timings"].transition(phase)

        def update_progress(completed: int, total: int) -> None:
            with jobs_lock:
                jobs[job_id].update(
                    status="uploading",
                    completedChunks=completed,
                    totalChunks=total,
                )

        try:
            with jobs_lock:
                jobs[job_id]["status"] = "preparing"
                jobs[job_id]["_timings"].transition("setup")
            job_service = transfer_pool.service()
            uploader = NameChunkUploader(
                job_service,
                settings.with_overrides(storage_encoding=encoding),
                database,
                safe_payload,
                backend,
                transfer_pool=transfer_pool,
            )
            manifest = uploader.upload_file(
                temp_path, filename=original_name, progress=update_progress, phase=update_phase
            )
            with jobs_lock:
                jobs[job_id]["_timings"].finish()
                jobs[job_id].update(
                    status="complete",
                    datasetId=manifest.dataset_id,
                    originalSize=manifest.original_size,
                    compressedSize=manifest.compressed_size,
                    compression=manifest.compression,
                    encoding=manifest.encoding,
                    sha256=manifest.sha256,
                    completedChunks=manifest.chunk_count,
                    totalChunks=manifest.chunk_count,
                )
        except Exception as exc:  # UI boundary: preserve a visible recoverable error.
            LOGGER.exception("Upload job %s failed", job_id)
            with jobs_lock:
                jobs[job_id]["_timings"].finish()
                jobs[job_id].update(status="error", error=str(exc))
        finally:
            with jobs_lock:
                # Numeric aggregates only: never log credentials or payloads.
                timings = jobs[job_id]["_timings"].snapshot()
                LOGGER.info("Upload timings job=%s status=%s total=%.3fs phases=%s",
                            job_id, jobs[job_id]["status"], timings["totalSeconds"],
                            {key: round(value, 3) for key, value in timings["seconds"].items()})
                if transfer_pool.request_pacer:
                    LOGGER.info("Upload pacing pool summary: %s", transfer_pool.request_pacer.snapshot())
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                LOGGER.warning("Could not remove temporary upload %s", temp_path)

    def restore_to_file(dataset_id, output_path, progress=None):
        return VerifiedDownloader(transfer_pool.service(), settings, database,
                                  transfer_pool=transfer_pool).download_file(
            dataset_id, output_path, progress=progress,
        )

    def run_download(job_id: str, dataset_id: str, output_path: Path) -> None:
        def update_progress(completed: int, total: int) -> None:
            with download_jobs_lock:
                download_jobs[job_id].update(
                    status="verifying",
                    completedChunks=completed,
                    totalChunks=total,
                )

        try:
            with download_jobs_lock:
                download_jobs[job_id]["status"] = "preparing"
            restore_to_file(dataset_id, output_path, update_progress)
            with download_jobs_lock:
                download_jobs[job_id].update(
                    status="complete",
                    savedPath=str(output_path.resolve()),
                )
        except Exception as exc:  # UI boundary: preserve a visible recoverable error.
            LOGGER.exception("Download job %s failed", job_id)
            output_path.unlink(missing_ok=True)
            with download_jobs_lock:
                download_jobs[job_id].update(status="error", error=str(exc))

    return app
