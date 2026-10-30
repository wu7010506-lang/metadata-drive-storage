#!/usr/bin/env python3
"""Compare worker/batch profiles using fresh, equal-sized Drive datasets."""

import argparse
import json
import logging
import math
import os
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth import build_drive_service, verify_target_folder, DriveServiceFactory
from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.errors import ConfigurationError
from src.retry import execute_with_backoff, is_retryable_error
from src.uploader import NameChunkUploader


class HttpTimings:
    """Thread-safe bounded latency samples; never retain URLs, headers or payloads."""

    def __init__(self, *, clock=time.perf_counter, sample_limit=10000):
        if not 1 <= sample_limit <= 10000:
            raise ValueError("sample_limit must be between 1 and 10000")
        self.clock = clock
        self.sample_limit = sample_limit
        self.lock = threading.Lock()
        self.groups = {}

    def wrap(self, service):
        original = service._http.request
        def request(uri, method="GET", *args, **kwargs):
            started = self.clock()
            outcome = "unknown"
            gzip_response, decoded_bytes = False, 0
            body = kwargs.get('body', args[0] if args else None)
            request_body_bytes = len(body.encode('utf-8')) if isinstance(body, str) else len(body) if isinstance(body, bytes) else 0
            try:
                result = original(uri, method, *args, **kwargs)
                outcome = str(result[0].get("status", "unknown"))
                gzip_response = result[0].get('-content-encoding', result[0].get('content-encoding')) == 'gzip'
                decoded_bytes = len(result[1]) if isinstance(result[1], bytes) else 0
                return result
            except Exception as error:
                outcome = type(error).__name__
                raise
            finally:
                category = "batch" if "/batch/" in uri else method.upper()
                self.record(category, self.clock() - started, outcome,
                            gzip_response=gzip_response, decoded_bytes=decoded_bytes,
                            request_body_bytes=request_body_bytes,
                            encoded_request_body_bytes=(getattr(service._http, 'last_batch_request_body_bytes', request_body_bytes)
                                if category == 'batch' and getattr(service._http, 'batch_request_gzip', False) else request_body_bytes))
        service._http.request = request
        return service

    def record(self, category, seconds, outcome, *, gzip_response=False, decoded_bytes=0,
               request_body_bytes=0, encoded_request_body_bytes=0):
        with self.lock:
            group = self.groups.setdefault(category, dict(count=0, total=0.0, maximum=0.0,
                                                         samples=[], outcomes={}, gzip_responses=0, decoded_bytes=0,
                                                         request_body_bytes=0, encoded_request_body_bytes=0))
            group["count"] += 1
            group["total"] += seconds
            group["maximum"] = max(group["maximum"], seconds)
            if len(group["samples"]) < self.sample_limit:
                group["samples"].append(seconds)
            group["outcomes"][outcome] = group["outcomes"].get(outcome, 0) + 1
            group['gzip_responses'] += int(gzip_response)
            group['decoded_bytes'] += decoded_bytes
            group['request_body_bytes'] += request_body_bytes
            group['encoded_request_body_bytes'] += encoded_request_body_bytes

    def reset(self):
        with self.lock:
            self.groups.clear()

    def snapshot(self):
        with self.lock:
            result = {}
            for category, group in self.groups.items():
                samples = sorted(group["samples"])
                result[category] = dict(count=group["count"], sample_count=len(samples),
                    total_seconds=round(group["total"], 3), max_seconds=round(group["maximum"], 3),
                    p50_seconds=round(samples[math.ceil(len(samples) * .5) - 1], 3),
                    p95_seconds=round(samples[math.ceil(len(samples) * .95) - 1], 3),
                    outcomes=dict(group["outcomes"]), gzip_responses=group['gzip_responses'],
                    decoded_bytes=group['decoded_bytes'], request_body_bytes=group['request_body_bytes'],
                    encoded_request_body_bytes=group['encoded_request_body_bytes'])
            return result


class MeasuredFactory:
    """Keep benchmark clients independent and explicitly close them between runs."""

    def __init__(self, base, timings):
        self.base, self.timings = base, timings
        self.lock = threading.Lock()
        self.clients = []

    def __call__(self):
        service = self.timings.wrap(self.base())
        with self.lock:
            self.clients.append(service)
        return service

    def close(self):
        with self.lock:
            clients, self.clients = self.clients, []
        for client in clients:
            client.close()


class InjectedBatchResponseLoss(TimeoutError):
    """Benchmark-only fault: a successful create response was deliberately dropped."""


class BatchResponseLoss:
    def __init__(self, count):
        self.remaining = count
        self.injected = 0
        self.enabled = True
        self.first_loss_at = None
        self.lock = threading.Lock()

    def wrap(self, service):
        original = service._http.request

        def request(uri, method='GET', *args, **kwargs):
            result = original(uri, method, *args, **kwargs)
            body = kwargs.get('body', args[0] if args else b'')
            is_create = ('POST /drive/v3/files' in body if isinstance(body, str)
                         else b'POST /drive/v3/files' in body if isinstance(body, bytes) else False)
            if (uri.split('?', 1)[0] == 'https://www.googleapis.com/batch/drive/v3'
                    and method.upper() == 'POST' and is_create and int(result[0]['status']) == 200):
                with self.lock:
                    if self.enabled and self.remaining:
                        self.remaining -= 1
                        self.injected += 1
                        if self.first_loss_at is None:
                            self.first_loss_at = time.perf_counter()
                        raise InjectedBatchResponseLoss('benchmark discarded a successful create batch response')
            return result

        service._http.request = request
        return service


class RetryCounter(logging.Handler):
    def __init__(self):
        super().__init__()
        self.retries = 0
        self.rate_limits = 0
        self.quota_retries = 0
        self.http_status_counts = {}
        self.ambiguous_chunks_confirmed = 0

    def emit(self, record):
        self.ambiguous_chunks_confirmed += getattr(record, 'ambiguous_chunks_confirmed', 0)
        if "retrying in" in record.getMessage():
            self.retries += 1
            status = int(getattr(record, "api_status", 0))
            self.http_status_counts[status] = self.http_status_counts.get(status, 0) + 1
            if getattr(record, "api_status", None) == 429:
                self.rate_limits += 1
            # Production retry logging emits a 403 only for recognized rate limits.
            if getattr(record, "api_status", None) in {403, 429}:
                self.quota_retries += 1


def stop_after_quota(counter, factory, phase):
    """Do not continue benchmark load after recognized quota backoff."""
    if counter.quota_retries:
        factory.close()
        print(json.dumps(dict(event="stopped_after_quota", phase=phase,
                              quota_retries=counter.quota_retries)), flush=True)
        raise SystemExit("rate limit observed; no next upload or immediate cleanup; retain the isolated index/report for recovery")


class TimedDatabase(Database):
    """Measure connection/transaction overhead, not just remote throughput."""

    def __init__(self, path):
        super().__init__(path)
        self.transactions = 0
        self.transaction_seconds = 0.0

    @contextmanager
    def connect(self):
        started = time.perf_counter()
        try:
            with super().connect() as connection:
                yield connection
        finally:
            self.transactions += 1
            self.transaction_seconds += time.perf_counter() - started


def clean_verified_dataset(service, database, dataset_id, settings=None, *,
                           service_factory=None, cleanup_workers=1):
    """Delete only IDs from this benchmark's verified manifest, never a folder."""
    if not 1 <= cleanup_workers <= 8:
        raise ValueError("cleanup_workers must be between 1 and 8")
    if cleanup_workers > 1 and service_factory is None:
        raise ValueError("parallel cleanup requires independent clients")
    records = database.get_chunks(dataset_id)
    manifest = database.get_dataset(dataset_id)
    if not manifest or len(records) != int(manifest["chunk_count"]) or any(
        row["status"] != "complete" for row in records
    ):
        raise RuntimeError("refusing cleanup of an incomplete benchmark")
    # Large delete batches can time out even when the remote deletion succeeds.
    # Small batches, stable IDs and treating 404 as success make cleanup resumable.
    local = threading.local()
    clients = []
    clients_lock = threading.Lock()

    def delete_group(start):
        if cleanup_workers == 1:
            client = service
        else:
            if not hasattr(local, "service"):
                local.service = service_factory()
                with clients_lock:
                    clients.append(local.service)
            client = local.service
        done = set()
        errors = {}
        def callback(request_id, response, exception):
            status = int(getattr(getattr(exception, "resp", None), "status", 0))
            if exception is None or status == 404:
                done.add(request_id)
                errors.pop(request_id, None)
            else:
                errors[request_id] = exception

        def request_factory():
            batch = client.new_batch_http_request(callback=callback)
            for row in records[start:start + 4]:
                if row["drive_file_id"] not in done:
                    batch.add(client.files().delete(fileId=row["drive_file_id"], supportsAllDrives=True),
                        request_id=row["drive_file_id"])
            class DeleteRound:
                def execute(self):
                    batch.execute()
                    if errors:
                        # DELETE is idempotent, but Drive can return 409 while
                        # mutating a shared parent. Retry only these same IDs;
                        # do not interpret conflict as successful deletion.
                        for error in errors.values():
                            status = int(getattr(getattr(error, "resp", None), "status", 0))
                            if status != 409 and not is_retryable_error(error):
                                raise error
                        error = next(iter(errors.values()))
                        status = int(getattr(getattr(error, "resp", None), "status", 0))
                        if status == 409:
                            raise TimeoutError("transient benchmark DELETE conflict (HTTP 409)") from error
                        raise error
            return DeleteRound()

        execute_with_backoff(request_factory, max_attempts=settings.max_attempts if settings else 5,
            initial_delay=settings.initial_delay_seconds if settings else 1,
            max_delay=settings.max_delay_seconds if settings else 16)

    try:
        if cleanup_workers == 1:
            for start in range(0, len(records), 4):
                delete_group(start)
        else:
            with ThreadPoolExecutor(max_workers=cleanup_workers) as executor:
                # Each group owns its callback/retry state and each thread its HTTP client.
                for _ in executor.map(delete_group, range(0, len(records), 4)):
                    pass
    finally:
        for client in clients:
            close = getattr(client, "close", None)
            if close:
                close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", nargs="+", type=int, default=[4, 8])
    parser.add_argument("--chunks", type=int, default=32)
    parser.add_argument("--profiles", nargs="+", help="WORKERSxBATCH_SIZE, e.g. 16x1 2x16 4x16")
    parser.add_argument("--storage-profiles", nargs="+", help="BACKEND:ENCODING, e.g. description:base64url combined:base85")
    parser.add_argument("--input-bytes", type=int, help="Use the same explicit source size across all profiles (max 64 MiB)")
    parser.add_argument("--shared-auth", action="store_true", help="Reuse initial token while each worker gets independent credentials/HTTP")
    parser.add_argument("--auth-profiles", nargs="+", choices=["legacy", "shared"],
        help="Compare authentication on the same input, reversing order each round")
    parser.add_argument("--http-timeouts", nargs="+", type=int,
        help="Compare HTTP socket timeout seconds (5..120), e.g. 60 20")
    parser.add_argument('--batch-compression-profiles', nargs='+', choices=['legacy', 'gzip'],
        default=['legacy'], help='A/B gzip negotiation on the batch response; full payload checks unchanged')
    parser.add_argument('--request-targets', nargs='+', type=int,
        help='Compare shared adaptive inner-request ceilings per minute; 0 is fixed control')
    parser.add_argument('--batch-request-compression-profiles', nargs='+', choices=['identity', 'gzip'],
        default=['identity'], help='Experimental batch POST body compression, not response compression')
    parser.add_argument('--ambiguous-recovery-profiles', nargs='+', choices=['legacy', 'readback'],
        default=['readback'], help='Compare blind replay with verified uncertain-write confirmation')
    parser.add_argument('--discard-batch-responses', type=int, default=0,
        help='Controlled test only: discard 0..4 successful create-batch responses per run')
    parser.add_argument("--single-chunk-protocols", nargs="+", choices=["auto", "batch"],
        default=["auto"], help="Serial microbenchmark: compare singleton fast path with forced batch envelope")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--download-workers", nargs="+", type=int, default=[1],
        help="Separately compare reconstruction with these reader counts")
    parser.add_argument("--baseline-download-once", action="store_true",
        help="Also time one sequential reconstruction before parallel readers")
    parser.add_argument("--database", type=Path, help="Use an isolated benchmark index")
    parser.add_argument("--report", type=Path, help="Append JSON lines of verified measurements")
    parser.add_argument("--cleanup", action="store_true", help="Delete only this run's verified remote test objects")
    parser.add_argument("--cleanup-at-end", action="store_true",
        help="With --cleanup, defer deletion until all profiles finish to isolate upload measurements")
    parser.add_argument("--cleanup-workers", type=int, default=1,
        help="Independent cleanup clients (1..8); deletion is outside measured upload time")
    parser.add_argument("--cleanup-report-only", action="store_true",
        help="Resume deletion of verified benchmark objects in --report and --database; no new uploads")
    parser.add_argument("--cleanup-dataset-id", help="Limit --cleanup-report-only to one verified test dataset")
    args = parser.parse_args()
    if args.shared_auth and args.auth_profiles:
        parser.error("use --shared-auth or --auth-profiles, not both")
    if not 1 <= args.cleanup_workers <= 8:
        parser.error("--cleanup-workers must be between 1 and 8")
    if args.cleanup_at_end and not args.cleanup:
        parser.error("--cleanup-at-end requires --cleanup")
    if not 1 <= args.chunks <= 4096:
        parser.error("--chunks must be between 1 and 4096")
    if not 1 <= args.repeats <= 10:
        parser.error("--repeats must be between 1 and 10")
    if not 0 <= args.discard_batch_responses <= 4:
        parser.error('--discard-batch-responses must be between 0 and 4')
    if args.input_bytes is not None and not 1 <= args.input_bytes <= 64 * 1024**2:
        parser.error("--input-bytes must be between 1 and 64 MiB")
    settings = Settings.from_env(PROJECT_ROOT / ".env")
    if args.cleanup_dataset_id and not args.cleanup_report_only:
        parser.error("--cleanup-dataset-id requires --cleanup-report-only")
    if args.cleanup_report_only:
        if args.report is None or args.database is None:
            parser.error("--cleanup-report-only requires --report and --database")
        database = Database(args.database)
        results = [json.loads(line) for line in args.report.read_text(encoding="utf-8").splitlines()]
        if args.cleanup_dataset_id:
            results = [r for r in results if r["dataset_id"] == args.cleanup_dataset_id]
            if not results:
                parser.error("dataset ID is absent from the verified benchmark report")
        for result in results:
            dataset = database.get_dataset(result["dataset_id"])
            if not result.get("verified") or not dataset or not str(dataset["filename"]).startswith("benchmark-"):
                raise RuntimeError("refusing cleanup of an unverified/non-benchmark dataset")
            records = database.get_chunks(result["dataset_id"])
            if len(records) != int(dataset["chunk_count"]) or any(r["status"] != "complete" for r in records):
                raise RuntimeError("refusing cleanup of an incomplete benchmark")
        cleanup_settings = settings.with_overrides(api_http_timeout_seconds=min(20, settings.api_http_timeout_seconds),
                                                  max_attempts=min(3, settings.max_attempts))
        service = build_drive_service(settings.credentials_path, http_timeout_seconds=cleanup_settings.api_http_timeout_seconds)
        verify_target_folder(service, cleanup_settings)
        cleanup_factory = DriveServiceFactory(settings.credentials_path, http_timeout_seconds=cleanup_settings.api_http_timeout_seconds)
        for result in results:
            clean_verified_dataset(service, database, result["dataset_id"], cleanup_settings,
                service_factory=cleanup_factory, cleanup_workers=args.cleanup_workers)
            print(json.dumps({"event": "test_objects_removed", "dataset_id": result["dataset_id"]}), flush=True)
        return
    try:
        profiles = [tuple(map(int, profile.split("x"))) for profile in args.profiles] if args.profiles else [
            (workers, settings.upload_batch_size) for workers in args.workers]
        for workers, batch_size in profiles:
            settings.with_overrides(upload_workers=workers, upload_batch_size=batch_size)
        if "batch" in args.single_chunk_protocols and any(workers != 1 for workers, _ in profiles):
            raise ConfigurationError("forced singleton envelope comparison requires workers=1")
        for readers in args.download_workers:
            settings.with_overrides(download_workers=readers)
        http_timeouts = args.http_timeouts or [settings.api_http_timeout_seconds]
        for timeout in http_timeouts:
            settings.with_overrides(api_http_timeout_seconds=timeout)
        request_targets = args.request_targets or [settings.upload_target_requests_per_minute]
        for target in request_targets:
            settings.with_overrides(upload_target_requests_per_minute=target)
        storage_profiles = ([tuple(profile.split(":")) for profile in args.storage_profiles]
                            if args.storage_profiles else [(settings.upload_backend, settings.storage_encoding)])
        for backend, encoding in storage_profiles:
            candidate = settings.with_overrides(upload_backend=backend, storage_encoding=encoding)
            if candidate.safe_payload_for(backend) < 1:
                raise ConfigurationError(f"configure measured capacities for {backend}")
    except (ValueError, TypeError, ConfigurationError) as exc:
        parser.error(str(exc))
    chunk_size = settings.safe_payload_for(settings.upload_backend)
    if chunk_size < 1:
        parser.error("configure a positive payload size first")
    database = TimedDatabase(args.database or settings.sqlite_path)
    auth_profiles = args.auth_profiles or (["shared"] if args.shared_auth else ["legacy"])
    # Random data keeps compression ratio stable; every run uses a new filename
    # so that resume skips cannot make the measurement artificially fast.
    data = os.urandom(args.input_bytes or (chunk_size * args.chunks * 3 // 4 - 128))
    run_id = uuid4().hex[:8]
    logging.basicConfig(level=logging.WARNING)
    counter = RetryCounter()
    logging.getLogger("src.retry").addHandler(counter)
    task_parent = settings.upload_temp_dir or PROJECT_ROOT / "data"
    task_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="metadata-benchmark-", dir=task_parent) as task_dir:
        source = Path(task_dir) / "benchmark.bin"
        source.write_bytes(data)
        combinations = [(workers, batch_size, backend, encoding, authentication, timeout, protocol, compression, target, request_compression, recovery)
                        for workers, batch_size in profiles
                        for backend, encoding in storage_profiles for authentication in auth_profiles
                        for timeout in http_timeouts for protocol in args.single_chunk_protocols
                        for compression in args.batch_compression_profiles for target in request_targets
                        for request_compression in args.batch_request_compression_profiles
                        for recovery in args.ambiguous_recovery_profiles]
        runs = [(repeat + 1, *profile) for repeat in range(args.repeats)
                for profile in (combinations if repeat % 2 == 0 else list(reversed(combinations)))]
        measured_download_baseline = False
        deferred_cleanup = []
        for repeat, workers, batch_size, backend, encoding, authentication, timeout, protocol, compression, target, request_compression, recovery in runs:
            current = settings.with_overrides(upload_workers=workers, upload_batch_size=batch_size,
                                             upload_backend=backend, storage_encoding=encoding,
                                             api_http_timeout_seconds=timeout,
                                             upload_target_requests_per_minute=target)
            current_chunk_size = current.safe_payload_for(backend)
            base_factory = (DriveServiceFactory(settings.credentials_path, http_timeout_seconds=timeout)
                            if authentication == "shared" else
                            lambda timeout=timeout: build_drive_service(settings.credentials_path,
                                                                       http_timeout_seconds=timeout))
            timings = HttpTimings()
            losses = BatchResponseLoss(args.discard_batch_responses)
            def compression_factory(base=base_factory, compression=compression, request_compression=request_compression, losses=losses):
                client = base()
                client._http.batch_response_gzip = compression == 'gzip'
                client._http.batch_request_gzip = request_compression == 'gzip'
                return losses.wrap(client) if args.discard_batch_responses else client
            factory = MeasuredFactory(compression_factory, timings)
            counter.retries = counter.rate_limits = counter.quota_retries = 0
            counter.http_status_counts.clear()
            counter.ambiguous_chunks_confirmed = 0
            service = factory()
            verify_target_folder(service, current)
            stop_after_quota(counter, factory, "startup")
            timings.reset()
            counter.retries = counter.rate_limits = counter.quota_retries = 0
            counter.http_status_counts.clear()
            database.transactions = 0
            database.transaction_seconds = 0.0
            print(json.dumps({"event": "starting", "repeat": repeat, "workers": workers,
                "batch_size": batch_size, "target_chunks": args.chunks,
                "backend": backend, "encoding": encoding, "authentication": authentication,
                "http_timeout_seconds": timeout, "single_chunk_protocol": protocol,
                'batch_response_compression': compression,
                'upload_target_requests_per_minute': target,
                'batch_request_compression': request_compression,
                'ambiguous_recovery': recovery,
                'planned_response_losses': args.discard_batch_responses,
                "original_bytes": len(data)}), flush=True)
            started = time.perf_counter()
            uploader = NameChunkUploader(
                service, current, database, current_chunk_size, current.upload_backend,
                service_factory=factory,
                confirm_ambiguous_writes=recovery == 'readback',
            )
            if protocol == "batch":
                # Controlled serial benchmark only. Production always selects the fast path.
                uploader._upload_group = uploader._upload_batch
            recovery_logger = logging.getLogger('src.uploader')
            recovery_logger.addHandler(counter)
            try:
                manifest = uploader.upload_file(source,
                    filename=f"benchmark-{run_id}-r{repeat}-{workers}x{batch_size}-{backend}-{encoding}-{authentication}-t{timeout}-{protocol}-{compression}-rpm{target}-req{request_compression}-{recovery}-loss{args.discard_batch_responses}.bin")
            finally:
                recovery_logger.removeHandler(counter)
            elapsed = time.perf_counter() - started
            recovery_elapsed = (time.perf_counter() - losses.first_loss_at
                                if losses.first_loss_at is not None else None)
            losses.enabled = False  # Never inject faults into verification or cleanup.
            retries, rate_limits, quota_retries = counter.retries, counter.rate_limits, counter.quota_retries
            retry_http_statuses = dict(counter.http_status_counts)
            ambiguous_chunks_confirmed = counter.ambiguous_chunks_confirmed
            upload_http = timings.snapshot()
            transactions, db_seconds = database.transactions, database.transaction_seconds
            print(json.dumps({"event": "upload_finished", "upload_seconds": round(elapsed, 3)}), flush=True)
            # Do not add a full verification download after observed quota load.
            # The isolated manifest remains available for targeted recovery.
            stop_after_quota(counter, factory, "upload")
            # Integrity verification is outside the upload timing window.
            downloads = []
            reader_counts = list(args.download_workers)
            if args.baseline_download_once and not measured_download_baseline and 1 not in reader_counts:
                reader_counts.insert(0, 1)
            measured_download_baseline = True
            for readers in reader_counts:
                download_started = time.perf_counter()
                restored, _ = VerifiedDownloader(service,
                    current.with_overrides(download_workers=readers), database,
                    service_factory=factory,
                ).download_bytes(manifest.dataset_id)
                if restored != data:
                    raise RuntimeError("benchmark round-trip verification failed")
                downloads.append({"workers": readers,
                    "seconds": round(time.perf_counter() - download_started, 3), "verified": True})
                print(json.dumps({"event": "download_verified", **downloads[-1]}), flush=True)
            result = {
                "repeat": repeat, "workers": workers, "batch_size": batch_size,
                "backend": backend, "encoding": encoding,
                "shared_auth": authentication == "shared",
                "http_timeout_seconds": timeout,
                "single_chunk_protocol": protocol,
                'batch_response_compression': compression,
                'upload_target_requests_per_minute': target,
                'batch_request_compression': request_compression,
                'ambiguous_recovery': recovery,
                'ambiguous_chunks_confirmed': ambiguous_chunks_confirmed,
                'injected_response_losses': losses.injected,
                'recovery_after_injected_loss_seconds': round(recovery_elapsed, 3) if recovery_elapsed is not None else None,
                'upload_pacing': uploader._request_pacer.snapshot() if uploader._request_pacer else None,
                "upload_http": upload_http,
                "chunks": manifest.chunk_count, "original_bytes": len(data),
                "upload_seconds": round(elapsed, 3),
                "upload_mib_per_second": round(len(data) / 1024**2 / elapsed, 4),
                "chunks_per_second": round(manifest.chunk_count / elapsed, 3),
                "sqlite_transactions": transactions, "sqlite_seconds": round(db_seconds, 3),
                "download_verify_seconds": downloads[0]["seconds"], "downloads": downloads,
                "retries": retries, "http_429_retries": rate_limits,
                "quota_retries": quota_retries,
                "retry_http_statuses": retry_http_statuses,
                "dataset_id": manifest.dataset_id, "verified": True,
            }
            print(json.dumps(result), flush=True)
            if args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                with args.report.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(result) + "\n")
            stop_after_quota(counter, factory, "transfer")
            if args.cleanup:
                if args.cleanup_at_end:
                    deferred_cleanup.append((manifest.dataset_id, current, factory))
                else:
                    clean_verified_dataset(service, database, manifest.dataset_id, current,
                        service_factory=factory, cleanup_workers=args.cleanup_workers)
                    print(json.dumps({"event": "test_objects_removed", "dataset_id": manifest.dataset_id}), flush=True)
            factory.close()
        for dataset_id, current, factory in deferred_cleanup:
            cleanup_service = factory()
            try:
                clean_verified_dataset(cleanup_service, database, dataset_id, current,
                    service_factory=factory, cleanup_workers=args.cleanup_workers)
                print(json.dumps({"event": "test_objects_removed", "dataset_id": dataset_id}), flush=True)
            finally:
                factory.close()


if __name__ == "__main__":
    main()

