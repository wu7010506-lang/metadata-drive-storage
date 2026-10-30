#!/usr/bin/env python3
"""EXPERIMENT ONLY: compare one parent with newly created temporary subfolders.

No production layout changes. Use an isolated index; retain it for recovery.
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from googleapiclient.errors import HttpError
from scripts.benchmark import HttpTimings, RetryCounter, TimedDatabase, clean_verified_dataset, stop_after_quota
from src.auth import DriveServiceFactory, verify_target_folder
from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.retry import execute_with_backoff
from src.uploader import NameChunkUploader


class ParentShardedService:
    """Benchmark-only proxy rewriting only fresh metadata-chunk parents."""

    def __init__(self, service, root, parents):
        if not 1 <= len(parents) <= 16:
            raise ValueError("use 1..16 benchmark parents")
        self.service, self.root, self.parents = service, root, tuple(parents)

    def __getattr__(self, name):
        return getattr(self.service, name)

    def files(self):
        owner = self
        resource = self.service.files()
        class Files:
            def __getattr__(self, name):
                return getattr(resource, name)

            def create(self, *, body, **kwargs):
                if "chunk_index" in body.get("appProperties", {}):
                    if body.get("parents") != [owner.root]:
                        raise ValueError("chunk parent differs from configured root")
                    parent = owner.parents[int(body["appProperties"]["chunk_index"]) % len(owner.parents)]
                    body = {**body, "parents": [parent]}
                return resource.create(body=body, **kwargs)
        return Files()


def execute(factory, settings):
    return execute_with_backoff(factory, max_attempts=settings.max_attempts,
        initial_delay=settings.initial_delay_seconds, max_delay=settings.max_delay_seconds)


def create_folders(service, database, settings, count, marker):
    with database.connect() as connection:
        connection.execute("""CREATE TABLE IF NOT EXISTS benchmark_folders
            (id TEXT PRIMARY KEY, marker TEXT, root TEXT, status TEXT)""")
    ids = execute(lambda: service.files().generateIds(count=count, space="drive", type="files"), settings)["ids"]
    if len(ids) != count:
        raise RuntimeError("wrong number of benchmark folder IDs")
    with database.connect() as connection:
        connection.executemany("INSERT INTO benchmark_folders VALUES (?, ?, ?, 'pending')",
            [(file_id, marker, settings.drive_folder_id) for file_id in ids])
    for index, file_id in enumerate(ids):
        body = dict(id=file_id, name=f"metadata-layout-benchmark-{marker}-{index}",
            mimeType="application/vnd.google-apps.folder", parents=[settings.drive_folder_id],
            appProperties={"benchmark_marker": marker})
        try:
            remote = execute(lambda: service.files().create(body=body,
                fields="id,mimeType,parents,appProperties", supportsAllDrives=True), settings)
        except HttpError as error:
            if int(error.resp.status) != 409:
                raise
            remote = execute(lambda: service.files().get(fileId=file_id,
                fields="id,mimeType,parents,appProperties", supportsAllDrives=True), settings)
        if any(remote.get(key) != body[key] for key in ("id", "mimeType", "parents", "appProperties")):
            raise RuntimeError("benchmark folder response mismatch")
        with database.connect() as connection:
            connection.execute("UPDATE benchmark_folders SET status='complete' WHERE id=?", (file_id,))
    return ids


def cleanup_empty_folders(service, database, settings):
    with database.connect() as connection:
        exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='benchmark_folders'").fetchone()
        rows = connection.execute("SELECT * FROM benchmark_folders WHERE status!='deleted'").fetchall() if exists else []
    for row in rows:
        if row["root"] != settings.drive_folder_id:
            raise RuntimeError("benchmark folder root changed; refusing deletion")
        file_id = row["id"]
        try:
            remote = execute(lambda: service.files().get(fileId=file_id,
                fields="id,mimeType,parents,appProperties", supportsAllDrives=True), settings)
            if (remote.get("id") != file_id or remote.get("mimeType") != "application/vnd.google-apps.folder"
                    or remote.get("parents") != [row["root"]]
                    or remote.get("appProperties") != {"benchmark_marker": row["marker"]}):
                raise RuntimeError("benchmark folder ownership mismatch; refusing deletion")
            # Include trashed children: never delete a folder containing user data.
            escaped = file_id.replace("\\", "\\\\").replace("'", "\\'")
            children = execute(lambda: service.files().list(q=f"'{escaped}' in parents", pageSize=1,
                fields="files(id),nextPageToken,incompleteSearch", supportsAllDrives=True,
                includeItemsFromAllDrives=True), settings)
            if children.get("files") or children.get("nextPageToken") or children.get("incompleteSearch"):
                raise RuntimeError("refusing deletion of a nonempty or incompletely listed benchmark folder")
            execute(lambda: service.files().delete(fileId=file_id, supportsAllDrives=True), settings)
        except HttpError as error:
            if int(error.resp.status) != 404:
                raise
        with database.connect() as connection:
            connection.execute("UPDATE benchmark_folders SET status='deleted' WHERE id=?", (file_id,))


def cleanup_report(service, database, settings, report, factory):
    results = ([json.loads(line) for line in report.read_text(encoding="utf-8").splitlines()]
               if report.exists() else [])
    for result in results:
        manifest = database.get_dataset(result["dataset_id"])
        if (not result.get("verified") or not manifest
                or not manifest["filename"].startswith("benchmark-layout-")
                or manifest["sha256"] != result["sha256"]):
            raise RuntimeError("refusing unverified/non-layout benchmark cleanup")
    for result in results:
        clean_verified_dataset(service, database, result["dataset_id"], settings,
                               service_factory=factory, cleanup_workers=8)
        print(json.dumps(dict(event="test_objects_removed", dataset_id=result["dataset_id"])), flush=True)
    cleanup_empty_folders(service, database, settings)
    print(json.dumps(dict(event="empty_test_folders_removed")), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--cleanup-only", action="store_true")
    parser.add_argument("--chunks", type=int, default=512)
    parser.add_argument("--parents", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    if not (1 <= args.chunks <= 1024 and 1 <= args.repeats <= 4
            and all(1 <= count <= 16 for count in args.parents)):
        parser.error("chunks 1..1024, repeats 1..4, parents 1..16")
    if not args.cleanup_only and (args.database.exists() or args.report.exists()):
        parser.error("use fresh benchmark index/report paths")
    if args.cleanup_only and not args.database.is_file():
        parser.error("cleanup requires the existing isolated benchmark index")
    settings = Settings.from_env(PROJECT_ROOT / ".env")
    settings = settings.with_overrides(upload_backend="description", storage_encoding="base64url")
    if settings.description_safe_payload_bytes < 1:
        parser.error("configure a measured description payload capacity")
    database = TimedDatabase(args.database)
    database.initialize()
    logging.basicConfig(level=logging.WARNING)
    counter = RetryCounter()
    logging.getLogger("src.retry").addHandler(counter)
    base_factory = DriveServiceFactory(settings.credentials_path,
                                      http_timeout_seconds=settings.api_http_timeout_seconds)
    service = base_factory()
    verify_target_folder(service, settings)
    if not args.cleanup_only:
        stop_after_quota(counter, service, "layout startup")
    if args.cleanup_only:
        cleanup_report(service, database, settings, args.report, base_factory)
        service.close()
        return
    source_bytes = os.urandom(settings.description_safe_payload_bytes * args.chunks * 3 // 4 - 128)
    marker = uuid4().hex[:12]
    directory = settings.upload_temp_dir or PROJECT_ROOT / "data"
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="metadata-layout-", dir=directory) as temporary:
        source = Path(temporary) / "benchmark.bin"
        source.write_bytes(source_bytes)
        for repeat in range(1, args.repeats + 1):
            for count in (args.parents if repeat % 2 else list(reversed(args.parents))):
                counter.retries = counter.rate_limits = counter.quota_retries = 0
                counter.http_status_counts.clear()
                setup_started = time.perf_counter()
                parents = ([settings.drive_folder_id] if count == 1 else create_folders(
                    service, database, settings, count, f"{marker}-r{repeat}-p{count}"))
                stop_after_quota(counter, service, "layout folder setup")
                setup_seconds = time.perf_counter() - setup_started
                timings = HttpTimings()
                clients, clients_lock = [], threading.Lock()
                def factory():
                    client = ParentShardedService(timings.wrap(base_factory()), settings.drive_folder_id, parents)
                    with clients_lock:
                        clients.append(client)
                    return client
                counter.retries = counter.rate_limits = counter.quota_retries = 0
                counter.http_status_counts.clear()
                try:
                    print(json.dumps(dict(event="starting", repeat=repeat, parents=count,
                                          original_bytes=len(source_bytes))), flush=True)
                    coordinator = factory()
                    verify_target_folder(coordinator, settings)
                    stop_after_quota(counter, coordinator, "layout worker setup")
                    timings.reset()
                    counter.retries = counter.rate_limits = counter.quota_retries = 0
                    counter.http_status_counts.clear()
                    database.transactions = database.transaction_seconds = 0
                    started = time.perf_counter()
                    manifest = NameChunkUploader(coordinator, settings, database,
                        settings.description_safe_payload_bytes, "description", service_factory=factory).upload_file(
                        source, filename=f"benchmark-layout-{marker}-r{repeat}-p{count}.bin")
                    elapsed = time.perf_counter() - started
                    upload_http = timings.snapshot()
                    retries, quota_retries = counter.retries, counter.quota_retries
                    statuses = dict(counter.http_status_counts)
                    db_seconds = database.transaction_seconds
                    print(json.dumps(dict(event="upload_finished", parents=count,
                                          upload_seconds=round(elapsed, 3))), flush=True)
                    restored, _ = VerifiedDownloader(coordinator, settings, database,
                        service_factory=factory).download_bytes(manifest.dataset_id)
                    if restored != source_bytes:
                        raise RuntimeError("layout benchmark restoration mismatch")
                    result = dict(repeat=repeat, parents=count, chunks=manifest.chunk_count,
                        workers=settings.upload_workers, batch_size=settings.upload_batch_size,
                        http_timeout_seconds=settings.api_http_timeout_seconds,
                        original_bytes=len(source_bytes), dataset_id=manifest.dataset_id, sha256=manifest.sha256,
                        setup_seconds=round(setup_seconds, 3), upload_seconds=round(elapsed, 3),
                        upload_mib_per_second=round(len(source_bytes) / 1024**2 / elapsed, 4),
                        retries=retries, quota_retries=quota_retries, retry_http_statuses=statuses,
                        upload_http=upload_http, sqlite_seconds=round(db_seconds, 3), verified=True)
                    print(json.dumps(result), flush=True)
                    args.report.parent.mkdir(parents=True, exist_ok=True)
                    with args.report.open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps(result) + "\n")
                    stop_after_quota(counter, coordinator, "layout transfer")
                finally:
                    for client in clients:
                        client.close()
    cleanup_report(service, database, settings, args.report, base_factory)
    service.close()


if __name__ == "__main__":
    main()
