from pathlib import Path
from typing import Any
import os
import threading

import pytest

from src.config import Settings
from src.database import Database
from src.uploader import NameChunkUploader


class Request:
    def __init__(self, value: dict[str, Any]) -> None:
        self.value = value

    def execute(self) -> dict[str, Any]:
        return self.value


class FakeDrive:
    def __init__(self) -> None:
        self.next_id = 0
        self.created: dict[str, dict[str, Any]] = {}
        self.generate_calls = 0
        self.get_calls = 0

    def files(self) -> "FakeDrive":
        return self

    def generateIds(self, **_: Any) -> Request:
        self.generate_calls += 1
        count = _["count"]
        ids = [f"id-{index}" for index in range(self.next_id + 1, self.next_id + count + 1)]
        self.next_id += count
        return Request({"ids": ids})

    def create(self, *, body: dict[str, Any], **_: Any) -> Request:
        self.created[body["id"]] = body
        return Request(dict(body))

    def get(self, *, fileId: str, **_: Any) -> Request:
        self.get_calls += 1
        if fileId not in self.created:
            import httplib2
            from googleapiclient.errors import HttpError
            raise HttpError(httplib2.Response({'status': '404'}), b'{"error":{"message":"not found"}}')
        body = self.created[fileId]
        return Request(dict(body))


def test_upload_creates_metadata_only_objects_and_resumes(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"compress me" * 50)
    settings = Settings(
        credentials_path=Path("unused.json"),
        drive_folder_id="folder-id",
        sqlite_path=tmp_path / "index.db",
        api_min_interval_seconds=0,
    )
    drive = FakeDrive()
    uploader = NameChunkUploader(drive, settings, Database(settings.sqlite_path), 20)

    first = uploader.upload_file(source)
    created_count = len(drive.created)
    second = uploader.upload_file(source)

    assert created_count == first.chunk_count
    assert len(drive.created) == created_count
    assert second.dataset_id == first.dataset_id
    assert all("name" in body and "appProperties" in body for body in drive.created.values())
    assert drive.generate_calls == 1
    assert drive.get_calls == 0
    changed = NameChunkUploader(drive, settings, Database(settings.sqlite_path), 30)
    resumed = changed.upload_file(source)
    assert resumed.dataset_id == first.dataset_id
    assert resumed.chunk_size == 20
    assert len(drive.created) == created_count


def test_description_backend_uses_readable_names(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"description payload")
    settings = Settings(
        credentials_path=Path("unused.json"),
        drive_folder_id="folder-id",
        sqlite_path=tmp_path / "index.db",
        api_min_interval_seconds=0,
    )
    drive = FakeDrive()
    manifest = NameChunkUploader(
        drive, settings, Database(settings.sqlite_path), 20, "description"
    ).upload_file(source)

    assert manifest.storage_backend == "description"
    assert all("description" in body for body in drive.created.values())
    assert all(body["name"].startswith("metadata-chunk-") for body in drive.created.values())


@pytest.mark.parametrize("interrupt", [False, True])
@pytest.mark.parametrize("workers", [4, 8, 16, 64])
def test_parallel_upload_has_independent_clients_and_resumes(tmp_path, interrupt, workers):
    from src.downloader import VerifiedDownloader

    source = tmp_path / "parallel.bin"
    original = os.urandom(max(2048, workers * 100))
    source.write_bytes(original)
    settings = Settings(
        credentials_path=Path("unused.json"), drive_folder_id="folder-id",
        sqlite_path=tmp_path / "parallel.db", upload_workers=workers,
        api_min_interval_seconds=0, max_attempts=1,
    )
    database = Database(settings.sqlite_path)
    drive = FakeDrive()
    barrier = threading.Barrier(workers, timeout=5)
    lock = threading.Lock()
    threads = set()
    first_requests = [0]
    failed = [False]

    class WorkerDrive:
        def __init__(self):
            self.owner = threading.get_ident()

        def files(self):
            assert self.owner == threading.get_ident()
            return self

        def get(self, **kwargs):
            assert self.owner == threading.get_ident()
            with lock:
                return drive.get(**kwargs)

        def create(self, *, body, **kwargs):
            with lock:
                threads.add(self.owner)
                first_requests[0] += 1
                sequence = first_requests[0]

            class CreateRequest:
                def execute(self):
                    if sequence <= workers:
                        barrier.wait()
                    with lock:
                        if interrupt and not failed[0]:
                            failed[0] = True
                            raise TimeoutError("simulated network interruption")
                        assert body["id"] not in drive.created
                        drive.created[body["id"]] = dict(body)
                    return dict(body)
            return CreateRequest()

    progress = []
    uploader = NameChunkUploader(
        drive, settings, database, 100, "description", service_factory=WorkerDrive,
    )
    if interrupt:
        with pytest.raises(TimeoutError):
            uploader.upload_file(source)
        assert 0 < len(drive.created)
    manifest = uploader.upload_file(source, progress=lambda n, total: progress.append(n))
    assert len(threads) >= workers
    assert len(drive.created) == manifest.chunk_count
    assert database.count_complete_chunks(manifest.dataset_id) == manifest.chunk_count
    assert progress == list(range(1, manifest.chunk_count + 1))
    restored, _ = VerifiedDownloader(drive, settings, database).download_bytes(manifest.dataset_id)
    assert restored == original
