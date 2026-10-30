"""Delete recovery must be idempotent and scoped to a completed test manifest."""

from pathlib import Path
import threading

import pytest

from scripts import benchmark
from src.config import Settings
from src.database import Database
from src.models import DatasetManifest
from src.retry import execute_with_backoff
from tests.test_batch_upload import http_error


@pytest.mark.parametrize("failure", ["lost", "conflict"])
@pytest.mark.parametrize("workers", [1, 2])
def test_cleanup_recovers_lost_response_and_already_deleted_ids(tmp_path, monkeypatch, failure, workers):
    settings = Settings(credentials_path=Path("unused"), drive_folder_id="folder")
    database = Database(tmp_path / "index.db")
    database.initialize()
    database.save_dataset(DatasetManifest(dataset_id="test", filename="benchmark-test.bin",
        original_size=1, compressed_size=1, compression="zstd", sha256="s", chunk_count=19))
    database.save_pending_chunks("test", [(i, f"id-{i}", "hash") for i in range(19)])
    database.mark_chunks_complete("test", range(19))
    objects = {f"id-{i}" for i in range(19)}
    # This unrelated object must survive every cleanup attempt.
    objects.add("user-object")
    sizes = []
    lost = [False]
    conflicted = [False]

    class Drive:
        def __init__(self):
            self.owner = threading.get_ident()

        def files(self):
            assert self.owner == threading.get_ident()
            return self

        def delete(self, fileId, **kwargs):
            class Request:
                def execute(self):
                    if fileId not in objects:
                        raise http_error(404)
                    objects.remove(fileId)
            return Request()

        def new_batch_http_request(self, callback):
            assert self.owner == threading.get_ident()
            class Batch:
                def __init__(self):
                    self.requests = []

                def add(self, request, request_id):
                    self.requests.append((request_id, request))

                def execute(self):
                    sizes.append(len(self.requests))
                    if failure == "lost" and not lost[0]:
                        lost[0] = True
                        for _, request in self.requests:
                            request.execute()
                        raise TimeoutError("response lost after successful deletion")
                    for request_id, request in self.requests:
                        if failure == "conflict" and not conflicted[0] and request_id == "id-0":
                            conflicted[0] = True
                            callback(request_id, None, http_error(409))
                            continue
                        try:
                            request.execute()
                        except Exception as error:
                            callback(request_id, None, error)
                        else:
                            callback(request_id, None, None)
            return Batch()

    monkeypatch.setattr(benchmark, "execute_with_backoff", lambda factory, **kw:
        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    benchmark.clean_verified_dataset(Drive(), database, "test", settings,
                                     service_factory=Drive, cleanup_workers=workers)
    assert objects == {"user-object"}
    assert max(sizes) <= 4
    benchmark.clean_verified_dataset(Drive(), database, "test", settings,
                                     service_factory=Drive, cleanup_workers=workers)
    assert objects == {"user-object"}


def test_cleanup_rejects_incomplete_manifest(tmp_path):
    database = Database(tmp_path / "index.db")
    database.initialize()
    database.save_dataset(DatasetManifest(dataset_id="test", filename="benchmark-test.bin",
        original_size=1, compressed_size=1, compression="zstd", sha256="s", chunk_count=1))
    database.save_pending_chunk("test", 0, "id", "hash")
    with pytest.raises(RuntimeError, match="incomplete"):
        benchmark.clean_verified_dataset(object(), database, "test")


@pytest.mark.parametrize("workers", [0, 9])
def test_cleanup_worker_count_is_bounded(workers):
    with pytest.raises(ValueError, match="between 1 and 8"):
        benchmark.clean_verified_dataset(None, None, "unused", cleanup_workers=workers)


def test_parallel_cleanup_requires_independent_clients():
    with pytest.raises(ValueError, match="independent clients"):
        benchmark.clean_verified_dataset(None, None, "unused", cleanup_workers=2)
