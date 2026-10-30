"""Exercise batching at the real upload/resume boundary, without a network."""

from pathlib import Path
import threading
import ssl
import httplib2
import pytest
from googleapiclient.errors import HttpError

from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.errors import DriveAPIError
from src.uploader import NameChunkUploader
from tests.test_uploader import FakeDrive


def http_error(status):
    return HttpError(httplib2.Response({"status": str(status)}), b'{"error":{"message":"injected"}}')


class BatchDrive(FakeDrive):
    def __init__(self, database, *, failure=None):
        super().__init__()
        self.database = database
        self.failure = failure
        self.failed = False
        self.batch_sizes = []
        self.create_ids = []

    def create(self, *, body, **kwargs):
        drive = self

        class DeferredRequest:
            def execute(self):
                row = drive.database.get_chunk(body["appProperties"]["dataset_id"], int(body["appProperties"]["chunk_index"]))
                assert row is not None and row["drive_file_id"] == body["id"]
                drive.create_ids.append(body["id"])
                if body["id"] in drive.created:
                    raise http_error(409)
                drive.created[body["id"]] = dict(body)
                return dict(body)

        return DeferredRequest()

    def new_batch_http_request(self, callback):
        drive = self

        class Batch:
            def __init__(self):
                self.requests = []

            def add(self, request, request_id):
                self.requests.append((request_id, request))

            def execute(self):
                drive.batch_sizes.append(len(self.requests))
                # Deliberately reverse replies: request IDs, not order, map chunks.
                for position, (request_id, request) in enumerate(reversed(self.requests)):
                    if drive.failure and not drive.failed and position == 1:
                        drive.failed = True
                        if drive.failure in {"timeout", "ambiguous", "ssl", "ssl_ambiguous"}:
                            if drive.failure in {"ambiguous", "ssl_ambiguous"}:
                                # Remote write succeeds, but its response is lost.
                                request.execute()
                            if drive.failure in {"ssl", "ssl_ambiguous"}:
                                raise ssl.SSLEOFError(8, "EOF occurred in violation of protocol (_ssl.c:2427)")
                            raise TimeoutError("batch connection interrupted")
                        callback(request_id, None, http_error(drive.failure))
                        continue
                    try:
                        response = request.execute()
                    except HttpError as error:
                        callback(request_id, None, error)
                    else:
                        callback(request_id, response, None)

        return Batch()


@pytest.mark.parametrize("backend", ["name", "description"])
@pytest.mark.parametrize("failure", [None, 503, 429, 400, "timeout", "ambiguous", "ssl", "ssl_ambiguous"])
def test_batch_verifies_partial_results_and_resumes(tmp_path, monkeypatch, backend, failure):
    from src.retry import execute_with_backoff
    monkeypatch.setattr("src.uploader.execute_with_backoff", lambda factory, **kw:
        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    source = tmp_path / "source.bin"
    original = bytes(range(256)) * 8
    source.write_bytes(original)
    settings = Settings(
        credentials_path=Path("unused"), drive_folder_id="folder",
        sqlite_path=tmp_path / "index.db", upload_batch_size=4,
        api_min_interval_seconds=0, max_attempts=2 if failure in {429, 503, "ssl", "ssl_ambiguous"} else 1,
    )
    database = Database(settings.sqlite_path)
    drive = BatchDrive(database, failure=failure)
    uploader = NameChunkUploader(drive, settings, database, 60, backend)
    progress = []

    def report(count, total):
        dataset = database.list_datasets()[0]
        assert database.count_complete_chunks(dataset["dataset_id"]) >= count
        progress.append(count)

    if failure in (400, "timeout", "ambiguous"):
        with pytest.raises(Exception):
            uploader.upload_file(source, progress=report)
        dataset = database.list_datasets()[0]
        assert database.count_complete_chunks(dataset["dataset_id"]) > 0
        confirmed_ids = {r["drive_file_id"] for r in database.get_chunks(dataset["dataset_id"]) if r["status"] == "complete"}
        drive.create_ids.clear()
        progress.clear()
    else:
        confirmed_ids = set()

    manifest = uploader.upload_file(source, progress=report)
    assert confirmed_ids.isdisjoint(drive.create_ids)
    assert len(drive.created) == manifest.chunk_count
    assert database.count_complete_chunks(manifest.dataset_id) == manifest.chunk_count
    assert progress == list(range(1, manifest.chunk_count + 1))
    assert max(drive.batch_sizes) == 4
    assert VerifiedDownloader(drive, settings, database).download_bytes(manifest.dataset_id)[0] == original


def test_pending_rows_are_atomic_and_conflicts_do_not_replace_ids(tmp_path):
    from src.models import DatasetManifest

    database = Database(tmp_path / "index.db")
    database.initialize()
    database.save_dataset(DatasetManifest(dataset_id="d", filename="f", original_size=1,
        compressed_size=1, compression="zstd", sha256="s", chunk_count=3))
    database.save_pending_chunks("d", [(0, "first", "hash"), (1, "second", "hash")])
    with pytest.raises(Exception):
        database.save_pending_chunks("d", [(2, "third", "hash"), (0, "different", "hash")])
    assert database.get_chunk("d", 2) is None
    assert database.get_chunk("d", 0)["drive_file_id"] == "first"
    with pytest.raises(ValueError):
        database.mark_chunks_complete("d", [0, 99])
    assert database.count_complete_chunks("d") == 0
    database.mark_chunks_complete("d", [0, 1])
    assert database.count_complete_chunks("d") == 2


@pytest.mark.parametrize("workers", [2, 4, 24, 32, 48, 64, 96, 128])
def test_parallel_batches_have_thread_owned_clients_and_drain_successes(tmp_path, workers):
    source = tmp_path / "parallel.bin"
    # Enough batches to occupy every worker, including experimental 96/128.
    import os
    original = os.urandom(max(4096, workers * 4 * 24)) if workers > 4 else bytes(range(256)) * 4
    source.write_bytes(original)
    settings = Settings(credentials_path=Path("unused"), drive_folder_id="folder",
        sqlite_path=tmp_path / "index.db", upload_workers=workers, upload_batch_size=4,
        api_min_interval_seconds=0, max_attempts=1)
    settings.validate()
    database = Database(settings.sqlite_path)
    drive = BatchDrive(database, failure=400)
    barrier = threading.Barrier(workers, timeout=5)
    lock = threading.Lock()
    sequences = [0]
    owners = set()

    class Worker:
        def __init__(self):
            self.owner = threading.get_ident()

        def files(self):
            assert threading.get_ident() == self.owner
            return drive

        def new_batch_http_request(self, callback):
            assert threading.get_ident() == self.owner
            batch = drive.new_batch_http_request(callback)
            execute = batch.execute
            owner = self.owner
            with lock:
                owners.add(owner)
                sequences[0] += 1
                sequence = sequences[0]

            def checked_execute():
                assert threading.get_ident() == owner
                if sequence <= workers:
                    barrier.wait()
                return execute()
            batch.execute = checked_execute
            return batch

    uploader = NameChunkUploader(drive, settings, database, 24, "description", service_factory=Worker)
    with pytest.raises(HttpError):
        uploader.upload_file(source)
    dataset = database.list_datasets()[0]
    confirmed = {row["drive_file_id"] for row in database.get_chunks(dataset["dataset_id"])
        if row["status"] == "complete"}
    assert len(owners) == workers
    assert len(confirmed) >= (workers - 1) * 4
    drive.create_ids.clear()
    manifest = uploader.upload_file(source)
    assert confirmed.isdisjoint(drive.create_ids)
    assert VerifiedDownloader(drive, settings, database).download_bytes(manifest.dataset_id)[0] == original


@pytest.mark.parametrize('readback', [False, True])
def test_lost_entire_response_reuses_ids_and_confirms_remote_objects(tmp_path, readback):
    source = tmp_path / "source.bin"
    source.write_bytes(bytes(range(256)))
    settings = Settings(credentials_path=Path("unused"), drive_folder_id="folder",
        sqlite_path=tmp_path / "index.db", upload_batch_size=100,
        api_min_interval_seconds=0, max_attempts=1)
    database = Database(settings.sqlite_path)
    drive = BatchDrive(database)
    original_batch = drive.new_batch_http_request
    lost = [False]

    def new_batch(callback):
        batch = original_batch(callback)
        execute = batch.execute
        def interrupt():
            if not lost[0]:
                lost[0] = True
                for _, request in batch.requests:
                    request.execute()
                raise TimeoutError("all writes succeeded but HTTP response was lost")
            return execute()
        batch.execute = interrupt
        return batch

    drive.new_batch_http_request = new_batch
    uploader = NameChunkUploader(drive, settings, database, 60, "description",
                                 confirm_ambiguous_writes=readback)
    if readback:
        uploader.upload_file(source)
    else:
        with pytest.raises(TimeoutError):
            uploader.upload_file(source)
    dataset = database.list_datasets()[0]
    assert database.count_complete_chunks(dataset["dataset_id"]) == (dataset['chunk_count'] if readback else 0)
    ids = set(drive.created)
    manifest = uploader.upload_file(source)
    assert set(drive.created) == ids
    assert drive.get_calls == manifest.chunk_count
    assert database.count_complete_chunks(manifest.dataset_id) == manifest.chunk_count


def test_corrupt_batch_response_never_marks_that_chunk_complete(tmp_path):
    source = tmp_path / "source.bin"
    source.write_bytes(bytes(range(256)))
    settings = Settings(credentials_path=Path("unused"), drive_folder_id="folder",
        sqlite_path=tmp_path / "index.db", upload_batch_size=4,
        api_min_interval_seconds=0, max_attempts=1)
    database = Database(settings.sqlite_path)
    drive = BatchDrive(database)
    new_batch = drive.new_batch_http_request
    corrupted_ids = []

    def corrupting_batch(callback):
        def corrupt(request_id, response, exception):
            if response is not None and not corrupted_ids:
                corrupted_ids.append(request_id)
                response = dict(response, description=response["description"] + "A")
            callback(request_id, response, exception)
        return new_batch(corrupt)

    drive.new_batch_http_request = corrupting_batch
    uploader = NameChunkUploader(drive, settings, database, 60, "description")
    with pytest.raises(DriveAPIError, match="conflicts"):
        uploader.upload_file(source)
    dataset = database.list_datasets()[0]
    rows = database.get_chunks(dataset["dataset_id"])
    assert next(r for r in rows if r["drive_file_id"] == corrupted_ids[0])["status"] == "pending"
    manifest = uploader.upload_file(source)
    assert len(drive.created) == manifest.chunk_count
    assert database.count_complete_chunks(manifest.dataset_id) == manifest.chunk_count
