import os
import hashlib
from pathlib import Path

import pytest

from src import encoder
from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.errors import IntegrityError
from src.streaming import BLOCK_SIZE, prepare_upload, restore_stream
from src.transfer_pool import TransferPool
from src.uploader import NameChunkUploader
from tests.test_uploader import FakeDrive


@pytest.mark.parametrize("gzip_fallback", [False, True])
@pytest.mark.parametrize("size", [0, 1, 2, 3, 4095, 1048577, 2097187])
def test_preparation_matches_legacy(tmp_path, monkeypatch, gzip_fallback, size):
    if gzip_fallback:
        monkeypatch.setattr(encoder, "zstandard", None)
    data = os.urandom(size)
    source = tmp_path / "source"
    source.write_bytes(data)
    compressed = encoder.compress_data(data)
    with prepare_upload(source, max_bytes=3 * BLOCK_SIZE, spool_dir=tmp_path) as prepared:
        assert prepared.stream.read() == compressed
        assert prepared.sha256 == hashlib.sha256(data).hexdigest()
        for width in (3, 31, 25000):
            assert "".join(prepared.chunks(width)) == encoder.encode_base64url(compressed)
        assert prepared.encoded_size == len(encoder.encode_base64url(compressed))


def test_real_caller_seam_streams_and_resumes(tmp_path, monkeypatch):
    data = os.urandom(3 * BLOCK_SIZE + 7)
    source = tmp_path / "source"
    source.write_bytes(data)
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "index.db",
                        api_min_interval_seconds=0)
    db = Database(settings.sqlite_path)
    drive = FakeDrive()
    # Production paths must not read a whole source or delegate disk writes to download_bytes.
    monkeypatch.setattr(Path, "read_bytes", lambda _: pytest.fail("whole-file read"))
    monkeypatch.setattr(db, "get_chunks", lambda _: pytest.fail("whole-index read"))
    uploader = NameChunkUploader(drive, settings, db, 25000, "description")
    manifest = uploader.upload_file(source)
    assert uploader.upload_file(source).dataset_id == manifest.dataset_id
    downloader = VerifiedDownloader(drive, settings, db)
    monkeypatch.setattr(downloader, "download_bytes", lambda *a, **kw: pytest.fail("whole-file download"))
    target = tmp_path / "output"
    downloader.download_file(manifest.dataset_id, target)
    with target.open("rb") as handle:
        assert handle.read() == data


@pytest.mark.parametrize("gzip_fallback", [False, True])
@pytest.mark.parametrize("width", [1, 2, 3, 31])
def test_restores_legacy_odd_chunks_with_bounded_writes(tmp_path, monkeypatch, gzip_fallback, width):
    if gzip_fallback:
        monkeypatch.setattr(encoder, "zstandard", None)
    data = b"x" * (7 * BLOCK_SIZE + 1)
    compressed = encoder.compress_data(data)
    manifest = dict(original_size=len(data), compressed_size=len(compressed),
                    sha256=hashlib.sha256(data).hexdigest(), compression=encoder.compression_name(compressed))
    class Sink:
        def __init__(self):
            self.digest = hashlib.sha256()
            self.count = 0
        def write(self, value):
            assert len(value) <= BLOCK_SIZE
            self.count += len(value)
            self.digest.update(value)
    sink = Sink()
    restore_stream(iter(encoder.split_chunks(encoder.encode_base64url(compressed), width)), manifest, sink)
    assert sink.count == len(data)
    assert sink.digest.hexdigest() == manifest["sha256"]
    with pytest.raises(IntegrityError):
        restore_stream(iter(encoder.split_chunks(encoder.encode_base64url(compressed), width)),
                       {**manifest, "sha256": "bad"}, Sink())


def test_over_limit_rejected_before_read(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.write_bytes(b"123")
    monkeypatch.setattr(Path, "stat", lambda _: type("Stat", (), {"st_size": 2 * 1024**3 + 1})())
    monkeypatch.setattr(Path, "open", lambda *a, **kw: pytest.fail("must not read over-limit file"))
    with pytest.raises(ValueError, match="limit"):
        with prepare_upload(source, max_bytes=2 * 1024**3):
            pytest.fail("must reject")


def test_pool_reuses_thread_owned_clients_and_ids(tmp_path):
    import threading
    drive = FakeDrive()
    clients = []
    class Client:
        def __init__(self):
            self.owner = threading.get_ident()
            clients.append(self)
        def files(self):
            assert self.owner == threading.get_ident()
            return drive
    pool = TransferPool(Client, 1)
    try:
        first = pool.executor.submit(pool.service).result()
        assert pool.executor.submit(pool.service).result() is first
        settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "index.db", api_min_interval_seconds=0)
        for index in range(2):
            source = tmp_path / f"source-{index}"
            source.write_bytes(b"hello")
            NameChunkUploader(pool.service(), settings, Database(settings.sqlite_path), 25000,
                              transfer_pool=pool).upload_file(source)
        assert len(clients) == 2  # Coordinator and one persistent network worker.
        assert drive.generate_calls == 1
    finally:
        pool.close()


@pytest.mark.parametrize("workers", [16, 24, 32, 48, 64, 96, 128])
def test_tiny_uploads_do_not_initialize_unused_pool_clients(tmp_path, workers):
    import threading
    from tests.test_batch_upload import BatchDrive

    database = Database(tmp_path / "index.db")
    drive = BatchDrive(database)
    clients = []

    class Client:
        def __init__(self):
            self.owner = threading.get_ident()
            clients.append(self)

        def files(self):
            assert self.owner == threading.get_ident()
            return drive

        def new_batch_http_request(self, callback):
            assert self.owner == threading.get_ident()
            return drive.new_batch_http_request(callback)

    settings = Settings(Path("unused"), "folder", upload_workers=workers,
                        upload_batch_size=8, api_min_interval_seconds=0)
    settings.validate()
    pool = TransferPool(Client, workers)
    try:
        for index in range(2):
            source = tmp_path / f"tiny-{index}.bin"
            source.write_bytes(b"tiny upload")
            manifest = NameChunkUploader(pool.service(), settings, database, 25000,
                                         "description", transfer_pool=pool).upload_file(source)
            assert manifest.chunk_count == 1
        assert len(clients) == 2  # Coordinator + one lazy, reused network worker.
        assert drive.generate_calls == 1
    finally:
        pool.close()


def test_failed_integrity_preserves_existing_destination(tmp_path):
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "index.db", api_min_interval_seconds=0)
    db = Database(settings.sqlite_path)
    drive = FakeDrive()
    source = tmp_path / "source"
    source.write_bytes(b"original" * 100)
    manifest = NameChunkUploader(drive, settings, db, 31).upload_file(source)
    with db.connect() as connection:
        connection.execute("UPDATE datasets SET sha256 = 'bad' WHERE dataset_id = ?", (manifest.dataset_id,))
    target = tmp_path / "target"
    target.write_bytes(b"existing")
    with pytest.raises(IntegrityError, match="SHA-256"):
        VerifiedDownloader(drive, settings, db).download_file(manifest.dataset_id, target)
    assert target.read_bytes() == b"existing"
    assert list(tmp_path.glob(".target.*")) == []


def test_resume_conflict_detected_before_new_remote_writes(tmp_path):
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "index.db", api_min_interval_seconds=0)
    db = Database(settings.sqlite_path)
    drive = FakeDrive()
    source = tmp_path / "source"
    source.write_bytes(os.urandom(4000))
    manifest = NameChunkUploader(drive, settings, db, 31).upload_file(source)
    with db.connect() as connection:
        connection.execute("DELETE FROM chunks WHERE dataset_id = ? AND chunk_index = 0", (manifest.dataset_id,))
        connection.execute("UPDATE chunks SET chunk_hash = 'bad' WHERE dataset_id = ? AND chunk_index = ?",
                           (manifest.dataset_id, manifest.chunk_count - 1))
    count = len(drive.created)
    from src.errors import DriveAPIError
    with pytest.raises(DriveAPIError, match="resume state"):
        NameChunkUploader(drive, settings, db, 25000).upload_file(source)
    assert len(drive.created) == count


def test_disk_spool_is_closed_on_transfer_failure(tmp_path, monkeypatch):
    from src import streaming
    monkeypatch.setattr(streaming, "MEMORY_SPOOL_BYTES", 4096)
    source = tmp_path / "source"
    source.write_bytes(os.urandom(64000))
    directory = tmp_path / "spool"
    with pytest.raises(RuntimeError, match="network failure"):
        with prepare_upload(source, max_bytes=64000, spool_dir=directory) as prepared:
            assert prepared.compressed_size > streaming.MEMORY_SPOOL_BYTES
            stream = prepared.stream
            raise RuntimeError("network failure")
    assert stream.closed
    assert list(directory.iterdir()) == []


def test_spool_full_disk_reports_storage_error_and_cleans(tmp_path, monkeypatch):
    from src import streaming
    import errno
    monkeypatch.setattr(streaming, "MEMORY_SPOOL_BYTES", 4096)
    monkeypatch.setattr(streaming.shutil, "disk_usage", lambda _: type("Usage", (), {"free": 0})())
    source = tmp_path / "source"
    source.write_bytes(os.urandom(64000))
    directory = tmp_path / "spool"
    with pytest.raises(OSError) as failure:
        with prepare_upload(source, max_bytes=64000, spool_dir=directory):
            pytest.fail("must reject a full spool disk")
    assert failure.value.errno == errno.ENOSPC
    assert list(directory.iterdir()) == []


@pytest.mark.parametrize("width", [1, 3, 7, 25000])
@pytest.mark.parametrize("gzip_fallback", [False, True])
def test_base85_stream_is_partition_independent(tmp_path, monkeypatch, width, gzip_fallback):
    if gzip_fallback:
        monkeypatch.setattr(encoder, "zstandard", None)
    source = tmp_path / "source"
    data = os.urandom(64000)
    source.write_bytes(data)
    import io
    with prepare_upload(source, max_bytes=len(data)) as prepared:
        expected = encoder.encode_payload(encoder.compress_data(data), "base85")
        assert "".join(prepared.chunks(width, "base85")) == expected
        assert prepared.encoded_size_for("base85") == len(expected)
        output = io.BytesIO()
        manifest = dict(original_size=len(data), compressed_size=prepared.compressed_size,
                        compression=prepared.compression, sha256=prepared.sha256, encoding="base85")
        restore_stream(prepared.chunks(width, "base85"), manifest, output)
        assert output.getvalue() == data
