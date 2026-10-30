"""Request-count feedback loop through real upload/restore/resume interfaces."""

import os
from pathlib import Path

import pytest

from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.errors import ChunkCorruptedError
from src.uploader import NameChunkUploader
from tests.test_batch_upload import BatchDrive


@pytest.mark.parametrize("backend", ["description", "combined"])
@pytest.mark.parametrize("encoding", ["base64url", "base85"])
@pytest.mark.parametrize("failure", [None, "ambiguous"])
def test_dense_payload_reduces_calls_without_losing_resume_integrity(tmp_path, backend, encoding, failure):
    original = bytes(range(256)) * 20 + os.urandom(100000)
    source = tmp_path / "source.bin"
    source.write_bytes(original)
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "index.db",
                        name_safe_payload_bytes=8500, description_safe_payload_bytes=25000,
                        upload_batch_size=4, max_attempts=1, api_min_interval_seconds=0,
                        storage_encoding=encoding, upload_backend=backend)
    settings.validate()
    database = Database(settings.sqlite_path)
    drive = BatchDrive(database, failure=failure)
    uploader = NameChunkUploader(drive, settings, database, settings.safe_payload_for(backend), backend)
    if failure:
        with pytest.raises(TimeoutError):
            uploader.upload_file(source)
    manifest = uploader.upload_file(source)
    assert manifest.encoding == encoding
    assert manifest.chunk_count == len(drive.created)
    for body in drive.created.values():
        assert "media_body" not in body
        if backend == "combined":
            assert len(body["name"].encode()) <= 8500
        assert len(body.get("description", "").encode()) <= 25000
    assert VerifiedDownloader(drive, settings, database).download_bytes(manifest.dataset_id)[0] == original
    assert uploader.upload_file(source).dataset_id == manifest.dataset_id

    # Same input, compression and storage field: only encoding/packing differs.
    baseline_settings = settings.with_overrides(storage_encoding="base64url", upload_backend="description")
    baseline = NameChunkUploader(drive, baseline_settings, database, 25000, "description").upload_file(source)
    if backend == "combined":
        assert manifest.chunk_count <= baseline.chunk_count * 0.8 + 1
    if encoding == "base85":
        assert manifest.dataset_id != baseline.dataset_id
    if backend == "combined":
        first = drive.created[database.get_chunk(manifest.dataset_id, 0)["drive_file_id"]]
        first["name"] += "x"
        with pytest.raises(ChunkCorruptedError):
            VerifiedDownloader(drive, baseline_settings, database).download_bytes(manifest.dataset_id)


def test_new_encoding_does_not_break_old_dataset_download_or_resume(tmp_path):
    source = tmp_path / "source.bin"
    source.write_bytes(os.urandom(80000))
    old = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "db.sqlite",
                   description_safe_payload_bytes=25000, name_safe_payload_bytes=8500,
                   api_min_interval_seconds=0)
    database = Database(old.sqlite_path)
    drive = BatchDrive(database)
    manifest = NameChunkUploader(drive, old, database, 25000, "description").upload_file(source)
    new = old.with_overrides(storage_encoding="base85", upload_backend="combined")
    assert VerifiedDownloader(drive, new, database).download_bytes(manifest.dataset_id)[0] == source.read_bytes()
    assert NameChunkUploader(drive, old, database, 25000, "description").upload_file(source).dataset_id == manifest.dataset_id


def test_combined_requires_both_measured_field_capacities():
    settings = Settings(Path("unused"), "folder", name_safe_payload_bytes=8500)
    assert settings.safe_payload_for("combined") == 0


def test_large_upload_reserves_ids_without_repeated_serial_calls(tmp_path):
    from tests.test_uploader import FakeDrive
    source = tmp_path / "source.bin"
    source.write_bytes(os.urandom(3 * 1024**2))
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "db.sqlite",
                        api_min_interval_seconds=0)
    drive = FakeDrive()
    manifest = NameChunkUploader(drive, settings, Database(settings.sqlite_path), 25000).upload_file(source)
    assert 100 < manifest.chunk_count < 1000
    assert drive.generate_calls == 1
