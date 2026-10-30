"""A single remaining chunk does not need a batch envelope."""

from pathlib import Path
import ssl

import pytest

from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.uploader import NameChunkUploader
from tests.test_batch_upload import BatchDrive


@pytest.mark.parametrize("ambiguous", [False, True])
def test_single_chunk_uses_direct_create_and_keeps_idempotent_confirmation(tmp_path, ambiguous):
    source = tmp_path / "tiny.bin"
    data = b"tiny payload"
    source.write_bytes(data)
    database = Database(tmp_path / "index.db")
    drive = BatchDrive(database)
    calls = []
    original_create = drive.create
    def create(**kwargs):
        request = original_create(**kwargs)
        execute = request.execute
        def execute_once():
            calls.append(kwargs["body"]["id"])
            result = execute()
            if ambiguous and len(calls) == 1:
                raise TimeoutError("response lost after create")
            return result
        request.execute = execute_once
        return request
    drive.create = create
    settings = Settings(Path("unused"), "root", upload_batch_size=8,
                        api_min_interval_seconds=0, max_attempts=1)
    uploader = NameChunkUploader(drive, settings, database, 25000, "description",
                                 confirm_ambiguous_writes=False)  # Explicit legacy/manual-resume control.
    if ambiguous:
        with pytest.raises(TimeoutError):
            uploader.upload_file(source)
    manifest = uploader.upload_file(source)
    assert manifest.chunk_count == 1
    assert drive.batch_sizes == []
    assert len(set(calls)) == 1
    assert len(drive.created) == 1
    assert VerifiedDownloader(drive, settings, database).download_bytes(manifest.dataset_id)[0] == data


@pytest.mark.parametrize('ambiguous', [False, True])
def test_single_chunk_tls_eof_retries_with_same_id_without_manual_resume(tmp_path, monkeypatch, ambiguous):
    from src.retry import execute_with_backoff
    monkeypatch.setattr('src.uploader.execute_with_backoff', lambda factory, **kw:
        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    source = tmp_path / 'tiny-eof.bin'
    source.write_bytes(b'tiny TLS recovery payload')
    database = Database(tmp_path / 'index.db')
    drive = BatchDrive(database)
    calls = []
    original_create = drive.create

    def create(**kwargs):
        request = original_create(**kwargs)
        original_execute = request.execute
        def execute():
            calls.append(kwargs['body']['id'])
            if len(calls) == 1:
                if ambiguous:
                    original_execute()
                raise ssl.SSLEOFError(8, 'EOF occurred in violation of protocol (_ssl.c:2427)')
            return original_execute()
        request.execute = execute
        return request

    drive.create = create
    settings = Settings(Path('unused'), 'root', upload_batch_size=8,
        api_min_interval_seconds=0, max_attempts=2)
    manifest = NameChunkUploader(drive, settings, database, 25000, 'description').upload_file(source)
    assert len(calls) == (1 if ambiguous else 2) and len(set(calls)) == 1
    assert len(drive.created) == 1 and drive.batch_sizes == []
    assert database.count_complete_chunks(manifest.dataset_id) == 1
    assert VerifiedDownloader(drive, settings, database).download_bytes(manifest.dataset_id)[0] == source.read_bytes()
