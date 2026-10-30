"""Lost create responses should be confirmed before another mutation is sent."""

from pathlib import Path

import pytest

from src.config import Settings
from src.database import Database
from src.models import DatasetManifest
from src.uploader import NameChunkUploader
from src.errors import DriveAPIError
from tests.test_batch_upload import BatchDrive, http_error


@pytest.mark.parametrize("created_before_timeout", [0, 2, 4])
def test_real_batch_retry_reads_back_uncertain_writes_and_never_recreates_confirmed_ids(
        tmp_path, monkeypatch, created_before_timeout):
    from src.retry import execute_with_backoff
    monkeypatch.setattr("src.uploader.execute_with_backoff", lambda factory, **kw:
        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    database = Database(tmp_path / "index.db")
    database.initialize()
    database.save_dataset(DatasetManifest("test", "f", 1, 1, "zstd", "s", 4))
    group = [(f"id-{i}", i, "payload", "hash") for i in range(4)]
    database.save_pending_chunks("test", [(i, ident, digest) for ident, i, _, digest in group])
    drive = BatchDrive(database)
    original_batch = drive.new_batch_http_request
    failed = False

    def new_batch(callback):
        batch = original_batch(callback)
        original_execute = batch.execute

        def execute():
            nonlocal failed
            if not failed:
                failed = True
                drive.batch_sizes.append(len(batch.requests))
                for _, request in batch.requests[:created_before_timeout]:
                    request.execute()
                raise TimeoutError("writes may have succeeded before response was lost")
            return original_execute()

        batch.execute = execute
        return batch

    drive.new_batch_http_request = new_batch

    def get(*, fileId, **kwargs):
        class Request:
            def execute(self):
                drive.get_calls += 1
                if fileId not in drive.created:
                    raise http_error(404)
                return dict(drive.created[fileId])
        return Request()

    drive.get = get
    settings = Settings(Path("unused"), "folder", api_min_interval_seconds=0,
                        upload_batch_size=4, max_attempts=2)
    uploader = NameChunkUploader(drive, settings, database, 25000, "description")
    indices, error = uploader._upload_group("test", group)
    assert error is None and sorted(indices) == [0, 1, 2, 3]
    assert drive.get_calls == 4
    assert len(drive.create_ids) == 4  # No second POST for an already verified object.
    assert drive.batch_sizes == ([4] if created_before_timeout == 4 else [4, 4-created_before_timeout])
    assert database.count_complete_chunks("test") == 0  # Worker never commits before coordinator.


def test_readback_conflict_is_fatal_and_is_never_overwritten(tmp_path, monkeypatch):
    from src.retry import execute_with_backoff
    monkeypatch.setattr("src.uploader.execute_with_backoff", lambda factory, **kw:
        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    source = tmp_path / "tiny.bin"
    source.write_bytes(b"payload")
    database = Database(tmp_path / "index.db")
    drive = BatchDrive(database)
    original_create = drive.create

    def create(**kwargs):
        original = original_create(**kwargs)
        class Request:
            def execute(self):
                original.execute()
                drive.created[kwargs["body"]["id"]]["description"] = "corrupted"
                raise TimeoutError("lost response")
        return Request()

    drive.create = create
    settings = Settings(Path("unused"), "folder", api_min_interval_seconds=0, max_attempts=2)
    uploader = NameChunkUploader(drive, settings, database, 25000, "description")
    with pytest.raises(DriveAPIError, match="conflict"):
        uploader.upload_file(source)
    assert len(drive.create_ids) == 1
    dataset = database.list_datasets()[0]
    assert database.count_complete_chunks(dataset["dataset_id"]) == 0


@pytest.mark.parametrize('failure', [403, 'timeout'])
def test_failed_confirmation_is_not_permission_to_replay_a_write(tmp_path, monkeypatch, failure):
    from src.retry import execute_with_backoff
    monkeypatch.setattr('src.uploader.execute_with_backoff', lambda factory, **kw:
        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    database = Database(tmp_path / 'index.db')
    source = tmp_path / 'source.bin'
    source.write_bytes(b'payload')
    drive = BatchDrive(database)
    original_create = drive.create

    def create(**kwargs):
        request = original_create(**kwargs)
        class Lost:
            def execute(self):
                request.execute()
                raise TimeoutError('lost after successful create')
        return Lost()

    def get(**kwargs):
        if failure == 'timeout':
            raise TimeoutError('lookup also timed out')
        raise http_error(failure)

    drive.create, drive.get = create, get
    settings = Settings(Path('unused'), 'folder', api_min_interval_seconds=0, max_attempts=2)
    with pytest.raises(DriveAPIError, match='unable to confirm'):
        NameChunkUploader(drive, settings, database, 25000, 'description').upload_file(source)
    assert len(drive.create_ids) == 1
    assert database.count_complete_chunks(database.list_datasets()[0]['dataset_id']) == 0
