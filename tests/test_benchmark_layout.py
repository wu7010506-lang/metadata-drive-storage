"""Experimental layout changes must not move or delete user objects."""

import os
from pathlib import Path

import pytest

from scripts.benchmark_layout import ParentShardedService, cleanup_empty_folders, create_folders
from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.uploader import NameChunkUploader
from tests.test_batch_upload import BatchDrive
from tests.test_batch_upload import http_error
from src.retry import execute_with_backoff


def test_sharded_layout_uses_real_upload_restore_and_resume(tmp_path):
    source = tmp_path / "source.bin"
    data = os.urandom(150000)
    source.write_bytes(data)
    settings = Settings(Path("unused"), "root", upload_batch_size=4, api_min_interval_seconds=0)
    database = Database(tmp_path / "isolated.db")
    drive = BatchDrive(database)
    service = ParentShardedService(drive, "root", ["shard-a", "shard-b", "shard-c"])
    uploader = NameChunkUploader(service, settings, database, 25000, "description")
    manifest = uploader.upload_file(source)
    assert {body["parents"][0] for body in drive.created.values()} == {"shard-a", "shard-b", "shard-c"}
    assert VerifiedDownloader(service, settings, database).download_bytes(manifest.dataset_id)[0] == data
    assert uploader.upload_file(source).dataset_id == manifest.dataset_id
    count = len(drive.created)
    with pytest.raises(ValueError, match="configured root"):
        service.files().create(body={"parents": ["unrelated"], "appProperties": {"chunk_index": "0"}})
    assert len(drive.created) == count


def test_folder_cleanup_refuses_nonempty_or_unowned_folder(tmp_path):
    database = Database(tmp_path / "isolated.db")
    database.initialize()
    with database.connect() as connection:
        connection.execute("CREATE TABLE benchmark_folders (id TEXT PRIMARY KEY, marker TEXT, root TEXT, status TEXT)")
        connection.execute("INSERT INTO benchmark_folders VALUES ('folder', 'ours', 'root', 'complete')")
    deleted = []
    class Request:
        def __init__(self, result):
            self.result = result
        def execute(self):
            return self.result
    class Drive:
        marker = "ours"
        children = [{"id": "user-file"}]
        next_page = None
        def files(self):
            return self
        def get(self, **kwargs):
            return Request(dict(id="folder", mimeType="application/vnd.google-apps.folder",
                                parents=["root"], appProperties={"benchmark_marker": self.marker}))
        def list(self, **kwargs):
            assert kwargs["q"] == "'folder' in parents"
            return Request({"files": self.children, "nextPageToken": self.next_page})
        def delete(self, **kwargs):
            deleted.append(kwargs["fileId"])
            return Request({})
    drive = Drive()
    settings = Settings(Path("unused"), "root", max_attempts=1)
    with pytest.raises(RuntimeError, match="nonempty"):
        cleanup_empty_folders(drive, database, settings)
    drive.children = []
    drive.next_page = "another-page"
    with pytest.raises(RuntimeError, match="incompletely listed"):
        cleanup_empty_folders(drive, database, settings)
    drive.next_page = None
    drive.marker = "not-ours"
    with pytest.raises(RuntimeError, match="ownership"):
        cleanup_empty_folders(drive, database, settings)
    assert deleted == []
    drive.marker = "ours"
    cleanup_empty_folders(drive, database, settings)
    assert deleted == ["folder"]


def test_folder_creation_journals_ids_before_writes_and_confirms_ambiguous_create(tmp_path, monkeypatch):
    database = Database(tmp_path / "isolated.db")
    database.initialize()
    objects = {}
    attempts = []
    class Request:
        def __init__(self, callback):
            self.callback = callback
        def execute(self):
            return self.callback()
    class Drive:
        def files(self):
            return self
        def generateIds(self, **kwargs):
            return Request(lambda: {"ids": ["folder-a", "folder-b"]})
        def create(self, body, **kwargs):
            def run():
                with database.connect() as connection:
                    row = connection.execute("SELECT status FROM benchmark_folders WHERE id=?", (body["id"],)).fetchone()
                    assert row["status"] == "pending"
                attempts.append(body["id"])
                if body["id"] in objects:
                    raise http_error(409)
                objects[body["id"]] = dict(body)
                if len(attempts) == 1:
                    raise TimeoutError("folder was created but response was lost")
                return dict(body)
            return Request(run)
        def get(self, fileId, **kwargs):
            return Request(lambda: dict(objects[fileId]))
    monkeypatch.setattr("scripts.benchmark_layout.execute_with_backoff", lambda factory, **kwargs:
        execute_with_backoff(factory, sleep=lambda _: None, **kwargs))
    settings = Settings(Path("unused"), "root", max_attempts=2)
    assert create_folders(Drive(), database, settings, 2, "marker") == ["folder-a", "folder-b"]
    assert attempts == ["folder-a", "folder-a", "folder-b"]
    with database.connect() as connection:
        assert [row["status"] for row in connection.execute("SELECT status FROM benchmark_folders")] == ["complete", "complete"]
