"""Deterministic phase accounting through the actual multipart upload caller."""

import io
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from src.config import Settings
from src.upload_timings import UploadTimings
from tests.test_uploader import FakeDrive
from ui import server


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_live_snapshots_are_non_mutating_and_finished_time_is_frozen():
    clock = Clock()
    timings = UploadTimings(clock=clock)
    clock.advance(2)
    assert timings.snapshot()["seconds"]["receiving"] == 2
    assert timings.snapshot()["seconds"]["receiving"] == 2
    timings.transition("queued")
    clock.advance(3)
    timings.transition("preparing")
    clock.advance(5)
    timings.finish()
    clock.advance(100)
    result = timings.snapshot()
    assert result["phase"] is None
    assert result["totalSeconds"] == 10
    assert result["seconds"]["receiving"] == 2
    assert result["seconds"]["queued"] == 3
    assert result["seconds"]["preparing"] == 5
    assert sum(result["seconds"].values()) == result["totalSeconds"]
    timings.finish()
    assert timings.snapshot() == result
    with pytest.raises(ValueError):
        timings.transition("not-a-phase")


@pytest.mark.parametrize("fail", [None, "preparing", "indexing", "transferring"])
def test_real_ui_pipeline_reports_correct_stages_and_freezes_errors(tmp_path, monkeypatch, fail):
    from src import uploader as uploader_module

    clock = Clock()
    monkeypatch.setattr(server, "UploadTimings", lambda: UploadTimings(clock=clock), raising=False)
    drive = FakeDrive()
    monkeypatch.setattr(server, "build_drive_service", lambda _: drive)
    monkeypatch.setattr(server, "verify_target_folder", lambda *_: {"name": "Test"})
    original_write = server._LimitedUploadStream.write

    def write(stream, data):
        clock.advance(2)
        return original_write(stream, data)

    monkeypatch.setattr(server._LimitedUploadStream, "write", write)
    original_prepare = uploader_module.prepare_upload

    @contextmanager
    def prepare(*args, **kwargs):
        with original_prepare(*args, **kwargs) as prepared:
            clock.advance(5)
            if fail == "preparing":
                raise ValueError("injected preparation failure")
            yield prepared

    monkeypatch.setattr(uploader_module, "prepare_upload", prepare)
    original_initialize = server.Database.initialize
    initialize_calls = 0

    def initialize(database):
        nonlocal initialize_calls
        initialize_calls += 1
        clock.advance(3)
        if fail == "indexing" and initialize_calls > 1:
            raise ValueError("injected indexing failure")
        return original_initialize(database)

    monkeypatch.setattr(server.Database, "initialize", initialize)
    original_generate = drive.generateIds

    def generate(**kwargs):
        clock.advance(7)
        return original_generate(**kwargs)

    drive.generateIds = generate
    original_create = drive.create

    def create(**kwargs):
        clock.advance(11)
        if fail == "transferring":
            raise ValueError("injected transfer failure")
        return original_create(**kwargs)

    drive.create = create
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "index.db",
                        upload_temp_dir=tmp_path / "staging", upload_backend="description",
                        description_safe_payload_bytes=25000, api_min_interval_seconds=0)
    app = server.create_app(settings)
    try:
        client = app.test_client()
        response = client.post("/api/uploads", data={"file": (io.BytesIO(b"small payload"), "test.bin")})
        assert response.status_code == 202
        endpoint = f'/api/uploads/{response.get_json()["jobId"]}'
        for _ in range(200):
            job = client.get(endpoint).get_json()
            if job["status"] in {"complete", "error"}:
                break
            time.sleep(0.005)
        assert job["status"] == ("error" if fail else "complete"), job
        timings = job["timings"]
        assert timings["phase"] is None
        expected = dict(receiving=2, queued=0, setup=0, preparing=5,
                        indexing=0 if fail == "preparing" else 3,
                        transferring=18 if fail in {None, "transferring"} else 0)
        assert timings["seconds"] == expected
        assert timings["totalSeconds"] == sum(expected.values())
        assert drive.get_calls == 0  # Measurement must not add read-back API calls.
        clock.advance(100)
        assert client.get(endpoint).get_json()["timings"] == timings
    finally:
        app.extensions["close_transfers"]()
    assert not list(settings.upload_temp_dir.iterdir())


def test_second_real_upload_keeps_queue_wait_separate(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(server, "UploadTimings", lambda: UploadTimings(clock=clock))
    drive = FakeDrive()
    monkeypatch.setattr(server, "build_drive_service", lambda _: drive)
    monkeypatch.setattr(server, "verify_target_folder", lambda *_: {"name": "Test"})
    entered, release = threading.Event(), threading.Event()
    original_create = drive.create

    def create(**kwargs):
        entered.set()
        assert release.wait(5), "test did not release first upload"
        return original_create(**kwargs)

    drive.create = create
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "queue.db",
                        upload_temp_dir=tmp_path / "staging", upload_backend="description",
                        description_safe_payload_bytes=25000, api_min_interval_seconds=0)
    app = server.create_app(settings)
    try:
        client = app.test_client()
        first = client.post("/api/uploads", data={"file": (io.BytesIO(b"first"), "first.bin")})
        assert first.status_code == 202 and entered.wait(5)
        second = client.post("/api/uploads", data={"file": (io.BytesIO(b"second"), "second.bin")})
        endpoint = f'/api/uploads/{second.get_json()["jobId"]}'
        clock.advance(7)
        queued = client.get(endpoint).get_json()
        assert queued["status"] == "queued"
        assert queued["timings"]["phase"] == "queued"
        assert queued["timings"]["seconds"]["queued"] == 7
        release.set()
        for _ in range(200):
            job = client.get(endpoint).get_json()
            if job["status"] in {"complete", "error"}:
                break
            time.sleep(0.005)
        assert job["status"] == "complete", job
        assert job["timings"]["seconds"]["queued"] == 7
        assert job["timings"]["seconds"]["transferring"] == 0
        assert job["timings"]["totalSeconds"] == 7
    finally:
        release.set()
        app.extensions["close_transfers"]()


def test_fully_completed_resume_does_not_report_new_transfer_phase(tmp_path):
    from src.database import Database
    from src.uploader import NameChunkUploader

    path = tmp_path / "tiny.bin"
    path.write_bytes(b"repeatable payload")
    drive = FakeDrive()
    settings = Settings(Path("unused"), "folder", api_min_interval_seconds=0)
    uploader = NameChunkUploader(drive, settings, Database(tmp_path / "resume.db"), 25000, "description")
    stages = []
    first = uploader.upload_file(path, phase=stages.append)
    assert stages == ["preparing", "indexing", "transferring"]
    stages.clear()
    assert uploader.upload_file(path, phase=stages.append).dataset_id == first.dataset_id
    assert stages == ["preparing", "indexing"]
    assert len(drive.created) == 1
