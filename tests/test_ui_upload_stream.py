import io
import time
from pathlib import Path

import pytest
from werkzeug.datastructures import FileStorage

from src.config import Settings
from ui import server


@pytest.fixture
def upload_app(tmp_path, monkeypatch):
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "db.sqlite",
                        max_upload_mb=1, upload_temp_dir=tmp_path / "staging",
                        name_safe_payload_bytes=100)
    monkeypatch.setattr(server, "build_drive_service", lambda _: object())
    monkeypatch.setattr(server, "verify_target_folder", lambda *_: {"name": "Test"})
    monkeypatch.setattr(FileStorage, "save", lambda *a, **kw: pytest.fail("second full-file copy"))
    seen = []
    class FakeUploader:
        def __init__(self, *args, **kwargs):
            pass
        def upload_file(self, path, *, filename, progress, phase=None):
            from src.models import DatasetManifest
            assert path.parent == settings.upload_temp_dir
            seen.append((path.stat().st_size, filename))
            return DatasetManifest("test", filename, path.stat().st_size, 1, "zstd", "0" * 64, 1)
    monkeypatch.setattr(server, "NameChunkUploader", FakeUploader)
    app = server.create_app(settings)
    try:
        yield app.test_client(), settings.upload_temp_dir, seen
    finally:
        app.extensions["close_transfers"]()


def test_exact_limit_accepted_without_copy_and_cleanup(upload_app):
    client, directory, seen = upload_app
    assert client.get("/api/status").get_json()["maxUploadMb"] == 1
    response = client.post("/api/uploads", data={"file": (io.BytesIO(b"x" * 1024**2), "limit.bin")})
    assert response.status_code == 202
    job_id = response.get_json()["jobId"]
    for _ in range(100):
        status = client.get(f"/api/uploads/{job_id}").get_json()
        if status["status"] == "complete" and not list(directory.iterdir()):
            break
        time.sleep(0.01)
    assert status["status"] == "complete"
    assert seen == [(1024**2, "limit.bin")]
    assert list(directory.iterdir()) == []


def test_actual_file_over_limit_rejected_and_cleaned(upload_app):
    client, directory, seen = upload_app
    # Multipart total is within MAX_CONTENT_LENGTH; actual file bytes are not.
    response = client.post("/api/uploads", data={"file": (io.BytesIO(b"x" * (1024**2 + 1)), "over.bin")})
    assert response.status_code == 413
    assert response.is_json
    assert list(directory.iterdir()) == []
    assert not seen


def test_invalid_backend_cleans_unclaimed_stage(upload_app):
    client, directory, seen = upload_app
    response = client.post("/api/uploads", data={"backend": "bad", "file": (io.BytesIO(b"x"), "small.bin")})
    assert response.status_code == 400
    assert list(directory.iterdir()) == []
    assert not seen


def test_invalid_encoding_cleans_unclaimed_stage(upload_app):
    client, directory, seen = upload_app
    response = client.post("/api/uploads", data={"encoding": "bad", "file": (io.BytesIO(b"x"), "small.bin")})
    assert response.status_code == 400
    assert list(directory.iterdir()) == []
    assert not seen


def test_ten_gib_file_limit_is_inclusive_without_allocating_large_buffer():
    from werkzeug.exceptions import RequestEntityTooLarge
    class Handle:
        def write(self, data):
            return len(data)
    limit = 10 * 1024**3
    stream = server._LimitedUploadStream(Handle(), limit)
    stream.size = limit - 1
    assert stream.write(b'x') == 1
    assert stream.size == limit
    with pytest.raises(RequestEntityTooLarge):
        stream.write(b'x')
    assert stream.size == limit


def test_ui_publishes_ten_gib_limit_and_allows_multipart_overhead(tmp_path, monkeypatch):
    settings = Settings(Path('unused'), 'folder', sqlite_path=tmp_path / 'db.sqlite',
                        max_upload_mb=10240, upload_temp_dir=tmp_path / 'staging')
    settings.validate()
    monkeypatch.setattr(server, 'build_drive_service', lambda _: object())
    monkeypatch.setattr(server, 'verify_target_folder', lambda *_: {'name': 'Test'})
    app = server.create_app(settings)
    try:
        assert app.config['MAX_CONTENT_LENGTH'] == 10 * 1024**3 + 1024**2
        assert app.test_client().get('/api/status').get_json()['maxUploadMb'] == 10240
    finally:
        app.extensions['close_transfers']()
