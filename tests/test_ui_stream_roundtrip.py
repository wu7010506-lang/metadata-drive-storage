import io
import time
from pathlib import Path

from src.config import Settings
from ui import server
from tests.test_uploader import FakeDrive
import pytest


def await_job(client, endpoint):
    for _ in range(200):
        status = client.get(endpoint).get_json()
        if status["status"] in {"complete", "error"}:
            assert status["status"] == "complete", status
            return status
        time.sleep(0.01)
    raise AssertionError("test job did not finish")


@pytest.mark.parametrize("backend,encoding", [("description", "base64url"), ("combined", "base85")])
def test_actual_ui_caller_round_trip_and_legacy_cleanup(tmp_path, monkeypatch, backend, encoding):
    import os
    drive = FakeDrive()
    monkeypatch.setattr(server, "build_drive_service", lambda _: drive)
    monkeypatch.setattr(server, "verify_target_folder", lambda *_: {"name": "Test"})
    directory = tmp_path / "downloads"
    monkeypatch.setenv("DOWNLOAD_OUTPUT_DIR", str(directory))
    settings = Settings(Path("unused"), "folder", sqlite_path=tmp_path / "db.sqlite",
                        upload_temp_dir=tmp_path / "staging", description_safe_payload_bytes=25000,
                        upload_backend="description", name_safe_payload_bytes=8500, api_min_interval_seconds=0)
    data = os.urandom(200000)
    app = server.create_app(settings)
    try:
        client = app.test_client()
        upload = client.post("/api/uploads", data={"file": (io.BytesIO(data), "original.bin"),
                                                   "backend": backend, "encoding": encoding})
        assert upload.status_code == 202
        uploaded = await_job(client, f'/api/uploads/{upload.get_json()["jobId"]}')
        assert uploaded["encoding"] == encoding
        started = client.post(f'/api/datasets/{uploaded["datasetId"]}/downloads')
        assert started.status_code == 202
        job_id = started.get_json()["jobId"]
        downloaded = await_job(client, f"/api/downloads/{job_id}")
        assert Path(downloaded["savedPath"]).read_bytes() == data
        assert downloaded["completedChunks"] == uploaded["totalChunks"]
        response = client.get(f'/api/datasets/{uploaded["datasetId"]}/download')
        assert response.status_code == 200
        assert response.data == data
        response.close()
        assert list(directory.iterdir()) == [Path(downloaded["savedPath"])]
    finally:
        app.extensions["close_transfers"]()
