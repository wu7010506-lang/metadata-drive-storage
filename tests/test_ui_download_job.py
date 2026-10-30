import time
from pathlib import Path

from src.config import Settings
from src.database import Database
from src.models import DatasetManifest
from ui import server


def test_download_job_prepares_then_serves_native_attachment(
    tmp_path: Path, monkeypatch
) -> None:
    output_dir = tmp_path / "downloads"
    monkeypatch.setenv("DOWNLOAD_OUTPUT_DIR", str(output_dir))
    settings = Settings(
        credentials_path=Path("unused.json"),
        drive_folder_id="folder-id",
        sqlite_path=tmp_path / "index.db",
        api_min_interval_seconds=0,
    )
    database = Database(settings.sqlite_path)
    database.initialize()
    manifest = DatasetManifest(
        dataset_id="dataset-1",
        filename="large.bin",
        original_size=12,
        compressed_size=8,
        compression="gzip",
        sha256="0" * 64,
        chunk_count=2,
        storage_backend="description",
        chunk_size=100,
    )
    database.save_dataset(manifest)

    class FakeDownloader:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def download_file(self, _dataset_id, output_path, *, progress=None):
            assert output_path.parent == output_dir
            if progress:
                progress(1, 2)
            output_path.write_bytes(b"verified-data")
            if progress:
                progress(2, 2)
            return output_path

    monkeypatch.setattr(server, "build_drive_service", lambda _path: object())
    monkeypatch.setattr(
        server, "verify_target_folder", lambda _service, _settings: {"name": "Test"}
    )
    monkeypatch.setattr(server, "VerifiedDownloader", FakeDownloader)

    app = server.create_app(settings)
    client = app.test_client()
    started = client.post("/api/datasets/dataset-1/downloads")
    assert started.status_code == 202
    job_id = started.get_json()["jobId"]

    status = None
    for _ in range(50):
        status = client.get(f"/api/downloads/{job_id}").get_json()
        if status["status"] == "complete":
            break
        time.sleep(0.01)

    assert status["status"] == "complete"
    assert status["completedChunks"] == 2
    assert Path(status["savedPath"]).parent == output_dir
    assert Path(status["savedPath"]).read_bytes() == b"verified-data"
    response = client.get(f"/api/downloads/{job_id}/file")
    assert response.status_code == 200
    assert response.data == b"verified-data"
    assert "attachment" in response.headers["Content-Disposition"]
    assert "large.bin" in response.headers["Content-Disposition"]
    response.close()
    assert Path(status["savedPath"]).exists()
    ranged = client.get(f"/api/downloads/{job_id}/file", headers={"Range": "bytes=0-3"})
    assert ranged.status_code == 206
    assert ranged.data == b"veri"
    ranged.close()
    app.extensions["close_transfers"]()
