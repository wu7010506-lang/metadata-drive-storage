from pathlib import Path

import pytest

from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.errors import ChunkCorruptedError
from src.uploader import NameChunkUploader
from tests.test_uploader import FakeDrive


def test_verified_download_round_trip_and_detects_corruption(tmp_path: Path) -> None:
    original = b"round-trip data" * 100
    source = tmp_path / "source.bin"
    source.write_bytes(original)
    settings = Settings(
        credentials_path=Path("unused.json"),
        drive_folder_id="folder-id",
        sqlite_path=tmp_path / "index.db",
        api_min_interval_seconds=0,
    )
    database = Database(settings.sqlite_path)
    drive = FakeDrive()
    manifest = NameChunkUploader(
        drive, settings, database, 31, "description"
    ).upload_file(source)

    restored, restored_manifest = VerifiedDownloader(
        drive, settings, database
    ).download_bytes(manifest.dataset_id)
    assert restored == original
    assert restored_manifest["sha256"] == manifest.sha256

    first = next(iter(drive.created.values()))
    first["description"] = first["description"] + "A"
    with pytest.raises(ChunkCorruptedError):
        VerifiedDownloader(drive, settings, database).download_bytes(
            manifest.dataset_id
        )
