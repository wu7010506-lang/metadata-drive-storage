"""Verify independent clients, bounded overlap and ordered reconstruction."""

import threading
from pathlib import Path

import pytest

from src.config import Settings
from src.database import Database
from src.downloader import VerifiedDownloader
from src.errors import ChunkCorruptedError
from src.uploader import NameChunkUploader
from tests.test_uploader import FakeDrive


@pytest.mark.parametrize("workers", [2, 4, 8, 16])
@pytest.mark.parametrize("corrupt", [False, True])
def test_parallel_download_is_ordered_bounded_and_verified(tmp_path, workers, corrupt):
    source = tmp_path / "source.bin"
    original = bytes(range(256)) * 4
    source.write_bytes(original)
    settings = Settings(credentials_path=Path("unused"), drive_folder_id="folder",
        sqlite_path=tmp_path / "index.db", api_min_interval_seconds=0)
    database = Database(settings.sqlite_path)
    drive = FakeDrive()
    manifest = NameChunkUploader(drive, settings, database, 16, "description").upload_file(source)
    if corrupt:
        next(iter(drive.created.values()))["description"] += "A"
    settings = settings.with_overrides(download_workers=workers)
    lock = threading.Lock()
    barrier = threading.Barrier(workers, timeout=5)
    calls = [0]
    active = [0]
    peak = [0]
    owners = set()

    class Worker:
        def __init__(self):
            self.owner = threading.get_ident()

        def files(self):
            assert self.owner == threading.get_ident()
            return self

        def get(self, *, fileId, **kwargs):
            assert self.owner == threading.get_ident()
            owner = self.owner
            with lock:
                calls[0] += 1
                sequence = calls[0]
                owners.add(owner)

            class Request:
                def execute(self):
                    assert owner == threading.get_ident()
                    with lock:
                        active[0] += 1
                        peak[0] = max(peak[0], active[0])
                    try:
                        if sequence <= workers:
                            barrier.wait()
                        return dict(drive.created[fileId])
                    finally:
                        with lock:
                            active[0] -= 1
            return Request()

    progress = []
    downloader = VerifiedDownloader(drive, settings, database, service_factory=Worker)
    output = tmp_path / "output.bin"
    if corrupt:
        with pytest.raises(ChunkCorruptedError):
            downloader.download_file(manifest.dataset_id, output)
        assert not output.exists()
    else:
        downloader.download_file(manifest.dataset_id, output,
            progress=lambda n, total: progress.append(n))
        assert output.read_bytes() == original
        assert progress == list(range(1, manifest.chunk_count + 1))
    assert len(owners) == workers
    assert peak[0] == workers


def test_parallel_download_requires_a_client_factory(tmp_path):
    settings = Settings(credentials_path=Path("unused"), drive_folder_id="folder",
        sqlite_path=tmp_path / "index.db", download_workers=2)
    with pytest.raises(ValueError, match="service_factory"):
        VerifiedDownloader(FakeDrive(), settings, Database(settings.sqlite_path))
