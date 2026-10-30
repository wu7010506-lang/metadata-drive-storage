from pathlib import Path

from src.database import Database
from src.models import DatasetManifest


def test_dataset_and_chunk_resume_state(tmp_path: Path) -> None:
    database = Database(tmp_path / "index.db")
    database.initialize()
    manifest = DatasetManifest(
        dataset_id="dataset",
        filename="example.bin",
        original_size=3,
        compressed_size=12,
        compression="zstd",
        sha256="digest",
        chunk_count=2,
    )
    database.save_dataset(manifest)
    assert database.find_matching_dataset(
        filename="example.bin",
        original_size=3,
        sha256="digest",
        compression="zstd",
        chunk_count=2,
    ) == "dataset"

    database.save_pending_chunk("dataset", 0, "drive-id", "chunk-hash")
    assert database.get_chunk("dataset", 0)["status"] == "pending"
    database.mark_chunk_complete("dataset", 0)
    assert database.count_complete_chunks("dataset") == 1


def test_initialize_migrates_legacy_dataset_table(tmp_path: Path) -> None:
    import sqlite3

    path = tmp_path / "legacy.db"
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE datasets (
        dataset_id TEXT PRIMARY KEY, filename TEXT NOT NULL,
        original_size INTEGER NOT NULL, compressed_size INTEGER NOT NULL,
        sha256 TEXT NOT NULL, chunk_count INTEGER NOT NULL,
        compression TEXT NOT NULL, created_at TEXT NOT NULL)"""
    )
    connection.commit()
    connection.close()

    database = Database(path)
    database.initialize()
    with database.connect() as migrated:
        columns = {
            row["name"] for row in migrated.execute("PRAGMA table_info(datasets)")
        }
    assert "version" in columns
    assert "encoding" in columns
    manifest = DatasetManifest("legacy", "old.bin", 1, 1, "gzip", "hash", 1)
    database.save_dataset(manifest)
    assert database.get_dataset("legacy")["encoding"] == "base64url"
