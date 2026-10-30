"""SQLite manifest and Drive-object index."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Iterable
from contextlib import contextmanager
from pathlib import Path

from src.models import DatasetManifest

SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    storage_backend TEXT NOT NULL,
    chunk_size INTEGER NOT NULL,
    filename TEXT NOT NULL,
    original_size INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    chunk_count INTEGER NOT NULL,
    compression TEXT NOT NULL,
    created_at TEXT NOT NULL,
    encoding TEXT NOT NULL DEFAULT 'base64url'
);
CREATE TABLE IF NOT EXISTS chunks (
    dataset_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    drive_file_id TEXT NOT NULL,
    chunk_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'complete')),
    PRIMARY KEY (dataset_id, chunk_index),
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS datasets_resume_idx
ON datasets(filename, original_size, sha256, compression, chunk_count);
"""


class Database:
    """Small transactional wrapper around the local SQLite index."""

    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Open a foreign-key-enabled connection and commit atomically."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create schema objects if they do not exist."""
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(datasets)").fetchall()
            }
            if "version" not in columns:
                connection.execute(
                    "ALTER TABLE datasets ADD COLUMN version INTEGER NOT NULL DEFAULT 1"
                )
            if "storage_backend" not in columns:
                connection.execute(
                    "ALTER TABLE datasets ADD COLUMN storage_backend TEXT NOT NULL DEFAULT 'name'"
                )
            if "chunk_size" not in columns:
                connection.execute(
                    "ALTER TABLE datasets ADD COLUMN chunk_size INTEGER NOT NULL DEFAULT 0"
                )
            if "encoding" not in columns:
                connection.execute(
                    "ALTER TABLE datasets ADD COLUMN encoding TEXT NOT NULL DEFAULT 'base64url'"
                )
            connection.execute("PRAGMA optimize")

    def save_dataset(self, manifest: DatasetManifest) -> None:
        """Insert a dataset manifest without overwriting existing state."""
        with self.connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO datasets
                (dataset_id, version, storage_backend, chunk_size, filename,
                 original_size, compressed_size, sha256, chunk_count,
                 compression, created_at, encoding)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    manifest.dataset_id,
                    manifest.version,
                    manifest.storage_backend,
                    manifest.chunk_size,
                    manifest.filename,
                    manifest.original_size,
                    manifest.compressed_size,
                    manifest.sha256,
                    manifest.chunk_count,
                    manifest.compression,
                    manifest.created_at,
                    manifest.encoding,
                ),
            )

    def find_matching_dataset(
        self,
        *,
        filename: str,
        original_size: int,
        sha256: str,
        compression: str,
        chunk_count: int,
        storage_backend: str = "name",
        chunk_size: int = 0,
        encoding: str = "base64url",
    ) -> str | None:
        """Find the newest manifest suitable for an automatic resume."""
        with self.connect() as connection:
            row = connection.execute(
                """SELECT dataset_id FROM datasets
                WHERE filename = ? AND original_size = ? AND sha256 = ?
                  AND compression = ? AND chunk_count = ?
                  AND storage_backend = ? AND chunk_size = ? AND encoding = ?
                ORDER BY created_at DESC LIMIT 1""",
                (
                    filename,
                    original_size,
                    sha256,
                    compression,
                    chunk_count,
                    storage_backend,
                    chunk_size,
                    encoding,
                ),
            ).fetchone()
        return str(row["dataset_id"]) if row else None

    def save_pending_chunk(
        self, dataset_id: str, chunk_index: int, drive_file_id: str, chunk_hash: str
    ) -> None:
        """Persist a generated Drive ID before creating the remote object."""
        self.save_pending_chunks(dataset_id, [(chunk_index, drive_file_id, chunk_hash)])

    def save_pending_chunks(
        self, dataset_id: str, records: Iterable[tuple[int, str, str]]
    ) -> None:
        """Commit a reservation window atomically; never replace a reserved ID."""
        records = list(records)
        if not records:
            return
        with self.connect() as connection:
            for index, file_id, chunk_hash in records:
                cursor = connection.execute(
                    """INSERT INTO chunks
                    (dataset_id, chunk_index, drive_file_id, chunk_hash, status)
                    VALUES (?, ?, ?, ?, 'pending')
                    ON CONFLICT(dataset_id, chunk_index) DO UPDATE
                    SET drive_file_id = excluded.drive_file_id
                    WHERE chunks.drive_file_id = excluded.drive_file_id
                      AND chunks.chunk_hash = excluded.chunk_hash""",
                    (dataset_id, index, file_id, chunk_hash),
                )
                if cursor.rowcount != 1:
                    raise ValueError(f"chunk {index} conflicts with a reserved Drive ID/hash")

    def mark_chunk_complete(self, dataset_id: str, chunk_index: int) -> None:
        """Mark one remote object durable immediately after verification."""
        self.mark_chunks_complete(dataset_id, [chunk_index])

    def mark_chunks_complete(self, dataset_id: str, indices: Iterable[int]) -> None:
        """Commit verified completions in one transaction before reporting progress."""
        indices = list(indices)
        if not indices:
            return
        with self.connect() as connection:
            cursor = connection.executemany(
                """UPDATE chunks SET status = 'complete'
                WHERE dataset_id = ? AND chunk_index = ?""",
                [(dataset_id, index) for index in indices],
            )
            if cursor.rowcount != len(indices):
                raise ValueError("cannot complete an unreserved chunk")

    def get_chunk(self, dataset_id: str, chunk_index: int) -> sqlite3.Row | None:
        """Return one chunk index record."""
        with self.connect() as connection:
            return connection.execute(
                """SELECT dataset_id, chunk_index, drive_file_id, chunk_hash, status
                FROM chunks WHERE dataset_id = ? AND chunk_index = ?""",
                (dataset_id, chunk_index),
            ).fetchone()

    def count_complete_chunks(self, dataset_id: str) -> int:
        """Count chunks that do not need to be recreated on resume."""
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM chunks WHERE dataset_id = ? AND status = 'complete'",
                (dataset_id,),
            ).fetchone()
        return int(row["total"])

    def count_chunks(self, dataset_id: str) -> int:
        with self.connect() as connection:
            return int(connection.execute(
                "SELECT COUNT(*) FROM chunks WHERE dataset_id = ?", (dataset_id,),
            ).fetchone()[0])

    def get_dataset(self, dataset_id: str) -> dict[str, object] | None:
        """Return one complete local manifest record."""
        with self.connect() as connection:
            row = connection.execute(
                """SELECT dataset_id, version, storage_backend, chunk_size,
                          filename, original_size, compressed_size, sha256,
                          chunk_count, compression, created_at, encoding
                   FROM datasets WHERE dataset_id = ?""",
                (dataset_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_chunks(self, dataset_id: str) -> list[dict[str, object]]:
        """Return all chunk records in reconstruction order."""
        with self.connect() as connection:
            rows = connection.execute(
                """SELECT dataset_id, chunk_index, drive_file_id, chunk_hash, status
                   FROM chunks WHERE dataset_id = ? ORDER BY chunk_index""",
                (dataset_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_chunk_window(self, dataset_id: str, start: int, count: int):
        """Read only one bounded range, releasing SQLite before remote I/O."""
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM chunks WHERE dataset_id = ? AND chunk_index >= ? "
                "AND chunk_index < ? ORDER BY chunk_index",
                (dataset_id, start, start + count),
            ).fetchall()
        return [dict(row) for row in rows]

    def iter_chunks(self, dataset_id: str, count: int, window: int = 128):
        """Page existing rows by indexed key, not every possibly empty chunk range."""
        if window < 1:
            raise ValueError("window must be positive")
        start = 0
        while start < count:
            with self.connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM chunks WHERE dataset_id = ? AND chunk_index >= ? "
                    "AND chunk_index < ? ORDER BY chunk_index LIMIT ?",
                    (dataset_id, start, count, window),
                ).fetchall()
            if not rows:
                return
            # Release SQLite before yielding to hashing or remote I/O consumers.
            yield from (dict(row) for row in rows)
            start = int(rows[-1]["chunk_index"]) + 1

    def list_datasets(self, limit: int = 20) -> list[dict[str, object]]:
        """Return recent manifests with their completed chunk counts."""
        with self.connect() as connection:
            rows = connection.execute(
                """SELECT d.dataset_id, d.filename, d.original_size,
                          d.compressed_size, d.sha256, d.chunk_count,
                          d.compression, d.created_at, d.storage_backend,
                          d.chunk_size, d.encoding,
                          COUNT(CASE WHEN c.status = 'complete' THEN 1 END) AS completed_chunks
                   FROM datasets AS d
                   LEFT JOIN chunks AS c ON c.dataset_id = d.dataset_id
                   GROUP BY d.dataset_id
                   ORDER BY d.created_at DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
