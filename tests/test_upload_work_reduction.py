"""Catch redundant encoding at the actual preparation/resume boundary."""

import io
from pathlib import Path
import random

import pytest

from src.encoder import encode_payload
from src.streaming import PreparedUpload
from src.config import Settings
from src.database import Database
from src.uploader import NameChunkUploader
from tests.test_uploader import FakeDrive


@pytest.mark.parametrize('encoding', ['base64url', 'base85'])
@pytest.mark.parametrize('width', [1, 2, 3, 7, 31, 25000])
@pytest.mark.parametrize('size', [0, 1, 2, 3, 4, 9, 64001])
def test_seeked_chunks_match_legacy_partition(encoding, width, size):
    data = random.Random(42).randbytes(size)
    prepared = PreparedUpload(io.BytesIO(data), size, size, 'unused', 'zstd')
    text = encode_payload(data, encoding)
    expected = [text[i:i+width] for i in range(0, len(text), width)]
    for start, stop in [(0, None), (1, 4), (3, 7), (5, 5), (len(expected), None)]:
        assert list(prepared.chunks(width, encoding, start_index=start, stop_index=stop)) == expected[start:stop]


def test_range_does_not_read_or_encode_skipped_megabytes():
    data = random.Random(1).randbytes(4 * 1024**2)
    reads = []
    class Stream(io.BytesIO):
        def read(self, count=-1):
            assert 0 <= count <= 192 * 1024
            value = super().read(count)
            reads.append(len(value))
            return value
    prepared = PreparedUpload(Stream(data), len(data), len(data), 'unused', 'zstd')
    text = encode_payload(data, 'base64url')
    assert ''.join(prepared.chunks(25000, start_index=150, stop_index=152)) == text[150*25000:152*25000]
    assert sum(reads) <= 37506


@pytest.mark.parametrize('start,stop', [(-1, None), (3, 2)])
def test_invalid_chunk_ranges_are_rejected(start, stop):
    prepared = PreparedUpload(io.BytesIO(b'abc'), 3, 3, '', 'zstd')
    with pytest.raises(ValueError, match='range'):
        list(prepared.chunks(3, start_index=start, stop_index=stop))


def test_truncated_spool_cannot_silently_drop_a_requested_chunk():
    from src.errors import IntegrityError
    prepared = PreparedUpload(io.BytesIO(b'abc'), 30, 30, '', 'zstd')
    with pytest.raises(IntegrityError, match='spool'):
        list(prepared.chunks(3))


def test_completed_resume_encodes_exactly_once(tmp_path, monkeypatch):
    source = tmp_path / 'random.bin'
    source.write_bytes(random.Random(2).randbytes(64000))
    database = Database(tmp_path / 'index.db')
    settings = Settings(Path('unused'), 'folder', api_min_interval_seconds=0)
    drive = FakeDrive()
    uploader = NameChunkUploader(drive, settings, database, 25000, 'description')
    manifest = uploader.upload_file(source)
    calls = []
    original = PreparedUpload.chunks
    def counted(self, *args, **kwargs):
        for payload in original(self, *args, **kwargs):
            calls.append(payload)
            yield payload
    monkeypatch.setattr(PreparedUpload, 'chunks', counted)
    assert uploader.upload_file(source).dataset_id == manifest.dataset_id
    assert len(calls) == manifest.chunk_count


def test_partial_resume_validates_only_reserved_prefix_before_new_writes(tmp_path, monkeypatch):
    source = tmp_path / 'random.bin'
    source.write_bytes(random.Random(3).randbytes(256000))
    database = Database(tmp_path / 'index.db')
    settings = Settings(Path('unused'), 'folder', api_min_interval_seconds=0)
    drive = FakeDrive()
    uploader = NameChunkUploader(drive, settings, database, 25000, 'description')
    manifest = uploader.upload_file(source)
    rows = database.get_chunks(manifest.dataset_id)
    for row in rows[2:]:
        drive.created.pop(row['drive_file_id'])
    with database.connect() as connection:
        connection.execute('DELETE FROM chunks WHERE dataset_id=? AND chunk_index>=2', (manifest.dataset_id,))
    passes = []
    original = PreparedUpload.chunks
    def counted(self, *args, **kwargs):
        seen = []
        passes.append(seen)
        for payload in original(self, *args, **kwargs):
            seen.append(payload)
            yield payload
    monkeypatch.setattr(PreparedUpload, 'chunks', counted)
    uploader.upload_file(source)
    assert len(passes[0]) == 2  # Every existing hash, but no unreserved tail.
    assert sum(map(len, passes)) == manifest.chunk_count  # No second encoding of completed prefix.


def test_sparse_index_iteration_does_not_query_unreserved_windows(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from src.models import DatasetManifest
    db = Database(tmp_path / 'sparse.db')
    db.initialize()
    db.save_dataset(DatasetManifest('d', 'large', 1, 1, 'zstd', 'hash', 1000000))
    db.save_pending_chunks('d', [(0, 'first', 'hash'), (500000, 'last', 'hash')])
    original = db.connect
    calls, active = [], []
    @contextmanager
    def bounded_connection():
        calls.append(1)
        assert len(calls) <= 3, 'querying empty index windows'
        with original() as connection:
            active.append(True)
            try:
                yield connection
            finally:
                active.pop()
    monkeypatch.setattr(db, 'connect', bounded_connection)
    indices = []
    for row in db.iter_chunks('d', 1000000, window=1):
        assert not active  # SQLite connection released before consumer/network work.
        indices.append(row['chunk_index'])
    assert indices == [0, 500000]
