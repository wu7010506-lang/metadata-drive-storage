"""Bounded-memory preparation and restoration, compatible with version-1 datasets."""

from __future__ import annotations

import errno
import gzip
import hashlib
import io
import shutil
import tempfile
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable, Iterator, Mapping
from typing import BinaryIO

from src import encoder
from src.errors import IntegrityError

BLOCK_SIZE = 1024 * 1024
MEMORY_SPOOL_BYTES = 8 * BLOCK_SIZE


@dataclass
class PreparedUpload:
    stream: BinaryIO
    original_size: int
    compressed_size: int
    sha256: str
    compression: str

    @property
    def encoded_size(self) -> int:
        return self.encoded_size_for("base64url")

    def encoded_size_for(self, encoding: str) -> int:
        if encoding == "base64url":
            return (self.compressed_size * 8 + 5) // 6
        if encoding == "base85":
            groups, residual = divmod(self.compressed_size, 4)
            return groups * 5 + (residual + 1 if residual else 0)
        raise ValueError("unsupported payload encoding")

    def chunks(self, chunk_size: int, encoding: str = "base64url", *,
               start_index: int = 0, stop_index: int | None = None) -> Iterator[str]:
        """Encode a seekable chunk range, preserving the legacy byte partition."""
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        if start_index < 0 or (stop_index is not None and stop_index < start_index):
            raise ValueError("invalid chunk range")
        pending = ""
        if encoding not in encoder.ENCODINGS:
            raise ValueError("unsupported payload encoding")
        group_bytes, group_chars = (3, 4) if encoding == "base64url" else (4, 5)
        start_char = start_index * chunk_size
        stop_char = min(self.encoded_size_for(encoding), stop_index * chunk_size) if stop_index is not None else self.encoded_size_for(encoding)
        remaining = stop_char - start_char
        if remaining <= 0:
            return
        byte_offset, skip = divmod(start_char, group_chars)
        self.stream.seek(byte_offset * group_bytes)
        block_size = max(group_bytes, min(192 * 1024, (chunk_size // group_chars + 1) * group_bytes))
        block_size -= block_size % group_bytes
        while remaining:
            read_size = min(block_size, ((remaining + skip + group_chars - 1) // group_chars) * group_bytes)
            data = self.stream.read(read_size)
            if not data:
                raise IntegrityError("compressed spool ended before expected chunk range")
            text = encoder.encode_payload(data, encoding)[skip:]
            skip = 0
            text = text[:remaining]
            remaining -= len(text)
            pending += text
            offset = 0
            while len(pending) - offset >= chunk_size:
                yield pending[offset:offset + chunk_size]
                offset += chunk_size
            pending = pending[offset:]
        if pending:
            yield pending


@contextmanager
def prepare_upload(path: Path, *, max_bytes: int, spool_dir: Path | None = None) -> Iterator[PreparedUpload]:
    """Hash/compress one bounded read at a time; close the compressed spool on failure."""
    size = path.stat().st_size
    if size > max_bytes:
        raise ValueError(f"file exceeds upload limit of {max_bytes} bytes")
    if spool_dir is not None:
        spool_dir.mkdir(parents=True, exist_ok=True)
    directory = spool_dir or Path(tempfile.gettempdir())
    with tempfile.SpooledTemporaryFile(max_size=MEMORY_SPOOL_BYTES, dir=directory) as spool:
        if encoder.zstandard is not None:
            compressor = encoder.zstandard.ZstdCompressor().compressobj(size=size)
            algorithm = "zstd"
        else:
            # gzip.compress(mtime=0) uses the same deterministic zlib gzip header.
            compressor = zlib.compressobj(level=9, wbits=31)
            algorithm = "gzip"
        digest = hashlib.sha256()
        total = compressed_size = 0

        def write(data):
            nonlocal compressed_size
            if compressed_size + len(data) > MEMORY_SPOOL_BYTES:
                required = len(data) + BLOCK_SIZE
                if compressed_size <= MEMORY_SPOOL_BYTES:
                    required += compressed_size
                if shutil.disk_usage(directory).free < required:
                    raise OSError(errno.ENOSPC, "compressed upload spool disk is full")
            spool.write(data)
            compressed_size += len(data)

        with path.open("rb") as source:
            while data := source.read(BLOCK_SIZE):
                total += len(data)
                if total > max_bytes or total > size:
                    raise ValueError("source grew during preparation or exceeds upload limit")
                digest.update(data)
                write(compressor.compress(data))
        if total != size:
            raise ValueError("source size changed during preparation")
        write(compressor.flush())
        spool.seek(0)
        yield PreparedUpload(spool, total, compressed_size, digest.hexdigest(), algorithm)


class _IterableReader(io.RawIOBase):
    """Expose an incremental byte iterator to bounded standard decompression readers."""

    def __init__(self, iterator):
        super().__init__()
        self.iterator = iter(iterator)
        self.pending = b""

    def readable(self):
        return True

    def readinto(self, target):
        offset = 0
        while offset < len(target):
            if not self.pending:
                self.pending = next(self.iterator, b"")
            if not self.pending:
                break
            count = min(len(target) - offset, len(self.pending))
            target[offset:offset + count] = self.pending[:count]
            self.pending = self.pending[count:]
            offset += count
        return offset

    def close(self):
        try:
            close = getattr(self.iterator, "close", None)
            if close:
                close()
        finally:
            super().close()


def restore_stream(payloads: Iterable[str], manifest: Mapping[str, object], output: BinaryIO) -> None:
    """Decode/decompress directly into a private destination; verify before publication."""
    compressed_size = 0
    encoding = str(manifest.get("encoding", "base64url"))
    if encoding not in encoder.ENCODINGS:
        raise IntegrityError("unsupported payload encoding in manifest")
    group_chars = 4 if encoding == "base64url" else 5

    def decoded():
        nonlocal compressed_size
        carry = ""
        try:
            for payload in payloads:
                carry += payload
                length = len(carry) // group_chars * group_chars
                if length:
                    data = encoder.decode_payload(carry[:length], encoding)
                    carry = carry[length:]
                    compressed_size += len(data)
                    if compressed_size > int(manifest["compressed_size"]):
                        raise IntegrityError("compressed size exceeds manifest")
                    if data:
                        yield data
            if carry:
                data = encoder.decode_payload(carry, encoding)
                compressed_size += len(data)
                yield data
        finally:
            close = getattr(payloads, "close", None)
            if close:
                close()

    digest = hashlib.sha256()
    total = 0
    try:
        with io.BufferedReader(_IterableReader(decoded()), buffer_size=64 * 1024) as source:
            algorithm = encoder.compression_name(source.peek(4)[:4])
            if algorithm != manifest["compression"]:
                raise IntegrityError("compression format does not match manifest")
            if algorithm == "zstd":
                if encoder.zstandard is None:
                    raise RuntimeError("zstandard is required to decompress this dataset")
                reader = encoder.zstandard.ZstdDecompressor().stream_reader(
                    source, read_size=64 * 1024, read_across_frames=True, closefd=False,
                )
            else:
                reader = gzip.GzipFile(fileobj=source, mode="rb")
            with reader:
                while data := reader.read(BLOCK_SIZE):
                    total += len(data)
                    if total > int(manifest["original_size"]):
                        raise IntegrityError("restored size exceeds manifest")
                    digest.update(data)
                    output.write(data)
            if source.read(1):
                raise IntegrityError("unconsumed compressed data")
        if compressed_size != int(manifest["compressed_size"]):
            raise IntegrityError("compressed size does not match manifest")
        if total != int(manifest["original_size"]):
            raise IntegrityError("restored size does not match manifest")
        if digest.hexdigest() != manifest["sha256"]:
            raise IntegrityError("restored SHA-256 does not match manifest")
    except (ValueError, RuntimeError, EOFError, zlib.error, gzip.BadGzipFile) as exc:
        raise IntegrityError("dataset decoding or decompression failed") from exc
    except Exception as exc:
        if encoder.zstandard is not None and isinstance(exc, encoder.zstandard.ZstdError):
            raise IntegrityError("dataset decoding or decompression failed") from exc
        raise
