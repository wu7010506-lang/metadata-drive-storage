"""Compression, Base64URL, and chunking primitives."""

from __future__ import annotations

import base64
import gzip

try:
    import zstandard
except ImportError:  # pragma: no cover - exercised only in minimal installations
    zstandard = None  # type: ignore[assignment]

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
GZIP_MAGIC = b"\x1f\x8b"
ENCODINGS = {"base64url", "base85"}


def encode_payload(data: bytes, encoding: str = "base64url") -> str:
    if encoding == "base64url":
        return encode_base64url(data)
    if encoding == "base85":
        return base64.b85encode(data).decode("ascii")
    raise ValueError(f"unsupported payload encoding: {encoding}")


def decode_payload(data: str, encoding: str = "base64url") -> bytes:
    if encoding == "base64url":
        return decode_base64url(data)
    if encoding == "base85":
        if len(data) % 5 == 1:
            raise ValueError("invalid Base85 residual group")
        return base64.b85decode(data.encode("ascii"))
    raise ValueError(f"unsupported payload encoding: {encoding}")


def compress_data(data: bytes) -> bytes:
    """Compress bytes with zstandard, falling back to deterministic gzip."""
    if zstandard is not None:
        return zstandard.ZstdCompressor().compress(data)
    return gzip.compress(data, mtime=0)


def decompress_data(data: bytes) -> bytes:
    """Decompress data after identifying its supported frame format."""
    if data.startswith(ZSTD_MAGIC):
        if zstandard is None:
            raise RuntimeError("zstandard is required to decompress this dataset")
        return zstandard.ZstdDecompressor().decompress(data)
    if data.startswith(GZIP_MAGIC):
        return gzip.decompress(data)
    raise ValueError("unknown compression format")


def compression_name(data: bytes) -> str:
    """Return the compression algorithm represented by encoded bytes."""
    if data.startswith(ZSTD_MAGIC):
        return "zstd"
    if data.startswith(GZIP_MAGIC):
        return "gzip"
    raise ValueError("unknown compression format")


def encode_base64url(data: bytes) -> str:
    """Encode bytes as unpadded URL-safe Base64 text."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def decode_base64url(data: str) -> bytes:
    """Decode padded or unpadded URL-safe Base64 text."""
    padding = "=" * (-len(data) % 4)
    return base64.b64decode(data + padding, altchars=b"-_", validate=True)


def split_chunks(data: str, chunk_size: int) -> list[str]:
    """Split text into ordered non-empty chunks."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    return [data[index : index + chunk_size] for index in range(0, len(data), chunk_size)]


def join_chunks(chunks: list[str]) -> str:
    """Join ordered chunks without separators."""
    return "".join(chunks)
