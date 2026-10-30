"""SHA- integrity helpers."""

from __future__ import annotations

import hashlib


def sha256_bytes(data: bytes) -> str:
    """Return the lowercase SHA-256 hex digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_text(data: str) -> str:
    """Return the SHA-256 digest of UTF-8 text."""
    return sha256_bytes(data.encode("utf-8"))
