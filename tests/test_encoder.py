import os

import pytest

from src.encoder import (
    compress_data,
    decode_base64url,
    decompress_data,
    encode_base64url,
    join_chunks,
    split_chunks,
)


@pytest.mark.parametrize("data", [b"", b"hello", os.urandom(4096)])
def test_compression_and_base64url_round_trip(data: bytes) -> None:
    compressed = compress_data(data)
    encoded = encode_base64url(compressed)
    assert "=" not in encoded
    assert decompress_data(decode_base64url(encoded)) == data


def test_chunk_round_trip() -> None:
    assert join_chunks(split_chunks("abcdefghij", 3)) == "abcdefghij"


def test_chunk_size_must_be_positive() -> None:
    with pytest.raises(ValueError):
        split_chunks("data", 0)


@pytest.mark.parametrize("size", [0, 1, 2, 3, 4, 5, 25001])
def test_base85_roundtrip_including_residual_groups(size):
    from src.encoder import encode_payload, decode_payload
    data = os.urandom(size)
    assert decode_payload(encode_payload(data, "base85"), "base85") == data


def test_base85_rejects_truncated_single_character_group():
    from src.encoder import decode_payload
    with pytest.raises(ValueError):
        decode_payload("0", "base85")
