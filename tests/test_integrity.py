from src.integrity import sha256_bytes, sha256_text


def test_known_sha256_vector() -> None:
    expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert sha256_bytes(b"abc") == expected
    assert sha256_text("abc") == expected
