"""Experimental wire compression must not alter stored metadata or TLS policy."""

import gzip

import httplib2
import pytest
from google.auth.credentials import AnonymousCredentials
from googleapiclient.discovery import build

from src import auth


def transport(monkeypatch):
    seen = []

    class Http:
        def request(self, uri, method="GET", **kwargs):
            seen.append((uri, method, kwargs))
            return httplib2.Response({"status": "200"}), b"{}"

        def close(self):
            pass

    monkeypatch.setattr(auth, "build_http", Http)
    client = auth._authorized_http(AnonymousCredentials(), 60)
    client.batch_request_gzip = True
    return client, seen


@pytest.mark.parametrize("body", ["ABC" * 10000, b"XYZ" * 10000], ids=["text", "bytes"])
def test_batch_post_is_gzipped_and_content_length_matches_exact_wire_bytes(monkeypatch, body):
    client, seen = transport(monkeypatch)
    headers = {"Content-Type": "multipart/mixed; boundary=test", "Content-Length": "30000"}
    client.request("https://www.googleapis.com/batch/drive/v3", method="POST", body=body, headers=headers)
    actual = seen[0][2]
    normalized = {key.lower(): value for key, value in actual["headers"].items()}
    assert normalized["content-encoding"] == "gzip"
    assert int(normalized["content-length"]) == len(actual["body"])
    assert gzip.decompress(actual["body"]) == (body.encode() if isinstance(body, str) else body)
    assert headers == {"Content-Type": "multipart/mixed; boundary=test", "Content-Length": "30000"}
    assert "accept-encoding" not in normalized  # Do not simultaneously change response negotiation.


@pytest.mark.parametrize("uri,method", [
    ("https://www.googleapis.com/drive/v3/files", "POST"),
    ("https://www.googleapis.com/batch/drive/v3", "GET"),
    ("http://www.googleapis.com/batch/drive/v3", "POST"),
    ("https://unrelated.example/batch/drive/v3", "POST"),
])
def test_only_the_exact_https_batch_post_is_eligible(monkeypatch, uri, method):
    client, seen = transport(monkeypatch)
    client.request(uri, method=method, body="payload", headers={"X-Test": "kept"})
    assert seen[0][2]["body"] == "payload"
    assert seen[0][2]["headers"] == {"X-Test": "kept"}


def test_preencoded_body_and_disabled_profile_stay_unchanged(monkeypatch):
    client, seen = transport(monkeypatch)
    body = gzip.compress(b"already compressed")
    headers = {"Content-Encoding": "gzip", "Content-Length": str(len(body))}
    client.request("https://www.googleapis.com/batch/drive/v3", method="POST", body=body, headers=headers)
    assert seen[-1][2]["body"] == body and seen[-1][2]["headers"] == headers
    client.batch_request_gzip = False
    client.request("https://www.googleapis.com/batch/drive/v3", method="POST", body="plain", headers={})
    assert seen[-1][2]["body"] == "plain" and seen[-1][2]["headers"] == {}


def test_real_sdk_serializes_identical_inner_requests_before_wire_compression(monkeypatch):
    client, seen = transport(monkeypatch)
    service = build("drive", "v3", http=client, cache_discovery=False)
    batch = service.new_batch_http_request()
    batch.add(service.files().create(body={"name": "zero-content", "description": "A" * 25000},
                                    fields="id,name,description,appProperties"))
    # Capture before the SDK attempts to parse our intentionally non-multipart reply.
    with pytest.raises(Exception):
        batch.execute()
    body = seen[0][2]["body"]
    assert isinstance(body, bytes) and body.startswith(b"\x1f\x8b")
    expanded = gzip.decompress(body).decode()
    assert "A" * 25000 in expanded
    assert "id%2Cname%2Cdescription%2CappProperties" in expanded
    assert "Content-Transfer-Encoding" in expanded
