"""Clients stay thread-confined while first-use token refresh happens only once."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
import ssl

import httplib2
from google.auth.credentials import AnonymousCredentials
from src.retry import execute_with_backoff

from src import auth
import pytest


@pytest.mark.parametrize("timeout", [20, 60])
def test_factory_loads_key_once_and_clones_cached_auth_per_worker(tmp_path, monkeypatch, timeout):
    key = tmp_path / "unused.json"
    key.write_text("{}")
    refreshes = []
    loads = []
    base = None

    class Credentials:
        token = None
        expiry = None

        @property
        def valid(self):
            return self.token is not None

        @classmethod
        def from_service_account_file(cls, path, **kwargs):
            nonlocal base
            loads.append(path)
            base = cls()
            return base

        def refresh(self, request):
            refreshes.append(threading.get_ident())
            self.token = object()
            self.expiry = object()

        def with_scopes(self, scopes):
            return Credentials()

    monkeypatch.setattr(auth.service_account, "Credentials", Credentials)
    transports = []
    def build(*args, **kwargs):
        transports.append(kwargs["http"])
        return kwargs["http"].credentials
    monkeypatch.setattr(auth, "build", build)
    factory = auth.DriveServiceFactory(key, http_timeout_seconds=timeout)
    barrier = threading.Barrier(8)
    def worker():
        barrier.wait()
        return factory()
    with ThreadPoolExecutor(max_workers=8) as executor:
        clients = list(executor.map(lambda _: worker(), range(8)))
    assert len(loads) == 1
    assert len(refreshes) == 1
    assert len({id(client) for client in clients}) == 8
    assert len({id(transport.http) for transport in transports}) == 8
    assert all(transport.http.timeout == timeout for transport in transports)
    assert all(308 not in transport.http.redirect_codes for transport in transports)
    assert all(client.token is base.token for client in clients)
    clients[0].token = None
    assert all(client.valid for client in clients[1:])
    base.token = None  # Token expiry triggers one refresh for a new client.
    assert factory().valid
    assert len(refreshes) == 2


@pytest.mark.parametrize("timeout", [0, 4, 121])
def test_factory_rejects_unbounded_http_timeout(timeout):
    with pytest.raises(ValueError, match="between 5 and 120"):
        auth.DriveServiceFactory(Path("unused"), http_timeout_seconds=timeout)


def test_tls_eof_discards_only_failed_clients_connections_before_retry(monkeypatch):
    class Http:
        def __init__(self):
            self.calls = 0
            self.closes = 0
            self.broken = True
        def request(self, *args, **kwargs):
            self.calls += 1
            if self.broken:
                raise ssl.SSLEOFError(8, 'EOF occurred in violation of protocol (_ssl.c:2427)')
            return httplib2.Response({'status': '200'}), b'{}'
        def close(self):
            self.closes += 1
            self.broken = False

    failed, other = Http(), Http()
    transports = iter([failed, other])
    monkeypatch.setattr(auth, 'build_http', lambda: next(transports))
    client = auth._authorized_http(AnonymousCredentials(), 60)
    auth._authorized_http(AnonymousCredentials(), 60)

    class Request:
        def execute(self):
            return client.request('https://www.googleapis.com/drive/v3/files', method='GET')

    result = execute_with_backoff(Request, max_attempts=3, initial_delay=0,
        max_delay=0, sleep=lambda _: None)
    assert result[0].status == 200
    assert failed.calls == 2 and failed.closes == 1
    assert other.calls == 0 and other.closes == 0


def test_real_http_close_discards_cached_connections_but_preserves_configuration():
    client = auth._authorized_http(AnonymousCredentials(), 20)
    closed = []
    class Connection:
        def close(self):
            closed.append(True)
    client.http.connections['https:example.test'] = Connection()
    client.http.close()
    assert closed == [True]
    assert client.http.connections == {}
    assert client.http.timeout == 20
    assert 308 not in client.http.redirect_codes


@pytest.mark.parametrize('enabled', [False, True])
def test_real_sdk_batch_negotiates_gzip_without_changing_request_body(monkeypatch, enabled):
    from googleapiclient.discovery import build
    seen = []
    class Captured(Exception):
        pass
    class Http:
        def request(self, uri, method='GET', **kwargs):
            seen.append(kwargs)
            raise Captured()
        def close(self):
            pass
    monkeypatch.setattr(auth, 'build_http', Http)
    transport = auth._authorized_http(AnonymousCredentials(), 60)
    transport.batch_response_gzip = enabled
    service = build('drive', 'v3', http=transport, cache_discovery=False)
    batch = service.new_batch_http_request()
    for i in range(2):
        batch.add(service.files().create(body={'name':str(i), 'description':'A'*25000},
            fields='id,name,description,appProperties'))
    with pytest.raises(Captured):
        batch.execute()
    headers = {k.lower():v for k,v in seen[0]['headers'].items()}
    assert ('gzip' in headers.get('user-agent', '')) is enabled
    assert ('gzip' in headers.get('accept-encoding', '')) is enabled
    assert 'content-encoding' not in headers  # Only response negotiation, not request compression.
    assert 'A'*25000 in seen[0]['body']


def test_batch_gzip_preserves_caller_headers_and_other_endpoints(monkeypatch):
    seen = []
    class Http:
        def request(self, *args, **kwargs):
            seen.append(kwargs['headers'])
            return httplib2.Response({'status':'200'}), b'{}'
    monkeypatch.setattr(auth, 'build_http', Http)
    client = auth._authorized_http(AnonymousCredentials(), 60)
    client.batch_response_gzip = True
    original = {'User-Agent':'custom', 'Accept-Encoding':'identity', 'X-Test':'kept'}
    client.request('https://www.googleapis.com/batch/drive/v3', headers=original)
    assert original == {'User-Agent':'custom', 'Accept-Encoding':'identity', 'X-Test':'kept'}
    assert seen[-1]['Accept-Encoding'] == 'identity'
    assert seen[-1]['User-Agent'] == 'custom (gzip)'
    assert seen[-1]['X-Test'] == 'kept'
    client.request('https://www.googleapis.com/drive/v3/files', headers={'X-Test':'direct'})
    assert seen[-1] == {'X-Test':'direct'}
