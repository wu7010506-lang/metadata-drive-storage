import logging

from scripts.benchmark import RetryCounter, HttpTimings, MeasuredFactory, stop_after_quota
import pytest
from types import SimpleNamespace


def test_loss_injector_is_explicit_bounded_and_never_changes_request_bodies():
    from scripts.benchmark import BatchResponseLoss, InjectedBatchResponseLoss
    calls = []
    def original(uri, method='GET', **kwargs):
        calls.append(kwargs.get('body'))
        return {'status': '200'}, b'unchanged reply'
    service = SimpleNamespace(_http=SimpleNamespace(request=original))
    losses = BatchResponseLoss(1)
    losses.wrap(service)
    uri = 'https://www.googleapis.com/batch/drive/v3'
    body = 'POST /drive/v3/files?fields=id HTTP/1.1\r\n\r\n{"id":"stable"}'
    with pytest.raises(InjectedBatchResponseLoss):
        service._http.request(uri, 'POST', body=body)
    assert service._http.request(uri, 'POST', body=body)[1] == b'unchanged reply'
    assert calls == [body, body] and losses.injected == 1
    losses.remaining = 1
    losses.enabled = False
    service._http.request(uri, 'POST', body=body)
    assert losses.injected == 1


def test_retry_counter_distinguishes_quota_backoff_from_timeouts():
    counter = RetryCounter()
    for status in (403, 429, 503, 0):
        record = logging.LogRecord("src.retry", logging.WARNING, "", 0,
                                   "request failed; retrying in 1s", (), None)
        record.api_status = status
        counter.emit(record)
    assert counter.retries == 4
    assert counter.rate_limits == 1  # Preserve the existing 429-only field.
    assert counter.quota_retries == 2
    assert counter.http_status_counts == {403: 1, 429: 1, 503: 1, 0: 1}
    record = logging.LogRecord('src.uploader', logging.WARNING, '', 0, 'verified read-back', (), None)
    record.ambiguous_chunks_confirmed = 4
    counter.emit(record)
    assert counter.ambiguous_chunks_confirmed == 4
    assert counter.retries == 4  # Read-back success is not another write retry.


def test_http_timings_preserve_results_and_exceptions_without_recording_payloads():
    clock = iter([0, 1, 3, 63])
    class Http:
        def request(self, uri, method="GET", **kwargs):
            if method == "POST":
                raise TimeoutError("secret must never be recorded")
            return {"status": "200"}, b"payload must never be recorded"
    service = SimpleNamespace(_http=Http())
    metrics = HttpTimings(clock=lambda: next(clock))
    metrics.wrap(service)
    assert service._http.request("https://example.test/files/private-id")[0]["status"] == "200"
    with pytest.raises(TimeoutError):
        service._http.request("https://example.test/batch/drive/v3", method="POST", body="secret")
    stats = metrics.snapshot()
    assert stats["GET"]["count"] == 1
    assert stats["GET"]["max_seconds"] == 1
    assert stats["batch"]["max_seconds"] == 60
    assert stats["batch"]["outcomes"] == {"TimeoutError": 1}
    assert stats['GET']['decoded_bytes'] == len(b'payload must never be recorded')
    assert stats['batch']['decoded_bytes'] == 0
    assert "secret" not in str(stats) and "private-id" not in str(stats)
    metrics.reset()
    assert metrics.snapshot() == {}


def test_http_metrics_count_decoded_gzip_without_claiming_wire_size():
    service = SimpleNamespace(_http=SimpleNamespace(request=lambda *a, **kw:
        ({'status':'200', '-content-encoding':'gzip'}, b'verified payload')))
    metrics = HttpTimings()
    metrics.wrap(service)
    service._http.request('https://example.test/batch/drive/v3', 'POST')
    group = metrics.snapshot()['batch']
    assert group['gzip_responses'] == 1
    assert group['decoded_bytes'] == len(b'verified payload')
    assert 'wire_bytes' not in group


def test_http_latency_samples_remain_bounded():
    metrics = HttpTimings(sample_limit=3)
    for _ in range(5):
        metrics.record("batch", 1.5, "200")
    summary = metrics.snapshot()["batch"]
    assert summary["count"] == 5
    assert summary["sample_count"] == 3
    assert summary["total_seconds"] == 7.5


@pytest.mark.parametrize("limit", [0, -1, 10001])
def test_http_sample_limit_is_bounded(limit):
    with pytest.raises(ValueError, match="between 1 and 10000"):
        HttpTimings(sample_limit=limit)


def test_measured_factory_closes_owned_clients_between_profiles():
    closed = []
    def base():
        client = SimpleNamespace(_http=SimpleNamespace(request=lambda *a, **k: ({"status": "200"}, b"")))
        client.close = lambda: closed.append(id(client))
        return client
    factory = MeasuredFactory(base, HttpTimings())
    first, second = factory(), factory()
    factory.close()
    factory.close()
    assert closed == [id(first), id(second)]
    third = factory()
    factory.close()
    assert closed[-1] == id(third)


def test_benchmark_stops_profiles_after_observed_quota():
    counter = RetryCounter()
    closed = []
    factory = SimpleNamespace(close=lambda: closed.append(True))
    stop_after_quota(counter, factory, "test")
    assert closed == []
    counter.quota_retries = 1
    with pytest.raises(SystemExit, match="rate limit"):
        stop_after_quota(counter, factory, "test")
    assert closed == [True]


def test_main_stops_before_verification_download_after_upload_quota(tmp_path, monkeypatch):
    from pathlib import Path
    from scripts import benchmark
    from src.config import Settings
    settings = Settings(Path('unused'), 'folder', upload_temp_dir=tmp_path,
        sqlite_path=tmp_path/'live.db', upload_backend='description',
        description_safe_payload_bytes=25000, api_min_interval_seconds=0)
    monkeypatch.setattr(benchmark.Settings, 'from_env', lambda *_: settings)
    closed = []
    service = SimpleNamespace(_http=SimpleNamespace(request=lambda *a, **kw: ({'status': '200'}, b'')),
                              close=lambda: closed.append(True))
    monkeypatch.setattr(benchmark, 'build_drive_service', lambda *a, **kw: service)
    monkeypatch.setattr(benchmark, 'verify_target_folder', lambda *a: None)
    class Uploader:
        def __init__(self, *a, **kw):
            pass
        def upload_file(self, *a, **kw):
            logging.getLogger('src.retry').warning('retrying in 1s', extra={'api_status':403})
            return SimpleNamespace(dataset_id='test')
    monkeypatch.setattr(benchmark, 'NameChunkUploader', Uploader)
    monkeypatch.setattr(benchmark, 'VerifiedDownloader', lambda *a, **kw: pytest.fail('verification after quota'))
    monkeypatch.setattr('sys.argv', ['benchmark.py', '--profiles', '1x1', '--chunks', '1',
        '--input-bytes', '16', '--database', str(tmp_path/'isolated.db')])
    logger = logging.getLogger('src.retry')
    handlers = list(logger.handlers)
    try:
        with pytest.raises(SystemExit, match='rate limit'):
            benchmark.main()
        assert closed == [True]
    finally:
        logger.handlers[:] = handlers
