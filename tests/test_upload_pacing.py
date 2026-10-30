"""Quota budgeting is shared across workers and counts inner requests/retries."""

from pathlib import Path

import pytest

from src.config import Settings
from src.database import Database
from src.upload_pacing import UploadRequestPacer
from src.uploader import NameChunkUploader
from tests.test_batch_upload import BatchDrive


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_inner_request_budget_is_charged_not_just_outer_batch_count():
    clock = Clock()
    pacer = UploadRequestPacer(11400, clock=clock, sleep=clock.sleep)
    for _ in range(3):
        pacer.acquire(8)
    assert clock.now == pytest.approx(0.5)  # Initial 32 requests/s, not 32 batches/s.
    assert pacer.snapshot()["admittedRequests"] == 24
    assert pacer.snapshot()["targetRequestsPerMinute"] == 11400


def test_healthy_feedback_ramps_to_target_but_timeout_wave_reduces_once():
    clock = Clock()
    pacer = UploadRequestPacer(11400, clock=clock, sleep=clock.sleep)
    for _ in range(1000):
        pacer.acquire(8)
        pacer.succeeded()
    assert pacer.snapshot()["currentRequestsPerMinute"] == 11400
    for _ in range(64):
        pacer.congested()
    assert pacer.snapshot()["currentRequestsPerMinute"] == 5700
    assert pacer.snapshot()["reductions"] == 1
    for _ in range(64):
        pacer.succeeded()
    assert pacer.snapshot()["currentRequestsPerMinute"] == 5700
    clock.sleep(5)
    for _ in range(16):
        pacer.succeeded()
    assert 5700 < pacer.snapshot()["currentRequestsPerMinute"] <= 11400


def test_small_configured_target_never_gets_overridden_by_initial_rate():
    clock = Clock()
    pacer = UploadRequestPacer(30, clock=clock, sleep=clock.sleep)
    pacer.acquire(1)
    pacer.acquire(1)
    assert clock.now == pytest.approx(2)
    pacer.congested()
    assert pacer.snapshot()["currentRequestsPerMinute"] <= 30
    for invalid in (0, -1):
        with pytest.raises(ValueError):
            pacer.acquire(invalid)


def test_actual_batch_retry_charges_only_remaining_items_and_keeps_durable_ids(tmp_path, monkeypatch):
    from src import uploader as module
    from src.retry import execute_with_backoff

    monkeypatch.setattr(module, "execute_with_backoff", lambda factory, **kw:
                        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    database = Database(tmp_path / "index.db")
    settings = Settings(Path("unused"), "folder", upload_batch_size=4,
                        api_min_interval_seconds=0, max_attempts=2)
    database.initialize()
    from src.models import DatasetManifest
    database.save_dataset(DatasetManifest("test", "f", 1, 1, "zstd", "s", 4))
    group = [(f"id-{i}", i, "payload", "hash") for i in range(4)]
    database.save_pending_chunks("test", [(i, ident, digest) for ident, i, _, digest in group])
    drive = BatchDrive(database, failure=429)
    uploader = NameChunkUploader(drive, settings, database, 25000, "description")

    class Probe:
        def __init__(self):
            self.costs = []
            self.errors = 0
            self.successes = 0

        def acquire(self, cost):
            self.costs.append(cost)

        def succeeded(self):
            self.successes += 1

        def congested(self):
            self.errors += 1

    probe = Probe()
    uploader._request_pacer = probe
    indices, error = uploader._upload_group("test", group)
    assert error is None and sorted(indices) == list(range(4))
    assert probe.costs == [4, 1]
    assert probe.errors == 1 and probe.successes == 1
    assert drive.batch_sizes == [4, 1]
    assert len(set(drive.create_ids)) == 4
    assert drive.get_calls == 0


def test_transport_failure_and_final_attempt_both_feed_shared_pacer(tmp_path):
    settings = Settings(Path("unused"), "folder", max_attempts=1, api_min_interval_seconds=0)
    uploader = NameChunkUploader(None, settings, Database(tmp_path / "index.db"), 25000)
    events = []
    from types import SimpleNamespace
    uploader._request_pacer = SimpleNamespace(acquire=lambda cost: events.append(("admit", cost)),
        succeeded=lambda: events.append("success"), congested=lambda: events.append("congested"))

    class Request:
        def execute(self):
            raise TimeoutError("read timed out")

    with pytest.raises(TimeoutError):
        uploader._execute(Request, request_cost=8)
    assert events == [("admit", 8), "congested"]
    events.clear()

    class CertificateRequest:
        def execute(self):
            import ssl
            raise ssl.SSLCertVerificationError(1, "bad certificate")

    with pytest.raises(OSError):
        uploader._execute(CertificateRequest)
    assert events == [("admit", 1)]  # Never disguise a security failure as congestion.


@pytest.mark.parametrize("use_pool", [False, True])
def test_parallel_uploader_children_share_one_budget_and_restore(tmp_path, monkeypatch, use_pool):
    from src import uploader as module
    from src.downloader import VerifiedDownloader
    from src.transfer_pool import TransferPool
    import random
    import threading

    class SharedProbe:
        def __init__(self):
            self.costs = []
            self.lock = threading.Lock()

        def acquire(self, cost):
            with self.lock:
                self.costs.append(cost)

        def succeeded(self):
            pass

        def congested(self):
            raise AssertionError("unexpected retry")

    database = Database(tmp_path / "parallel.db")
    drive = BatchDrive(database)
    probe = SharedProbe()
    # Creating a private pacer for any worker must not allocate another quota.
    monkeypatch.setattr(module, "UploadRequestPacer", lambda *_: pytest.fail("private worker budget"))
    settings = Settings(Path("unused"), "folder", upload_workers=4, upload_batch_size=4,
                        upload_target_requests_per_minute=11400, api_min_interval_seconds=0)
    source = tmp_path / "source.bin"
    original = random.Random(10).randbytes(200000)
    source.write_bytes(original)
    with_pool = TransferPool(lambda: drive, 4, request_pacer=probe)
    try:
        manifest = NameChunkUploader(with_pool.service(), settings, database, 25000, "description",
            transfer_pool=with_pool if use_pool else None,
            service_factory=None if use_pool else lambda: drive,
            request_pacer=probe).upload_file(source)
        assert sum(probe.costs) == manifest.chunk_count + 1  # Includes generateIds once.
        assert max(probe.costs) == 4
        assert VerifiedDownloader(drive, settings, database).download_bytes(manifest.dataset_id)[0] == original
    finally:
        with_pool.close()


@pytest.mark.parametrize("target", [-1, 1000001])
def test_invalid_target_is_rejected_before_requests(target):
    from src.errors import ConfigurationError
    with pytest.raises(ConfigurationError):
        Settings(Path("unused"), "folder").with_overrides(upload_target_requests_per_minute=target)


def test_simultaneous_worker_errors_are_coalesced_into_one_reduction():
    from concurrent.futures import ThreadPoolExecutor
    import threading

    clock = Clock()
    pacer = UploadRequestPacer(11400, clock=clock, sleep=clock.sleep)
    for _ in range(1000):
        pacer.succeeded()
    barrier = threading.Barrier(16, timeout=5)

    def failed_worker():
        barrier.wait()
        pacer.congested()

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: failed_worker(), range(16)))
    assert pacer.snapshot()["reductions"] == 1
    assert pacer.snapshot()["currentRequestsPerMinute"] == 5700


def test_env_parses_explicit_budget_and_default_stays_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "unused")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "folder")
    monkeypatch.delenv("UPLOAD_TARGET_REQUESTS_PER_MINUTE", raising=False)
    assert Settings.from_env(tmp_path / "absent.env").upload_target_requests_per_minute == 0
    monkeypatch.setenv("UPLOAD_TARGET_REQUESTS_PER_MINUTE", "11400")
    assert Settings.from_env(tmp_path / "absent.env").upload_target_requests_per_minute == 11400


def test_benchmark_compares_fresh_equal_inputs_in_reversed_pacing_order(tmp_path, monkeypatch):
    import json
    import logging
    from types import SimpleNamespace
    from scripts import benchmark
    from src import uploader as module
    from tests.test_uploader import FakeDrive

    settings = Settings(Path("unused"), "folder", upload_backend="description",
        description_safe_payload_bytes=25000, api_min_interval_seconds=0,
        upload_temp_dir=tmp_path, sqlite_path=tmp_path / "unused.db")
    monkeypatch.setattr(benchmark.Settings, "from_env", lambda *_: settings)
    drive = FakeDrive()
    drive._http = SimpleNamespace(request=lambda *a, **kw: ({"status": "200"}, b""))
    drive.close = lambda: None
    monkeypatch.setattr(benchmark, "build_drive_service", lambda *a, **kw: drive)
    monkeypatch.setattr(benchmark, "verify_target_folder", lambda *_: None)
    clock = Clock()
    monkeypatch.setattr(module, "UploadRequestPacer", lambda target:
                        UploadRequestPacer(target, clock=clock, sleep=clock.sleep))
    report = tmp_path / "profiles.jsonl"
    monkeypatch.setattr("sys.argv", ["benchmark.py", "--profiles", "1x1", "--chunks", "1",
        "--input-bytes", "16", "--request-targets", "0", "11400", "--repeats", "2",
        "--database", str(tmp_path / "isolated.db"), "--report", str(report)])
    logger = logging.getLogger("src.retry")
    previous_handlers = list(logger.handlers)
    try:
        benchmark.main()
    finally:
        logger.handlers[:] = previous_handlers
    rows = [json.loads(line) for line in report.read_text().splitlines()]
    assert [row["upload_target_requests_per_minute"] for row in rows] == [0, 11400, 11400, 0]
    assert len({row["dataset_id"] for row in rows}) == 4  # Never measure a resumed control.
    assert all(row["verified"] and row["original_bytes"] == 16 for row in rows)
    assert rows[0]["upload_pacing"] is None
    assert rows[1]["upload_pacing"]["admittedRequests"] == 2
    database = Database(tmp_path / "isolated.db")
    assert len({database.get_dataset(row["dataset_id"])["sha256"] for row in rows}) == 1
