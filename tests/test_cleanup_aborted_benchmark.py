from pathlib import Path

import pytest

from scripts.cleanup_aborted_benchmark import cleanup_aborted
from src.config import Settings
from src.database import Database
from src.integrity import sha256_text
from src.models import DatasetManifest
from tests.test_batch_upload import http_error


@pytest.mark.parametrize("change", [None, "payload", "parent", "content", "name"])
@pytest.mark.parametrize('prefilter,batch_size', [(False, 1), (True, 1), (False, 4)])
def test_aborted_cleanup_verifies_remote_identity_without_faking_completion(tmp_path, change, prefilter, batch_size):
    database = Database(tmp_path / "isolated.db")
    database.initialize()
    filename = "benchmark-own-test.bin"
    dataset = DatasetManifest("dataset", filename, 1, 1, "zstd", "unused", 2,
                              storage_backend="description")
    database.save_dataset(dataset)
    database.save_pending_chunks("dataset", [(0, "our-id", sha256_text("payload")),
                                             (1, "not-created", sha256_text("missing"))])
    remote = dict(id="our-id", name="metadata-chunk-dataset-00000000",
        mimeType="application/octet-stream", parents=["root"], size="0", description="payload",
        appProperties=dict(dataset_id="dataset", chunk_index="0", chunk_hash=sha256_text("payload"), version="1"))
    if change == "payload": remote["description"] = "user changed it"
    if change == "parent": remote["parents"] = ["elsewhere"]
    if change == "content": remote["size"] = "1024"
    if change == "name": remote["name"] = "user document"
    deleted = []
    reads = []
    class Request:
        def __init__(self, value): self.value = value
        def execute(self):
            if isinstance(self.value, Exception): raise self.value
            return self.value
    class Drive:
        def files(self): return self
        def get(self, fileId, **kwargs):
            reads.append((fileId, kwargs['fields']))
            return Request(dict(remote) if fileId == "our-id" else http_error(404))
        def new_batch_http_request(self, callback):
            class Batch:
                def __init__(self): self.requests = []
                def add(self, request, request_id): self.requests.append((request_id, request))
                def execute(self):
                    for ident, request in self.requests:
                        try: response = request.execute()
                        except Exception as error: callback(ident, None, error)
                        else: callback(ident, response, None)
            return Batch()
        def delete(self, fileId, **kwargs):
            assert fileId == "our-id"
            deleted.append(fileId)
            return Request({})
        def close(self): pass
    settings = Settings(Path("unused"), "root", max_attempts=1)
    if change:
        with pytest.raises(RuntimeError, match="mismatch"):
            cleanup_aborted(database, settings, "dataset", filename, Drive, workers=1,
                            prefilter_absent=prefilter, batch_size=batch_size)
        assert deleted == []
    else:
        result = cleanup_aborted(database, settings, "dataset", filename, Drive, workers=1,
                                 prefilter_absent=prefilter, batch_size=batch_size)
        assert result == {"deleted": 1, "absent": 1}
        if prefilter:
            assert reads.count(('not-created', 'id')) == 1
            assert all(fields == 'id' for ident, fields in reads if ident == 'not-created')
        assert cleanup_aborted(database, settings, 'dataset', filename,
            lambda: pytest.fail('journaled removals must not send another request'), workers=1,
            batch_size=batch_size, skip_journaled=True) == result
    assert database.count_complete_chunks("dataset") == 0


def test_aborted_cleanup_rejects_live_index(tmp_path):
    database = Database(tmp_path / "isolated.db")
    settings = Settings(Path("unused"), "root", sqlite_path=database.path)
    with pytest.raises(ValueError, match="live index"):
        cleanup_aborted(database, settings, "dataset", "benchmark-own.bin", lambda: None)


def test_aborted_cleanup_requires_exact_filename(tmp_path):
    database = Database(tmp_path / "isolated.db")
    database.initialize()
    database.save_dataset(DatasetManifest("dataset", "benchmark-own.bin", 1, 1, "zstd", "s", 1))
    settings = Settings(Path("unused"), "root")
    with pytest.raises(ValueError, match="filename/dataset mismatch"):
        cleanup_aborted(database, settings, "dataset", "benchmark-other.bin", lambda: pytest.fail("network"))


@pytest.mark.parametrize("workers", [0, 5])
def test_recovery_worker_count_is_bounded(tmp_path, workers):
    with pytest.raises(ValueError, match="1..4"):
        cleanup_aborted(Database(tmp_path / "isolated.db"), Settings(Path("unused"), "root"),
                        "dataset", "benchmark-own.bin", lambda: None, workers=workers)


def test_batched_cleanup_confirms_lost_delete_response_without_replaying(tmp_path, monkeypatch):
    from src.retry import execute_with_backoff
    monkeypatch.setattr('scripts.cleanup_aborted_benchmark.execute_with_backoff', lambda factory, **kw:
        execute_with_backoff(factory, sleep=lambda _: None, **kw))
    database = Database(tmp_path / 'isolated.db')
    database.initialize()
    database.save_dataset(DatasetManifest('dataset', 'benchmark-own.bin', 1, 1, 'zstd', 's', 1,
                                          storage_backend='description'))
    database.save_pending_chunk('dataset', 0, 'ours', sha256_text('payload'))
    remote = dict(id='ours', name='metadata-chunk-dataset-00000000', description='payload',
        mimeType='application/octet-stream', parents=['root'], size='0',
        appProperties=dict(dataset_id='dataset', chunk_index='0', chunk_hash=sha256_text('payload'), version='1'))
    objects = {'ours': remote, 'user-object': {'id': 'user-object'}}
    deleted = []
    class Request:
        def __init__(self, run, mutation=False): self.run, self.mutation = run, mutation
        def execute(self): return self.run()
    class Drive:
        def files(self): return self
        def close(self): pass
        def get(self, fileId, **kwargs):
            def run():
                if fileId not in objects: raise http_error(404)
                return objects[fileId]
            return Request(run)
        def delete(self, fileId, **kwargs):
            def run():
                deleted.append(fileId)
                objects.pop(fileId)
            return Request(run, True)
        def new_batch_http_request(self, callback):
            class Batch:
                def __init__(self): self.requests = []
                def add(self, request, request_id): self.requests.append((request_id, request))
                def execute(self):
                    for ident, request in self.requests:
                        try: response = request.execute()
                        except Exception as error: callback(ident, None, error)
                        else:
                            if request.mutation: raise TimeoutError('deletion succeeded; response lost')
                            callback(ident, response, None)
            return Batch()
    result = cleanup_aborted(database, Settings(Path('unused'), 'root'), 'dataset',
        'benchmark-own.bin', Drive, workers=1, batch_size=4)
    assert result == {'deleted': 0, 'absent': 1}
    assert deleted == ['ours'] and set(objects) == {'user-object'}
    assert database.count_complete_chunks('dataset') == 0
