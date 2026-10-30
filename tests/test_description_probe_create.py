"""Capacity experiment must stop at rejection and delete only its own objects."""

import json
from pathlib import Path

import pytest
from httplib2 import Response
from googleapiclient.errors import HttpError

from src.config import Settings
from scripts import probe_description_create as probe


class Request:
    def __init__(self, operation):
        self.operation = operation
    def execute(self):
        return self.operation()


class Drive:
    def __init__(self, corrupt=False):
        self.objects = {'unrelated': {'id':'unrelated'}}
        self.sizes, self.deleted = [], []
        self.next_id, self.closed, self.corrupt = 0, False, corrupt
    def files(self):
        return self
    def generateIds(self, **kw):
        self.next_id += 1
        return Request(lambda: {'ids':[str(self.next_id)]})
    def create(self, *, body, **kw):
        def operation():
            self.sizes.append(len(body['description']))
            if len(body['description']) > 25000:
                raise HttpError(Response({'status':'413'}), b'{}')
            self.objects[body['id']] = dict(body)
            return dict(body)
        return Request(operation)
    def get(self, *, fileId, **kw):
        def operation():
            if fileId not in self.objects:
                raise HttpError(Response({'status':'404'}), b'{}')
            result = dict(self.objects[fileId])
            if self.corrupt and kw['fields'].startswith('id,name'):
                result['parents'] = ['other-folder']
            return result
        return Request(operation)
    def delete(self, *, fileId, **kw):
        def operation():
            self.deleted.append(fileId)
            self.objects.pop(fileId)
            return {}
        return Request(operation)
    def close(self):
        self.closed = True


@pytest.mark.parametrize('corrupt', [False, True])
def test_probe_scope_and_rejection_stop(tmp_path, monkeypatch, corrupt):
    drive = Drive(corrupt)
    settings = Settings(Path('unused'), 'folder')
    monkeypatch.setattr(probe.Settings, 'from_env', lambda:settings)
    monkeypatch.setattr(probe, 'DriveServiceFactory', lambda *a, **kw:lambda:drive)
    monkeypatch.setattr(probe, 'verify_target_folder', lambda *a:None)
    report = tmp_path/'report.jsonl'
    monkeypatch.setattr('sys.argv', ['probe.py', '--report', str(report)])
    if corrupt:
        with pytest.raises(RuntimeError, match='identity mismatch'):
            probe.main()
        assert drive.deleted == []
        assert drive.sizes == [25000]
    else:
        probe.main()
        assert drive.sizes == [25000, 32768]
        assert drive.deleted == ['1']
        assert set(drive.objects) == {'unrelated'}
        records = [json.loads(line) for line in report.read_text().splitlines()]
        assert any(r['event']=='capacity_rejected' and r['size']==32768 for r in records)
        assert records[-1]['event'] == 'absent'
    assert drive.closed
