"""Trash only verified objects belonging to one explicitly identified failed UI upload."""

import argparse
import json
import ssl
import sys
from pathlib import Path
from urllib.request import urlopen
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.auth import DriveServiceFactory
from src.config import Settings
from src.database import Database
from src.integrity import sha256_text
from src.payload import extract_payload
from src.retry import execute_with_backoff


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset-id', required=True)
    parser.add_argument('--job-id', required=True)
    parser.add_argument('--filename', required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--ui-url', default='http://127.0.0.1:8770')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    dataset_id = str(UUID(args.dataset_id))
    job_id = str(UUID(args.job_id))
    settings = Settings.from_env()
    database = Database(settings.sqlite_path)
    manifest = database.get_dataset(dataset_id)
    if not manifest or manifest['filename'] != args.filename or manifest['sha256'] != args.sha256:
        raise RuntimeError('Exact dataset identity mismatch')
    records = database.get_chunks(dataset_id)
    by_id = {r['drive_file_id']: r for r in records}
    if len(by_id) != len(records):
        raise RuntimeError('Duplicate Drive IDs')

    def check_job():
        with urlopen(f'{args.ui_url}/api/uploads/{job_id}', timeout=10) as response:
            job = json.load(response)
        if (job.get('status') != 'error' or job.get('filename') != args.filename
                or job.get('totalChunks') != manifest['chunk_count']):
            raise RuntimeError('UI task is not the expected failed upload')

    check_job()
    service = DriveServiceFactory(settings.credentials_path, http_timeout_seconds=30)()

    def execute(factory):
        # Cleanup is idempotent (trash=True); discard a broken TLS connection
        # before retrying. Do not alter production upload retry behavior here.
        def request():
            class Request:
                def execute(self):
                    try:
                        return factory().execute()
                    except ssl.SSLEOFError as exc:
                        service._http.http.close()
                        raise TimeoutError('TLS EOF during targeted cleanup') from exc
            return Request()
        return execute_with_backoff(request, max_attempts=3,
            initial_delay=1, max_delay=8)

    query = (f"'{settings.drive_folder_id}' in parents and "
             f"appProperties has {{ key='dataset_id' and value='{dataset_id}' }}")

    def list_objects():
        result, token = [], None
        while True:
            page = execute(lambda: service.files().list(q=query, spaces='drive',
                pageSize=100, pageToken=token, supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                fields='nextPageToken,incompleteSearch,files(id,name,description,mimeType,parents,size,appProperties,trashed)'))
            if page.get('incompleteSearch'):
                raise RuntimeError('Incomplete Drive search; refusing cleanup')
            result.extend(page.get('files', []))
            token = page.get('nextPageToken')
            if not token:
                return result

    try:
        remote = list_objects()
        for item in remote:
            row = by_id.get(item['id'])
            if row is None:
                raise RuntimeError('Unregistered object; refusing cleanup')
            index = int(row['chunk_index'])
            expected = dict(dataset_id=dataset_id, chunk_index=str(index),
                chunk_hash=row['chunk_hash'], version=str(manifest['version']))
            if int(manifest['version']) > 1:
                expected['encoding'] = manifest['encoding']
            if (not 0 <= index < int(manifest['chunk_count'])
                    or item.get('appProperties') != expected
                    or item.get('parents') != [settings.drive_folder_id]
                    or item.get('mimeType') != 'application/octet-stream'
                    or item.get('size') not in (None, '0', 0)
                    or sha256_text(extract_payload(item, manifest['storage_backend'])) != row['chunk_hash']
                    or (manifest['storage_backend'] == 'description' and
                        item.get('name') != f'metadata-chunk-{dataset_id[:8]}-{index:08d}')):
                raise RuntimeError('Remote identity/content mismatch; refusing cleanup')
        active = [item['id'] for item in remote if not item.get('trashed')]
        print(json.dumps(dict(event='verified_scope', dataset_id=dataset_id,
            registered=len(records), remote=len(remote), active=len(active),
            already_trashed=len(remote)-len(active))), flush=True)
        if not args.apply:
            return

        audit = settings.sqlite_path.parent / f'cleanup-failed-{dataset_id}.json'
        if not audit.exists():
            with audit.open('x', encoding='utf-8') as handle:
                json.dump(dict(manifest=manifest, chunks=records, job_id=job_id,
                    folder_id=settings.drive_folder_id), handle, ensure_ascii=True, indent=2)
        else:
            saved = json.loads(audit.read_text(encoding='utf-8'))
            if saved['manifest'] != manifest or saved['chunks'] != records:
                raise RuntimeError('Cleanup audit differs from current index')

        trashed = set()
        for start in range(0, len(active), 20):
            group = active[start:start+20]
            failures = {}

            def callback(request_id, response, exception):
                if exception is not None:
                    failures[request_id] = exception
                elif response.get('id') != request_id or response.get('trashed') is not True:
                    failures[request_id] = RuntimeError('Unconfirmed trash operation')
                else:
                    trashed.add(request_id)
                    failures.pop(request_id, None)

            def batch_factory():
                batch = service.new_batch_http_request(callback=callback)
                for file_id in group:
                    if file_id not in trashed:
                        batch.add(service.files().update(fileId=file_id,
                            body={'trashed': True}, fields='id,trashed',
                            supportsAllDrives=True), request_id=file_id)
                return batch

            execute(batch_factory)
            if failures or any(file_id not in trashed for file_id in group):
                raise RuntimeError('Trash incomplete; local index retained')
            print(json.dumps(dict(event='trashed', count=len(trashed), total=len(active))), flush=True)

        if any(not item.get('trashed') for item in list_objects()):
            raise RuntimeError('Active remote objects remain; local index retained')
        check_job()
        with database.connect() as connection:
            current = dict(connection.execute('SELECT * FROM datasets WHERE dataset_id=?', (dataset_id,)).fetchone())
            current_chunks = [dict(row) for row in connection.execute(
                'SELECT * FROM chunks WHERE dataset_id=? ORDER BY chunk_index', (dataset_id,))]
            if current != manifest or current_chunks != records:
                raise RuntimeError('Index changed during cleanup; refusing local deletion')
            connection.execute('DELETE FROM datasets WHERE dataset_id=? AND filename=? AND sha256=?',
                (dataset_id, args.filename, args.sha256))
        assert database.get_dataset(dataset_id) is None
        assert database.get_chunks(dataset_id) == []
        print(json.dumps(dict(event='cleanup_complete', trashed=len(trashed),
            absent_registered=len(records)-len(remote), audit=str(audit))), flush=True)
    finally:
        service.close()


if __name__ == '__main__':
    main()
