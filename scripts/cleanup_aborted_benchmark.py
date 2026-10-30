#!/usr/bin/env python3
"""Recover only explicitly identified benchmark objects, never mark an upload complete."""

import argparse
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from googleapiclient.errors import HttpError
from src.auth import DriveServiceFactory
from src.config import Settings
from src.database import Database
from src.integrity import sha256_text
from src.payload import extract_payload
from src.retry import execute_with_backoff, is_retryable_error


def verify_remote_chunk(remote, row, manifest, settings):
    file_id, index = row['drive_file_id'], int(row['chunk_index'])
    expected = dict(dataset_id=manifest['dataset_id'], chunk_index=str(index),
                    chunk_hash=row['chunk_hash'], version=str(manifest['version']))
    if int(manifest['version']) > 1:
        expected['encoding'] = manifest['encoding']
    if (remote.get('id') != file_id or remote.get('mimeType') != 'application/octet-stream'
            or remote.get('parents') != [settings.drive_folder_id]
            or remote.get('size') not in (None, '0', 0)
            or remote.get('appProperties') != expected
            or (manifest['storage_backend'] == 'description' and
                remote.get('name') != f"metadata-chunk-{manifest['dataset_id'][:8]}-{index:08d}")
            or sha256_text(extract_payload(remote, manifest['storage_backend'])) != row['chunk_hash']):
        raise RuntimeError('remote benchmark identity/content mismatch; refusing deletion')


def cleanup_aborted(database, settings, dataset_id, expected_filename, factory, *, workers=2,
                    prefilter_absent=False, batch_size=1, skip_journaled=False):
    if database.path.resolve() == settings.sqlite_path.resolve():
        raise ValueError("refusing recovery cleanup against the live index")
    if not 1 <= workers <= 4:
        raise ValueError("recovery cleanup workers must be 1..4")
    if not 1 <= batch_size <= 4 or (batch_size > 1 and prefilter_absent):
        raise ValueError('cleanup batch size must be 1..4, without a second existence prefilter')
    manifest = database.get_dataset(dataset_id)
    if (not expected_filename.startswith("benchmark-") or not manifest
            or manifest["filename"] != expected_filename):
        raise ValueError("exact benchmark filename/dataset mismatch")
    records = database.get_chunks(dataset_id)
    record_ids = {row['drive_file_id'] for row in records}
    if any(not 0 <= int(row["chunk_index"]) < int(manifest["chunk_count"]) for row in records):
        raise ValueError("invalid benchmark chunk index")
    with database.connect() as connection:
        connection.execute("""CREATE TABLE IF NOT EXISTS benchmark_recovery_cleanup
            (dataset_id TEXT, drive_file_id TEXT, state TEXT,
             PRIMARY KEY(dataset_id, drive_file_id))""")
        cached = {row['drive_file_id']: row['state'] for row in connection.execute(
            'SELECT drive_file_id,state FROM benchmark_recovery_cleanup WHERE dataset_id=?', (dataset_id,))
            if row['state'] in {'deleted', 'absent'} and row['drive_file_id'] in record_ids} if skip_journaled else {}
    records = [row for row in records if row['drive_file_id'] not in cached]
    local, lock, clients = threading.local(), threading.Lock(), []

    def execute(request):
        return execute_with_backoff(request, max_attempts=min(3, settings.max_attempts),
            initial_delay=settings.initial_delay_seconds, max_delay=settings.max_delay_seconds)

    def remove(row):
        if not hasattr(local, "service"):
            local.service = factory()
            with lock:
                clients.append(local.service)
        service = local.service
        file_id, index = row["drive_file_id"], int(row["chunk_index"])
        try:
            remote = execute(lambda: service.files().get(fileId=file_id, supportsAllDrives=True,
                fields="id,name,description,mimeType,parents,size,appProperties"))
            verify_remote_chunk(remote, row, manifest, settings)
            execute(lambda: service.files().delete(fileId=file_id, supportsAllDrives=True))
            state = "deleted"
        except HttpError as error:
            if int(error.resp.status) != 404:
                raise
            state = "absent"
        with database.connect() as connection:
            connection.execute("INSERT OR REPLACE INTO benchmark_recovery_cleanup VALUES (?, ?, ?)",
                               (dataset_id, file_id, state))
        return {state: 1}

    def read_group(service, group, fields):
        replies = {}
        def callback(ident, response, error):
            replies[ident] = (response, error)
        def request_factory():
            batch = service.new_batch_http_request(callback=callback)
            for row in group:
                prior = replies.get(row['drive_file_id'])
                if prior is None or (prior[1] is not None and is_retryable_error(prior[1])):
                    batch.add(service.files().get(fileId=row['drive_file_id'], fields=fields,
                        supportsAllDrives=True), request_id=row['drive_file_id'])
            class Round:
                def execute(self):
                    batch.execute()
                    for _, error in replies.values():
                        if error is not None and is_retryable_error(error):
                            raise error
            return Round()
        execute(request_factory)
        result = {}
        for row in group:
            ident = row['drive_file_id']
            if ident not in replies:
                raise RuntimeError('missing benchmark metadata response; refusing deletion')
            response, error = replies[ident]
            if isinstance(error, HttpError) and int(error.resp.status) == 404:
                result[ident] = None
            elif error is not None:
                raise error
            elif not response or response.get('id') != ident:
                raise RuntimeError('benchmark metadata identity mismatch; refusing deletion')
            else:
                result[ident] = response
        return result

    def remove_group(group):
        if not hasattr(local, 'service'):
            local.service = factory()
            with lock:
                clients.append(local.service)
        service = local.service
        remotes = read_group(service, group, 'id,name,description,mimeType,parents,size,appProperties')
        states, errors = {}, {}
        for row in group:
            remote = remotes[row['drive_file_id']]
            if remote is None:
                states[row['drive_file_id']] = 'absent'
            else:
                verify_remote_chunk(remote, row, manifest, settings)
        def callback(ident, response, error):
            if error is None or (isinstance(error, HttpError) and int(error.resp.status) == 404):
                states[ident] = 'deleted' if error is None else 'absent'
                errors.pop(ident, None)
            else:
                errors[ident] = error
        def request_factory():
            batch = service.new_batch_http_request(callback=callback)
            for row in group:
                ident = row['drive_file_id']
                if ident not in states:
                    batch.add(service.files().delete(fileId=ident, supportsAllDrives=True), request_id=ident)
            class Round:
                def execute(self):
                    try:
                        batch.execute()
                    except OSError as error:
                        if not is_retryable_error(error):
                            raise
                        pending_rows = [row for row in group if row['drive_file_id'] not in states]
                        checked = read_group(service, pending_rows, 'id')
                        for ident, remote in checked.items():
                            if remote is None:
                                states[ident] = 'absent'
                                errors.pop(ident, None)
                        if any(row['drive_file_id'] not in states for row in group):
                            raise
                    for error in errors.values():
                        if isinstance(error, HttpError) and int(error.resp.status) == 409:
                            raise TimeoutError('transient scoped benchmark delete conflict') from error
                        raise error
            return Round()
        try:
            if len(states) != len(group):
                execute(request_factory)
        finally:
            if states:
                with database.connect() as connection:
                    connection.executemany('INSERT OR REPLACE INTO benchmark_recovery_cleanup VALUES (?, ?, ?)',
                        [(dataset_id, ident, state) for ident, state in states.items()])
        return {state: sum(value == state for value in states.values()) for state in ('deleted', 'absent')}

    totals = {state: sum(value == state for value in cached.values()) for state in ('deleted', 'absent')}

    def existing_records():
        # Read-only batches skip already deleted/uncreated IDs efficiently.
        # Every present object still passes remove()'s full identity/hash check.
        probe = factory()
        try:
            for start in range(0, len(records), 64):
                group = records[start:start + 64]
                replies = {}

                def callback(ident, response, error):
                    replies[ident] = (response, error)

                def request_factory():
                    batch = probe.new_batch_http_request(callback=callback)
                    for row in group:
                        prior = replies.get(row['drive_file_id'])
                        if prior is None or (prior[1] is not None and is_retryable_error(prior[1])):
                            batch.add(probe.files().get(fileId=row['drive_file_id'], fields='id',
                                supportsAllDrives=True), request_id=row['drive_file_id'])

                    class ReadRound:
                        def execute(self):
                            batch.execute()
                            for _, error in replies.values():
                                if error is not None and is_retryable_error(error):
                                    raise error
                    return ReadRound()

                execute(request_factory)
                absent, present = [], []
                for row in group:
                    if row['drive_file_id'] not in replies:
                        raise RuntimeError('missing benchmark existence response; refusing deletion')
                    response, error = replies[row['drive_file_id']]
                    if isinstance(error, HttpError) and int(error.resp.status) == 404:
                        absent.append(row['drive_file_id'])
                    elif error is not None:
                        raise error
                    elif not response or response.get('id') != row['drive_file_id']:
                        raise RuntimeError('benchmark existence identity mismatch; refusing deletion')
                    else:
                        present.append(row)
                if absent:
                    with database.connect() as connection:
                        connection.executemany('INSERT OR REPLACE INTO benchmark_recovery_cleanup VALUES (?, ?, ?)',
                            [(dataset_id, ident, 'absent') for ident in absent])
                    totals['absent'] += len(absent)
                yield from present
        finally:
            probe.close()

    source_iterator = existing_records() if prefilter_absent else iter(records)
    def groups():
        while True:
            group = []
            for _ in range(batch_size):
                row = next(source_iterator, None)
                if row is not None:
                    group.append(row)
            if not group:
                return
            yield group
    iterator = groups()
    def remove_task(group):
        return remove(group[0]) if batch_size == 1 else remove_group(group)
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending, failure = set(), None
            for _ in range(workers):
                row = next(iterator, None)
                if row is not None:
                    pending.add(executor.submit(remove_task, row))
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        for state, count in future.result().items():
                            totals[state] += count
                    except Exception as error:
                        failure = failure or error
                if not failure:
                    for _ in done:
                        row = next(iterator, None)
                        if row is not None:
                            pending.add(executor.submit(remove_task, row))
            if failure:
                raise failure
    finally:
        close_iterator = getattr(iterator, 'close', None)
        if close_iterator:
            close_iterator()
        close_source = getattr(source_iterator, 'close', None)
        if close_source:
            close_source()
        for client in clients:
            client.close()
    return totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--filename", required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument('--prefilter-absent', action='store_true',
        help='Use bounded read-only existence batches before strictly verified individual deletion')
    args = parser.parse_args()
    if not args.database.is_file():
        parser.error("the isolated benchmark index must already exist")
    settings = Settings.from_env(PROJECT_ROOT / ".env")
    factory = DriveServiceFactory(settings.credentials_path,
                                  http_timeout_seconds=min(20, settings.api_http_timeout_seconds))
    result = cleanup_aborted(Database(args.database), settings, args.dataset_id,
                             args.filename, factory, workers=args.workers,
                             prefilter_absent=args.prefilter_absent)
    print(json.dumps(dict(event="benchmark_recovery_removed", dataset_id=args.dataset_id, **result)), flush=True)


if __name__ == "__main__":
    main()
