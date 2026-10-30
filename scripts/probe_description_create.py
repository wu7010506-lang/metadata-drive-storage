"""Bounded random-ASCII create/read probe; stop at the first capacity rejection."""

import argparse
import base64
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from googleapiclient.errors import HttpError
from src.auth import DriveServiceFactory, verify_target_folder
from src.config import Settings
from src.retry import execute_with_backoff


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    settings = Settings.from_env()
    client = DriveServiceFactory(settings.credentials_path, http_timeout_seconds=30)()
    run = str(uuid4())
    def emit(**value):
        value['run_id'] = run
        with args.report.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(value)+'\n')
        print(json.dumps(value), flush=True)
    def execute(factory):
        return execute_with_backoff(factory, max_attempts=3,
            initial_delay=1, max_delay=8)
    try:
        verify_target_folder(client, settings)
        for size in [25000, 32768, 65536, 131072, 262144]:
            file_id = execute(lambda: client.files().generateIds(count=1, space='drive', type='files'))['ids'][0]
            name = f'metadata-capacity-{run}-{size}'
            marker = {'capacity_probe':run}
            payload = base64.urlsafe_b64encode(os.urandom(size))[:size].decode('ascii')
            emit(event='reserved', file_id=file_id, size=size)
            rejected = False
            try:
                body = dict(id=file_id, name=name, parents=[settings.drive_folder_id],
                    mimeType='application/octet-stream', description=payload, appProperties=marker)
                try:
                    response = execute(lambda: client.files().create(body=body,
                        fields='id,description', supportsAllDrives=True))
                except HttpError as error:
                    if int(error.resp.status) == 409:
                        response = execute(lambda: client.files().get(fileId=file_id,
                            fields='id,description', supportsAllDrives=True))
                    else:
                        raise
                assert response['id'] == file_id and response['description'] == payload
                remote = execute(lambda: client.files().get(fileId=file_id,
                    fields='id,description,size', supportsAllDrives=True))
                assert remote['description'] == payload and remote.get('size') in (None, '0', 0)
                emit(event='capacity_verified', file_id=file_id, size=size)
            except HttpError as error:
                if int(error.resp.status) not in {400, 413}:
                    raise
                emit(event='capacity_rejected', file_id=file_id, size=size, status=int(error.resp.status))
                rejected = True
            finally:
                try:
                    remote = execute(lambda: client.files().get(fileId=file_id,
                        fields='id,name,parents,appProperties,mimeType,size', supportsAllDrives=True))
                    if (remote.get('id') != file_id or remote.get('name') != name
                            or remote.get('parents') != [settings.drive_folder_id]
                            or remote.get('appProperties') != marker
                            or remote.get('mimeType') != 'application/octet-stream'
                            or remote.get('size') not in (None, '0', 0)):
                        raise RuntimeError('probe identity mismatch; refusing deletion')
                    execute(lambda: client.files().delete(fileId=file_id, supportsAllDrives=True))
                    emit(event='removed', file_id=file_id, size=size)
                except HttpError as error:
                    if int(error.resp.status) != 404:
                        raise
                    emit(event='absent', file_id=file_id, size=size)
            if rejected:
                break
    finally:
        client.close()


if __name__ == '__main__':
    main()
