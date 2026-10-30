"""One-off, explicit allowlist for this session's benchmark artifacts only."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.auth import DriveServiceFactory
from src.config import Settings
from src.database import Database
from scripts.cleanup_aborted_benchmark import cleanup_aborted

# These are freshly generated test manifests, not IDs read from the live index.
SCOPES = [
    ('data/pacing-live-20261022.db', 'benchmark-824e7b06-', [
        '99500dba-f7a6-41d7-b07e-9961b3fded62',
        '60e0c291-0fa5-467a-9aaf-7808398b02fc',
        '49f3e881-58e1-4213-8469-5db140966a4e']),
    ('data/request-body-gzip-20261022.db', 'benchmark-58698a98-', [
        '557f3011-d281-4062-b3f0-13f4a029cbf9',
        'a2d3a177-7196-485f-b569-ee546ba2c70f',
        'a5d49a01-fa2b-42db-8740-f13b49eda228']),
    ('data/readback-natural-20261022.db', 'benchmark-1ad0c4fe-', [
        '93b8874d-083b-49ee-91e4-4fe72b3722ff',
        'ac1c9e79-e0c3-4ff2-8c23-cd3a87633621',
        'f8607b4e-49ba-4d1d-a164-c3ca227dda6a',
        '814c1dd9-0ca8-498d-9e78-e1905915f56d']),
]


def main():
    settings = Settings.from_env(PROJECT_ROOT / '.env')
    factory = DriveServiceFactory(settings.credentials_path, http_timeout_seconds=20)
    scopes = []
    for relative, prefix, identifiers in SCOPES:
        path = PROJECT_ROOT / relative
        if not path.is_file() or path.resolve() == settings.sqlite_path.resolve():
            raise RuntimeError('refusing missing/live index')
        database = Database(path)
        for ident in identifiers:
            manifest = database.get_dataset(ident)
            if not manifest or not manifest['filename'].startswith(prefix):
                raise RuntimeError('explicit benchmark manifest mismatch')
            scopes.append((database, ident, manifest['filename']))
    for database, ident, filename in scopes:
        print(json.dumps(dict(event='strict_cleanup_starting', database=database.path.name,
                              dataset_id=ident)), flush=True)
        result = cleanup_aborted(database, settings, ident, filename, factory,
                                 workers=4, batch_size=4, skip_journaled=True)
        print(json.dumps(dict(event='strict_cleanup_finished', dataset_id=ident, **result)), flush=True)


if __name__ == '__main__':
    main()
