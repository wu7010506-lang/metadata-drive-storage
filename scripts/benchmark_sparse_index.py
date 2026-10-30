"""Offline sparse resume-index A/B; no Drive calls or source-file allocation."""

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import Settings
from src.database import Database
from src.models import DatasetManifest


class CountedDatabase(Database):
    queries = 0
    @contextmanager
    def connect(self):
        self.queries += 1
        with super().connect() as connection:
            yield connection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    settings = Settings.from_env()
    with tempfile.TemporaryDirectory(prefix='sparse-index-benchmark-', dir=settings.upload_temp_dir) as directory:
        db = CountedDatabase(Path(directory) / 'sparse.db')
        db.initialize()
        count, window = 100000, 128
        db.save_dataset(DatasetManifest('d', 'virtual', 1, 1, 'zstd', 'hash', count))
        db.save_pending_chunks('d', [(0, 'first', 'hash'), (50000, 'last', 'hash')])
        for repeat in range(4):
            for method in (['legacy', 'keyset'] if repeat % 2 == 0 else ['keyset', 'legacy']):
                db.queries = 0
                began = time.perf_counter()
                if method == 'legacy':
                    rows = [r for start in range(0, count, window)
                            for r in db.get_chunk_window('d', start, min(window, count-start))]
                else:
                    rows = list(db.iter_chunks('d', count, window))
                elapsed = time.perf_counter()-began
                assert [r['chunk_index'] for r in rows] == [0, 50000]
                result = dict(repeat=repeat+1, method=method, seconds=round(elapsed, 6),
                    connections=db.queries, declared_chunks=count, stored_rows=2,
                    window=window, verified=True, index_only=True)
                print(json.dumps(result), flush=True)
                if args.report:
                    with args.report.open('a', encoding='utf-8') as handle:
                        handle.write(json.dumps(result)+'\n')


if __name__ == '__main__':
    main()
