#!/usr/bin/env python3
"""Compare per-chunk and batched durable transactions on the local disk only."""

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.database import Database
from src.models import DatasetManifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks", type=int, default=1024)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.chunks <= 10000 or not 1 <= args.repeats <= 10:
        parser.error("chunks must be 1..10000; repeats must be 1..10")
    parent = PROJECT_ROOT / "data"
    parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sqlite-benchmark-", dir=parent) as task_dir:
        for repeat in range(args.repeats):
            for batch_size in ([1, 64] if repeat % 2 == 0 else [64, 1]):
                database = Database(Path(task_dir) / f"r{repeat}-b{batch_size}.db")
                database.initialize()
                database.save_dataset(DatasetManifest(dataset_id="test", filename="test.bin",
                    original_size=1, compressed_size=1, compression="zstd", sha256="s",
                    chunk_count=args.chunks))
                started = time.perf_counter()
                transactions = 0
                for start in range(0, args.chunks, batch_size):
                    indices = list(range(start, min(start + batch_size, args.chunks)))
                    database.save_pending_chunks("test", [(i, f"id-{i}", "hash") for i in indices])
                    database.mark_chunks_complete("test", indices)
                    transactions += 2
                elapsed = time.perf_counter() - started
                assert database.count_complete_chunks("test") == args.chunks
                print(json.dumps({"repeat": repeat + 1, "batch_size": batch_size,
                    "chunks": args.chunks, "transactions": transactions,
                    "seconds": round(elapsed, 4), "verified": True}), flush=True)


if __name__ == "__main__":
    main()
