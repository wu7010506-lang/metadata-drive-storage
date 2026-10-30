#!/usr/bin/env python3
"""Read-only cold versus reused Drive connection benchmark (no upload/deletion)."""

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth import build_drive_service, verify_target_folder
from src.config import Settings
from src.transfer_pool import TransferPool


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    args = parser.parse_args()
    if not 1 <= args.repeats <= 5:
        parser.error("repeats must be 1..5")
    settings = Settings.from_env(args.env_file)
    factory = lambda: build_drive_service(settings.credentials_path)
    cold, warm = [], []
    pool = TransferPool(factory, 1)
    try:
        # Exclude the first connection/token establishment from the warm readings.
        pool.executor.submit(lambda: verify_target_folder(pool.service(), settings)).result()
        for _ in range(args.repeats):
            started = time.perf_counter()
            service = factory()
            try:
                verify_target_folder(service, settings)
            finally:
                service.close()
            cold.append(round(time.perf_counter() - started, 3))
            started = time.perf_counter()
            pool.executor.submit(lambda: verify_target_folder(pool.service(), settings)).result()
            warm.append(round(time.perf_counter() - started, 3))
    finally:
        pool.close()
    print(json.dumps(dict(cold_seconds=cold, warm_seconds=warm,
                         cold_median=statistics.median(cold), warm_median=statistics.median(warm),
                         read_only=True, note="Folder metadata reads, not end-to-end file throughput")))


if __name__ == "__main__":
    main()
