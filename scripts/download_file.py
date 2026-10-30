#!/usr/bin/env python3
"""Download and atomically restore a verified dataset."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth import build_drive_service, verify_target_folder  # noqa: E402
from src.config import Settings  # noqa: E402
from src.database import Database  # noqa: E402
from src.downloader import VerifiedDownloader  # noqa: E402
from src.errors import MetadataDriveError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore and verify a dataset")
    parser.add_argument("dataset_id")
    parser.add_argument("output", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    args = parser.parse_args()
    try:
        settings = Settings.from_env(args.env_file)
        logging.basicConfig(
            level=getattr(logging, settings.log_level, logging.INFO),
            format="%(levelname)s %(name)s: %(message)s",
        )
        service = build_drive_service(settings.credentials_path,
                                      http_timeout_seconds=settings.api_http_timeout_seconds)
        verify_target_folder(service, settings)
        path = VerifiedDownloader(
            service, settings, Database(settings.sqlite_path),
            service_factory=lambda: build_drive_service(settings.credentials_path,
                http_timeout_seconds=settings.api_http_timeout_seconds),
        ).download_file(args.dataset_id, args.output)
        print(f"Verified download complete: {path}")
        return 0
    except (MetadataDriveError, OSError, ValueError, sqlite3.Error) as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
