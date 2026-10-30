#!/usr/bin/env python3
"""Upload a file as encoded metadata on zero-content Drive objects."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth import DriveServiceFactory, verify_target_folder  # noqa: E402
from src.config import Settings  # noqa: E402
from src.database import Database  # noqa: E402
from src.errors import MetadataDriveError  # noqa: E402
from src.uploader import NameChunkUploader  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compress and upload a file using configured Drive metadata fields."
    )
    parser.add_argument("path", type=Path)
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="override the measured payload size for the configured backend",
    )
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        settings = Settings.from_env(args.env_file)
        logging.basicConfig(
            level=getattr(logging, settings.log_level, logging.INFO),
            format="%(levelname)s %(name)s: %(message)s",
        )
        factory = DriveServiceFactory(settings.credentials_path,
                                      http_timeout_seconds=settings.api_http_timeout_seconds)
        service = factory()
        verify_target_folder(service, settings)
        chunk_size = args.chunk_size or settings.safe_payload_for(settings.upload_backend)
        if chunk_size < 1:
            raise ValueError(
                "probe the configured backend, then set its measured payload capacity "
                "or pass --chunk-size"
            )
        uploader = NameChunkUploader(
            service,
            settings,
            Database(settings.sqlite_path),
            chunk_size,
            settings.upload_backend,
            service_factory=factory,
        )
        manifest = uploader.upload_file(args.path)
        print("Upload complete")
        print(f"Dataset ID: {manifest.dataset_id}")
        print(f"Original SHA-256: {manifest.sha256}")
        print(f"Compression: {manifest.compression}")
        print(f"Chunks: {manifest.chunk_count}")
        return 0
    except (MetadataDriveError, OSError, ValueError, sqlite3.Error) as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
