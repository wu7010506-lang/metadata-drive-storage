#!/usr/bin/env python3
"""CLI for the Google Drive metadata capacity probe."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth import build_drive_service, verify_target_folder  # noqa: E402
from src.config import Settings  # noqa: E402
from src.errors import MetadataDriveError  # noqa: E402
from src.probe import SUPPORTED_BACKENDS, create_backend  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure reliable Google Drive metadata payload capacity."
    )
    parser.add_argument("--backend", choices=SUPPORTED_BACKENDS)
    parser.add_argument("--initial-bytes", type=int)
    parser.add_argument("--max-bytes", type=int)
    parser.add_argument("--safe-margin-percent", type=float)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        settings = Settings.from_env(args.env_file)
        overrides = {
            name: value
            for name, value in {
                "metadata_backend": args.backend,
                "probe_initial_bytes": args.initial_bytes,
                "probe_max_bytes": args.max_bytes,
                "safe_margin_percent": args.safe_margin_percent,
            }.items()
            if value is not None
        }
        settings = settings.with_overrides(**overrides)
        logging.basicConfig(
            level=getattr(logging, settings.log_level, logging.INFO),
            format="%(levelname)s %(name)s: %(message)s",
        )

        service = build_drive_service(settings.credentials_path)
        folder = verify_target_folder(service, settings)
        backend = create_backend(settings.metadata_backend, service, settings)
        print(f"Connected to Google Drive folder: {folder.get('name', folder['id'])}")
        print(f"Testing metadata capacity ({settings.metadata_backend})...")
        result = backend.probe_capacity()

        for attempt in result.attempts:
            print(f"{attempt.size_bytes:>8} bytes  {'PASS' if attempt.succeeded else 'FAIL'}")
        print("\nMetadata Probe Result")
        print(f"Backend: {result.backend}")
        print(f"Metadata field tested: {result.field_tested}")
        maximum_label = "Maximum successful payload"
        if result.capped:
            maximum_label = "Maximum successful payload (probe cap reached)"
        print(f"{maximum_label}: {result.maximum_successful_payload} bytes")
        print(f"Recommended safe payload: {result.safe_payload} bytes")
        print(f"Average write latency: {result.average_write_latency_seconds:.3f} s")
        print(f"Average read latency: {result.average_read_latency_seconds:.3f} s")
        print(f"Number of API requests: {result.api_requests}")
        print(f"Errors encountered: {len(result.errors)}")
        print("All temporary test objects deleted.")
        return 0
    except MetadataDriveError as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
