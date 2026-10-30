#!/usr/bin/env python3
"""Start the local metadata upload interface."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import Settings  # noqa: E402
from src.auth import DriveServiceFactory  # noqa: E402
from ui.server import create_app  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local upload interface")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    args = parser.parse_args()

    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("the UI may only bind to a loopback address")
    settings = Settings.from_env(args.env_file)
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(levelname)s %(name)s: %(message)s",
    )
    app = create_app(settings, service_factory=DriveServiceFactory(
        settings.credentials_path, http_timeout_seconds=settings.api_http_timeout_seconds))
    try:
        app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
    finally:
        app.extensions["close_transfers"]()


if __name__ == "__main__":
    main()
