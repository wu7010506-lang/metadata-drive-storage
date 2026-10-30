from pathlib import Path

import pytest

from src.config import Settings
from src.errors import ConfigurationError


def make_settings(**overrides: object) -> Settings:
    values = {
        "credentials_path": Path("credentials.json"),
        "drive_folder_id": "folder-id",
    }
    values.update(overrides)
    return Settings(**values)


def test_settings_reject_unbounded_retry_count() -> None:
    with pytest.raises(ConfigurationError):
        make_settings(max_attempts=0).validate()


def test_settings_reject_invalid_probe_range() -> None:
    with pytest.raises(ConfigurationError):
        make_settings(probe_initial_bytes=200, probe_max_bytes=100).validate()


@pytest.mark.parametrize("size", [0, 101])
def test_settings_reject_invalid_batch_size(size):
    with pytest.raises(ConfigurationError):
        make_settings(upload_batch_size=size).validate()


@pytest.mark.parametrize("workers", [0, 17])
def test_settings_reject_invalid_download_workers(workers):
    with pytest.raises(ConfigurationError):
        make_settings(download_workers=workers).validate()


@pytest.mark.parametrize("limit", [0, 10241])
def test_settings_limit_is_bounded(limit):
    with pytest.raises(ConfigurationError):
        make_settings(max_upload_mb=limit).validate()


def test_default_limit_is_ten_gib():
    assert make_settings().max_upload_mb * 1024**2 == 10 * 1024**3


@pytest.mark.parametrize("limit", [1, 2048, 2049, 10240])
def test_settings_accept_limits_through_ten_gib(limit):
    assert make_settings().with_overrides(max_upload_mb=limit).max_upload_mb == limit


@pytest.mark.parametrize("workers", [16, 24, 32, 48, 64, 96, 128])
def test_upload_workers_support_bounded_scaling(workers):
    assert make_settings().with_overrides(upload_workers=workers).upload_workers == workers


@pytest.mark.parametrize("workers", [0, 129])
def test_upload_workers_remain_bounded(workers):
    with pytest.raises(ConfigurationError):
        make_settings(upload_workers=workers).validate()


@pytest.mark.parametrize("timeout", [0, 4, 121])
def test_http_timeout_remains_bounded(timeout):
    with pytest.raises(ConfigurationError):
        make_settings(api_http_timeout_seconds=timeout).validate()


def test_http_timeout_preserves_existing_default():
    assert make_settings().api_http_timeout_seconds == 60
    assert make_settings().with_overrides(api_http_timeout_seconds=20).api_http_timeout_seconds == 20

