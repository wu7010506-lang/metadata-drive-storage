from pathlib import Path
from typing import Any

from src.config import Settings
from src.probe import AppPropertiesBackend, CapacityProbe, MetadataBackend


class MemoryBackend(MetadataBackend):
    field_name = "memory"

    def __init__(self, settings: Settings, capacity: int) -> None:
        self.settings = settings
        self.capacity = capacity
        self.api_requests = 0
        self.payload = ""
        self.deleted = False

    def create_object(self) -> str:
        return "test-id"

    def write_payload(self, file_id: str, payload: str) -> None:
        if len(payload.encode("utf-8")) > self.capacity:
            self.payload = "rejected"
        else:
            self.payload = payload

    def read_payload(self, file_id: str) -> str:
        return self.payload

    def delete_object(self, file_id: str) -> None:
        self.deleted = True


def settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "credentials_path": Path("unused.json"),
        "drive_folder_id": "folder",
        "probe_initial_bytes": 10,
        "probe_max_bytes": 100,
        "safe_margin_percent": 15,
    }
    values.update(overrides)
    return Settings(**values)


def test_probe_finds_capacity_and_always_cleans_up() -> None:
    backend = MemoryBackend(settings(), capacity=73)
    result = CapacityProbe(backend, backend.settings).run()
    assert result.maximum_successful_payload == 73
    assert result.safe_payload == 62
    assert backend.deleted


def test_app_properties_respects_documented_key_value_limit() -> None:
    assert AppPropertiesBackend.value_bytes_per_property == 121
    assert AppPropertiesBackend.documented_max_payload_bytes == 3_630
