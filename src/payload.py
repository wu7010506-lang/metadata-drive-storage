"""Metadata packing shared by upload verification and reconstruction."""

BACKENDS = {"name", "description", "combined"}


def payload_fields(backend: str) -> str:
    return "name,description" if backend == "combined" else backend


def extract_payload(remote, backend: str) -> str:
    if backend == "combined":
        name, description = remote.get("name"), remote.get("description", "")
        if not isinstance(name, str) or not isinstance(description, str):
            raise ValueError("combined metadata payload must contain strings")
        return name + description
    value = remote.get(backend)
    if not isinstance(value, str):
        raise ValueError("metadata payload must be a string")
    return value


def pack_payload(payload, backend, name_capacity=0, description_capacity=0):
    if backend == "combined":
        if name_capacity < 1 or description_capacity < 1:
            raise ValueError("combined mode requires both measured metadata capacities")
        if len(payload) > name_capacity + description_capacity:
            raise ValueError("resume partition exceeds current combined metadata capacity")
        return {"name": payload[:name_capacity], "description": payload[name_capacity:]}
    return {backend: payload}
