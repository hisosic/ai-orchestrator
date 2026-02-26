"""Simple file-based state store for managed services."""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from .models import ServiceInfo


def _state_path() -> str:
    base = os.environ.get("ORCHESTRATOR_STATE_DIR", "/data")
    if not base or base == "/data":
        # Prefer current dir for tests when /data not writable
        try:
            Path("/data").mkdir(parents=True, exist_ok=True)
        except OSError:
            base = os.path.join(os.path.dirname(__file__), "..", "..", ".state")
    return os.path.join(base, "services.json")


def _ensure_dir():
    path = _state_path()
    dirname = os.path.dirname(path)
    if dirname:
        Path(dirname).mkdir(parents=True, exist_ok=True)


def load_services() -> Dict[str, dict]:
    _ensure_dir()
    path = _state_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_services(services: Dict[str, dict]) -> None:
    _ensure_dir()
    path = _state_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(services, f, ensure_ascii=False, indent=2)


def get_service(name: str) -> Optional[dict]:
    return load_services().get(name)


def upsert_service(
    name: str,
    image: str,
    replicas: int = 1,
    memory_limit: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    container_ids: Optional[List[str]] = None,
    environment: Optional[List[str]] = None,
    volumes: Optional[List[str]] = None,
    ports: Optional[List[str]] = None,
    user: Optional[str] = None,
) -> None:
    services = load_services()
    existing = services.get(name) or {}
    services[name] = {
        "name": name,
        "image": image,
        "replicas": replicas,
        "memory_limit": memory_limit,
        "cpu_limit": cpu_limit,
        "container_ids": container_ids if container_ids is not None else existing.get("container_ids", []),
        "environment": environment if environment is not None else existing.get("environment"),
        "volumes": volumes if volumes is not None else existing.get("volumes"),
        "ports": ports if ports is not None else existing.get("ports"),
        "user": user if user is not None else existing.get("user"),
    }
    save_services(services)


def update_service_containers(name: str, container_ids: List[str]) -> None:
    services = load_services()
    if name not in services:
        return
    services[name]["container_ids"] = container_ids
    save_services(services)


def delete_service(name: str) -> None:
    services = load_services()
    services.pop(name, None)
    save_services(services)


def list_services() -> List[ServiceInfo]:
    raw = load_services()
    return [
        ServiceInfo(
            name=s["name"],
            image=s["image"],
            replicas=s.get("replicas", 1),
            memory_limit=s.get("memory_limit"),
            cpu_limit=s.get("cpu_limit"),
            status="running" if s.get("container_ids") else "stopped",
            container_ids=s.get("container_ids", []),
        )
        for s in raw.values()
    ]
