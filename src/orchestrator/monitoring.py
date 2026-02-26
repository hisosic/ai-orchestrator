"""Monitoring: Docker containers, stats, system/environment info."""
import os
import platform
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import docker
from docker.errors import APIError

from .runtime import LABEL_ORCHESTRATOR, LABEL_SERVICE, _docker_client


def _safe(f, default=None):
    try:
        return f()
    except (APIError, Exception):
        return default


def get_system_info() -> Dict[str, Any]:
    """Host and environment info."""
    info = {
        "hostname": platform.node(),
        "python": platform.python_version(),
        "state_dir": os.environ.get("ORCHESTRATOR_STATE_DIR", "/data"),
    }
    try:
        client = _docker_client()
        dinfo = client.info()
        info["docker"] = {
            "containers": dinfo.get("Containers", 0),
            "containers_running": dinfo.get("ContainersRunning", 0),
            "containers_paused": dinfo.get("ContainersPaused", 0),
            "containers_stopped": dinfo.get("ContainersStopped", 0),
            "images": dinfo.get("Images", 0),
            "server_version": dinfo.get("ServerVersion", ""),
            "operating_system": dinfo.get("OperatingSystem", ""),
            "architecture": dinfo.get("Architecture", ""),
        }
    except Exception as e:
        info["docker"] = {"error": str(e)}
    return info


def list_containers(include_stopped: bool = True) -> List[Dict[str, Any]]:
    """List all containers with basic info and managed label. Returns [] on error."""
    def _run():
        client = _docker_client()
        containers = client.containers.list(all=include_stopped)
        out = []
        for c in containers:
            try:
                attrs = c.attrs or {}
                config = attrs.get("Config") or {}
                labels = config.get("Labels") or {}
                state = attrs.get("State") or {}
                ns = attrs.get("NetworkSettings") or {}
                cid = getattr(c, "id", None) or ""
                out.append({
                    "id": cid[:12] if cid else "",
                    "name": (getattr(c, "name", None) or "").strip("/"),
                    "image": config.get("Image", ""),
                    "status": getattr(c, "status", state.get("Status", "unknown")),
                    "state": state.get("Status", "unknown"),
                    "created": attrs.get("Created", ""),
                    "managed": labels.get(LABEL_ORCHESTRATOR) == "true",
                    "service": labels.get(LABEL_SERVICE) or "",
                    "ports": _format_ports(ns.get("Ports") or {}),
                })
            except Exception:
                continue
        return out
    return _safe(_run, default=[])


def _format_ports(ports: Dict) -> List[str]:
    out = []
    for k, v in (ports or {}).items():
        if v:
            host = v[0].get("HostPort", "") if isinstance(v, list) else ""
            out.append(f"{k}->{host}" if host else k)
        else:
            out.append(k)
    return out[:10]


def get_container_stats(container_id: str) -> Optional[Dict[str, Any]]:
    """Get current stats for one container (cpu %, memory usage, limit)."""
    def _run():
        client = _docker_client()
        c = client.containers.get(container_id)
        raw = c.stats(stream=False)
        return _parse_stats(raw)
    return _safe(_run)


def _parse_stats(raw: Dict) -> Dict[str, Any]:
    """Parse Docker stats response to cpu_percent, memory_usage, memory_limit."""
    out = {"cpu_percent": 0.0, "memory_usage_mb": 0, "memory_limit_mb": 0, "memory_percent": 0.0}
    if not raw:
        return out
    try:
        cpu = raw.get("cpu_stats", {})
        precpu = raw.get("precpu_stats", {})
        cpu_delta = (cpu.get("cpu_usage", {}).get("total_usage", 0) or 0) - (precpu.get("cpu_usage", {}).get("total_usage", 0) or 0)
        system_delta = (cpu.get("system_cpu_usage", 0) or 0) - (precpu.get("system_cpu_usage", 0) or 0)
        if system_delta > 0 and cpu_delta > 0:
            ncpu = len((cpu.get("cpu_usage", {}).get("percpu_usage") or [])) or 1
            out["cpu_percent"] = round((cpu_delta / system_delta) * ncpu * 100.0, 2)
    except (KeyError, TypeError, ZeroDivisionError):
        pass
    try:
        mem = raw.get("memory_stats", {})
        usage = mem.get("usage", 0) or 0
        limit = mem.get("limit", 0) or 0
        out["memory_usage_mb"] = round(usage / (1024 * 1024), 2)
        out["memory_limit_mb"] = round(limit / (1024 * 1024), 2) if limit else 0
        out["memory_percent"] = round((usage / limit) * 100.0, 2) if limit else 0
    except (KeyError, TypeError, ZeroDivisionError):
        pass
    return out


def get_all_containers_with_stats() -> List[Dict[str, Any]]:
    """List all containers and attach current stats (parallel fetch for speed)."""
    containers = list_containers(include_stopped=True)
    running = [(i, c) for i, c in enumerate(containers) if (c.get("state") or "").lower() == "running" and c.get("id")]
    for c in containers:
        c["stats"] = None
    if not running:
        return containers

    def fetch_one(item: tuple) -> tuple:
        idx, c = item
        cid = c.get("id")
        st = get_container_stats(cid) if cid else None
        return idx, st

    max_workers = min(10, len(running))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_one, item): item for item in running}
        for fut in as_completed(futures):
            try:
                idx, st = fut.result()
                containers[idx]["stats"] = st
            except Exception:
                pass
    return containers


def list_images() -> List[Dict[str, Any]]:
    """List Docker images (id, tags, size). One row per image."""
    def _run():
        client = _docker_client()
        out = []
        for img in client.images.list():
            tags = img.tags or ["<none>:<none>"]
            size = img.attrs.get("Size") or 0
            out.append({
                "id": img.short_id.replace("sha256:", ""),
                "tags": tags,
                "repository": tags[0].split(":")[0] if tags and ":" in tags[0] else (tags[0] if tags else ""),
                "tag": tags[0].split(":")[1] if tags and ":" in tags[0] else "latest",
                "size_mb": round(size / (1024 * 1024), 2),
            })
        return out
    return _safe(_run, default=[])
