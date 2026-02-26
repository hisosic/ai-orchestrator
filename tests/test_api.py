"""API tests (no Docker required for command parsing and list)."""
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from orchestrator.main import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "version" in data


def test_services_empty_or_existing():
    r = client.get("/v1/services")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_command_parse_only():
    """Command endpoint returns parsed intent; may fail at runtime if Docker unavailable."""
    r = client.post(
        "/v1/command",
        json={"command": "nginx를 3개로 스케일해줘", "dry_run": True},
    )
    assert r.status_code == 200
    data = r.json()
    assert "intent" in data
    assert data["intent"]["action"] == "scale"
    assert data["intent"]["service_name"] == "nginx"
    assert data["intent"]["replicas"] == 3


def test_command_list():
    r = client.post("/v1/command", json={"command": "서비스 목록"})
    assert r.status_code == 200
    data = r.json()
    assert data["success"] is True
    assert "details" in data
    assert "services" in data["details"]


def test_system():
    r = client.get("/v1/system")
    assert r.status_code == 200
    data = r.json()
    assert "hostname" in data
    assert "docker" in data


def test_containers():
    r = client.get("/v1/containers")
    assert r.status_code == 200
    data = r.json()
    assert "containers" in data
    assert isinstance(data["containers"], list)
    assert "error" in data


def test_dashboard_served():
    r = client.get("/dashboard")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("content-type", "")
    assert b"AI Container Orchestrator" in r.content or b"Dashboard" in r.content


def test_images():
    r = client.get("/v1/images")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_action_list():
    r = client.post("/v1/action", json={"action": "list"})
    assert r.status_code == 200
    data = r.json()
    assert data["success"] is True
    assert "services" in data.get("details", {})
