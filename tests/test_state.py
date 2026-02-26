"""Tests for state store (using temp dir)."""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from orchestrator import state
from orchestrator.state import (
    delete_service,
    get_service,
    list_services,
    load_services,
    save_services,
    upsert_service,
)


def test_state_isolated():
    with tempfile.TemporaryDirectory() as tmp:
        prev = os.environ.get("ORCHESTRATOR_STATE_DIR")
        os.environ["ORCHESTRATOR_STATE_DIR"] = tmp
        try:
            assert load_services() == {}
            upsert_service("web", "nginx:latest", replicas=2, memory_limit="512m")
            s = get_service("web")
            assert s is not None
            assert s["image"] == "nginx:latest"
            assert s["replicas"] == 2
            assert s["memory_limit"] == "512m"
            svcs = list_services()
            assert len(svcs) == 1
            assert svcs[0].name == "web"
            delete_service("web")
            assert get_service("web") is None
        finally:
            if prev is not None:
                os.environ["ORCHESTRATOR_STATE_DIR"] = prev
            else:
                os.environ.pop("ORCHESTRATOR_STATE_DIR", None)
