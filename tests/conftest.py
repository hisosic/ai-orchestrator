"""Pytest fixtures: use temp dir for state so /data is not required."""
import os
import tempfile

import pytest


@pytest.fixture(autouse=True)
def state_dir():
    """Use a temp directory for orchestrator state in tests."""
    with tempfile.TemporaryDirectory() as tmp:
        prev = os.environ.get("ORCHESTRATOR_STATE_DIR")
        os.environ["ORCHESTRATOR_STATE_DIR"] = tmp
        try:
            yield tmp
        finally:
            if prev is not None:
                os.environ["ORCHESTRATOR_STATE_DIR"] = prev
            else:
                os.environ.pop("ORCHESTRATOR_STATE_DIR", None)
