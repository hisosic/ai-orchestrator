#!/usr/bin/env python3
"""NL Command Console for AI Container Orchestrator (Claude tool_use)."""
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from orchestrator.console import main

if __name__ == "__main__":
    main()
