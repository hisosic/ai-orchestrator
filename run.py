#!/usr/bin/env python3
"""Run the AI Container Orchestrator API."""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "orchestrator.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
