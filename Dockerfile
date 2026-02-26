# AI Container Orchestrator - containerized
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application (src layout)
ENV PYTHONPATH=/app/src
COPY src /app/src
COPY run.py /app/

# Persistent state (mount volume at /data)
ENV ORCHESTRATOR_STATE_DIR=/data
RUN mkdir -p /data

EXPOSE 8000

# Run API; Docker socket must be mounted at /var/run/docker.sock
CMD ["python", "run.py"]
