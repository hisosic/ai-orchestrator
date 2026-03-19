#!/bin/bash
# Provision a new worker node via SSH
# Usage: provision-node.sh <node_ip> <node_name> <ssh_user> <ssh_password> <master_ip> <api_token>
set -e

NODE_IP="${1:?Usage: provision-node.sh <ip> <name> <user> <password> <master_ip> <token>}"
NODE_NAME="${2:-worker-${NODE_IP##*.}}"
SSH_USER="${3:-root}"
SSH_PASS="${4}"
MASTER_IP="${5:-127.0.0.1:8000}"
API_TOKEN="${6}"
ORCHESTRATOR_IMAGE="${7:-ai-orchestrator-go:latest}"

SSH_KEY_PATH="${8}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# Auto-discover SSH key if not explicitly provided
if [ -z "${SSH_KEY_PATH}" ] || [ ! -f "${SSH_KEY_PATH}" ]; then
    for trykey in /ssh-keys/loopvm.pem /ssh-keys/id_rsa /root/.ssh/id_rsa; do
        if [ -f "${trykey}" ]; then
            SSH_KEY_PATH="${trykey}"
            break
        fi
    done
fi

# Key takes priority: if key exists, use it (ignore password)
USE_KEY=false
if [ -n "${SSH_KEY_PATH}" ] && [ -f "${SSH_KEY_PATH}" ]; then
    SSH_OPTS="${SSH_OPTS} -i ${SSH_KEY_PATH}"
    USE_KEY=true
    echo "  Auth: SSH key (${SSH_KEY_PATH})"
elif [ -n "${SSH_PASS}" ]; then
    echo "  Auth: password"
else
    echo "  Auth: default (no key, no password)"
fi

# SSH command helper
run_ssh() {
    if [ "${USE_KEY}" = "true" ]; then
        ssh ${SSH_OPTS} ${SSH_USER}@${NODE_IP} "$@"
    elif [ -n "${SSH_PASS}" ]; then
        sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} ${SSH_USER}@${NODE_IP} "$@"
    else
        ssh ${SSH_OPTS} ${SSH_USER}@${NODE_IP} "$@"
    fi
}

# SCP helper
run_scp() {
    if [ "${USE_KEY}" = "true" ]; then
        scp ${SSH_OPTS} "$@"
    elif [ -n "${SSH_PASS}" ]; then
        sshpass -p "${SSH_PASS}" scp ${SSH_OPTS} "$@"
    else
        scp ${SSH_OPTS} "$@"
    fi
}

echo "=== Provisioning node: ${NODE_NAME} (${NODE_IP}) ==="
echo "  SSH: ${SSH_USER}@${NODE_IP}"
echo "  Master: ${MASTER_IP}"

# Step 1: Test SSH connectivity
echo "[1/6] Testing SSH connection..."
run_ssh "echo 'SSH OK: \$(hostname)'" 2>&1 || { echo "FAIL: SSH connection failed"; exit 1; }
echo "  SSH connected"

# Step 2: Check Docker
echo "[2/6] Checking Docker..."
DOCKER_VER=$(run_ssh "docker --version 2>/dev/null" || true)
if [ -z "${DOCKER_VER}" ]; then
    echo "  Docker not found. Installing..."
    run_ssh "curl -fsSL https://get.docker.com | sh" 2>&1 || { echo "FAIL: Docker install failed"; exit 1; }
    run_ssh "systemctl enable docker && systemctl start docker" 2>&1
    DOCKER_VER=$(run_ssh "docker --version")
fi
echo "  ${DOCKER_VER}"

# Step 3: Transfer orchestrator image
echo "[3/6] Transferring orchestrator image..."
# Save image locally in container
IMAGE_TAR="/tmp/orch-provision-${NODE_IP}.tar.gz"
docker save "${ORCHESTRATOR_IMAGE}" | gzip > "${IMAGE_TAR}"
IMAGE_SIZE=$(du -h "${IMAGE_TAR}" | cut -f1)
echo "  Image: ${IMAGE_SIZE}"

run_scp "${IMAGE_TAR}" "${SSH_USER}@${NODE_IP}:/tmp/ai-orch-go.tar.gz" 2>&1
echo "  Transferred"

# Step 4: Load image and setup on remote
echo "[4/6] Setting up remote node..."
run_ssh "
    docker load < /tmp/ai-orch-go.tar.gz
    rm -f /tmp/ai-orch-go.tar.gz
    docker network create orch-internal 2>/dev/null || true
    mkdir -p /data/orchestrator
" 2>&1
echo "  Image loaded"
rm -f "${IMAGE_TAR}"

# Step 5: Start worker container
echo "[5/6] Starting worker container..."
run_ssh "
    docker rm -f ai-orchestrator 2>/dev/null || true
    docker run -d \
        --name ai-orchestrator \
        --restart unless-stopped \
        -p 8000:8000 \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v /data/orchestrator:/data \
        -e ORCHESTRATOR_ROLE=worker \
        -e ORCHESTRATOR_NODE_NAME=${NODE_NAME} \
        -e ORCHESTRATOR_MASTER_URL=http://${MASTER_IP} \
        -e ORCHESTRATOR_HEARTBEAT_INTERVAL=10 \
        -e ORCHESTRATOR_STATE_DIR=/data \
        -e ORCHESTRATOR_API_TOKEN=${API_TOKEN} \
        --network orch-internal \
        --health-cmd='wget -q --spider http://localhost:8000/health || exit 1' \
        --health-interval=10s \
        --health-timeout=5s \
        --health-retries=3 \
        ${ORCHESTRATOR_IMAGE}
    sleep 3
    docker ps --filter name=ai-orchestrator --format '{{.Names}} {{.Status}}'
" 2>&1
echo "  Container started"

# Step 6: Register with master
echo "[6/6] Registering with master..."
# Use internal API (we're running inside the master container)
wget -q -O - --post-data="{\"name\":\"${NODE_NAME}\",\"address\":\"${NODE_IP}:8000\",\"token\":\"${API_TOKEN}\"}" \
    --header="Content-Type: application/json" \
    "http://127.0.0.1:8000/v1/cluster/nodes" 2>&1 || \
curl -s -X POST "http://127.0.0.1:8000/v1/cluster/nodes" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${NODE_NAME}\",\"address\":\"${NODE_IP}:8000\",\"token\":\"${API_TOKEN}\"}" 2>&1

echo ""
echo "=== Provisioning complete ==="
echo "  Node: ${NODE_NAME}"
echo "  IP: ${NODE_IP}"
echo "  Worker URL: http://${NODE_IP}:8000"

# Output JSON result
python3 -c "
import json
print(json.dumps({
    'success': True,
    'node_name': '${NODE_NAME}',
    'node_ip': '${NODE_IP}',
    'message': 'Node provisioned and registered'
}))
" 2>/dev/null || echo '{"success":true}'
