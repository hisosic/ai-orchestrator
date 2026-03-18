#!/usr/bin/env bash
# AI Container Orchestrator — 원격 베어메탈 서버에 워커 에이전트 설치
#
# 사용법:
#   ./scripts/install-agent.sh <server_ip> <node_name> <master_url> [api_token]
#
# 예:
#   ./scripts/install-agent.sh 10.0.0.12 node-b http://10.0.0.1:8000 mytoken123
#
# 이 스크립트는:
# 1. SSH로 대상 서버에 접속
# 2. Docker가 설치되어 있는지 확인
# 3. 오케스트레이터 코드를 전송
# 4. Worker 모드로 docker-compose 기동
# 5. 마스터에 노드 등록

set -euo pipefail

SERVER_IP="${1:-}"
NODE_NAME="${2:-}"
MASTER_URL="${3:-}"
API_TOKEN="${4:-}"
SSH_USER="${SSH_USER:-root}"

if [ -z "$SERVER_IP" ] || [ -z "$NODE_NAME" ] || [ -z "$MASTER_URL" ]; then
  echo "사용법: $0 <server_ip> <node_name> <master_url> [api_token]"
  echo ""
  echo "환경변수:"
  echo "  SSH_USER  — SSH 사용자 (기본: root)"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE_DIR="/opt/ai-orchestrator"

echo "========================================="
echo " AI Container Orchestrator Worker 설치"
echo "========================================="
echo "  서버: ${SSH_USER}@${SERVER_IP}"
echo "  노드명: ${NODE_NAME}"
echo "  마스터: ${MASTER_URL}"
echo "  원격경로: ${REMOTE_DIR}"
echo "========================================="
echo ""

# 1. Docker 확인
echo "[1/5] Docker 설치 확인..."
ssh "${SSH_USER}@${SERVER_IP}" "docker --version" || {
  echo "오류: 대상 서버에 Docker가 설치되어 있지 않습니다."
  echo "Docker 설치 후 다시 시도하세요: https://docs.docker.com/engine/install/"
  exit 1
}

# 2. 디렉터리 생성
echo "[2/5] 원격 디렉터리 생성..."
ssh "${SSH_USER}@${SERVER_IP}" "mkdir -p ${REMOTE_DIR}"

# 3. 소스 코드 전송
echo "[3/5] 소스 코드 전송..."
rsync -avz --exclude='.git' --exclude='__pycache__' --exclude='.env' --exclude='.state' \
  "${ROOT_DIR}/" "${SSH_USER}@${SERVER_IP}:${REMOTE_DIR}/"

# 4. .env 생성 및 기동
echo "[4/5] Worker 환경 설정 및 기동..."
ssh "${SSH_USER}@${SERVER_IP}" bash -s <<REMOTE_SCRIPT
set -e
cd ${REMOTE_DIR}

# .env 생성
cat > .env <<EOF
ORCHESTRATOR_ROLE=worker
ORCHESTRATOR_NODE_NAME=${NODE_NAME}
ORCHESTRATOR_MASTER_URL=${MASTER_URL}
ORCHESTRATOR_API_TOKEN=${API_TOKEN}
ORCHESTRATOR_STATE_DIR=/data
ORCHESTRATOR_HEARTBEAT_INTERVAL=10
EOF

# 네트워크 생성
docker network create orch-internal 2>/dev/null || true

# 워커 모드로 기동
if docker compose version >/dev/null 2>&1; then
  docker compose -f docker-compose.worker.yml up -d --build
else
  docker-compose -f docker-compose.worker.yml up -d --build
fi

echo "워커 에이전트 기동 완료!"
REMOTE_SCRIPT

# 5. 마스터에 노드 등록
echo "[5/5] 마스터에 노드 등록..."
NODE_ADDR="${SERVER_IP}:8000"
curl -sS -X POST "${MASTER_URL}/v1/cluster/nodes" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"${NODE_NAME}\", \"address\": \"${NODE_ADDR}\", \"token\": \"${API_TOKEN}\"}" \
  | python3 -m json.tool 2>/dev/null || echo "등록 요청 전송됨 (마스터 응답 확인 필요)"

echo ""
echo "========================================="
echo " 설치 완료!"
echo "========================================="
echo "  노드: ${NODE_NAME} (${SERVER_IP})"
echo "  상태 확인: ./scripts/operator.sh cluster nodes"
echo "  대시보드: ${MASTER_URL}/dashboard"
echo "========================================="
