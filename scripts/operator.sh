#!/usr/bin/env bash
# AI Container Orchestrator — 운영 담당자용 단일 스크립트
# 사용법: ./scripts/operator.sh { start | stop | restart | status | health | logs }

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE_CMD="${COMPOSE_CMD:-docker compose}"
SERVICE_NAME="${ORCHESTRATOR_SERVICE:-orchestrator}"
BASE_URL="${BASE_URL:-http://localhost:8000}"

usage() {
  echo "사용법: $0 { start | stop | restart | status | health | logs }"
  echo ""
  echo "  start   — 이미지 빌드 후 서비스 기동 (docker compose up -d --build)"
  echo "  stop    — 서비스 중지 (docker compose down)"
  echo "  restart — 서비스 재시작"
  echo "  status  — Compose/컨테이너 상태 요약"
  echo "  health  — /health 엔드포인트 확인"
  echo "  logs    — 오케스트레이터 로그 실시간 출력 (Ctrl+C로 종료)"
  exit 1
}

cmd_start() {
  echo "[운영] 서비스 기동 중..."
  $COMPOSE_CMD up -d --build
  echo "[운영] 기동 완료. 상태 확인: $0 status / 헬스: $0 health"
}

cmd_stop() {
  echo "[운영] 서비스 중지 중..."
  $COMPOSE_CMD down
  echo "[운영] 중지 완료."
}

cmd_restart() {
  echo "[운영] 서비스 재시작 중..."
  $COMPOSE_CMD restart "$SERVICE_NAME"
  echo "[운영] 재시작 완료. 헬스 확인: $0 health"
}

cmd_status() {
  echo "=== Docker Compose 상태 ==="
  $COMPOSE_CMD ps
  echo ""
  echo "=== 헬스 (간단) ==="
  curl -sf "$BASE_URL/health" 2>/dev/null && echo "" || echo "연결 실패 (서비스가 아직 안 떴거나 8000 포트 확인)"
}

cmd_health() {
  echo "GET $BASE_URL/health"
  curl -sS "$BASE_URL/health" | head -c 500
  echo ""
}

cmd_logs() {
  $COMPOSE_CMD logs -f "$SERVICE_NAME"
}

case "${1:-}" in
  start)   cmd_start   ;;
  stop)    cmd_stop    ;;
  restart) cmd_restart ;;
  status)  cmd_status  ;;
  health)  cmd_health  ;;
  logs)    cmd_logs    ;;
  *)       usage       ;;
esac
