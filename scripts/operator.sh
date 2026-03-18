#!/usr/bin/env bash
# AI Container Orchestrator — 운영 담당자용 단일 스크립트
# 사용법: ./scripts/operator.sh { start | stop | restart | status | health | logs | cluster ... }

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -z "${COMPOSE_CMD:-}" ]; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
  else
    echo "오류: docker compose 또는 docker-compose 를 찾을 수 없습니다."
    exit 1
  fi
fi
SERVICE_NAME="${ORCHESTRATOR_SERVICE:-orchestrator}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
ORCH_NETWORK="${ORCH_NETWORK:-orch-internal}"

usage() {
  echo "사용법: $0 <명령> [인자]"
  echo ""
  echo "  기본 명령:"
  echo "    start            — 이미지 빌드 후 서비스 기동"
  echo "    stop             — 서비스 중지"
  echo "    restart          — 서비스 재시작"
  echo "    status           — Compose/컨테이너 상태 요약"
  echo "    health           — /health 엔드포인트 확인"
  echo "    logs             — 로그 실시간 출력"
  echo ""
  echo "  클러스터 명령:"
  echo "    cluster status   — 클러스터 전체 상태"
  echo "    cluster nodes    — 노드 목록"
  echo "    cluster add <name> <ip:port> [token]  — 노드 추가"
  echo "    cluster remove <name>                  — 노드 제거"
  echo "    cluster cordon <name>                  — 노드 스케줄링 중지"
  echo "    cluster uncordon <name>                — 노드 스케줄링 재개"
  echo "    cluster drain <name>                   — 노드 드레인 (모든 컨테이너 이동)"
  echo ""
  echo "  마이그레이션 명령:"
  echo "    migrate <container_id> <source> <dest> — 컨테이너 마이그레이션"
  echo "    migrations                             — 마이그레이션 이력"
  echo ""
  echo "  알림 명령:"
  echo "    alerts           — 활성 알림 목록"
  echo "    alerts ack <id>  — 알림 확인"
  exit 1
}

cmd_start() {
  echo "[운영] 서비스 기동 중..."
  docker network create "$ORCH_NETWORK" 2>/dev/null || true
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

# --- Cluster commands ---

cmd_cluster() {
  local sub="${1:-}"
  shift || true

  case "$sub" in
    status)
      echo "=== 클러스터 상태 ==="
      curl -sS "$BASE_URL/v1/cluster/status" 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "조회 실패"
      ;;
    nodes)
      echo "=== 노드 목록 ==="
      curl -sS "$BASE_URL/v1/cluster/nodes" 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "조회 실패"
      ;;
    add)
      local name="${1:-}"
      local addr="${2:-}"
      local token="${3:-}"
      if [ -z "$name" ] || [ -z "$addr" ]; then
        echo "사용법: $0 cluster add <name> <ip:port> [token]"
        exit 1
      fi
      echo "[클러스터] 노드 추가: $name ($addr)"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes" \
        -H "Content-Type: application/json" \
        -d "{\"name\": \"$name\", \"address\": \"$addr\", \"token\": \"$token\"}" | python3 -m json.tool
      ;;
    remove)
      local name="${1:-}"
      if [ -z "$name" ]; then
        echo "사용법: $0 cluster remove <name>"
        exit 1
      fi
      echo "[클러스터] 노드 제거: $name"
      curl -sS -X DELETE "$BASE_URL/v1/cluster/nodes/$name" | python3 -m json.tool
      ;;
    cordon)
      local name="${1:-}"
      if [ -z "$name" ]; then echo "사용법: $0 cluster cordon <name>"; exit 1; fi
      echo "[클러스터] 노드 cordon: $name"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes/$name/cordon" | python3 -m json.tool
      ;;
    uncordon)
      local name="${1:-}"
      if [ -z "$name" ]; then echo "사용법: $0 cluster uncordon <name>"; exit 1; fi
      echo "[클러스터] 노드 uncordon: $name"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes/$name/uncordon" | python3 -m json.tool
      ;;
    drain)
      local name="${1:-}"
      if [ -z "$name" ]; then echo "사용법: $0 cluster drain <name>"; exit 1; fi
      echo "[클러스터] 노드 드레인: $name"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes/$name/drain" \
        -H "Content-Type: application/json" -d '{}' | python3 -m json.tool
      ;;
    *)
      echo "알 수 없는 클러스터 명령: $sub"
      echo "사용 가능: status, nodes, add, remove, cordon, uncordon, drain"
      exit 1
      ;;
  esac
}

cmd_migrate() {
  local container_id="${1:-}"
  local source="${2:-}"
  local dest="${3:-}"
  if [ -z "$container_id" ] || [ -z "$source" ] || [ -z "$dest" ]; then
    echo "사용법: $0 migrate <container_id> <source_node> <dest_node>"
    exit 1
  fi
  echo "[마이그레이션] $container_id: $source -> $dest"
  curl -sS -X POST "$BASE_URL/v1/cluster/migrate" \
    -H "Content-Type: application/json" \
    -d "{\"container_id\": \"$container_id\", \"source_node\": \"$source\", \"destination_node\": \"$dest\"}" \
    | python3 -m json.tool
}

cmd_migrations() {
  echo "=== 마이그레이션 이력 ==="
  curl -sS "$BASE_URL/v1/cluster/migrations" 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "조회 실패"
}

cmd_alerts() {
  local sub="${1:-}"
  if [ "$sub" = "ack" ]; then
    local alert_id="${2:-}"
    if [ -z "$alert_id" ]; then echo "사용법: $0 alerts ack <id>"; exit 1; fi
    echo "[알림] 확인: $alert_id"
    curl -sS -X POST "$BASE_URL/v1/cluster/alerts/$alert_id/ack" | python3 -m json.tool
  else
    echo "=== 활성 알림 ==="
    curl -sS "$BASE_URL/v1/cluster/alerts" 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "조회 실패"
  fi
}

case "${1:-}" in
  start)       cmd_start       ;;
  stop)        cmd_stop        ;;
  restart)     cmd_restart     ;;
  status)      cmd_status      ;;
  health)      cmd_health      ;;
  logs)        cmd_logs        ;;
  cluster)     shift; cmd_cluster "$@" ;;
  migrate)     shift; cmd_migrate "$@" ;;
  migrations)  cmd_migrations  ;;
  alerts)      shift; cmd_alerts "$@" ;;
  *)           usage           ;;
esac
