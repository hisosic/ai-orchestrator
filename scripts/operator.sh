#!/usr/bin/env bash
# AI Container Orchestrator — 운영 담당자용 완전한 기능 스크립트
# 사용법: ./scripts/operator.sh { start | stop | restart | status | health | logs | console | ... }

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

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Helper functions
log_info() { echo -e "${BLUE}ℹ${NC} $*"; }
log_ok() { echo -e "${GREEN}✓${NC} $*"; }
log_warn() { echo -e "${YELLOW}⚠${NC} $*"; }
log_error() { echo -e "${RED}✗${NC} $*"; }
pretty_json() { python3 -m json.tool 2>/dev/null || cat; }

usage() {
  cat << 'EOF'
🐳 AI Container Orchestrator — 운영 스크립트

사용법: ./scripts/operator.sh <명령> [인자]

📋 기본 명령:
    start            — 이미지 빌드 후 서비스 기동
    stop             — 서비스 중지
    restart          — 서비스 재시작
    status           — Compose/컨테이너 상태 정보
    health           — API 헬스 체크
    logs             — 로그 실시간 출력

🤖 AI 콘솔:
    console          — 자연어 명령 콘솔 시작

📦 서비스 관리:
    services         — 배포된 서비스 목록
    list             — 배포된 서비스 목록 (alias)
    deploy <image> [name] [replicas]  — 서비스 배포
    scale <name> <replicas>           — 서비스 스케일링
    stop <name>                       — 서비스 중지
    update <name> <image> [strategy]  — 서비스 업데이트

🖥️  클러스터 관리:
    cluster status   — 클러스터 전체 상태
    cluster nodes    — 노드 목록
    cluster add <name> <ip:port> [token]   — 노드 추가
    cluster remove <name>                   — 노드 제거
    cluster cordon <name>                   — 노드 스케줄링 중지
    cluster uncordon <name>                 — 노드 스케줄링 재개
    cluster drain <name>                    — 노드 드레인

➡️  마이그레이션:
    migrate <container_id> <src> <dest>  — 컨테이너 마이그레이션
    migrations                           — 마이그레이션 이력

🚨 알림:
    alerts          — 활성 알림 목록
    alerts ack <id> — 알림 확인

🚀 배포 마법:
    deploy-all      — 샘플 서비스들 배포 (nginx, redis, mysql)

🛠️  고급:
    install-worker <ip> <name> — 워커 노드 자동 설치

예시:
    ./scripts/operator.sh start
    ./scripts/operator.sh deploy nginx:latest web 3
    ./scripts/operator.sh scale web 5
    ./scripts/operator.sh cluster status
    ./scripts/operator.sh console
EOF
  exit 1
}

# ===== 기본 명령 =====

cmd_start() {
  log_info "서비스 기동 중..."
  docker network create "$ORCH_NETWORK" 2>/dev/null || true
  $COMPOSE_CMD up -d --build
  log_ok "기동 완료"
  sleep 2
  log_info "헬스 체크 중..."
  if cmd_health > /dev/null 2>&1; then
    log_ok "API 서버 정상"
  else
    log_warn "API 서버 초기화 중... (몇 초 대기)"
  fi
}

cmd_stop() {
  log_info "서비스 중지 중..."
  $COMPOSE_CMD down
  log_ok "중지 완료"
}

cmd_restart() {
  log_info "서비스 재시작 중..."
  $COMPOSE_CMD restart "$SERVICE_NAME"
  log_ok "재시작 완료"
}

cmd_status() {
  echo ""
  echo "=== Docker Compose 상태 ==="
  $COMPOSE_CMD ps || log_error "Compose 정보 조회 실패"
  echo ""
  log_info "헬스 체크..."
  if curl -sf "$BASE_URL/health" 2>/dev/null >/dev/null; then
    log_ok "✓ API 서버 정상"
  else
    log_error "✗ API 서버 연결 실패"
  fi
  echo ""
}

cmd_health() {
  curl -sS "$BASE_URL/health" 2>/dev/null || echo '{"status":"error"}'
}

cmd_logs() {
  $COMPOSE_CMD logs -f "$SERVICE_NAME"
}

# ===== AI 콘솔 =====

cmd_console() {
  log_info "AI Container Orchestrator Console 시작"
  echo "📌 팁: 자연어로 명령 입력하세요"
  echo "   예: 'nginx를 3개로 스케일해줘'"
  echo "   예: 'redis 배포해줘'"
  echo "   예: 'help' (도움말), 'exit' (종료)"
  echo ""
  
  PYTHON="${PYTHON:-python3}"
  
  if [ ! -f "$ROOT_DIR/cli.py" ]; then
    log_error "cli.py 를 찾을 수 없습니다"
    exit 1
  fi
  
  $PYTHON "$ROOT_DIR/cli.py" --api "$BASE_URL"
}

# ===== 서비스 관리 =====

cmd_services() {
  log_info "배포된 서비스 목록..."
  echo ""
  curl -sS "$BASE_URL/v1/services" 2>/dev/null | pretty_json || log_error "조회 실패"
}

cmd_deploy() {
  local image="${1:-}"
  local name="${2:-}"
  local replicas="${3:-1}"
  
  if [ -z "$image" ]; then
    log_error "사용법: $0 deploy <image> [name] [replicas]"
    echo "예: ./scripts/operator.sh deploy nginx:latest web 3"
    exit 1
  fi
  
  name="${name:-$(echo "$image" | cut -d: -f1 | rev | cut -d/ -f1 | rev)}"
  
  log_info "서비스 배포: $name (이미지: $image, 레플리카: $replicas)"
  curl -sS -X POST "$BASE_URL/v1/cluster/deploy" \
    -H "Content-Type: application/json" \
    -d "{\"image\": \"$image\", \"name\": \"$name\", \"replicas\": $replicas, \"strategy\": \"spread\"}" \
    | pretty_json
  log_ok "배포 완료"
}

cmd_scale() {
  local name="${1:-}"
  local replicas="${2:-}"
  
  if [ -z "$name" ] || [ -z "$replicas" ]; then
    log_error "사용법: $0 scale <서비스명> <레플리카수>"
    exit 1
  fi
  
  log_info "스케일링: $name → $replicas개"
  curl -sS -X POST "$BASE_URL/v1/cluster/scale" \
    -H "Content-Type: application/json" \
    -d "{\"service_name\": \"$name\", \"replicas\": $replicas}" \
    | pretty_json
  log_ok "스케일링 완료"
}

cmd_service_stop() {
  local name="${1:-}"
  
  if [ -z "$name" ]; then
    log_error "사용법: $0 stop <서비스명>"
    exit 1
  fi
  
  log_info "서비스 중지: $name"
  curl -sS -X POST "$BASE_URL/v1/cluster/stop" \
    -H "Content-Type: application/json" \
    -d "{\"service_name\": \"$name\"}" \
    | pretty_json
  log_ok "중지 완료"
}

cmd_update() {
  local name="${1:-}"
  local image="${2:-}"
  local strategy="${3:-rolling}"
  
  if [ -z "$name" ] || [ -z "$image" ]; then
    log_error "사용법: $0 update <서비스명> <이미지> [strategy]"
    exit 1
  fi
  
  log_info "서비스 업데이트: $name → $image"
  curl -sS -X POST "$BASE_URL/v1/cluster/deploy" \
    -H "Content-Type: application/json" \
    -d "{\"image\": \"$image\", \"name\": \"$name\", \"replicas\": 1, \"strategy\": \"$strategy\"}" \
    | pretty_json
  log_ok "업데이트 완료"
}

# ===== 클러스터 관리 =====

cmd_cluster() {
  local sub="${1:-}"
  shift || true

  case "$sub" in
    status)
      log_info "클러스터 상태 조회..."
      echo ""
      curl -sS "$BASE_URL/v1/cluster/status" 2>/dev/null | pretty_json || log_error "조회 실패"
      ;;
    nodes)
      log_info "노드 목록 조회..."
      echo ""
      curl -sS "$BASE_URL/v1/cluster/nodes" 2>/dev/null | pretty_json || log_error "조회 실패"
      ;;
    add)
      local name="${1:-}"
      local addr="${2:-}"
      local token="${3:-}"
      if [ -z "$name" ] || [ -z "$addr" ]; then
        log_error "사용법: $0 cluster add <name> <ip:port> [token]"
        exit 1
      fi
      log_info "노드 추가: $name ($addr)"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes" \
        -H "Content-Type: application/json" \
        -d "{\"name\": \"$name\", \"address\": \"$addr\", \"token\": \"$token\"}" | pretty_json
      log_ok "노드 추가 완료"
      ;;
    remove)
      local name="${1:-}"
      if [ -z "$name" ]; then
        log_error "사용법: $0 cluster remove <name>"
        exit 1
      fi
      log_info "노드 제거: $name"
      curl -sS -X DELETE "$BASE_URL/v1/cluster/nodes/$name" | pretty_json
      log_ok "노드 제거 완료"
      ;;
    cordon)
      local name="${1:-}"
      if [ -z "$name" ]; then log_error "사용법: $0 cluster cordon <name>"; exit 1; fi
      log_info "노드 cordon: $name"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes/$name/cordon" | pretty_json
      log_ok "Cordon 완료"
      ;;
    uncordon)
      local name="${1:-}"
      if [ -z "$name" ]; then log_error "사용법: $0 cluster uncordon <name>"; exit 1; fi
      log_info "노드 uncordon: $name"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes/$name/uncordon" | pretty_json
      log_ok "Uncordon 완료"
      ;;
    drain)
      local name="${1:-}"
      if [ -z "$name" ]; then log_error "사용법: $0 cluster drain <name>"; exit 1; fi
      log_info "노드 드레인: $name"
      curl -sS -X POST "$BASE_URL/v1/cluster/nodes/$name/drain" \
        -H "Content-Type: application/json" -d '{}' | pretty_json
      log_ok "드레인 완료"
      ;;
    *)
      log_error "알 수 없는 클러스터 명령: $sub"
      echo "사용 가능: status, nodes, add, remove, cordon, uncordon, drain"
      exit 1
      ;;
  esac
}

# ===== 마이그레이션 =====

cmd_migrate() {
  local container_id="${1:-}"
  local source="${2:-}"
  local dest="${3:-}"
  if [ -z "$container_id" ] || [ -z "$source" ] || [ -z "$dest" ]; then
    log_error "사용법: $0 migrate <container_id> <source_node> <dest_node>"
    exit 1
  fi
  log_info "마이그레이션: $container_id ($source → $dest)"
  curl -sS -X POST "$BASE_URL/v1/cluster/migrate" \
    -H "Content-Type: application/json" \
    -d "{\"container_id\": \"$container_id\", \"source_node\": \"$source\", \"destination_node\": \"$dest\"}" \
    | pretty_json
  log_ok "마이그레이션 시작됨"
}

cmd_migrations() {
  log_info "마이그레이션 이력..."
  echo ""
  curl -sS "$BASE_URL/v1/cluster/migrations" 2>/dev/null | pretty_json || log_error "조회 실패"
}

# ===== 알림 =====

cmd_alerts() {
  local sub="${1:-}"
  if [ "$sub" = "ack" ]; then
    local alert_id="${2:-}"
    if [ -z "$alert_id" ]; then log_error "사용법: $0 alerts ack <id>"; exit 1; fi
    log_info "알림 확인: $alert_id"
    curl -sS -X POST "$BASE_URL/v1/cluster/alerts/$alert_id/ack" | pretty_json
    log_ok "알림 확인 완료"
  else
    log_info "활성 알림 목록..."
    echo ""
    curl -sS "$BASE_URL/v1/cluster/alerts" 2>/dev/null | pretty_json || log_error "조회 실패"
  fi
}

# ===== 배포 마법 =====

cmd_deploy_all() {
  log_info "샘플 서비스들 배포 중..."
  echo ""
  
  log_info "[1/3] nginx 웹 서버 배포..."
  curl -sS -X POST "$BASE_URL/v1/cluster/deploy" \
    -H "Content-Type: application/json" \
    -d '{"image":"nginx:alpine","name":"web","replicas":2,"strategy":"spread"}' > /dev/null 2>&1 || true
  log_ok "nginx 배포"
  sleep 1
  
  log_info "[2/3] redis 캐시 배포..."
  curl -sS -X POST "$BASE_URL/v1/cluster/deploy" \
    -H "Content-Type: application/json" \
    -d '{"image":"redis:7-alpine","name":"cache","replicas":1,"strategy":"spread"}' > /dev/null 2>&1 || true
  log_ok "redis 배포"
  sleep 1
  
  log_info "[3/3] mysql 데이터베이스 배포..."
  curl -sS -X POST "$BASE_URL/v1/cluster/deploy" \
    -H "Content-Type: application/json" \
    -d '{"image":"mysql:8-alpine","name":"db","replicas":1,"strategy":"spread"}' > /dev/null 2>&1 || true
  log_ok "mysql 배포"
  
  echo ""
  log_ok "모든 샘플 서비스 배포 완료!"
  log_info "서비스 확인: $0 services"
}

# ===== 워커 노드 설치 =====

cmd_install_worker() {
  local ip="${1:-}"
  local name="${2:-}"
  
  if [ -z "$ip" ] || [ -z "$name" ]; then
    log_error "사용법: $0 install-worker <ip> <name>"
    echo "예: ./scripts/operator.sh install-worker 192.168.1.10 node-b"
    exit 1
  fi
  
  log_info "워커 노드 자동 설치 시작..."
  if [ -f "$ROOT_DIR/scripts/install-agent.sh" ]; then
    bash "$ROOT_DIR/scripts/install-agent.sh" "$ip" "$name" "$BASE_URL"
  else
    log_error "install-agent.sh 를 찾을 수 없습니다"
    exit 1
  fi
}

# ===== 명령 라우팅 =====

case "${1:-}" in
  start)        cmd_start ;;
  stop)         cmd_stop ;;
  restart)      cmd_restart ;;
  status)       cmd_status ;;
  health)       cmd_health ;;
  logs)         cmd_logs ;;
  console)      cmd_console ;;
  services)     cmd_services ;;
  list)         cmd_services ;;
  deploy)       shift; cmd_deploy "$@" ;;
  scale)        shift; cmd_scale "$@" ;;
  stop)         shift; cmd_service_stop "$@" ;;
  update)       shift; cmd_update "$@" ;;
  cluster)      shift; cmd_cluster "$@" ;;
  migrate)      shift; cmd_migrate "$@" ;;
  migrations)   cmd_migrations ;;
  alerts)       shift; cmd_alerts "$@" ;;
  deploy-all)   cmd_deploy_all ;;
  install-worker) shift; cmd_install_worker "$@" ;;
  *)            usage ;;
esac
