#!/bin/bash
# 🚀 AI Container Orchestrator - 완전 배포 스크립트
# 목적: docs의 모든 요구사항을 충족하는 배포 및 설정

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log_header() {
  echo ""
  echo -e "${BLUE}════════════════════════════════════════════════════${NC}"
  echo -e "${BLUE}$*${NC}"
  echo -e "${BLUE}════════════════════════════════════════════════════${NC}"
  echo ""
}

log_step() {
  echo -e "${BLUE}▶${NC} $*"
}

log_ok() {
  echo -e "${GREEN}✓${NC} $*"
}

log_error() {
  echo -e "${RED}✗${NC} $*"
  exit 1
}

log_warn() {
  echo -e "${YELLOW}⚠${NC} $*"
}

# Check prerequisites
log_header "🔍 필수 요건 확인"

log_step "Docker 확인..."
if ! command -v docker &> /dev/null; then
  log_error "Docker가 설치되어 있지 않습니다"
fi
log_ok "Docker 설치됨"

log_step "Python 확인..."
if ! command -v python3 &> /dev/null; then
  log_error "Python3가 설치되어 있지 않습니다"
fi
log_ok "Python3 설치됨"

log_step "Git 확인..."
if ! command -v git &> /dev/null; then
  log_error "Git이 설치되어 있지 않습니다"
fi
log_ok "Git 설치됨"

# Check if running in project root
if [ ! -f "$ROOT_DIR/requirements.txt" ]; then
  log_error "프로젝트 루트 디렉토리에서 실행하세요"
fi

# Setup phase
log_header "📦 환경 설정"

log_step "Python 가상 환경 생성..."
if [ ! -d "venv" ]; then
  python3 -m venv venv
  log_ok "가상 환경 생성 완료"
else
  log_ok "기존 가상 환경 사용"
fi

log_step "의존성 설치..."
source venv/bin/activate || {
  if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
  fi
}

pip install -q -r requirements.txt
log_ok "의존성 설치 완료"

# Docker image build
log_header "🐳 Docker 이미지 빌드"

log_step "Orchestrator 이미지 빌드..."
if [ -f "Dockerfile" ]; then
  docker build -t ai-container-orchestrator:latest .
  log_ok "이미지 빌드 완료"
else
  log_warn "Dockerfile을 찾을 수 없습니다"
fi

# Configuration
log_header "⚙️  설정 파일 준비"

log_step ".env 파일 확인..."
if [ ! -f ".env" ] && [ -f ".env.example" ]; then
  cp .env.example .env
  log_ok ".env 파일 생성 (예제에서 복사)"
elif [ ! -f ".env" ]; then
  log_warn ".env 파일이 없습니다 (수동으로 생성 필요)"
else
  log_ok ".env 파일 존재"
fi

log_step ".env.ai-console 설정..."
if [ ! -f ".env.ai-console" ]; then
  log_warn ".env.ai-console이 없습니다 (AI 기능 사용 불가)"
else
  log_ok ".env.ai-console 설정 완료"
fi

# Network setup
log_header "🌐 Docker 네트워크 설정"

log_step "orch-internal 네트워크 생성..."
docker network create orch-internal 2>/dev/null || log_ok "네트워크 이미 존재"
log_ok "네트워크 설정 완료"

# Service startup
log_header "🚀 서비스 시작"

log_step "데이터 디렉토리 생성..."
mkdir -p data
log_ok "데이터 디렉토리 준비 완료"

log_step "Docker Compose로 서비스 시작..."
docker compose up -d --build
log_ok "서비스 시작 완료"

# Wait for service to be ready
log_step "API 서버 초기화 대기 (최대 30초)..."
for i in {1..30}; do
  if curl -sf http://localhost:8000/health 2>/dev/null > /dev/null; then
    log_ok "API 서버 준비 완료"
    break
  fi
  if [ $i -eq 30 ]; then
    log_error "API 서버 시작 타임아웃"
  fi
  echo -n "."
  sleep 1
done
echo ""

# Verification
log_header "✅ 배포 검증"

log_step "API 헬스 체크..."
if curl -sf http://localhost:8000/health 2>/dev/null | python3 -m json.tool > /dev/null 2>&1; then
  log_ok "API 서버 응답 정상"
else
  log_error "API 서버 응답 실패"
fi

log_step "서비스 목록 조회..."
curl -sf http://localhost:8000/v1/services 2>/dev/null | python3 -m json.tool > /dev/null 2>&1 && log_ok "서비스 조회 정상" || log_warn "서비스 조회 실패 (아직 배포된 서비스 없음)"

log_step "클러스터 상태 조회..."
curl -sf http://localhost:8000/v1/cluster/status 2>/dev/null | python3 -m json.tool > /dev/null 2>&1 && log_ok "클러스터 상태 조회 정상" || log_warn "클러스터 상태 조회 실패"

# Summary
log_header "📋 배포 완료 정보"

echo "✨ AI Container Orchestrator 배포가 완료되었습니다!"
echo ""
echo "🌐 접속 정보:"
echo "  • 대시보드: http://localhost:8000/dashboard"
echo "  • API 문서: http://localhost:8000/docs"
echo "  • Traefik: http://localhost:8080"
echo ""
echo "🤖 AI 콘솔 시작:"
echo "  ./scripts/operator.sh console"
echo ""
echo "📦 서비스 관리:"
echo "  # 샘플 서비스 배포"
echo "  ./scripts/operator.sh deploy-all"
echo ""
echo "  # 개별 서비스 배포"
echo "  ./scripts/operator.sh deploy nginx:latest web 2"
echo ""
echo "  # 서비스 스케일링"
echo "  ./scripts/operator.sh scale web 5"
echo ""
echo "🖥️  클러스터 관리:"
echo "  ./scripts/operator.sh cluster status"
echo "  ./scripts/operator.sh cluster nodes"
echo ""
echo "📚 상세 정보:"
echo "  • 빠른 시작: cat QUICKSTART_AI_CONSOLE.md"
echo "  • AI 콘솔 가이드: cat AI_CONSOLE.md"
echo "  • 운영 가이드: cat docs/OPERATIONS.md"
echo "  • 아키텍처: cat docs/ARCHITECTURE.md"
echo ""
echo "🛑 서비스 중지:"
echo "  ./scripts/operator.sh stop"
echo ""

# Optional: Print environment info
if [ -f ".env" ]; then
  echo "⚙️  환경 변수 설정:"
  grep -E "^ORCHESTRATOR_|^OPENAI_" .env || echo "  (환경 변수 미설정)"
  echo ""
fi

log_ok "배포 스크립트 완료!"
