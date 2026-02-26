#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/4] (선택) 가상환경 활성화"
if [ -d ".venv" ]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
else
  echo "  .venv 디렉터리가 없어, 시스템 python을 그대로 사용합니다."
fi

echo "[2/4] 의존성 설치"
pip install -q -r requirements.txt

echo "[3/4] 테스트 실행"
export PYTHONPATH=src
pytest tests/ -v

echo "[4/4] Docker 빌드 및 배포 (docker-compose up -d --build)"
docker network create orch-internal 2>/dev/null || true
docker-compose up -d --build

echo
echo "=== Health 체크 ==="
curl -s http://localhost:8000/health || true
echo

echo "완료: 테스트 + 빌드 + 배포가 끝났습니다."

