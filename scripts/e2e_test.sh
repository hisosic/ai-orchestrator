#!/usr/bin/env bash
# E2E test: requires orchestrator API at http://localhost:8000 and Docker.
# Run: docker-compose up -d  (or python run.py with PYTHONPATH=src)
# Then: ./scripts/e2e_test.sh

set -e
BASE="${BASE_URL:-http://localhost:8000}"

echo "=== Health ==="
curl -s "$BASE/health" | head -1
echo ""

echo "=== Deploy nginx (natural language) ==="
curl -s -X POST "$BASE/v1/command" -H "Content-Type: application/json" \
  -d '{"command": "nginx 배포해줘"}' | python3 -m json.tool
echo ""

echo "=== Scale nginx to 2 ==="
curl -s -X POST "$BASE/v1/command" -H "Content-Type: application/json" \
  -d '{"command": "nginx를 2개로 스케일해줘"}' | python3 -m json.tool
echo ""

echo "=== Set memory limit (natural language) ==="
curl -s -X POST "$BASE/v1/command" -H "Content-Type: application/json" \
  -d '{"command": "nginx 메모리 256m"}' | python3 -m json.tool
echo ""

echo "=== List services ==="
curl -s "$BASE/v1/services" | python3 -m json.tool
echo ""

echo "=== Dry run: scale to 3 ==="
curl -s -X POST "$BASE/v1/command" -H "Content-Type: application/json" \
  -d '{"command": "nginx를 3개로 스케일해줘", "dry_run": true}' | python3 -m json.tool
echo ""

echo "E2E steps completed. Check responses above for success: true."
