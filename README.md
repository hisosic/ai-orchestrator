# AI Container Orchestrator

컨테이너 기반 서비스를 **자연어**로 스케일링·리소스 제어·배포할 수 있는 오케스트레이션 도구입니다.  
오케스트레이터 자체도 **컨테이너**로 실행할 수 있어, 확장과 배포가 쉽습니다.

## 기능

- **자연어 명령**: "nginx를 5개로 스케일해줘", "redis 메모리 512MB 제한", "webapp 배포해줘"
- **스케일링**: 레플리카 수 조절
- **리소스 제어**: 메모리/CPU 제한
- **배포**: Docker 이미지 기반 서비스 배포·업데이트
- **컨테이너 기반 실행**: Docker 이미지로 오케스트레이터 실행

## 운영 담당자용 (한눈에 보기)

서비스 **시작/중지/상태/헬스/로그**를 한 스크립트로 실행할 수 있습니다.

```bash
./scripts/operator.sh start    # 시작
./scripts/operator.sh stop     # 중지
./scripts/operator.sh status   # 상태 확인
./scripts/operator.sh health   # 헬스 체크
./scripts/operator.sh logs     # 로그 보기
```

**상세 운영 가이드**(환경 변수, 트러블슈팅 등): [docs/OPERATIONS.md](docs/OPERATIONS.md)  
**환경 설정 예시**: `.env.example` 참고 후 `.env`로 복사해 사용

---

## 빠른 시작

### 1) 로컬 (Python)

```bash
cd /Users/jinseonglee/work/ai-container
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export PYTHONPATH=src
python run.py
```

### 2) Docker Compose (오케스트레이터를 컨테이너로 실행)

```bash
docker compose build
docker compose up -d
```

API: `http://localhost:8000`  
**대시보드**: `http://localhost:8000/` 또는 `http://localhost:8000/dashboard` — 컨테이너/환경 모니터링 + 자연어 명령 콘솔

### 3) 자연어 명령 실행

```bash
# 배포
curl -X POST http://localhost:8000/v1/command \
  -H "Content-Type: application/json" \
  -d '{"command": "nginx 배포해줘"}'

# 스케일
curl -X POST http://localhost:8000/v1/command \
  -H "Content-Type: application/json" \
  -d '{"command": "nginx를 3개로 스케일해줘"}'

# 리소스 제한
curl -X POST http://localhost:8000/v1/command \
  -H "Content-Type: application/json" \
  -d '{"command": "nginx 메모리 256m"}'

# 서비스 목록
curl http://localhost:8000/v1/services
```

## API

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/` | 대시보드 (HTML) |
| GET | `/dashboard` | 대시보드 (HTML) |
| GET | `/health` | 헬스체크 |
| GET | `/v1/services` | 관리 중인 서비스 목록 |
| GET | `/v1/system` | 시스템/환경 정보 (호스트, Docker) |
| GET | `/v1/containers` | 전체 컨테이너 목록 (쿼리 `?stats=true` 시 CPU/메모리 포함) |
| POST | `/v1/command` | 자연어 명령 실행 (`{"command": "..."}`) |

## 설계

- [docs/DESIGN.md](docs/DESIGN.md) 참고

## 테스트

```bash
export PYTHONPATH=src
pip install -r requirements.txt
pytest tests/ -v
```

### E2E (실제 Docker 연동)

오케스트레이터가 떠 있는 상태에서 (로컬 `python run.py` 또는 `docker-compose up -d`):

```bash
chmod +x scripts/e2e_test.sh
./scripts/e2e_test.sh
```
