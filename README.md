# AI Container Orchestrator

베어메탈 서버에서 컨테이너를 관리하는 **멀티노드 클러스터 오케스트레이션 플랫폼**입니다.
관리자 대시보드에서 서버 IP를 추가하여 클러스터를 확장하고, 컨테이너를 노드 간 자유롭게 이동할 수 있습니다.

## 주요 기능

- **멀티노드 클러스터**: Master-Worker 아키텍처, 서버 IP 추가만으로 확장
- **이미지 자동 배포**: 이미지 경로 입력 → 자동 Pull → 스케줄링 → 컨테이너 기동
- **드래그 앤 드롭 이동/마이그레이션**: 배치 그리드에서 컨테이너를 다른 노드로 드래그
- **클러스터 스케일링**: 서비스 단위 레플리카 수 조정 (전체 노드에 분산)
- **실시간 모니터링**: 노드 CPU/메모리/네트워크, 컨테이너별 자원 사용량
- **자연어 명령**: 한/영 자연어로 배포·스케일·마이그레이션·드레인 제어
- **Traefik 통합 라우팅**: 모든 서비스가 Master IP를 통해 접근 가능
- **알림 엔진**: CPU/메모리/디스크/노드 상태 자동 모니터링

## 아키텍처

```
Master (Control Plane)              Worker Nodes (Bare-metal)
┌──────────────────────────┐       ┌─────────────────┐
│ FastAPI + Web Dashboard  │  HTTP │ Agent (heartbeat)│
│ Scheduler (spread/       │◄────►│ Docker Engine    │
│   least-loaded/binpack)  │       │ Traefik (LB)    │
│ Migration Controller     │       └─────────────────┘
│ Alert Engine             │       ┌─────────────────┐
│ SQLite Cluster State     │◄────►│ Agent (heartbeat)│
│ Traefik (Ingress)        │       │ Docker Engine    │
└──────────────────────────┘       └─────────────────┘
```

## 빠른 시작

### 1. Master 노드 설치

```bash
cp .env.example .env
# .env 에서 ORCHESTRATOR_ROLE=master 설정

docker network create orch-internal
docker compose up -d --build
```

대시보드: `http://<master-ip>:8000/dashboard`

### 2. Worker 노드 추가

```bash
# 원격 서버에 자동 설치
./scripts/install-agent.sh <worker-ip> <node-name> http://<master-ip>:8000 <api-token>
```

또는 대시보드에서 **[노드 추가]** 버튼으로 등록

### 3. 서비스 배포

**대시보드에서:**
1. "클러스터 배포" 패널에서 이미지 경로 입력 (예: `nginx:alpine`)
2. 레플리카 수, 배치 전략 선택 후 **[배포]** 클릭

**CLI에서:**
```bash
# 클러스터 배포 (이미지 자동 Pull + 스케줄링)
curl -X POST http://<master-ip>:8000/v1/cluster/deploy \
  -H "Content-Type: application/json" \
  -d '{"image":"nginx:alpine","name":"web","replicas":3,"strategy":"spread"}'

# 스케일
curl -X POST http://<master-ip>:8000/v1/cluster/scale \
  -H "Content-Type: application/json" \
  -d '{"service_name":"web","replicas":6}'

# 중지
curl -X POST http://<master-ip>:8000/v1/cluster/stop \
  -H "Content-Type: application/json" \
  -d '{"service_name":"web"}'
```

**자연어 명령:**
```bash
curl -X POST http://<master-ip>:8000/v1/command \
  -H "Content-Type: application/json" \
  -d '{"command": "nginx를 3개로 스케일해줘"}'
```

## 대시보드

`http://<master-ip>:8000/dashboard`

| 패널 | 기능 |
|------|------|
| **클러스터 오버뷰** | 노드 헬스 카드 (CPU/MEM/NET 바), 상태 배지 |
| **서비스 관리** | 서비스 목록, Endpoint 링크, 스케일, 중지 |
| **클러스터 배포** | 이미지 경로 입력 → Pull + 자동 기동 |
| **배치 현황** | 노드별 컨테이너 칩 (드래그로 이동/삭제), 자원 사용량 |
| **명령 콘솔** | 한/영 자연어 명령 실행 |
| **마이그레이션 현황** | 진행 중인 마이그레이션 프로그레스 바 |

### 컨테이너 이동 (드래그 앤 드롭)

배치 현황에서 컨테이너 칩을 드래그:
- **다른 노드로 드롭** → 이동(기존 이미지 사용, 빠름) 또는 마이그레이션(상태 보존, 느림) 선택
- **삭제 영역으로 드롭** → 컨테이너 단일 삭제 또는 서비스 전체 삭제 선택

## 서비스 접근 (Endpoint)

배포된 서비스는 **Master IP를 통해** 접근:

```
http://<master-ip>/<서비스명>/
```

예: `http://20.25.0.190/web/`, `http://20.25.0.190/httpbin/`

포트 매핑 서비스: `http://<master-ip>:<port>/`

## 운영 CLI

```bash
./scripts/operator.sh start              # 서비스 기동
./scripts/operator.sh stop               # 서비스 중지
./scripts/operator.sh status             # 상태 확인
./scripts/operator.sh health             # 헬스 체크
./scripts/operator.sh logs               # 로그 보기

./scripts/operator.sh cluster status     # 클러스터 전체 상태
./scripts/operator.sh cluster nodes      # 노드 목록
./scripts/operator.sh cluster add <name> <ip:port> [token]
./scripts/operator.sh cluster drain <name>

./scripts/operator.sh migrate <container> <source> <dest>
./scripts/operator.sh alerts             # 활성 알림
```

## API

### 클러스터 관리

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/v1/cluster/deploy` | 이미지 Pull + 스케줄링 + 기동 |
| POST | `/v1/cluster/scale` | 서비스 스케일 (전체 노드) |
| POST | `/v1/cluster/stop` | 서비스 전체 중지 |
| POST | `/v1/cluster/move` | 컨테이너 이동 (기존 이미지) |
| POST | `/v1/cluster/migrate` | 컨테이너 마이그레이션 (상태 보존) |
| GET | `/v1/cluster/status` | 클러스터 전체 상태 |
| GET | `/v1/cluster/placements` | 컨테이너 배치 현황 |
| GET | `/v1/cluster/nodes` | 노드 목록 |
| POST | `/v1/cluster/nodes` | 노드 추가 |
| POST | `/v1/cluster/nodes/{name}/drain` | 노드 드레인 |

### 기본 API

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/dashboard` | 웹 대시보드 |
| GET | `/health` | 헬스체크 |
| GET | `/v1/services` | 서비스 목록 (Endpoint 포함) |
| GET | `/v1/system` | 시스템 정보 |
| POST | `/v1/command` | 자연어 명령 실행 |
| POST | `/v1/images/pull` | 이미지 Pull |
| DELETE | `/v1/images/{id}` | 이미지 삭제 (전체 클러스터) |

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `ORCHESTRATOR_ROLE` | `master` | 노드 역할 (`master` / `worker`) |
| `ORCHESTRATOR_NODE_NAME` | 호스트명 | 노드 이름 |
| `ORCHESTRATOR_MASTER_URL` | - | Worker용 마스터 URL |
| `ORCHESTRATOR_API_TOKEN` | - | API 인증 토큰 |
| `ORCHESTRATOR_STATE_DIR` | `/data` | 상태 저장 디렉터리 |
| `ORCHESTRATOR_HEARTBEAT_INTERVAL` | `10` | 하트비트 간격 (초) |

## 프로젝트 구조

```
src/orchestrator/
├── main.py              # FastAPI 앱 (Master/Worker)
├── cluster_models.py    # 클러스터 데이터 모델
├── cluster_state.py     # SQLite 상태 관리 (WAL)
├── scheduler.py         # 스케줄러 (spread/least-loaded/binpack)
├── agent.py             # Worker 에이전트 (하트비트, 자원 수집)
├── migrate.py           # 마이그레이션 컨트롤러
├── alerts.py            # 알림 엔진
├── discovery.py         # 서비스 디스커버리
├── runtime.py           # Docker 런타임 어댑터
├── nl_engine.py         # 자연어 파서 (한/영)
├── models.py            # API 모델
├── monitoring.py        # 컨테이너 모니터링
├── state.py             # 로컬 상태 (JSON)
└── static/
    └── dashboard.html   # 웹 대시보드
```

## 문서

- [아키텍처](docs/ARCHITECTURE.md)
- [배포 가이드](docs/DEPLOYMENT.md)
- [운영 가이드](docs/OPERATIONS.md)
- [설계 문서](docs/DESIGN.md)

## 테스트

```bash
export PYTHONPATH=src
pytest tests/ -v
```

## 라이선스

MIT
