<img width="1470" height="1020" alt="image" src="https://github.com/user-attachments/assets/6a07615e-98e1-401a-bb17-33e32b343b57" />


# AI Container Orchestrator

베어메탈 서버에서 컨테이너를 관리하는 **멀티노드 클러스터 오케스트레이션 플랫폼**입니다.
Go 네이티브 구현으로 경량 고성능 운영이 가능하며, 자연어(한/영) 명령과 AI 어드바이저를 통해 직관적으로 클러스터를 제어할 수 있습니다.

## 주요 기능

- **멀티노드 클러스터**: Master-Worker 아키텍처, 서버 IP 추가만으로 확장
- **이미지 자동 배포**: 이미지 경로 입력 → 자동 Pull → 스케줄링 → 컨테이너 기동
- **드래그 앤 드롭 마이그레이션**: 배치 그리드에서 컨테이너를 다른 노드로 드래그
- **스케줄링 전략**: Spread / Least-Loaded / Binpack 알고리즘 지원
- **실시간 모니터링**: 노드 CPU/메모리/디스크/네트워크, 컨테이너별 자원 사용량
- **자연어 명령**: 한/영 자연어로 배포·스케일·마이그레이션·드레인 제어
- **AI 어드바이저**: Claude API 연동으로 클러스터 최적화 제안
- **Traefik 통합 라우팅**: 모든 서비스가 Master IP를 통해 접근 가능
- **알림 엔진**: CPU/메모리/디스크/노드 상태 자동 모니터링 및 자동 복구(Auto-heal)
- **블록체인 QuickStart**: Goloop(ICON) Validator/Citizen 노드 원클릭 구성
- **SSE 실시간 이벤트**: Server-Sent Events로 대시보드 라이브 업데이트

## 아키텍처

```
Master (Control Plane)                Worker Nodes (Bare-metal)
┌───────────────────────────────┐    ┌──────────────────────┐
│ Go HTTP Server (Chi Router)   │    │ Agent (heartbeat)    │
│ ├─ Web Dashboard              │    │ ├─ Docker Engine     │
│ ├─ REST API (/v1/*)           │ ◄──┤ ├─ Resource Monitor  │
│ ├─ SSE Event Stream           │    │ ├─ Container Export/ │
│ ├─ Scheduler (spread/         │    │ │  Import (migration)│
│ │   least-loaded/binpack)     │──► │ └─ Traefik (LB)     │
│ ├─ Migration Controller       │    └──────────────────────┘
│ ├─ Alert Engine + Auto-heal   │    ┌──────────────────────┐
│ ├─ NL Engine (한/영)           │    │ Agent (heartbeat)    │
│ ├─ AI Advisor (Claude API)    │ ◄──┤ ├─ Docker Engine     │
│ ├─ Service Discovery + DNS    │    │ └─ Traefik (LB)     │
│ ├─ SQLite Cluster State (WAL) │──► └──────────────────────┘
│ └─ Traefik (Ingress)          │
└───────────────────────────────┘
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
# 클러스터 배포
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
| **클러스터 오버뷰** | 노드 헬스 카드 (CPU/MEM/DISK/NET 바), 상태 배지 |
| **서비스 관리** | 서비스 목록, Endpoint 링크, 스케일, 중지 |
| **클러스터 배포** | 이미지 경로 입력 → Pull + 자동 기동 |
| **배치 현황** | 노드별 컨테이너 칩 (드래그로 이동/삭제), 자원 사용량 |
| **명령 콘솔** | 한/영 자연어 명령 실행 |
| **AI 어드바이저** | Claude 기반 클러스터 최적화 채팅 |
| **마이그레이션 현황** | 진행 중인 마이그레이션 프로그레스 바 |
| **알림** | 활성 알림 목록, 확인(Ack) 처리 |

### 컨테이너 이동 (드래그 앤 드롭)

배치 현황에서 컨테이너 칩을 드래그:
- **다른 노드로 드롭** → 이동(기존 이미지 사용, 빠름) 또는 마이그레이션(상태 보존, 느림) 선택
- **삭제 영역으로 드롭** → 컨테이너 단일 삭제 또는 서비스 전체 삭제 선택

## 서비스 접근 (Endpoint)

배포된 서비스는 **Master IP를 통해** 접근:

```
http://<master-ip>/<서비스명>/
```

예: `http://<master-ip>/web/`, `http://<master-ip>/httpbin/`

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
| GET | `/v1/cluster/services` | 서비스 배포 요약 |
| GET | `/v1/cluster/migrations` | 마이그레이션 목록 |

### 노드 관리

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/v1/cluster/nodes` | 노드 목록 |
| POST | `/v1/cluster/nodes` | 노드 추가 |
| DELETE | `/v1/cluster/nodes/{name}` | 노드 삭제 |
| POST | `/v1/cluster/nodes/{name}/cordon` | 스케줄링 차단 |
| POST | `/v1/cluster/nodes/{name}/uncordon` | 스케줄링 재개 |
| POST | `/v1/cluster/nodes/{name}/drain` | 노드 드레인 |
| POST | `/v1/cluster/nodes/provision` | 노드 프로비저닝 |

### 서비스 디스커버리

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/v1/cluster/discovery` | 등록된 서비스 목록 |
| GET | `/v1/cluster/discovery/{service}` | 서비스 엔드포인트 |
| GET | `/v1/services` | 서비스 목록 (Endpoint 포함) |

### 알림 & 자동 복구

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/v1/cluster/alerts` | 미확인 알림 목록 |
| POST | `/v1/cluster/alerts/{id}/ack` | 알림 확인 |
| GET | `/v1/cluster/autoheal` | 자동 복구 상태 |
| POST | `/v1/cluster/autoheal/toggle` | 자동 복구 토글 |

### AI & 자연어

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/v1/command` | 자연어 명령 실행 (한/영) |
| POST | `/v1/ai/chat` | AI 어드바이저 채팅 |
| GET | `/v1/ai/advisor` | AI 추천 조회 |
| GET | `/v1/ai/status` | AI 엔진 상태 |

### 기본 API

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/dashboard` | 웹 대시보드 |
| GET | `/health` | 헬스체크 |
| GET | `/v1/system` | 시스템 정보 |
| GET | `/v1/stream` | SSE 실시간 이벤트 |
| GET | `/v1/containers` | 컨테이너 목록 |
| GET | `/v1/images` | 이미지 목록 |
| POST | `/v1/images/pull` | 이미지 Pull |

### 블록체인 QuickStart

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/v1/quickstart/blockchain` | 블록체인 노드 배포 |
| POST | `/v1/quickstart/blockchain/distributed` | 분산 블록체인 배포 |
| GET | `/v1/quickstart/blockchain/status` | 블록체인 상태 |

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

./scripts/operator.sh deploy <image> [name] [replicas]
./scripts/operator.sh scale <name> <replicas>
./scripts/operator.sh migrate <container> <source> <dest>
./scripts/operator.sh alerts             # 활성 알림
./scripts/operator.sh console            # 자연어 명령 콘솔
```

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `ORCHESTRATOR_ROLE` | `master` | 노드 역할 (`master` / `worker`) |
| `ORCHESTRATOR_NODE_NAME` | 호스트명 | 노드 이름 |
| `ORCHESTRATOR_ADVERTISE_ADDR` | `localhost` | 노드 간 통신 주소 |
| `ORCHESTRATOR_MASTER_URL` | - | Worker용 마스터 URL |
| `ORCHESTRATOR_API_TOKEN` | - | API 인증 토큰 (Bearer) |
| `ORCHESTRATOR_STATE_DIR` | `/data` | 상태 저장 디렉터리 (SQLite) |
| `ORCHESTRATOR_HEARTBEAT_INTERVAL` | `10` | 하트비트 간격 (초) |
| `ANTHROPIC_API_KEY` | - | AI 어드바이저용 Claude API 키 |
| `PORT` | `8000` | HTTP 서버 포트 |

## 프로젝트 구조

```
├── cmd/orchestrator/
│   └── main.go                # 엔트리포인트
├── internal/
│   ├── server/server.go       # HTTP 서버 & API 라우팅 (Chi)
│   ├── agent/agent.go         # Worker 에이전트 (하트비트, 자원 수집)
│   ├── runtime/runtime.go     # Docker 런타임 어댑터
│   ├── clusterstate/          # SQLite 클러스터 상태 (WAL)
│   ├── scheduler/             # 스케줄러 (spread/least-loaded/binpack)
│   ├── migrate/               # 마이그레이션 컨트롤러
│   ├── nlengine/              # 자연어 파서 (한/영)
│   ├── alerts/                # 알림 엔진
│   ├── discovery/             # 서비스 디스커버리 + DNS
│   ├── monitoring/            # 컨테이너 모니터링
│   ├── models/                # 데이터 모델
│   └── state/                 # 로컬 상태 (JSON)
├── src/orchestrator/          # Python 참조 구현 (FastAPI)
├── static/
│   └── dashboard.html         # 웹 대시보드
├── services/
│   ├── blockchain/            # Goloop 블록체인 QuickStart
│   ├── was/                   # 샘플 WAS 서비스
│   └── web-check/             # 샘플 웹 체크 서비스
├── scripts/
│   ├── operator.sh            # 운영 CLI
│   ├── install-agent.sh       # Worker 원격 설치
│   └── build_and_deploy.sh    # 빌드 & 배포
├── tests/                     # 테스트 (pytest)
├── docs/                      # 문서
├── Dockerfile                 # Go 프로덕션 이미지
├── docker-compose.yml         # Master 구성
└── docker-compose.worker.yml  # Worker 구성
```

## 기술 스택

| 분류 | 기술 |
|------|------|
| **언어** | Go 1.25 |
| **HTTP** | Chi Router (go-chi/chi/v5) |
| **컨테이너** | Docker SDK (docker/docker v27.5.1) |
| **상태 관리** | SQLite (WAL 모드) + JSON 파일 |
| **시스템 메트릭** | gopsutil v3 |
| **AI** | Anthropic Claude API |
| **리버스 프록시** | Traefik |
| **DNS** | dnsmasq (*.svc.local) |

## 테스트

```bash
# Python 테스트
export PYTHONPATH=src
pytest tests/ -v

# Go 테스트
go test ./internal/... -v
```

## 라이선스

MIT
