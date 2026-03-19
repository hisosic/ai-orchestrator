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

## 블록체인 QuickStart

블록체인(Goloop) 네트워크를 자동 구성합니다. Validator + Citizen 노드를 한 번에 기동합니다.

**API 호출:**
```bash
curl -X POST http://<master-ip>:8000/v1/quickstart/blockchain \
  -H "Content-Type: application/json" \
  -d '{"validators":4,"citizens":0,"channel":"seoul"}'
```

**상태 확인:**
```bash
curl http://<master-ip>:8000/v1/quickstart/blockchain/status
```

### 기동 단계별 상세

#### Phase 0: 이전 배포 정리

기존 블록체인 컨테이너와 데이터를 모두 제거합니다.

<details>
<summary>상세 명령어</summary>

```bash
# 동일 채널의 기존 컨테이너 제거
docker ps -a --filter "label=blockchain.channel=${CHANNEL}" -q | xargs -r docker rm -f

# 모든 블록체인 컨테이너 제거
docker ps -a --filter "label=blockchain.channel" -q | xargs -r docker rm -f

# 작업 디렉터리 초기화
rm -rf ${WORK_DIR}/*
mkdir -p ${WORK_DIR}
chmod 777 ${WORK_DIR}
```
</details>

#### Phase 1: 키스토어 생성

각 노드(Validator + Citizen)에 대해 개별 키스토어를 생성합니다.

<details>
<summary>상세 명령어</summary>

```bash
# 각 노드별 디렉터리 생성
mkdir -p ${WORK_DIR}/node-${i}/conf ${WORK_DIR}/node-${i}/data

# 키스토어 생성 (goloop ks gen)
docker run --rm --entrypoint "" \
    -v "${CONF_DIR}:/work" -w /work \
    "${IMAGE}" \
    goloop ks gen --out /work/keystore.json --password "${KS_PW}"

# 키 비밀번호 파일 생성
echo -n "${KS_PW}" > ${CONF_DIR}/keysecret

# 주소 추출
python3 -c "import json; print(json.load(open('${CONF_DIR}/keystore.json'))['address'])"
```
</details>

#### Phase 2: 제네시스 생성

Validator 주소 목록으로 genesis.json을 생성합니다.

<details>
<summary>상세 명령어</summary>

```bash
# 제네시스 파일 생성 (goloop gn gen)
docker run --rm --entrypoint "" \
    -v "${GENESIS_DIR}:/work" -w /work \
    "${IMAGE}" \
    goloop gn gen \
    --out /work/genesis.json \
    --god "${GOD_ADDR}" \
    --config "revision=0x8,minimizeBlockGen=0x1" \
    ${VALIDATOR_ADDR_0} ${VALIDATOR_ADDR_1} ${VALIDATOR_ADDR_2} ${VALIDATOR_ADDR_3}

# GOD_ADDR = 첫 번째 Validator 주소
# Validator 주소 목록이 순서대로 전달됨
```
</details>

#### Phase 3: 제네시스 스토리지 (gs.zip)

genesis.json으로부터 gs.zip을 생성하고 모든 노드에 배포합니다.

<details>
<summary>상세 명령어</summary>

```bash
# gs.zip 생성 (goloop gs gen)
docker run --rm --entrypoint "" \
    -v "${GENESIS_DIR}:/work" -w /work \
    "${IMAGE}" \
    goloop gs gen --input /work/genesis.json --out /work/gs.zip

# 모든 노드에 gs.zip 복사
for i in $(seq 1 $((TOTAL - 1))); do
    cp "${GENESIS_DIR}/gs.zip" "${WORK_DIR}/node-${i}/conf/gs.zip"
done
```
</details>

#### Phase 4: 라이선스 생성

issuer 키스토어로 전체 노드에 대한 무기한 라이선스를 발급합니다.

<details>
<summary>상세 명령어</summary>

```bash
# 라이선스 생성 (goloop lc gen)
docker run --rm --entrypoint "" \
    -v "${BLOCKCHAIN_SRC}:/issuer:ro" \
    -v "${WORK_DIR}:/work" -w /work \
    "${IMAGE}" \
    goloop lc gen \
    --keystore /issuer/issuer.json \
    --password "${ISSUER_PW}" \
    --out /work/license.json \
    --duration infinite \
    --subject "${CHANNEL}" \
    ${ALL_NODE_ADDRESSES}

# 모든 노드에 라이선스 배포
for i in $(seq 0 $((TOTAL - 1))); do
    cp "${WORK_DIR}/license.json" "${WORK_DIR}/node-${i}/conf/license.json"
done
```
</details>

#### Phase 5: 컨테이너 기동

각 노드를 Docker 컨테이너로 실행합니다. Seed 노드는 순환 구조로 연결됩니다.

<details>
<summary>상세 명령어</summary>

```bash
# goloop.env 환경변수 파일 (각 노드별 생성)
cat > "${NODE_DIR}/goloop.env" <<EOF
GOLOOP_NODE_DIR=/goloop/data
GOLOOP_ENGINES=python
GOLOOP_P2P=${CONTAINER_NAME}:8080
GOLOOP_P2P_LISTEN=:8080
GOLOOP_RPC_ADDR=:9080
GOLOOP_RPC_DUMP=false
GOLOOP_KEY_STORE=/goloop/conf/keystore.json
GOLOOP_KEY_SECRET=/goloop/conf/keysecret
GOLOOP_LICENSE_FILE=/goloop/conf/license.json
GOLOOP_CONSOLE_LEVEL=warn
GOLOOP_LOG_LEVEL=${LOG_LEVEL}
GOLOOP_LOG_WRITER_FILENAME=/goloop/data/log/goloop.log
GOLOOP_LOG_WRITER_COMPRESS=true
GOLOOP_LOG_WRITER_MAXSIZE=100
EOF

# 컨테이너 실행
docker run -d \
    --name "blockchain-${CHANNEL}-${i}" \
    --network "${NETWORK}" \
    --env-file "${NODE_DIR}/goloop.env" \
    -v "${NODE_DIR}/conf:/goloop/conf" \
    -v "${NODE_DIR}/data:/goloop/data" \
    -p "${P2P_PORT}:8080" \
    -p "${RPC_PORT}:9080" \
    --ulimit nofile=98304:98304 \
    -l "blockchain.role=${ROLE}" \
    -l "blockchain.channel=${CHANNEL}" \
    -l "blockchain.index=${i}" \
    -l "blockchain.p2p_port=${P2P_PORT}" \
    -l "blockchain.rpc_port=${RPC_PORT}" \
    "${IMAGE}"

# Seed 노드 연결 구조:
#   Validator: 순환 연결 (node-0→node-1→node-2→...→node-0)
#   Citizen:   첫 1~2개 Validator에 연결
```
</details>

#### Phase 6: 체인 조인 및 시작

각 노드에서 체인에 조인하고 합의를 시작합니다.

<details>
<summary>상세 명령어</summary>

```bash
# 컨테이너 초기화 대기 (8초)
sleep 8

# Validator 노드 조인
docker exec "blockchain-${CHANNEL}-${i}" \
    goloop chain join \
    --genesis /goloop/conf/gs.zip \
    --seed "blockchain-${CHANNEL}-${SEED_1}:8080,blockchain-${CHANNEL}-${SEED_2}:8080" \
    --channel ${CHANNEL}

# Citizen 노드 조인 (--role 0 플래그 추가)
docker exec "blockchain-${CHANNEL}-${i}" \
    goloop chain join \
    --genesis /goloop/conf/gs.zip \
    --seed "blockchain-${CHANNEL}-0:8080,blockchain-${CHANNEL}-1:8080" \
    --channel ${CHANNEL} \
    --role 0

# 체인 시작
docker exec "blockchain-${CHANNEL}-${i}" \
    goloop chain start "${CHANNEL}"
```
</details>

#### Phase 7: 검증

모든 노드의 체인 상태를 확인합니다.

<details>
<summary>상세 명령어</summary>

```bash
# 안정화 대기 (3초)
sleep 3

# 각 노드 체인 상태 확인
for i in $(seq 0 $((TOTAL - 1))); do
    docker exec "blockchain-${CHANNEL}-${i}" goloop chain ls
done

# RPC로 블록 높이 확인
curl -s http://localhost:${RPC_PORT}/api/v3/${CHANNEL} \
    -d '{"jsonrpc":"2.0","method":"icx_getLastBlock","id":1}'
```
</details>

### 설정 파라미터

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `validators` | `4` | Validator 노드 수 (1~20) |
| `citizens` | `0` | Citizen 노드 수 |
| `channel` | `seoul` | 블록체인 채널명 |
| `image` | `goloop:v1.2.5-seoul-test` | 컨테이너 이미지 |
| `p2p_port` | `7100` | P2P 베이스 포트 |
| `rpc_port` | `9100` | RPC 베이스 포트 |
| `log_level` | `trace` | 로그 레벨 |
| `network` | `orch-internal` | Docker 네트워크 |

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
