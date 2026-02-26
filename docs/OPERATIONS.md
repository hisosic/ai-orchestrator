# 운영 가이드 (서비스 운영 담당자용)

AI Container Orchestrator를 **쉽고 직관적으로** 운영하기 위한 안내입니다.

---

## 1. 한눈에 보기

| 하고 싶은 일 | 방법 |
|-------------|------|
| **서비스 시작** | `./scripts/operator.sh start` 또는 `docker compose up -d` |
| **서비스 중지** | `./scripts/operator.sh stop` |
| **상태 확인** | `./scripts/operator.sh status` 또는 브라우저에서 [대시보드](http://localhost:8000/dashboard) |
| **헬스 체크** | `./scripts/operator.sh health` 또는 `curl http://localhost:8000/health` |
| **로그 보기** | `./scripts/operator.sh logs` 또는 `docker compose logs -f orchestrator` |
| **환경 설정** | 프로젝트 루트의 `.env` 수정 후 `docker compose up -d` (또는 [환경 변수](#2-환경-변수) 참고) |

---

## 2. 환경 변수

| 변수명 | 설명 | 기본값 |
|--------|------|--------|
| `ORCHESTRATOR_STATE_DIR` | 서비스 상태 파일(`services.json`)을 저장할 디렉터리 | `/data` |

Docker Compose 사용 시 기본값 `/data`가 볼륨(`orchestrator-data`)에 매핑되어 **재시작 후에도 상태가 유지**됩니다.  
다른 경로를 쓰려면 `docker-compose.yml`의 `environment`와 `volumes`를 함께 수정하세요.

자세한 예시는 프로젝트 루트의 **`.env.example`**을 참고하세요.

---

## 3. 시작 / 중지 / 재시작

### 권장: 운영 스크립트 사용

```bash
# 시작 (이미지 빌드 포함)
./scripts/operator.sh start

# 중지
./scripts/operator.sh stop

# 재시작
./scripts/operator.sh restart
```

### Docker Compose 직접 사용

```bash
# 시작
docker compose up -d

# 빌드 후 시작
docker compose up -d --build

# 중지
docker compose down

# 재시작
docker compose restart orchestrator
```

---

## 4. 상태 확인

- **스크립트**: `./scripts/operator.sh status`  
  - Compose 서비스 상태와 컨테이너 요약을 출력합니다.
- **대시보드**: 브라우저에서 `http://localhost:8000` 또는 `http://localhost:8000/dashboard`  
  - 시스템/환경, 관리 서비스, 컨테이너 목록, 자연어 명령 콘솔을 한 화면에서 확인할 수 있습니다.
- **API**:
  - 헬스: `GET /health` → `{"status":"ok","version":"..."}`
  - 서비스 목록: `GET /v1/services`
  - 시스템 정보: `GET /v1/system`

---

## 5. 헬스 체크

- **스크립트**: `./scripts/operator.sh health`
- **직접 호출**: `curl -s http://localhost:8000/health`

정상이면 `{"status":"ok","version":"..."}` 형태로 응답합니다.  
Docker Compose 헬스체크도 이 엔드포인트를 사용합니다.

---

## 6. 로그 보기

- **스크립트**: `./scripts/operator.sh logs` (실시간 tail)
- **직접**: `docker compose logs -f orchestrator`

---

## 7. 자주 하는 작업

- **배포/스케일/리소스 제한**: 대시보드의 "명령 콘솔"에서 자연어로 입력하거나, API `POST /v1/command` 사용.
- **관리 서비스 목록**: 대시보드 "관리 서비스" 패널 또는 `GET /v1/services`.
- **전체 컨테이너/리소스**: 대시보드 "컨테이너 목록" 또는 `GET /v1/containers?stats=true`.

---

## 8. 문제 해결 (트러블슈팅)

| 증상 | 확인할 것 |
|------|-----------|
| 서비스가 기동하지 않음 | `docker compose ps`, `docker compose logs orchestrator`로 오류 메시지 확인. 포트 8000 사용 여부 확인. |
| `/health` 응답 없음 | 컨테이너가 실행 중인지, 방화벽/보안 그룹에서 8000 허용 여부 확인. |
| "Docker 소켓" 관련 오류 | 오케스트레이터 컨테이너에 `/var/run/docker.sock`이 마운트되어 있는지 확인 (`docker-compose.yml`의 `volumes`). |
| 상태가 재시작 후 사라짐 | `ORCHESTRATOR_STATE_DIR`이 볼륨에 매핑된 경로인지 확인 (기본: `/data` ↔ `orchestrator-data`). |

추가 문의는 프로젝트 담당자에게 연락하세요.
