# AI Container Orchestrator — Deployment Guide

## 현재 클러스터 구성

| 노드 | IP | 역할 | 상태 |
|------|-----|------|------|
| master | 20.25.0.190 | Master (Control Plane) | healthy |
| node-b | 20.25.0.191 | Worker | healthy |
| node-c | 20.25.0.192 | Worker | healthy |

## 접속 정보

| 서비스 | URL |
|--------|-----|
| 대시보드 | http://20.25.0.190:8000/dashboard |
| API 문서 | http://20.25.0.190:8000/docs |
| Traefik | http://20.25.0.190:8080 |

## API 토큰
```
3e86051f8aad1f200864e2fe34b953f95f51e2ea81c68120565fa95d4b416bb2
```

## SSH 접속
```bash
ssh -i ~/aws-key/loopvm.pem root@20.25.0.190  # master
ssh -i ~/aws-key/loopvm.pem root@20.25.0.191  # node-b
ssh -i ~/aws-key/loopvm.pem root@20.25.0.192  # node-c
```

## 서비스 배포 예시

### 대시보드에서
1. http://20.25.0.190:8000/dashboard 접속
2. "클러스터 배포" 패널에서 이미지 경로 입력 (예: `nginx:alpine`)
3. 레플리카 수, 전략 설정 후 [배포] 클릭

### CLI에서
```bash
# 배포
curl -X POST http://20.25.0.190:8000/v1/cluster/deploy \
  -H "Content-Type: application/json" \
  -d '{"image":"nginx:alpine","name":"web","replicas":3,"strategy":"spread"}'

# 스케일
curl -X POST http://20.25.0.190:8000/v1/cluster/scale \
  -H "Content-Type: application/json" \
  -d '{"service_name":"web","replicas":6}'

# 중지
curl -X POST http://20.25.0.190:8000/v1/cluster/stop \
  -H "Content-Type: application/json" \
  -d '{"service_name":"web"}'
```

### operator.sh
```bash
./scripts/operator.sh cluster status
./scripts/operator.sh cluster nodes
./scripts/operator.sh cluster drain node-b
./scripts/operator.sh migrate <container_id> node-b master
```

## 워커 노드 추가
```bash
./scripts/install-agent.sh <new-ip> <node-name> http://20.25.0.190:8000 <token>
```

## 서비스 URL 접근
배포된 서비스는 해당 노드의 Traefik을 통해 접근:
```
http://<노드IP>/<서비스명>/
```
예: `http://20.25.0.190/web/`, `http://20.25.0.191/web/`

## 파일 위치 (서버)
- 코드: `/opt/ai-orchestrator/`
- 상태 DB: `/data/cluster.db` (Docker volume `orchestrator-data`)
- 로그: `docker logs ai-orchestrator`
