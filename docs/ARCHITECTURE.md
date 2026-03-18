# AI Container Orchestrator — Architecture

## Overview

베어메탈 서버에 컨테이너를 관리하는 멀티노드 오케스트레이션 플랫폼.
관리자 화면에서 서버 IP 추가로 클러스터 확장, 컨테이너 자유 마이그레이션 지원.

## Cluster Architecture

```
Master (Control Plane)              Worker Nodes (Bare-metal)
┌──────────────────────────┐       ┌─────────────────┐
│ FastAPI + Dashboard      │  HTTP │ Agent (heartbeat)│
│ Scheduler (spread/       │◄────►│ Docker Engine    │
│   least-loaded/binpack)  │       │ Traefik (LB)    │
│ Migration Controller     │       └─────────────────┘
│ Alert Engine             │       ┌─────────────────┐
│ SQLite Cluster State     │◄────►│ Agent (heartbeat)│
│ Service Discovery        │       │ Docker Engine    │
└──────────────────────────┘       └─────────────────┘
```

### Communication
- **Master ↔ Worker**: HTTP REST (heartbeat 10초 간격)
- **Scheduling**: Master가 결정, Worker에 proxy 실행
- **Migration**: docker commit + save → transfer → load + run

## Module Structure

```
src/orchestrator/
├── main.py              # FastAPI app (Master/Worker role split)
├── cluster_models.py    # Pydantic models (Node, Heartbeat, Migration, Alert)
├── cluster_state.py     # SQLite state manager (WAL mode)
├── scheduler.py         # Multi-node scheduler (3 strategies)
├── agent.py             # Worker agent (heartbeat, export/import)
├── migrate.py           # Migration controller (5-step workflow)
├── alerts.py            # Alert engine (CPU/MEM/Disk/Node monitoring)
├── discovery.py         # Service discovery registry
├── runtime.py           # Docker runtime adapter (auto-pull)
├── nl_engine.py         # NL parser (Korean + English, cluster commands)
├── models.py            # API models (Intent, Request, Response)
├── monitoring.py        # Container stats, system info
├── state.py             # File-based local state (JSON)
└── static/
    └── dashboard.html   # Web dashboard (cluster UI)
```

## Key APIs

### Cluster Management
| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/cluster/deploy` | Image pull + schedule + run across nodes |
| POST | `/v1/cluster/scale` | Scale service across cluster |
| POST | `/v1/cluster/stop` | Stop service on all nodes |
| POST | `/v1/cluster/migrate` | Migrate container between nodes |
| GET | `/v1/cluster/status` | Full cluster overview |
| GET | `/v1/cluster/placements` | Container placement map |
| POST | `/v1/cluster/nodes` | Add/register node |
| POST | `/v1/cluster/nodes/{name}/drain` | Evacuate node |

### Service URL Routing (Traefik)
- **Path**: `http://<node-ip>/<service-name>/`
- **Host**: `http://<service-name>.local`

## Deployment

### Master
```bash
ORCHESTRATOR_ROLE=master docker compose up -d --build
```

### Worker
```bash
./scripts/install-agent.sh <ip> <name> <master-url> <token>
```

### CLI
```bash
./scripts/operator.sh cluster status
./scripts/operator.sh cluster add <name> <ip:port> [token]
./scripts/operator.sh cluster drain <name>
./scripts/operator.sh migrate <container> <src> <dest>
```

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Architecture | Master-Worker centralized | Simple, matches existing hub-spoke design |
| Communication | HTTP REST | Entire stack already HTTP-based |
| State (Master) | SQLite WAL | Concurrent-safe, no external dependency |
| State (Worker) | JSON files | Low concurrency, simple |
| Scheduler | Spread default | Anti-affinity for HA |
| Migration | docker commit + save/load | Works without private registry |
| System filter | SYSTEM_SERVICES set | Clean operator UX |
