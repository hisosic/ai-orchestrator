"""FastAPI application for AI Container Orchestrator.

Supports two roles:
- master: Full API including cluster management, scheduling, migration
- worker: Local container management + agent heartbeat to master
"""
import os
import threading
import time
import base64
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse

from . import __version__
from .models import (
    ActionExecuteRequest,
    CommandRequest,
    CommandResponse,
    RunContainerRequest,
    ScaleServiceRequest,
    ServiceInfo,
)
from . import state
from .nl_engine import parse
from .runtime import (
    execute_intent,
    execute_scale,
    reconcile_replicas,
    pull_image,
    remove_container_by_id,
    remove_image_by_id,
    run_container,
    stop_container_by_id,
    inspect_container_by_id,
    _docker_client,
)
from . import monitoring

# Role: "master" or "worker"
ORCHESTRATOR_ROLE = os.environ.get("ORCHESTRATOR_ROLE", "master").lower()

# System containers to hide from user-facing service/placement lists
SYSTEM_SERVICES = {
    "ai-orchestrator", "ai-orchestrator-worker", "orch-traefik", "orch-dns",
    "zbx-agent", "zabbix-agent", "zabbix-agent2",
}

# Cluster management (only master)
_cluster_state = None
_scheduler = None
_migration_controller = None
_alert_engine = None
_service_registry = None
_worker_agent = None


def _init_cluster():
    """Initialize cluster components based on role."""
    global _cluster_state, _scheduler, _migration_controller, _alert_engine, _service_registry, _worker_agent

    if ORCHESTRATOR_ROLE == "master":
        from .cluster_state import ClusterStateManager
        from .scheduler import Scheduler
        from .migrate import MigrationController
        from .alerts import AlertEngine
        from .discovery import ServiceRegistry

        _cluster_state = ClusterStateManager()
        _scheduler = Scheduler(_cluster_state)
        _migration_controller = MigrationController(_cluster_state)
        _alert_engine = AlertEngine(_cluster_state)
        _service_registry = ServiceRegistry(_cluster_state)

        # Start alert engine
        _alert_engine.start()

    if ORCHESTRATOR_ROLE == "worker":
        from .agent import WorkerAgent
        _worker_agent = WorkerAgent()
        _worker_agent.start()


def _node_by_name(name: str) -> Optional[dict]:
    for n in state.list_nodes():
        if (n.get("name") or "") == name:
            return n
    return None


def _normalize_base_url(url: str) -> str:
    return (url or "").rstrip("/")


ALLOWED_PROXY_PREFIXES = (
    "/v1/system",
    "/v1/services",
    "/v1/containers",
    "/v1/images",
    "/v1/command",
    "/v1/action",
    "/v1/services/scale",
    "/v1/containers/run",
    "/v1/agent/",
)

RECONCILE_INTERVAL_SEC = 15


def _reconcile_loop() -> None:
    while True:
        try:
            reconcile_replicas()
        except Exception:
            pass
        time.sleep(RECONCILE_INTERVAL_SEC)


def _cluster_health_loop() -> None:
    """Periodically check node health and update master's own resources."""
    while True:
        try:
            if _cluster_state:
                _cluster_state.check_node_health()
                # Master self-heartbeat: update own resources and status
                _master_self_heartbeat()
        except Exception:
            pass
        time.sleep(10)


def _master_self_heartbeat() -> None:
    """Update master node's own resources and heartbeat timestamp."""
    if not _cluster_state:
        return
    try:
        from .agent import WorkerAgent
        agent = WorkerAgent()
        resources_dict = agent.get_node_resources()
        containers_list = agent.get_managed_containers()

        from .cluster_models import NodeResources, ContainerPlacement
        resources = NodeResources(**resources_dict)
        containers = [ContainerPlacement(**c) for c in containers_list]

        node_name = os.environ.get("ORCHESTRATOR_NODE_NAME", "master")
        _cluster_state.process_heartbeat(node_name, resources, containers)
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_cluster()

    t = threading.Thread(target=_reconcile_loop, daemon=True)
    t.start()

    if ORCHESTRATOR_ROLE == "master":
        t2 = threading.Thread(target=_cluster_health_loop, daemon=True)
        t2.start()

    yield

    # Cleanup
    if _alert_engine:
        _alert_engine.stop()
    if _worker_agent:
        _worker_agent.stop()


app = FastAPI(
    title="AI Container Orchestrator",
    description="Bare-metal container orchestration with multi-node cluster management",
    version=__version__,
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_TOKEN_ENV = "ORCHESTRATOR_API_TOKEN"


@app.middleware("http")
async def bearer_token_auth(request: Request, call_next):
    """Optional Bearer token auth for /v1/* endpoints. Disabled if env not set."""
    token = (os.environ.get(API_TOKEN_ENV) or "").strip()
    path = request.url.path or ""
    method = request.method.upper()
    if token and path.startswith("/v1/") and not path.startswith("/v1/cluster/") and not path.startswith("/v1/agent/"):
        # Allow GET on read endpoints without token (dashboard)
        if method == "GET" and (
            path in ("/v1/system", "/v1/services", "/v1/containers", "/v1/images")
            or path.startswith("/v1/containers")
        ):
            return await call_next(request)
        # Allow POST/DELETE from dashboard on management endpoints without token
        if path.startswith("/v1/services/") or path.startswith("/v1/containers/") or path.startswith("/v1/images/") or path in ("/v1/command", "/v1/action"):
            auth = request.headers.get("authorization") or ""
            if not auth:
                # No auth header = likely dashboard, allow it
                return await call_next(request)
            if auth != f"Bearer {token}":
                return JSONResponse({"success": False, "message": "Unauthorized"}, status_code=401)
            return await call_next(request)
        auth = request.headers.get("authorization") or ""
        if auth != f"Bearer {token}":
            return JSONResponse({"success": False, "message": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ==========================================
# Health & Info
# ==========================================

@app.get("/health")
def health():
    return {"status": "ok", "version": __version__, "role": ORCHESTRATOR_ROLE}


@app.get("/v1/services")
def list_services(show_system: bool = False):
    """List managed services. Filters out system services by default.
    Use ?show_system=true to include system containers."""
    local_services = state.list_services()

    if not _cluster_state:
        services = [s.model_dump() for s in local_services]
        if not show_system:
            services = [s for s in services if s.get("name", "") not in SYSTEM_SERVICES]
        return services

    # Build merged view: cluster placements are the source of truth
    cluster_svcs = _cluster_state.get_service_placement_summary()
    local_map = {s.name: s for s in local_services}
    result = []

    seen = set()
    for svc_name, info in cluster_svcs.items():
        # Filter system services
        if not show_system and svc_name in SYSTEM_SERVICES:
            continue
        seen.add(svc_name)
        local = local_map.get(svc_name)
        nodes = info.get("nodes", {})
        node_list = [f"{n}:{v['running']}" for n, v in nodes.items()]
        cluster_svc = _cluster_state.get_service(svc_name)
        image = ""
        if cluster_svc:
            image = cluster_svc.get("image", "")
        elif local:
            image = local.image

        result.append({
            "name": svc_name,
            "image": image,
            "replicas": info.get("total", 0),
            "running": info.get("running", 0),
            "memory_limit": cluster_svc.get("memory_limit") if cluster_svc else (local.memory_limit if local else None),
            "cpu_limit": cluster_svc.get("cpu_limit") if cluster_svc else (local.cpu_limit if local else None),
            "status": "running" if info.get("running", 0) > 0 else "stopped",
            "container_ids": [],
            "nodes": node_list,
        })

    # Add local-only services not in cluster placements (skip stopped stale + system)
    for s in local_services:
        if s.name not in seen:
            if not show_system and s.name in SYSTEM_SERVICES:
                continue
            # Skip stale stopped services with no containers
            if s.replicas == 0 and not s.container_ids:
                continue
            result.append(s.model_dump())

    return result


@app.get("/v1/system")
def get_system():
    """System and Docker environment info."""
    info = monitoring.get_system_info()
    info["role"] = ORCHESTRATOR_ROLE
    return info


# ==========================================
# Command / NL Engine
# ==========================================

@app.post("/v1/command", response_model=CommandResponse)
def run_command(req: CommandRequest):
    """Execute a natural language command."""
    intent = parse(req.command)
    if intent.action.value == "unknown":
        return CommandResponse(
            success=False,
            intent=intent,
            message="명령을 이해하지 못했습니다. 예: 'nginx를 3개로 스케일해줘', 'redis 메모리 512m', 'nginx를 node-b로 마이그레이션해줘'",
        )
    success, message, details = execute_intent(intent, dry_run=req.dry_run)
    return CommandResponse(
        success=success,
        intent=intent,
        message=message,
        details=details,
    )


# ==========================================
# Monitoring API
# ==========================================

@app.get("/v1/containers")
def get_containers(stats: bool = False):
    """List all containers."""
    try:
        if stats:
            data = monitoring.get_all_containers_with_stats()
        else:
            data = monitoring.list_containers(include_stopped=True)
        return {"containers": data, "error": None}
    except Exception as e:
        return {"containers": [], "error": str(e)}


@app.get("/v1/images")
def get_images():
    return monitoring.list_images()


# ==========================================
# Container Management API
# ==========================================

@app.post("/v1/containers/run")
def api_run_container(req: RunContainerRequest):
    try:
        client = _docker_client()
        ok, msg, details = run_container(
            client, req.image, name=req.name, memory=req.memory, cpu=req.cpu,
            replicas=req.replicas, use_internal_network=req.use_internal_network,
            environment=req.environment, volumes=req.volumes, ports=req.ports,
            user=req.user, volume_mode=req.volume_mode,
        )
        return {"success": ok, "message": msg, "details": details}
    except Exception as e:
        return {"success": False, "message": str(e), "details": {}}


@app.post("/v1/containers/{container_id}/stop")
def api_stop_container(container_id: str):
    try:
        client = _docker_client()
        ok, msg, _ = stop_container_by_id(client, container_id)
        return {"success": ok, "message": msg}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.delete("/v1/containers/{container_id}")
def api_remove_container(container_id: str):
    try:
        client = _docker_client()
        ok, msg, details = remove_container_by_id(client, container_id)
        service_name = details.get("service_name") if isinstance(details, dict) else None
        if ok and service_name:
            info = state.get_service(service_name)
            if info and (info.get("replicas") or 0) > 0:
                execute_scale(client, service_name, info["replicas"])
        return {"success": ok, "message": msg}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.get("/v1/containers/{container_id}/inspect")
def api_inspect_container(container_id: str):
    try:
        client = _docker_client()
        ok, msg, details = inspect_container_by_id(client, container_id)
        return {"success": ok, "message": msg, "details": details}
    except Exception as e:
        return {"success": False, "message": str(e), "details": {}}


@app.delete("/v1/images/{image_id}")
def api_remove_image(image_id: str):
    try:
        client = _docker_client()
        ok, msg, _ = remove_image_by_id(client, image_id)
        return {"success": ok, "message": msg}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.post("/v1/services/scale")
def api_scale_service(req: ScaleServiceRequest):
    try:
        client = _docker_client()
        ok, msg, details = execute_scale(client, req.service_name, req.replicas)
        return {"success": ok, "message": msg, "details": details}
    except Exception as e:
        return {"success": False, "message": str(e), "details": {}}


@app.post("/v1/action")
def execute_action(req: ActionExecuteRequest):
    try:
        client = _docker_client()
    except Exception as e:
        return CommandResponse(success=False, message=f"Docker 연결 실패: {e}", details={})

    action = (req.action or "").strip().lower()
    if action == "deploy":
        name = req.service_name or req.image
        if not name:
            return CommandResponse(success=False, message="서비스명 또는 이미지를 지정하세요.", details={})
        from .runtime import execute_deploy
        ok, msg, details = execute_deploy(client, name, req.image)
    elif action == "scale":
        if not req.service_name or req.replicas is None:
            return CommandResponse(success=False, message="서비스명과 레플리카 수를 지정하세요.", details={})
        ok, msg, details = execute_scale(client, req.service_name, req.replicas)
    elif action == "resource":
        if not req.service_name:
            return CommandResponse(success=False, message="서비스명을 지정하세요.", details={})
        from .runtime import execute_resource
        ok, msg, details = execute_resource(client, req.service_name, req.memory, req.cpu)
    elif action == "stop":
        if not req.service_name and not req.container_id:
            return CommandResponse(success=False, message="서비스명 또는 컨테이너 ID를 지정하세요.", details={})
        from .runtime import execute_stop
        target = req.service_name or req.container_id
        ok, msg, details = execute_stop(client, target)
    elif action == "run_image":
        if not req.image:
            return CommandResponse(success=False, message="이미지를 지정하세요.", details={})
        ok, msg, details = run_container(client, req.image, name=req.service_name, memory=req.memory, cpu=req.cpu)
    elif action == "container_stop":
        if not req.container_id:
            return CommandResponse(success=False, message="컨테이너 ID를 지정하세요.", details={})
        ok, msg, _ = stop_container_by_id(client, req.container_id)
        details = {}
    elif action == "container_remove":
        if not req.container_id:
            return CommandResponse(success=False, message="컨테이너 ID를 지정하세요.", details={})
        ok, msg, _ = remove_container_by_id(client, req.container_id)
        details = {}
    elif action == "image_remove":
        if not req.image_id:
            return CommandResponse(success=False, message="이미지 ID를 지정하세요.", details={})
        ok, msg, _ = remove_image_by_id(client, req.image_id)
        details = {}
    elif action == "list":
        svcs = state.list_services()
        ok, msg, details = True, "서비스 목록", {"services": [s.model_dump() for s in svcs]}
    else:
        return CommandResponse(success=False, message=f"지원하지 않는 동작: {action}", details={})

    return CommandResponse(success=ok, message=msg, details=details)


# ==========================================
# Cluster API (Master only)
# ==========================================

@app.get("/v1/cluster/status")
def cluster_status():
    """Full cluster overview with all nodes, resources, placements."""
    if not _cluster_state:
        return {"error": "Cluster management not available (not master role)"}
    status = _cluster_state.get_cluster_status()
    return status.model_dump()


@app.get("/v1/cluster/nodes")
def cluster_list_nodes():
    """List known nodes with health and resources."""
    if _cluster_state:
        nodes = _cluster_state.list_nodes()
        return {"nodes": [n.model_dump() for n in nodes]}
    # Fallback to legacy state
    nodes = []
    for n in state.list_nodes():
        nodes.append({"name": n.get("name"), "base_url": n.get("base_url"), "address": n.get("base_url", "")})
    return {"nodes": nodes}


@app.post("/v1/cluster/nodes")
async def cluster_add_node(req: Request):
    """Add/update a cluster node."""
    body = await req.json()
    name = (body.get("name") or "").strip()
    address = (body.get("address") or body.get("base_url") or "").strip()
    token = (body.get("token") or "").strip()

    if not name or not address:
        return {"success": False, "message": "name과 address(IP:port)는 필수입니다."}

    # Legacy state for backward compatibility
    base_url = address if address.startswith("http") else f"http://{address}"
    state.upsert_node(name, base_url, token)

    # Cluster state (master)
    if _cluster_state:
        from .cluster_models import NodeInfo, NodeStatus
        node = NodeInfo(
            name=name,
            address=address,
            token=token,
            status=NodeStatus.HEALTHY,
            role=body.get("role", "worker"),
            labels=body.get("labels", {}),
        )
        _cluster_state.register_node(node)

    return {"success": True, "message": f"노드 '{name}' 등록됨."}


@app.delete("/v1/cluster/nodes/{name}")
def cluster_delete_node(name: str):
    state.delete_node(name)
    if _cluster_state:
        _cluster_state.remove_node(name)
    return {"success": True, "message": f"노드 '{name}' 삭제됨."}


@app.post("/v1/cluster/nodes/{name}/cordon")
def cluster_cordon_node(name: str):
    """Prevent scheduling to this node."""
    if not _cluster_state:
        return {"error": "Cluster not available"}
    from .cluster_models import NodeStatus
    _cluster_state.update_node_status(name, NodeStatus.CORDONED)
    return {"success": True, "message": f"노드 '{name}' cordoned (스케줄링 중지)."}


@app.post("/v1/cluster/nodes/{name}/uncordon")
def cluster_uncordon_node(name: str):
    """Re-enable scheduling to this node."""
    if not _cluster_state:
        return {"error": "Cluster not available"}
    from .cluster_models import NodeStatus
    _cluster_state.update_node_status(name, NodeStatus.HEALTHY)
    return {"success": True, "message": f"노드 '{name}' uncordoned (스케줄링 재개)."}


@app.post("/v1/cluster/nodes/{name}/drain")
async def cluster_drain_node(name: str, request: Request):
    """Migrate all containers off this node."""
    if not _cluster_state or not _migration_controller:
        return {"error": "Cluster not available"}
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    target = body.get("target_node")
    result = await _migration_controller.drain_node(name, target_node=target)
    return result


# ==========================================
# Heartbeat API (receives heartbeats from workers)
# ==========================================

@app.post("/v1/cluster/heartbeat")
async def cluster_heartbeat(req: Request):
    """Receive heartbeat from a worker node."""
    if not _cluster_state:
        return {"ack": False, "error": "Not master"}

    body = await req.json()
    node_name = body.get("node_name", "")
    resources_data = body.get("resources", {})
    containers_data = body.get("containers", [])

    from .cluster_models import NodeResources, ContainerPlacement

    resources = NodeResources(**resources_data)
    containers = [ContainerPlacement(**c) for c in containers_data]

    _cluster_state.process_heartbeat(node_name, resources, containers)

    # Return any pending commands for this node
    commands = []  # TODO: implement command queue per node
    return {"ack": True, "commands": commands}


# ==========================================
# Scheduling API
# ==========================================

@app.post("/v1/cluster/schedule")
async def cluster_schedule(req: Request):
    """Schedule a service across cluster nodes."""
    if not _cluster_state or not _scheduler:
        return {"error": "Cluster not available"}

    body = await req.json()
    from .cluster_models import ScheduleConstraints

    constraints = None
    if body.get("constraints"):
        constraints = ScheduleConstraints(**body["constraints"])

    try:
        decisions = _scheduler.schedule(
            service_name=body.get("service_name", ""),
            image=body.get("image", ""),
            replicas=body.get("replicas", 1),
            constraints=constraints,
            strategy=body.get("strategy", "spread"),
        )

        # Execute scheduling: proxy container run to each target node
        results = []
        for decision in decisions:
            node_name = decision["node_name"]
            count = decision["count"]
            node = _cluster_state.get_node(node_name)
            if not node:
                results.append({"node": node_name, "error": "Node not found"})
                continue

            base_url = node.address if node.address.startswith("http") else f"http://{node.address}"
            headers = {"Authorization": f"Bearer {node.token}"} if node.token else {}

            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        f"{base_url}/v1/containers/run",
                        json={
                            "image": body.get("image", ""),
                            "name": body.get("service_name"),
                            "replicas": count,
                            "memory": body.get("memory_limit"),
                            "cpu": body.get("cpu_limit"),
                            "use_internal_network": True,
                            "environment": body.get("environment", []),
                            "volumes": body.get("volumes", []),
                            "ports": body.get("ports", []),
                        },
                        headers=headers,
                    )
                    results.append({"node": node_name, "count": count, "result": resp.json()})
            except Exception as e:
                results.append({"node": node_name, "error": str(e)})

        # Save service in cluster state
        _cluster_state.save_service(
            name=body.get("service_name", ""),
            image=body.get("image", ""),
            desired_replicas=body.get("replicas", 1),
            memory_limit=body.get("memory_limit"),
            cpu_limit=body.get("cpu_limit"),
            environment=body.get("environment", []),
            volumes=body.get("volumes", []),
            ports=body.get("ports", []),
        )

        return {
            "success": True,
            "message": f"서비스 스케줄링 완료",
            "decisions": decisions,
            "results": results,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ==========================================
# Migration API
# ==========================================

@app.post("/v1/cluster/migrate")
async def cluster_migrate(req: Request):
    """Initiate container migration between nodes."""
    if not _cluster_state or not _migration_controller:
        return {"error": "Cluster not available"}

    body = await req.json()
    result = await _migration_controller.migrate(
        container_id=body.get("container_id", ""),
        source_node=body.get("source_node", ""),
        destination_node=body.get("destination_node", ""),
        container_name=body.get("container_name", ""),
        service_name=body.get("service_name"),
    )
    return result


@app.get("/v1/cluster/migrations")
def cluster_migrations():
    """List migration history."""
    if not _cluster_state:
        return {"migrations": []}
    migrations = _cluster_state.list_migrations()
    return {"migrations": [m.model_dump() for m in migrations]}


@app.get("/v1/cluster/migrations/{migration_id}")
def cluster_migration_detail(migration_id: str):
    """Get migration status."""
    if not _cluster_state:
        return {"error": "Not available"}
    m = _cluster_state.get_migration(migration_id)
    if not m:
        return {"error": "Migration not found"}
    return m.model_dump()


# ==========================================
# Placements API
# ==========================================

@app.get("/v1/cluster/placements")
def cluster_placements(service_name: Optional[str] = None, node_name: Optional[str] = None, show_system: bool = False):
    """Get container placements across cluster. Filters system containers by default."""
    if not _cluster_state:
        return {"placements": []}
    placements = _cluster_state.get_placements(service_name=service_name, node_name=node_name)
    if not show_system:
        placements = [p for p in placements if p.service_name not in SYSTEM_SERVICES]
    return {"placements": [p.model_dump() for p in placements]}


@app.get("/v1/cluster/services")
def cluster_services():
    """Cluster-wide service view with per-node breakdown."""
    if not _cluster_state:
        return {"services": {}}
    return {"services": _cluster_state.get_service_placement_summary()}


# ==========================================
# Alerts API
# ==========================================

@app.get("/v1/cluster/alerts")
def cluster_alerts(all: bool = False):
    """List active alerts."""
    if not _cluster_state:
        return {"alerts": []}
    alerts = _cluster_state.list_alerts(unacknowledged_only=not all)
    return {"alerts": [a.model_dump() for a in alerts]}


@app.post("/v1/cluster/alerts/{alert_id}/ack")
def cluster_ack_alert(alert_id: str):
    """Acknowledge an alert."""
    if not _cluster_state:
        return {"error": "Not available"}
    ok = _cluster_state.acknowledge_alert(alert_id)
    return {"success": ok, "message": f"Alert {alert_id} acknowledged" if ok else "Alert not found"}


# ==========================================
# Service Discovery API
# ==========================================

@app.get("/v1/cluster/discovery")
def cluster_discovery():
    """List all discovered services."""
    if not _service_registry:
        return {"services": {}}
    return {"services": _service_registry.list_services()}


@app.get("/v1/cluster/discovery/{service_name}")
def cluster_discovery_service(service_name: str):
    """Get endpoints for a specific service."""
    if not _service_registry:
        return {"endpoints": []}
    return {"endpoints": _service_registry.get_service(service_name)}


# ==========================================
# Agent API (Worker endpoints for migration)
# ==========================================

@app.post("/v1/agent/export/{container_id}")
def agent_export_container(container_id: str):
    """Export a container as base64 image data (for migration)."""
    if not _worker_agent and ORCHESTRATOR_ROLE != "master":
        # Also allow master to export local containers
        pass

    try:
        from .agent import WorkerAgent
        agent = _worker_agent or WorkerAgent()
        tar_data, config = agent.export_container(container_id)
        return {
            "success": True,
            "image_data": base64.b64encode(tar_data).decode(),
            "config": config,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/v1/agent/import")
async def agent_import_container(req: Request):
    """Import a container from migration data."""
    try:
        body = await req.json()
        image_data = base64.b64decode(body.get("image_data", ""))
        config = body.get("config", {})
        service_name = body.get("service_name")

        if service_name:
            config.setdefault("labels", {})
            config["labels"]["ai.orchestrator.service"] = service_name

        from .agent import WorkerAgent
        agent = _worker_agent or WorkerAgent()
        container_id = agent.import_container(image_data, config)
        return {"success": True, "container_id": container_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/v1/agent/resources")
def agent_resources():
    """Get current node resource usage."""
    from .agent import WorkerAgent
    agent = _worker_agent or WorkerAgent()
    return agent.get_node_resources()


# ==========================================
# Cluster Scale / Stop / Delete (multi-node)
# ==========================================

@app.post("/v1/cluster/scale")
async def cluster_scale_service(req: Request):
    """Scale a service across the cluster. Adjusts replicas on all nodes."""
    body = await req.json()
    service_name = (body.get("service_name") or "").strip()
    replicas = int(body.get("replicas", 1))

    if not service_name:
        return {"success": False, "message": "서비스명을 입력하세요."}

    if not _cluster_state:
        # Fallback: local-only
        try:
            client = _docker_client()
            ok, msg, details = execute_scale(client, service_name, replicas)
            return {"success": ok, "message": msg, "details": details}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # Get current cluster placements for this service
    placements = _cluster_state.get_placements(service_name=service_name)
    current_count = len(placements)

    if replicas == 0:
        # Stop all - delegate to cluster stop
        return await cluster_stop_service_internal(service_name)

    if replicas == current_count:
        return {"success": True, "message": f"'{service_name}' 이미 {replicas}개 실행 중.", "details": {}}

    # Get service info for image
    svc_info = _cluster_state.get_service(service_name)
    if not svc_info and current_count == 0:
        return {"success": False, "message": f"'{service_name}' 서비스를 찾을 수 없습니다. 먼저 '클러스터 배포'로 이미지를 지정하여 배포하세요."}
    image = svc_info["image"] if svc_info else service_name

    if replicas > current_count:
        # Scale up: deploy more using scheduler
        new_count = replicas - current_count
        try:
            decisions = _scheduler.schedule(
                service_name=service_name, image=image, replicas=new_count, strategy="spread",
            )
        except Exception as e:
            return {"success": False, "message": f"스케줄링 실패: {e}"}

        results = []
        for d in decisions:
            node = _cluster_state.get_node(d["node_name"])
            if not node:
                continue
            base_url = node.address if node.address.startswith("http") else f"http://{node.address}"
            headers = {"Content-Type": "application/json"}
            if node.token:
                headers["Authorization"] = f"Bearer {node.token}"
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    resp = await client.post(f"{base_url}/v1/containers/run", json={
                        "image": image, "name": service_name, "replicas": d["count"],
                    }, headers=headers)
                    results.append({"node": d["node_name"], **resp.json()})
            except Exception as e:
                results.append({"node": d["node_name"], "success": False, "message": str(e)})

        if svc_info:
            _cluster_state.save_service(name=service_name, image=image, desired_replicas=replicas)
        return {"success": True, "message": f"'{service_name}' {current_count} -> {replicas}개로 스케일업.", "details": {"results": results}}
    else:
        # Scale down: remove containers from nodes (remove from most-loaded first)
        to_remove = current_count - replicas
        # Group by node, remove from nodes with most containers
        by_node = {}
        for p in placements:
            by_node.setdefault(p.node_name, []).append(p)
        sorted_nodes = sorted(by_node.items(), key=lambda x: -len(x[1]))

        removed = []
        remaining = to_remove
        for node_name, node_placements in sorted_nodes:
            if remaining <= 0:
                break
            node = _cluster_state.get_node(node_name)
            if not node:
                continue
            base_url = node.address if node.address.startswith("http") else f"http://{node.address}"
            headers = {"Content-Type": "application/json"}
            if node.token:
                headers["Authorization"] = f"Bearer {node.token}"
            # Remove up to 'remaining' from this node
            remove_from_here = min(remaining, len(node_placements))
            for p in node_placements[:remove_from_here]:
                try:
                    async with httpx.AsyncClient(timeout=30) as client:
                        try:
                            await client.post(f"{base_url}/v1/containers/{p.container_id}/stop", headers=headers)
                        except Exception:
                            pass
                        resp = await client.delete(f"{base_url}/v1/containers/{p.container_id}", headers=headers)
                        if resp.status_code != 200:
                            await client.delete(f"{base_url}/v1/containers/{p.container_name}", headers=headers)
                    removed.append({"node": node_name, "container": p.container_name})
                    remaining -= 1
                except Exception as e:
                    removed.append({"node": node_name, "container": p.container_name, "error": str(e)})

        # Update local state on each node to match remaining replicas
        # Recalculate per-node counts after removal
        for node_name, node_placements in sorted_nodes:
            node = _cluster_state.get_node(node_name)
            if not node:
                continue
            removed_on_node = sum(1 for r in removed if r.get("node") == node_name and "error" not in r)
            remaining_on_node = len(node_placements) - removed_on_node
            if remaining_on_node <= 0:
                await _set_node_service_replicas(node, service_name, 0)
            else:
                await _set_node_service_replicas(node, service_name, remaining_on_node)

        if svc_info:
            _cluster_state.save_service(name=service_name, image=image, desired_replicas=replicas)
        return {"success": True, "message": f"'{service_name}' {current_count} -> {replicas}개로 스케일다운.", "details": {"removed": removed}}


async def _set_node_service_replicas(node, service_name: str, replicas: int):
    """Set a service's replicas to 0 on a remote node's local state, preventing reconcile from restarting."""
    base_url = node.address if node.address.startswith("http") else f"http://{node.address}"
    headers = {"Content-Type": "application/json"}
    if node.token:
        headers["Authorization"] = f"Bearer {node.token}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(f"{base_url}/v1/services/scale", json={
                "service_name": service_name, "replicas": replicas,
            }, headers=headers)
    except Exception:
        pass


async def cluster_stop_service_internal(service_name: str):
    """Stop and remove all containers for a service across all nodes."""
    placements = _cluster_state.get_placements(service_name=service_name)
    nodes_involved = set()

    # Even if no placements, still need to set replicas=0 on all nodes to prevent reconcile
    all_nodes = _cluster_state.list_nodes()

    # First: set replicas=0 on ALL nodes to stop reconcile from restarting
    for node in all_nodes:
        await _set_node_service_replicas(node, service_name, 0)
        nodes_involved.add(node.name)

    if not placements:
        return {"success": True, "message": f"'{service_name}' 서비스 중지됨 (실행 중인 컨테이너 없음)."}

    removed = []
    for p in placements:
        node = _cluster_state.get_node(p.node_name)
        if not node:
            continue
        base_url = node.address if node.address.startswith("http") else f"http://{node.address}"
        headers = {"Content-Type": "application/json"}
        if node.token:
            headers["Authorization"] = f"Bearer {node.token}"
        ok = False
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # Try delete by ID
                resp = await client.delete(f"{base_url}/v1/containers/{p.container_id}", headers=headers)
                if resp.status_code == 200:
                    ok = True
                if not ok:
                    # Try by name
                    resp2 = await client.delete(f"{base_url}/v1/containers/{p.container_name}", headers=headers)
                    if resp2.status_code == 200:
                        ok = True
            removed.append({"node": p.node_name, "container": p.container_name, "ok": ok})
        except Exception as e:
            removed.append({"node": p.node_name, "container": p.container_name, "ok": False, "error": str(e)})

    # Update cluster state
    svc_info = _cluster_state.get_service(service_name)
    if svc_info:
        _cluster_state.save_service(name=service_name, image=svc_info["image"], desired_replicas=0)

    return {"success": True, "message": f"'{service_name}' 전체 중지: {len(removed)}개 컨테이너 제거.", "details": {"removed": removed}}


@app.post("/v1/cluster/stop")
async def cluster_stop_service(req: Request):
    """Stop all containers for a service across the cluster."""
    body = await req.json()
    service_name = (body.get("service_name") or "").strip()
    if not service_name:
        return {"success": False, "message": "서비스명을 입력하세요."}
    if not _cluster_state:
        return {"success": False, "message": "클러스터 모드가 아닙니다."}
    return await cluster_stop_service_internal(service_name)


# ==========================================
# ==========================================
# Image Pull API
# ==========================================

@app.post("/v1/images/pull")
async def api_pull_image(req: Request):
    """Pull a Docker image by name/path. Works on local node."""
    body = await req.json()
    image = (body.get("image") or "").strip()
    if not image:
        return {"success": False, "message": "이미지 경로를 입력하세요."}
    try:
        client = _docker_client()
        ok, msg = pull_image(client, image)
        return {"success": ok, "message": msg}
    except Exception as e:
        return {"success": False, "message": str(e)}


# ==========================================
# Cluster Deploy API (pull + schedule + run)
# ==========================================

@app.post("/v1/cluster/deploy")
async def cluster_deploy(req: Request):
    """Deploy a service across the cluster: auto-pull image on target nodes + run containers.

    Body: {image, name?, replicas?, strategy?, memory?, cpu?, environment?, volumes?, ports?, nodes?}
    If 'nodes' is specified, deploy only to those nodes. Otherwise use scheduler.
    """
    if not _cluster_state or not _scheduler:
        # Fallback: single-node deploy
        body = await req.json()
        image = (body.get("image") or "").strip()
        if not image:
            return {"success": False, "message": "이미지 경로를 입력하세요."}
        try:
            client = _docker_client()
            ok, msg, details = run_container(
                client, image,
                name=body.get("name"),
                replicas=body.get("replicas", 1),
                memory=body.get("memory"),
                cpu=body.get("cpu"),
                environment=body.get("environment", []),
                volumes=body.get("volumes", []),
                ports=body.get("ports", []),
                auto_pull=True,
            )
            return {"success": ok, "message": msg, "details": details}
        except Exception as e:
            return {"success": False, "message": str(e)}

    body = await req.json()
    image = (body.get("image") or "").strip()
    name = (body.get("name") or "").strip()
    replicas = int(body.get("replicas", 1))
    strategy = body.get("strategy", "spread")
    target_nodes = body.get("nodes", [])  # specific nodes or empty for auto-schedule

    if not image:
        return {"success": False, "message": "이미지 경로를 입력하세요."}

    # Derive service name from image if not provided
    if not name:
        name = image.split(":")[0].split("/")[-1]

    # Clean up existing service first (stop old containers, reset state)
    existing = _cluster_state.get_placements(service_name=name)
    if existing:
        await cluster_stop_service_internal(name)
        # Wait briefly for containers to be removed
        import asyncio
        await asyncio.sleep(2)

    # Determine target nodes
    if target_nodes:
        decisions = [{"node_name": n, "count": max(1, replicas // len(target_nodes))} for n in target_nodes]
        # Distribute remainder
        remainder = replicas - sum(d["count"] for d in decisions)
        for i in range(remainder):
            decisions[i % len(decisions)]["count"] += 1
    else:
        try:
            from .cluster_models import ScheduleConstraints
            constraints = None
            if body.get("constraints"):
                constraints = ScheduleConstraints(**body["constraints"])
            decisions = _scheduler.schedule(
                service_name=name, image=image, replicas=replicas,
                constraints=constraints, strategy=strategy,
            )
        except Exception as e:
            return {"success": False, "message": f"스케줄링 실패: {e}"}

    # Execute on each node: pull image + run containers
    results = []
    total_created = 0
    for decision in decisions:
        node_name = decision["node_name"]
        count = decision["count"]
        node = _cluster_state.get_node(node_name)
        if not node:
            results.append({"node": node_name, "success": False, "error": "노드를 찾을 수 없습니다."})
            continue

        base_url = node.address if node.address.startswith("http") else f"http://{node.address}"
        headers = {}
        if node.token:
            headers["Authorization"] = f"Bearer {node.token}"
        headers["Content-Type"] = "application/json"

        try:
            async with httpx.AsyncClient(timeout=120) as client:
                # Step 1: Pull image on target node
                pull_resp = await client.post(
                    f"{base_url}/v1/images/pull",
                    json={"image": image},
                    headers=headers,
                )
                pull_data = pull_resp.json()
                if not pull_data.get("success", False):
                    results.append({"node": node_name, "success": False, "phase": "pull", "error": pull_data.get("message", "Pull failed")})
                    continue

                # Step 2: Run containers on target node
                run_resp = await client.post(
                    f"{base_url}/v1/containers/run",
                    json={
                        "image": image,
                        "name": name,
                        "replicas": count,
                        "memory": body.get("memory"),
                        "cpu": body.get("cpu"),
                        "use_internal_network": True,
                        "environment": body.get("environment", []),
                        "volumes": body.get("volumes", []),
                        "ports": body.get("ports", []),
                    },
                    headers=headers,
                )
                run_data = run_resp.json()
                created = len(run_data.get("details", {}).get("container_ids", []))
                total_created += created
                results.append({"node": node_name, "success": run_data.get("success", False), "created": created, "message": run_data.get("message", "")})
        except Exception as e:
            results.append({"node": node_name, "success": False, "error": str(e)})

    # Save cluster service
    _cluster_state.save_service(
        name=name, image=image, desired_replicas=replicas,
        memory_limit=body.get("memory"), cpu_limit=body.get("cpu"),
        environment=body.get("environment", []),
        volumes=body.get("volumes", []),
        ports=body.get("ports", []),
    )

    all_ok = all(r.get("success") for r in results)
    return {
        "success": all_ok,
        "message": f"'{name}' 배포 {'완료' if all_ok else '일부 실패'}: {total_created}개 컨테이너 생성 ({len(decisions)}개 노드)",
        "details": {
            "service_name": name,
            "image": image,
            "total_created": total_created,
            "decisions": decisions,
            "results": results,
        },
    }


# ==========================================
# Cluster Proxy (legacy + enhanced)
# ==========================================

@app.api_route("/v1/cluster/{node_name}/proxy", methods=["GET", "POST", "DELETE"])
async def cluster_proxy(node_name: str, request: Request, path: str):
    """Proxy selected /v1/* request to a remote node."""
    if not path.startswith("/v1/") or path.startswith("/v1/cluster/"):
        return JSONResponse({"success": False, "message": "Invalid path"}, status_code=400)
    if not any(path.startswith(p) for p in ALLOWED_PROXY_PREFIXES):
        return JSONResponse({"success": False, "message": "Path not allowed"}, status_code=400)

    # Try cluster state first, then legacy state
    node = None
    if _cluster_state:
        node_info = _cluster_state.get_node(node_name)
        if node_info:
            base_url = node_info.address if node_info.address.startswith("http") else f"http://{node_info.address}"
            node = {"base_url": base_url, "token": node_info.token}

    if not node:
        legacy = _node_by_name(node_name)
        if not legacy:
            return JSONResponse({"success": False, "message": f"Unknown node: {node_name}"}, status_code=404)
        node = legacy

    url = _normalize_base_url(node.get("base_url") or "") + path
    method = request.method.upper()
    headers = {"Authorization": f"Bearer {node.get('token')}"}
    content = None
    if method in ("POST", "DELETE"):
        try:
            content = await request.body()
        except Exception:
            content = None
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.request(method, url, headers=headers, content=content)
        return JSONResponse(resp.json(), status_code=resp.status_code)
    except Exception as e:
        return JSONResponse({"success": False, "message": str(e)}, status_code=502)


# ==========================================
# Dashboard
# ==========================================

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
@app.get("/dashboard/", response_class=HTMLResponse)
def dashboard():
    """Serve dashboard HTML."""
    content = _dashboard_html()
    if content is None:
        return HTMLResponse(
            "<h1>Dashboard not found</h1><p>static/dashboard.html not found.</p>"
            "<p>API: <a href='/docs'>/docs</a>, <a href='/health'>/health</a></p>",
            status_code=404,
        )
    return HTMLResponse(content)


def _dashboard_html() -> Optional[str]:
    path = Path(__file__).resolve().parent / "static" / "dashboard.html"
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return None


def create_app():
    return app
