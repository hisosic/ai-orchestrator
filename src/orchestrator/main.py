"""FastAPI application for AI Container Orchestrator."""
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse

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
    remove_container_by_id,
    remove_image_by_id,
    run_container,
    stop_container_by_id,
    _docker_client,
)
from . import monitoring

RECONCILE_INTERVAL_SEC = 15


def _reconcile_loop() -> None:
    while True:
        try:
            reconcile_replicas()
        except Exception:
            pass
        time.sleep(RECONCILE_INTERVAL_SEC)


@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=_reconcile_loop, daemon=True)
    t.start()
    yield


app = FastAPI(
    title="AI Container Orchestrator",
    description="Natural language container orchestration: scale, deploy, resource control",
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

@app.get("/health")
def health():
    return {"status": "ok", "version": __version__}


@app.get("/v1/services", response_model=list[ServiceInfo])
def list_services():
    """List all managed services."""
    return state.list_services()


@app.post("/v1/command", response_model=CommandResponse)
def run_command(req: CommandRequest):
    """Execute a natural language command."""
    intent = parse(req.command)
    if intent.action.value == "unknown":
        return CommandResponse(
            success=False,
            intent=intent,
            message="명령을 이해하지 못했습니다. 예: 'nginx를 3개로 스케일해줘', 'redis 메모리 512m'",
        )
    success, message, details = execute_intent(intent, dry_run=req.dry_run)
    return CommandResponse(
        success=success,
        intent=intent,
        message=message,
        details=details,
    )


# ----- Monitoring API -----
@app.get("/v1/system")
def get_system():
    """System and Docker environment info."""
    return monitoring.get_system_info()


@app.get("/v1/containers")
def get_containers(stats: bool = False):
    """List all containers. If stats=true, include CPU/memory stats (slower). Always 200; error in body if Docker fails."""
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
    """List Docker images (id, tags, size)."""
    return monitoring.list_images()


@app.post("/v1/containers/run")
def api_run_container(req: RunContainerRequest):
    """Run a container from image (optional: name, memory, cpu). Joins internal network for DNS."""
    try:
        client = _docker_client()
        ok, msg, details = run_container(
            client,
            req.image,
            name=req.name,
            memory=req.memory,
            cpu=req.cpu,
            replicas=req.replicas,
            use_internal_network=req.use_internal_network,
            environment=req.environment,
            volumes=req.volumes,
            ports=req.ports,
            user=req.user,
            volume_mode=req.volume_mode,
        )
        return {"success": ok, "message": msg, "details": details}
    except Exception as e:
        return {"success": False, "message": str(e), "details": {}}


@app.post("/v1/containers/{container_id}/stop")
def api_stop_container(container_id: str):
    """Stop a container by id or name."""
    try:
        client = _docker_client()
        ok, msg, _ = stop_container_by_id(client, container_id)
        return {"success": ok, "message": msg}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.delete("/v1/containers/{container_id}")
def api_remove_container(container_id: str):
    """Stop and remove a container by id or name. If it had a service label, restore replica count."""
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


@app.delete("/v1/images/{image_id}")
def api_remove_image(image_id: str):
    """Remove a Docker image by id or tag."""
    try:
        client = _docker_client()
        ok, msg, _ = remove_image_by_id(client, image_id)
        return {"success": ok, "message": msg}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.post("/v1/services/scale")
def api_scale_service(req: ScaleServiceRequest):
    """Scale a managed service/group by name (used from container list)."""
    try:
        from .runtime import execute_scale

        client = _docker_client()
        ok, msg, details = execute_scale(client, req.service_name, req.replicas)
        return {"success": ok, "message": msg, "details": details}
    except Exception as e:
        return {"success": False, "message": str(e), "details": {}}


@app.post("/v1/action")
def execute_action(req: ActionExecuteRequest):
    """Execute by action + params (for drag-and-drop builder)."""
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
        from .runtime import execute_scale
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


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
@app.get("/dashboard/", response_class=HTMLResponse)
def dashboard():
    """Serve dashboard HTML (from package resource or static path)."""
    content = _dashboard_html()
    if content is None:
        return HTMLResponse(
            "<h1>Dashboard not found</h1><p>static/dashboard.html not found.</p>"
            "<p>API: <a href='/docs'>/docs</a>, <a href='/health'>/health</a></p>",
            status_code=404,
        )
    return HTMLResponse(content)


def _dashboard_html() -> Optional[str]:
    """Load dashboard HTML (path relative to this module)."""
    path = Path(__file__).resolve().parent / "static" / "dashboard.html"
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return None


def create_app():
    return app
