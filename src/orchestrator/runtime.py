"""Docker runtime adapter: scale, deploy, resource limits."""
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import docker
from docker.errors import APIError, ImageNotFound, NotFound
from docker.types import Mount

from . import state
from .models import IntentAction, ParsedIntent

# Label to mark containers managed by this orchestrator
LABEL_ORCHESTRATOR = "ai.orchestrator.managed"
LABEL_SERVICE = "ai.orchestrator.service"
# Internal DNS: all managed containers join this network and resolve by service name
ORCH_NETWORK = "orch-internal"
# Ingress: Traefik uses these labels to route and load-balance (default HTTP port)
TRAEFIK_HTTP_PORT = 80


def _docker_client():
    return docker.from_env()


def _ensure_network(client):
    """Create internal network for service discovery if not exists."""
    try:
        nets = client.networks.list(names=[ORCH_NETWORK])
        if nets:
            return nets[0]
        return client.networks.create(ORCH_NETWORK, driver="bridge")
    except APIError:
        return None


def _traefik_labels(service_name: str, port: int = TRAEFIK_HTTP_PORT) -> dict:
    """Labels for Traefik ingress: same router/service name groups replicas for LB."""
    safe = re.sub(r"[^a-z0-9-]", "-", (service_name or "").lower()).strip("-") or "svc"
    path_prefix = (service_name or "svc").strip("/")
    return {
        "traefik.enable": "true",
        f"traefik.http.routers.{safe}.rule": f"Host(`{service_name}.local`)",
        f"traefik.http.routers.{safe}-path.rule": f"PathPrefix(`/{path_prefix}/`)",
        f"traefik.http.routers.{safe}-path.service": safe,
        f"traefik.http.routers.{safe}-path.middlewares": f"{safe}-strip",
        f"traefik.http.middlewares.{safe}-strip.stripprefix.prefixes": f"/{path_prefix}",
        f"traefik.http.services.{safe}.loadbalancer.server.port": str(port),
    }


def _build_run_extra(
    client,
    environment: Optional[List[str]] = None,
    volumes: Optional[List[str]] = None,
    ports: Optional[List[str]] = None,
    user: Optional[str] = None,
) -> Dict[str, Any]:
    """Build kwargs for container run (high-level API: environment, mounts, ports, user)."""
    out: Dict[str, Any] = {}
    if environment:
        out["environment"] = [e.strip() for e in environment if (e or "").strip()]
    if user and (user or "").strip():
        out["user"] = (user or "").strip()
    if volumes:
        mounts = []
        for v in volumes:
            v = (v or "").strip()
            if not v:
                continue
            parts = v.split(":")
            read_only = len(parts) > 2 and (parts[2].lower() == "ro")
            if len(parts) >= 2:
                host_path = parts[0].strip()
                container_path = parts[1].strip()
                mounts.append(
                    Mount(type="bind", source=host_path, target=container_path, read_only=read_only)
                )
        if mounts:
            out["mounts"] = mounts
    if ports:
        port_dict = {}
        for p in ports:
            p = (p or "").strip()
            if ":" in p:
                parts = p.split(":", 1)
                try:
                    host_p = int(parts[0].strip())
                    cont_p = int(parts[1].strip())
                    port_dict[f"{cont_p}/tcp"] = host_p
                except (ValueError, IndexError):
                    pass
        if port_dict:
            out["ports"] = port_dict
    return out


def _parse_memory(s: str) -> int:
    """Convert '512m'/'1g' to bytes for Docker API."""
    if not s:
        return 0
    s = s.strip().lower()
    m = re.match(r"^(\d+)\s*(m|mb|g|gb)?$", s)
    if not m:
        return 0
    num = int(m.group(1))
    unit = (m.group(2) or "m").replace("mb", "m").replace("gb", "g")
    if unit == "g":
        return num * 1024 * 1024 * 1024
    return num * 1024 * 1024


def _container_name(service: str, index: int) -> str:
    return f"orch-{service}-{index}"


def _index_from_container_name(service_name: str, name: str) -> Optional[int]:
    """Parse orch-{service}-{index} from container name. Returns None if not matching."""
    if not name:
        return None
    name = (name or "").strip().lstrip("/")
    prefix = f"orch-{service_name}-"
    if not name.startswith(prefix):
        return None
    suffix = name[len(prefix) :]
    if not suffix.isdigit():
        return None
    return int(suffix)


def _get_containers_for_service(client, service_name: str) -> List:
    try:
        return client.containers.list(
            all=True,
            filters={"label": f"{LABEL_SERVICE}={service_name}"},
        )
    except APIError:
        return []


def execute_scale(client, service_name: str, replicas: int) -> Tuple[bool, str, dict]:
    """Scale service to N replicas. Stopped containers are removed first, then add/remove to match target."""
    info = state.get_service(service_name)
    if not info:
        return False, f"서비스 '{service_name}'를 찾을 수 없습니다. 먼저 배포하세요.", {}

    all_containers = _get_containers_for_service(client, service_name)
    running = [c for c in all_containers if getattr(c, "status", None) == "running"]
    stopped = [c for c in all_containers if c not in running]
    removed = []
    for c in stopped:
        try:
            c.stop(timeout=5)
            c.remove()
            removed.append(c.id[:12])
        except (APIError, NotFound):
            pass
    current = running
    target = replicas
    image = info["image"]
    memory = info.get("memory_limit")
    cpu = info.get("cpu_limit")
    env = info.get("environment")
    vols = info.get("volumes")
    prts = info.get("ports")
    usr = info.get("user")
    extra = _build_run_extra(client, environment=env, volumes=vols, ports=prts, user=usr)

    # Remove excess (running only)
    to_remove = len(current) - target
    for i in range(to_remove):
        if current:
            c = current.pop()
            try:
                c.stop(timeout=5)
                c.remove()
                removed.append(c.id[:12])
            except (APIError, NotFound):
                pass

    net = _ensure_network(client)
    for c in current:
        try:
            net.connect(c, aliases=[service_name])
        except (APIError, NotFound):
            pass
    used_indices = set()
    for c in current:
        n = getattr(c, "name", None) or (c.attrs or {}).get("Name", "")
        i = _index_from_container_name(service_name, n)
        if i is not None:
            used_indices.add(i)
    created = []
    for i in range(target - len(current)):
        idx = 0
        while idx in used_indices:
            idx += 1
        used_indices.add(idx)
        name = _container_name(service_name, idx)
        labels = {
            LABEL_ORCHESTRATOR: "true",
            LABEL_SERVICE: service_name,
            **_traefik_labels(service_name),
        }
        kwargs = {
            "image": image,
            "name": name,
            "labels": labels,
            "detach": True,
            **extra,
        }
        if memory:
            kwargs["mem_limit"] = _parse_memory(memory)
        if cpu:
            try:
                kwargs["nano_cpus"] = int(float(cpu) * 1e9)
            except ValueError:
                pass
        try:
            c = client.containers.run(**kwargs)
            cid = c.id[:12] if hasattr(c, "id") else (c[:12] if isinstance(c, str) else "")
            if net and c:
                try:
                    net.connect(c, aliases=[service_name])
                except (APIError, NotFound):
                    pass
            created.append(cid)
        except (APIError, ImageNotFound) as e:
            return False, f"컨테이너 생성 실패: {e}", {"removed": removed}

    all_ids = [c.id[:12] for c in current] + created
    state.upsert_service(
        service_name, image, replicas=target,
        memory_limit=memory, cpu_limit=cpu, container_ids=all_ids,
        environment=env, volumes=vols, ports=prts, user=usr,
    )
    state.update_service_containers(service_name, all_ids)

    return True, f"'{service_name}'를 {target}개 레플리카로 스케일했습니다.", {
        "replicas": target,
        "removed": removed,
        "created": created,
    }


def reconcile_replicas(client=None) -> None:
    """실시간 레플리카 유지: 상태의 desired replicas와 실제 실행 중인 컨테이너 수를 맞춤 (모든 replicas 수 대상)."""
    if client is None:
        client = _docker_client()
    for svc in state.list_services():
        desired = svc.replicas if svc.replicas is not None else 0
        try:
            current = _get_containers_for_service(client, svc.name)
            running = [c for c in current if getattr(c, "status", None) == "running"]
            if len(running) != desired:
                execute_scale(client, svc.name, desired)
        except (APIError, Exception):
            pass


def execute_deploy(client, name: str, image: Optional[str] = None) -> Tuple[bool, str, dict]:
    """Deploy a new service (or update). If image is None, use name as image."""
    img = image or name
    replicas = 1
    info = state.get_service(name)
    if info:
        replicas = info.get("replicas", 1)
        memory = info.get("memory_limit")
        cpu = info.get("cpu_limit")
    else:
        memory = cpu = None

    state.upsert_service(name, img, replicas=replicas, memory_limit=memory, cpu_limit=cpu)
    success, msg, details = execute_scale(client, name, replicas)
    if success:
        msg = f"서비스 '{name}' (이미지: {img}) 배포 완료."
    return success, msg, details


def execute_resource(
    client, service_name: str, memory: Optional[str] = None, cpu: Optional[str] = None
) -> Tuple[bool, str, dict]:
    """Update resource limits; requires recreating containers."""
    info = state.get_service(service_name)
    if not info:
        return False, f"서비스 '{service_name}'를 찾을 수 없습니다.", {}

    new_memory = memory or info.get("memory_limit")
    new_cpu = cpu or info.get("cpu_limit")
    state.upsert_service(
        info["name"], info["image"],
        replicas=info.get("replicas", 1),
        memory_limit=new_memory,
        cpu_limit=new_cpu,
        container_ids=info.get("container_ids", []),
    )
    # Recreate with new limits
    return execute_scale(client, service_name, info.get("replicas", 1))


def _get_containers_by_name(client, name: str) -> List:
    """Find containers whose name equals or starts with name (e.g. nginx or orch-nginx-0)."""
    try:
        all_containers = client.containers.list(all=True)
        name_clean = name.strip().lower()
        out = []
        for c in all_containers:
            cname = (c.name or "").strip("/").lower()
            if cname == name_clean or cname.startswith(name_clean + "-") or cname.startswith("orch-" + name_clean + "-"):
                out.append(c)
        return out
    except APIError:
        return []


def execute_stop(client, service_name: str) -> Tuple[bool, str, dict]:
    """Stop and remove: managed service first; else any container matching the name."""
    info = state.get_service(service_name)
    if info:
        current = _get_containers_for_service(client, service_name)
        removed = []
        for c in current:
            try:
                c.stop(timeout=5)
                c.remove()
                removed.append(c.id[:12])
            except (APIError, NotFound):
                pass
        state.update_service_containers(service_name, [])
        state.upsert_service(
            service_name, info["image"], replicas=0,
            memory_limit=info.get("memory_limit"), cpu_limit=info.get("cpu_limit"),
            container_ids=[],
        )
        return True, f"서비스 '{service_name}'를 중지했습니다.", {"removed": removed}

    # 관리 서비스가 아니면 컨테이너 이름으로 찾아서 종료
    current = _get_containers_by_name(client, service_name)
    if not current:
        return False, f"서비스 또는 컨테이너 '{service_name}'를 찾을 수 없습니다.", {}

    removed = []
    for c in current:
        try:
            c.stop(timeout=5)
            c.remove()
            removed.append(c.id[:12])
        except (APIError, NotFound):
            pass
    return True, f"컨테이너 '{service_name}'를 종료했습니다.", {"removed": removed}


def run_container(
    client,
    image: str,
    name: Optional[str] = None,
    memory: Optional[str] = None,
    cpu: Optional[str] = None,
    replicas: Optional[int] = 1,
    use_internal_network: bool = True,
    environment: Optional[List[str]] = None,
    volumes: Optional[List[str]] = None,
    ports: Optional[List[str]] = None,
    user: Optional[str] = None,
) -> Tuple[bool, str, dict]:
    """Run one or more containers from image.

    Containers are labeled so that they can be scaled later as a group
    (LABEL_SERVICE = service/group name).
    """
    try:
        count = replicas or 1
        ids: List[str] = []
        net = _ensure_network(client) if use_internal_network else None
        group_name = name or image.split(":")[0].split("/")[-1]
        extra = _build_run_extra(client, environment=environment, volumes=volumes, ports=ports, user=user)
        for i in range(count):
            labels = {
                LABEL_ORCHESTRATOR: "true",
                LABEL_SERVICE: group_name,
                **_traefik_labels(group_name),
            }
            kwargs = {
                "image": image,
                "name": _container_name(group_name, i),
                "detach": True,
                "labels": labels,
                **extra,
            }
            if memory:
                kwargs["mem_limit"] = _parse_memory(memory)
            if cpu:
                try:
                    kwargs["nano_cpus"] = int(float(cpu) * 1e9)
                except ValueError:
                    pass
            c = client.containers.run(**kwargs)
            cid = c.id[:12] if hasattr(c, "id") else (c[:12] if isinstance(c, str) else "")
            if use_internal_network and c and net:
                try:
                    net.connect(c, aliases=[group_name or name or cid])
                except (APIError, NotFound):
                    pass
            if cid:
                ids.append(cid)
        if group_name:
            state.upsert_service(
                group_name,
                image,
                replicas=len(ids),
                memory_limit=memory,
                cpu_limit=cpu,
                container_ids=ids,
                environment=environment,
                volumes=volumes,
                ports=ports,
                user=user,
            )
        msg = (
            f"컨테이너 {len(ids)}개 기동 완료"
            if len(ids) > 1
            else (f"컨테이너 기동: {ids[0]}" if ids else "컨테이너를 기동하지 못했습니다.")
        )
        return True, msg, {"container_ids": ids, "service_name": group_name}
    except (APIError, ImageNotFound) as e:
        return False, str(e), {}


def stop_container_by_id(client, container_id: str) -> Tuple[bool, str, dict]:
    """Stop a container by id or name (does not remove)."""
    try:
        c = client.containers.get(container_id)
        c.stop(timeout=10)
        return True, f"컨테이너 '{container_id}' 중지됨.", {}
    except (APIError, NotFound) as e:
        return False, str(e), {}


def remove_container_by_id(client, container_id: str) -> Tuple[bool, str, dict]:
    """Stop and remove a container by id or name. Returns service_name from labels if set."""
    try:
        c = client.containers.get(container_id)
        attrs = c.attrs or {}
        labels = (attrs.get("Config") or {}).get("Labels") or {}
        service_name = labels.get(LABEL_SERVICE) or None
        c.stop(timeout=10)
        c.remove()
        return True, f"컨테이너 '{container_id}' 삭제됨.", {"service_name": service_name}
    except (APIError, NotFound) as e:
        return False, str(e), {}


def remove_image_by_id(client, image_id: str) -> Tuple[bool, str, dict]:
    """Remove a Docker image by id or tag."""
    try:
        client.images.remove(image_id, force=True)
        return True, f"이미지 '{image_id}' 삭제됨.", {}
    except (APIError, NotFound) as e:
        return False, str(e), {}


def execute_intent(intent: ParsedIntent, dry_run: bool = False) -> Tuple[bool, str, dict]:
    """Execute a parsed intent. Returns (success, message, details)."""
    if dry_run:
        return True, f"[Dry run] 실행 예정: {intent.action.value} {intent.model_dump()}", {}

    try:
        client = _docker_client()
    except Exception as e:
        return False, f"Docker 연결 실패: {e}", {}

    if intent.action == IntentAction.SCALE:
        if not intent.service_name or intent.replicas is None:
            return False, "스케일할 서비스명과 레플리카 수를 지정해주세요.", {}
        return execute_scale(client, intent.service_name, intent.replicas)

    if intent.action == IntentAction.DEPLOY:
        name = intent.service_name or intent.image
        if not name:
            return False, "배포할 서비스명 또는 이미지를 지정해주세요.", {}
        if not intent.service_name and intent.image:
            name = intent.image.split(":")[0].split("/")[-1]
        return execute_deploy(client, name, intent.image or (intent.service_name and None))

    if intent.action == IntentAction.RESOURCE:
        if not intent.service_name:
            return False, "리소스를 설정할 서비스명을 지정해주세요.", {}
        return execute_resource(client, intent.service_name, intent.memory, intent.cpu)

    if intent.action == IntentAction.STOP:
        if not intent.service_name:
            return False, "중지할 서비스명을 지정해주세요.", {}
        return execute_stop(client, intent.service_name)

    if intent.action == IntentAction.LIST:
        services = state.list_services()
        return True, "서비스 목록", {"services": [s.model_dump() for s in services]}

    return False, f"지원하지 않는 명령입니다: {intent.raw}", {}
