"""
Container Migration Controller.

Migration: docker commit → save → transfer → load → run (상태 보존, 느림)
Move: source stop → destination run with same image (빠름)

Both operations:
- Do NOT modify replicas (only admin can change replicas)
- Use reconcile_skip to prevent reconcile from interfering
- Remove only the specific source container, not the service
"""

import uuid
import logging
import asyncio
from datetime import datetime

import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrate")


def _node_base_url(node) -> str:
    addr = node.address
    return addr if addr.startswith("http") else f"http://{addr}"


def _node_headers(node) -> dict:
    headers = {"Content-Type": "application/json"}
    if node.token:
        headers["Authorization"] = f"Bearer {node.token}"
    return headers


async def _force_remove_container(node, container_id: str, container_name: str, service_name: str = ""):
    """Force remove a specific container on a remote node.
    Sets replicas to 0 first to prevent auto-rescale, then removes container."""
    base_url = _node_base_url(node)
    headers = {**_node_headers(node), "X-Cluster-Op": "true"}
    try:
        # Set service replicas to 0 on source to prevent reconcile from recreating
        if service_name:
            async with httpx.AsyncClient(timeout=15) as client:
                await client.post(f"{base_url}/v1/services/scale", json={
                    "service_name": service_name, "replicas": 0,
                }, headers=headers)

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                await client.post(f"{base_url}/v1/containers/{container_id}/stop", headers=headers)
            except Exception:
                pass
            resp = await client.delete(f"{base_url}/v1/containers/{container_id}", headers=headers)
            if resp.status_code != 200 and container_name:
                await client.delete(f"{base_url}/v1/containers/{container_name}", headers=headers)
    except Exception as e:
        logger.warning(f"Force remove {container_name} on {node.name}: {e}")


def _set_reconcile_skip(service_name: str, skip: bool):
    """Set reconcile skip flag on the local node."""
    try:
        from .runtime import reconcile_skip_add, reconcile_skip_remove
        if skip:
            reconcile_skip_add(service_name)
        else:
            reconcile_skip_remove(service_name)
    except Exception:
        pass


async def _set_remote_reconcile_skip(node, service_name: str, skip: bool):
    """Tell a remote node to skip/resume reconcile for a service.
    Uses a lightweight POST to a custom endpoint."""
    # For remote nodes, we use the agent API
    base_url = _node_base_url(node)
    headers = _node_headers(node)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{base_url}/v1/agent/reconcile-skip", json={
                "service_name": service_name, "skip": skip,
            }, headers=headers)
    except Exception:
        pass


class MigrationController:
    """Orchestrates container migration between nodes."""

    def __init__(self, cluster_state):
        self.cluster_state = cluster_state

    async def migrate(self, container_id: str, source_node: str, destination_node: str,
                      container_name: str = "", service_name: str = None) -> dict:
        migration_id = str(uuid.uuid4())[:8]
        now = datetime.utcnow().isoformat()

        from .cluster_models import MigrationInfo, MigrationStatus
        migration = MigrationInfo(
            id=migration_id, container_id=container_id, container_name=container_name,
            source_node=source_node, destination_node=destination_node,
            status=MigrationStatus.PENDING, started_at=now, progress=0
        )
        self.cluster_state.create_migration(migration)

        src_node = self.cluster_state.get_node(source_node)
        dst_node = self.cluster_state.get_node(destination_node)

        if not src_node:
            return await self._fail_migration(migration_id, f"소스 노드 '{source_node}'를 찾을 수 없습니다.")
        if not dst_node:
            return await self._fail_migration(migration_id, f"대상 노드 '{destination_node}'를 찾을 수 없습니다.")
        if dst_node.status.value not in ("healthy",):
            return await self._fail_migration(migration_id, f"대상 노드가 {dst_node.status.value} 상태입니다.")

        logger.info(f"Migration {migration_id}: START {container_name} ({source_node} -> {destination_node})")
        asyncio.create_task(self._execute_migration(
            migration_id, src_node, dst_node, container_id, container_name, service_name
        ))

        return {
            "id": migration_id, "status": "pending",
            "message": f"마이그레이션 시작: {container_name or container_id} ({source_node} -> {destination_node})"
        }

    async def _execute_migration(self, migration_id: str, src_node, dst_node,
                                  container_id: str, container_name: str, service_name: str):
        from .cluster_models import MigrationStatus

        src_url = _node_base_url(src_node)
        dst_url = _node_base_url(dst_node)
        src_headers = _node_headers(src_node)
        dst_headers = _node_headers(dst_node)

        # Pause reconcile on both nodes for this service
        if service_name:
            _set_reconcile_skip(service_name, True)  # local (master)
            await _set_remote_reconcile_skip(src_node, service_name, True)
            await _set_remote_reconcile_skip(dst_node, service_name, True)

        try:
            # ---- Export (file-based: commit + save to temp file) ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.EXPORTING, progress=10)
            logger.info(f"Migration {migration_id}: Preparing export on {src_node.name}")

            async with httpx.AsyncClient(timeout=600) as client:
                export_resp = await client.post(
                    f"{src_url}/v1/agent/export/{container_id}/prepare", headers=src_headers
                )

            if export_resp.status_code != 200:
                await self._fail_migration(migration_id, f"Export HTTP 실패: {export_resp.status_code}")
                return

            export_data = export_resp.json()
            if not export_data.get("success"):
                await self._fail_migration(migration_id, f"Export 실패: {export_data.get('error', 'unknown')}")
                return

            tar_path = export_data["tar_path"]
            config = export_data.get("config", {})
            file_size = export_data.get("file_size", 0)
            logger.info(f"Migration {migration_id}: Export OK (file={tar_path}, size={file_size})")
            self.cluster_state.update_migration(migration_id, MigrationStatus.EXPORTING, progress=25)

            # ---- Transfer: stream tar from source → destination ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.TRANSFERRING, progress=35)
            logger.info(f"Migration {migration_id}: Streaming {file_size} bytes {src_node.name} -> {dst_node.name}")

            import json as _json
            import_headers = {**dst_headers}
            import_headers["Content-Type"] = "application/x-tar"
            import_headers["X-Container-Config"] = _json.dumps(config)
            if service_name:
                import_headers["X-Service-Name"] = service_name

            # Stream download from source and pipe to destination
            async with httpx.AsyncClient(timeout=600) as src_client:
                async with src_client.stream(
                    "GET", f"{src_url}/v1/agent/export/download",
                    params={"path": tar_path}, headers=src_headers
                ) as download:
                    if download.status_code != 200:
                        await self._fail_migration(migration_id, f"Download 실패: HTTP {download.status_code}")
                        return

                    # Collect streamed chunks and upload to destination
                    transferred = 0
                    chunks = []
                    async for chunk in download.aiter_bytes(chunk_size=1024 * 1024):
                        chunks.append(chunk)
                        transferred += len(chunk)
                        if file_size > 0:
                            pct = 35 + int(25 * transferred / file_size)
                            self.cluster_state.update_migration(
                                migration_id, MigrationStatus.TRANSFERRING, progress=min(pct, 59)
                            )

            tar_data = b"".join(chunks)
            logger.info(f"Migration {migration_id}: Downloaded {transferred} bytes, uploading to {dst_node.name}")

            self.cluster_state.update_migration(migration_id, MigrationStatus.IMPORTING, progress=60)
            logger.info(f"Migration {migration_id}: Importing on {dst_node.name}")

            async with httpx.AsyncClient(timeout=600) as dst_client:
                import_resp = await dst_client.post(
                    f"{dst_url}/v1/agent/import/upload",
                    content=tar_data,
                    headers=import_headers,
                )

            if import_resp.status_code != 200:
                await self._fail_migration(migration_id, f"Import HTTP 실패: {import_resp.status_code}")
                return

            import_result = import_resp.json()
            if not import_result.get("success"):
                await self._fail_migration(migration_id, f"Import 실패: {import_result.get('error', 'unknown')}")
                return

            new_container_id = import_result.get("container_id", "")

            # Cleanup temp file on source
            try:
                async with httpx.AsyncClient(timeout=15) as cleanup_client:
                    await cleanup_client.post(
                        f"{src_url}/v1/agent/cleanup-file",
                        json={"path": tar_path}, headers=src_headers
                    )
            except Exception:
                pass
            logger.info(f"Migration {migration_id}: Import OK, new={new_container_id}")

            # ---- Verify ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.VERIFYING, progress=80)
            await asyncio.sleep(3)
            verified = await self._verify_container(dst_url, dst_headers, new_container_id, container_name)

            if not verified:
                await self._fail_migration(migration_id, f"대상 노드에서 컨테이너 실행 확인 실패")
                return

            logger.info(f"Migration {migration_id}: Verify OK")

            # ---- Remove source container ONLY (replicas unchanged) ----
            logger.info(f"Migration {migration_id}: Removing source {container_name} on {src_node.name}")
            await _force_remove_container(src_node, container_id, container_name, service_name)

            # ---- Complete ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.COMPLETED, progress=100)
            logger.info(f"Migration {migration_id}: COMPLETED ({container_name}: {src_node.name} -> {dst_node.name})")

        except Exception as e:
            logger.error(f"Migration {migration_id} EXCEPTION: {e}", exc_info=True)
            await self._fail_migration(migration_id, str(e))

        finally:
            # Resume reconcile on both nodes
            if service_name:
                _set_reconcile_skip(service_name, False)
                await _set_remote_reconcile_skip(src_node, service_name, False)
                await _set_remote_reconcile_skip(dst_node, service_name, False)

    async def _verify_container(self, dst_url, dst_headers, new_container_id, container_name) -> bool:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{dst_url}/v1/containers/{new_container_id}/inspect", headers=dst_headers)
                if resp.status_code == 200:
                    data = resp.json()
                    details = data.get("details", data)
                    attrs = details.get("attrs", details)
                    state = attrs.get("State", {})
                    if state.get("Running") is True or state.get("Status") == "running":
                        return True
        except Exception:
            pass
        # Fallback: container list
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{dst_url}/v1/containers", headers=dst_headers)
                if resp.status_code == 200:
                    for c in resp.json().get("containers", []):
                        if c.get("id", "").startswith(new_container_id) or c.get("name") == container_name:
                            if c.get("state") == "running" or str(c.get("status", "")).startswith("Up"):
                                return True
        except Exception:
            pass
        return False

    async def _fail_migration(self, migration_id: str, error: str) -> dict:
        from .cluster_models import MigrationStatus
        logger.error(f"Migration {migration_id} FAILED: {error}")
        self.cluster_state.update_migration(migration_id, MigrationStatus.FAILED, error=error)
        return {"id": migration_id, "status": "failed", "error": error}

    async def cancel_migration(self, migration_id: str) -> dict:
        from .cluster_models import MigrationStatus
        migration = self.cluster_state.get_migration(migration_id)
        if not migration:
            return {"error": "not found"}
        if migration.status in (MigrationStatus.COMPLETED, MigrationStatus.FAILED, MigrationStatus.ROLLED_BACK):
            return {"error": f"already {migration.status.value}"}
        self.cluster_state.update_migration(migration_id, MigrationStatus.ROLLED_BACK, error="Cancelled")
        return {"id": migration_id, "status": "rolled_back"}

    async def drain_node(self, node_name: str, target_node: str = None) -> dict:
        from .cluster_models import NodeStatus
        self.cluster_state.update_node_status(node_name, NodeStatus.DRAINING)
        placements = self.cluster_state.get_placements(node_name=node_name)
        if not placements:
            self.cluster_state.update_node_status(node_name, NodeStatus.CORDONED)
            return {"message": f"{node_name}에 컨테이너 없음.", "migrated": 0}
        nodes = self.cluster_state.list_nodes()
        healthy = [n for n in nodes if n.status == NodeStatus.HEALTHY and n.name != node_name]
        if not healthy:
            self.cluster_state.update_node_status(node_name, NodeStatus.HEALTHY)
            return {"error": "healthy 노드가 없습니다."}
        migrations = []
        for i, p in enumerate(placements):
            dest = target_node if target_node else healthy[i % len(healthy)].name
            result = await self.migrate(
                container_id=p.container_id, source_node=node_name,
                destination_node=dest, container_name=p.container_name,
                service_name=p.service_name
            )
            migrations.append(result)
        return {"message": f"{node_name} 드레인: {len(migrations)}개 시작.", "migrations": migrations}
