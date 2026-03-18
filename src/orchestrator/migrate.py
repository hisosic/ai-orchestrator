"""
Container Migration Controller.
Orchestrates live container migration between cluster nodes.

Migration flow:
1. Validate: Check source container exists, destination node is healthy
2. Freeze: Set source service replicas to prevent reconcile interference
3. Export: docker commit + docker save on source node
4. Transfer + Import: Stream image to destination, docker load + run
5. Verify: Check new container is running on destination
6. Cleanup: Remove source container only (not the whole service)
7. Update: Adjust replicas on source/destination nodes

Rollback: If any step after export fails, keep source container running.
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


async def _get_service_replicas(node, service_name: str) -> int:
    """Get current replicas count for a service on a remote node."""
    base_url = _node_base_url(node)
    headers = _node_headers(node)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{base_url}/v1/services", headers=headers)
            if resp.status_code == 200:
                for s in resp.json():
                    if isinstance(s, dict) and s.get("name") == service_name:
                        return s.get("replicas", 0)
    except Exception:
        pass
    return 0


async def _set_service_replicas(node, service_name: str, replicas: int):
    """Set service replicas on a remote node's local state."""
    base_url = _node_base_url(node)
    headers = _node_headers(node)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(f"{base_url}/v1/services/scale", json={
                "service_name": service_name, "replicas": replicas,
            }, headers=headers)
    except Exception as e:
        logger.warning(f"Failed to set replicas on {node.name}: {e}")


async def _force_remove_container(node, container_id: str, container_name: str):
    """Force remove a specific container on a remote node (stop + delete)."""
    base_url = _node_base_url(node)
    headers = _node_headers(node)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Stop
            try:
                await client.post(f"{base_url}/v1/containers/{container_id}/stop", headers=headers)
            except Exception:
                pass
            # Delete by ID
            resp = await client.delete(f"{base_url}/v1/containers/{container_id}", headers=headers)
            if resp.status_code != 200:
                # Fallback: delete by name
                await client.delete(f"{base_url}/v1/containers/{container_name}", headers=headers)
    except Exception as e:
        logger.warning(f"Force remove {container_name} failed: {e}")


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
            return await self._fail_migration(migration_id, f"대상 노드 '{destination_node}'가 {dst_node.status.value} 상태입니다.")

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

        try:
            # Get source replicas for later adjustment (do NOT change yet -
            # changing replicas before export causes reconcile to kill the container)
            src_replicas = 0
            if service_name:
                src_replicas = await _get_service_replicas(src_node, service_name)

            # ---- Step 1: Export from source (replicas untouched) ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.EXPORTING, progress=15)
            logger.info(f"Migration {migration_id}: Exporting {container_name} from {src_node.name}")

            async with httpx.AsyncClient(timeout=300) as client:
                export_resp = await client.post(
                    f"{src_url}/v1/agent/export/{container_id}", headers=src_headers
                )

            if export_resp.status_code != 200:
                logger.error(f"Migration {migration_id}: Export HTTP {export_resp.status_code}")
                await self._fail_migration(migration_id, f"Export HTTP 실패: {export_resp.status_code}")
                return

            export_data = export_resp.json()
            if not export_data.get("success"):
                logger.error(f"Migration {migration_id}: Export failed: {export_data.get('error')}")
                await self._fail_migration(migration_id, f"Export 실패: {export_data.get('error', 'unknown')}")
                return

            image_data_b64 = export_data.get("image_data")
            config = export_data.get("config", {})
            logger.info(f"Migration {migration_id}: Export OK ({len(image_data_b64)} chars)")

            # ---- Step 3: Transfer + Import to destination ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.TRANSFERRING, progress=40)
            self.cluster_state.update_migration(migration_id, MigrationStatus.IMPORTING, progress=60)
            logger.info(f"Migration {migration_id}: Importing on {dst_node.name}")

            async with httpx.AsyncClient(timeout=300) as client:
                import_resp = await client.post(
                    f"{dst_url}/v1/agent/import",
                    json={"image_data": image_data_b64, "config": config, "service_name": service_name},
                    headers=dst_headers
                )

            if import_resp.status_code != 200:
                logger.error(f"Migration {migration_id}: Import HTTP {import_resp.status_code}")
                await self._fail_migration(migration_id, f"Import HTTP 실패: {import_resp.status_code}")
                return

            import_result = import_resp.json()
            if not import_result.get("success"):
                logger.error(f"Migration {migration_id}: Import failed: {import_result.get('error')}")
                await self._fail_migration(migration_id, f"Import 실패: {import_result.get('error', 'unknown')}")
                return

            new_container_id = import_result.get("container_id", "")
            logger.info(f"Migration {migration_id}: Import OK, new container={new_container_id}")

            # ---- Step 4: Verify destination ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.VERIFYING, progress=80)
            await asyncio.sleep(3)

            verified = await self._verify_container(dst_url, dst_headers, new_container_id, container_name)

            if not verified:
                logger.error(f"Migration {migration_id}: Verify FAILED on {dst_node.name}")
                await self._fail_migration(migration_id, f"대상 노드에서 컨테이너 실행 확인 실패 (id={new_container_id})")
                return

            logger.info(f"Migration {migration_id}: Verify OK on {dst_node.name}")

            # ---- Step 5: Adjust replicas THEN remove source container ----
            # Reduce source replicas first so reconcile doesn't restart the container
            if service_name and src_replicas > 0:
                logger.info(f"Migration {migration_id}: Reducing source replicas {src_replicas} -> {src_replicas - 1}")
                await _set_service_replicas(src_node, service_name, max(0, src_replicas - 1))
                await asyncio.sleep(1)  # brief pause for reconcile to pick up

            logger.info(f"Migration {migration_id}: Removing source container {container_name} on {src_node.name}")
            await _force_remove_container(src_node, container_id, container_name)

            # Update destination replicas to account for the new container
            if service_name:
                dst_replicas = await _get_service_replicas(dst_node, service_name)
                logger.info(f"Migration {migration_id}: Updating destination replicas {dst_replicas} -> {dst_replicas + 1}")
                await _set_service_replicas(dst_node, service_name, dst_replicas + 1)

            # ---- Done ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.COMPLETED, progress=100)
            logger.info(f"Migration {migration_id}: COMPLETED ({container_name}: {src_node.name} -> {dst_node.name})")

        except Exception as e:
            logger.error(f"Migration {migration_id} EXCEPTION: {e}", exc_info=True)
            await self._fail_migration(migration_id, str(e))

    async def _verify_container(self, dst_url: str, dst_headers: dict, new_container_id: str, container_name: str) -> bool:
        """Verify container is running on destination."""
        # Method 1: inspect
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

        # Method 2: container list
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

    async def _rollback_replicas(self, node, service_name: str, original_replicas: int):
        """Rollback source replicas to original value on failure."""
        if service_name and original_replicas > 0:
            logger.info(f"Rollback: restoring {node.name} replicas to {original_replicas}")
            await _set_service_replicas(node, service_name, original_replicas)

    async def _fail_migration(self, migration_id: str, error: str) -> dict:
        from .cluster_models import MigrationStatus
        logger.error(f"Migration {migration_id} FAILED: {error}")
        self.cluster_state.update_migration(migration_id, MigrationStatus.FAILED, error=error)
        return {"id": migration_id, "status": "failed", "error": error}

    async def cancel_migration(self, migration_id: str) -> dict:
        from .cluster_models import MigrationStatus
        migration = self.cluster_state.get_migration(migration_id)
        if not migration:
            return {"error": f"Migration {migration_id} not found"}
        if migration.status in (MigrationStatus.COMPLETED, MigrationStatus.FAILED, MigrationStatus.ROLLED_BACK):
            return {"error": f"Migration already {migration.status.value}"}
        self.cluster_state.update_migration(migration_id, MigrationStatus.ROLLED_BACK, error="Cancelled by user")
        return {"id": migration_id, "status": "rolled_back", "message": "Migration cancelled"}

    async def drain_node(self, node_name: str, target_node: str = None) -> dict:
        from .cluster_models import NodeStatus
        self.cluster_state.update_node_status(node_name, NodeStatus.DRAINING)
        placements = self.cluster_state.get_placements(node_name=node_name)

        if not placements:
            self.cluster_state.update_node_status(node_name, NodeStatus.CORDONED)
            return {"message": f"{node_name}에 컨테이너 없음, cordoned 처리.", "migrated": 0}

        nodes = self.cluster_state.list_nodes()
        healthy_nodes = [n for n in nodes if n.status == NodeStatus.HEALTHY and n.name != node_name]

        if not healthy_nodes:
            self.cluster_state.update_node_status(node_name, NodeStatus.HEALTHY)
            return {"error": "드레인 가능한 healthy 노드가 없습니다."}

        migrations = []
        for i, placement in enumerate(placements):
            dest = target_node if target_node else healthy_nodes[i % len(healthy_nodes)].name
            result = await self.migrate(
                container_id=placement.container_id, source_node=node_name,
                destination_node=dest, container_name=placement.container_name,
                service_name=placement.service_name
            )
            migrations.append(result)

        return {"message": f"{node_name} 드레인: {len(migrations)}개 마이그레이션 시작.", "migrations": migrations}
