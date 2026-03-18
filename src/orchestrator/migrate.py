"""
Container Migration Controller.
Orchestrates live container migration between cluster nodes.

Migration flow:
1. Validate: Check source container exists, destination node is healthy
2. Export: docker commit + docker save on source node
3. Transfer: Stream image tar from source to destination
4. Import: docker load + docker run on destination node
5. Verify: Check new container is running on destination
6. Cleanup: Stop and remove source container, update local state
7. Update: Update cluster state

Rollback: If any step after export fails, keep source container running.
"""

import uuid
import logging
import asyncio
from typing import Optional
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)


def _node_base_url(node) -> str:
    addr = node.address
    return addr if addr.startswith("http") else f"http://{addr}"


def _node_headers(node) -> dict:
    headers = {"Content-Type": "application/json"}
    if node.token:
        headers["Authorization"] = f"Bearer {node.token}"
    return headers


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
            id=migration_id,
            container_id=container_id,
            container_name=container_name,
            source_node=source_node,
            destination_node=destination_node,
            status=MigrationStatus.PENDING,
            started_at=now,
            progress=0
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

        asyncio.create_task(self._execute_migration(
            migration_id, src_node, dst_node, container_id, container_name, service_name
        ))

        return {
            "id": migration_id,
            "status": "pending",
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
            # ---- Step 1: Export from source ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.EXPORTING, progress=10)
            logger.info(f"Migration {migration_id}: Exporting {container_name} from {src_node.name}")

            async with httpx.AsyncClient(timeout=300) as client:
                export_resp = await client.post(
                    f"{src_url}/v1/agent/export/{container_id}",
                    headers=src_headers
                )

            if export_resp.status_code != 200:
                await self._fail_migration(migration_id, f"Export HTTP 실패: {export_resp.status_code}")
                return

            export_data = export_resp.json()
            if not export_data.get("success"):
                await self._fail_migration(migration_id, f"Export 실패: {export_data.get('error', 'unknown')}")
                return

            image_data_b64 = export_data.get("image_data")
            config = export_data.get("config", {})

            if not image_data_b64:
                await self._fail_migration(migration_id, "Export 데이터가 비어있습니다.")
                return

            # ---- Step 2: Transfer + Import to destination ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.TRANSFERRING, progress=40)
            logger.info(f"Migration {migration_id}: Transferring to {dst_node.name}")

            self.cluster_state.update_migration(migration_id, MigrationStatus.IMPORTING, progress=60)

            async with httpx.AsyncClient(timeout=300) as client:
                import_resp = await client.post(
                    f"{dst_url}/v1/agent/import",
                    json={
                        "image_data": image_data_b64,
                        "config": config,
                        "service_name": service_name
                    },
                    headers=dst_headers
                )

            if import_resp.status_code != 200:
                await self._fail_migration(migration_id, f"Import HTTP 실패: {import_resp.status_code}")
                return

            import_result = import_resp.json()
            if not import_result.get("success"):
                await self._fail_migration(migration_id, f"Import 실패: {import_result.get('error', 'unknown')}")
                return

            new_container_id = import_result.get("container_id", "")

            # ---- Step 3: Verify destination ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.VERIFYING, progress=80)
            logger.info(f"Migration {migration_id}: Verifying on {dst_node.name} (new_id={new_container_id})")

            await asyncio.sleep(2)

            # Verify: check container exists and is running via containers list
            verified = False
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    verify_resp = await client.get(
                        f"{dst_url}/v1/containers/{new_container_id}/inspect",
                        headers=dst_headers
                    )
                    if verify_resp.status_code == 200:
                        data = verify_resp.json()
                        # API returns: {"success": true, "details": {"attrs": {"State": {"Running": true}}}}
                        details = data.get("details", data)
                        attrs = details.get("attrs", details)
                        state = attrs.get("State", {})
                        if state.get("Running") is True or state.get("Status") == "running":
                            verified = True
                        else:
                            # Maybe container just started, check status field
                            status = state.get("Status", "")
                            if status in ("running", "created"):
                                verified = True
            except Exception as e:
                logger.warning(f"Migration {migration_id}: Verify inspect error: {e}")

            # Fallback verify: check containers list
            if not verified:
                try:
                    async with httpx.AsyncClient(timeout=15) as client:
                        list_resp = await client.get(f"{dst_url}/v1/containers", headers=dst_headers)
                        if list_resp.status_code == 200:
                            containers = list_resp.json().get("containers", [])
                            for c in containers:
                                if c.get("id", "").startswith(new_container_id) or c.get("name") == container_name:
                                    if c.get("state") == "running" or c.get("status", "").startswith("Up"):
                                        verified = True
                                        break
                except Exception:
                    pass

            if not verified:
                await self._fail_migration(migration_id, f"대상 노드에서 컨테이너 실행 확인 실패 (id={new_container_id})")
                return

            # ---- Step 4: Cleanup source ----
            logger.info(f"Migration {migration_id}: Cleaning up source on {src_node.name}")
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    # Stop source container
                    await client.post(f"{src_url}/v1/containers/{container_id}/stop", headers=src_headers)
                    # Remove source container
                    resp = await client.delete(f"{src_url}/v1/containers/{container_id}", headers=src_headers)
                    if resp.status_code != 200:
                        # Try by name
                        await client.delete(f"{src_url}/v1/containers/{container_name}", headers=src_headers)

                    # Update source node's local state to reduce replicas (prevent reconcile from restarting)
                    if service_name:
                        await client.post(f"{src_url}/v1/services/scale", json={
                            "service_name": service_name, "replicas": 0,
                        }, headers=src_headers)
            except Exception as e:
                logger.warning(f"Migration {migration_id}: Source cleanup warning: {e}")

            # ---- Step 5: Complete ----
            self.cluster_state.update_migration(migration_id, MigrationStatus.COMPLETED, progress=100)
            logger.info(f"Migration {migration_id}: COMPLETED ({container_name}: {src_node.name} -> {dst_node.name})")

        except Exception as e:
            logger.error(f"Migration {migration_id} failed: {e}")
            await self._fail_migration(migration_id, str(e))

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
                container_id=placement.container_id,
                source_node=node_name,
                destination_node=dest,
                container_name=placement.container_name,
                service_name=placement.service_name
            )
            migrations.append(result)

        return {
            "message": f"{node_name} 드레인: {len(migrations)}개 마이그레이션 시작.",
            "migrations": migrations
        }
