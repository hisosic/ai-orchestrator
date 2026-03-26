"""
Worker Agent Module.
Runs on each worker node, periodically sends heartbeats to the master,
and handles container migration export/import operations.
"""

import os
import io
import time
import logging
import threading
import platform
from typing import Optional, List
from datetime import datetime

import docker
import psutil  # will need this for system resources - may need to add to requirements

logger = logging.getLogger(__name__)

class WorkerAgent:
    """Agent that runs on worker nodes to communicate with the master."""

    def __init__(self, node_name: str = None, master_url: str = None, token: str = None):
        self.node_name = node_name or os.environ.get("ORCHESTRATOR_NODE_NAME", platform.node())
        self.master_url = master_url or os.environ.get("ORCHESTRATOR_MASTER_URL", "")
        self.token = token or os.environ.get("ORCHESTRATOR_API_TOKEN", "")
        self.heartbeat_interval = int(os.environ.get("ORCHESTRATOR_HEARTBEAT_INTERVAL", "10"))
        self.agent_version = "0.2.0"
        self._running = False
        self._thread: Optional[threading.Thread] = None

        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            logger.error(f"Failed to connect to Docker: {e}")
            self.docker_client = None

    def start(self):
        """Start the heartbeat loop in a background thread."""
        if not self.master_url:
            logger.info("No master URL configured, agent heartbeat disabled")
            return

        self._running = True
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()
        logger.info(f"Worker agent started: node={self.node_name}, master={self.master_url}, interval={self.heartbeat_interval}s")

    def stop(self):
        """Stop the heartbeat loop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _heartbeat_loop(self):
        """Periodically send heartbeat to master."""
        import httpx

        while self._running:
            try:
                payload = self._build_heartbeat()
                headers = {}
                if self.token:
                    headers["Authorization"] = f"Bearer {self.token}"

                with httpx.Client(timeout=10) as client:
                    resp = client.post(
                        f"{self.master_url}/v1/cluster/heartbeat",
                        json=payload,
                        headers=headers
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        # Process any commands from master
                        commands = data.get("commands", [])
                        for cmd in commands:
                            self._handle_master_command(cmd)
                    else:
                        logger.warning(f"Heartbeat failed: {resp.status_code}")
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

            time.sleep(self.heartbeat_interval)

    def _build_heartbeat(self) -> dict:
        """Build heartbeat payload with system resources and container info."""
        resources = self.get_node_resources()
        containers = self.get_managed_containers()

        return {
            "node_name": self.node_name,
            "timestamp": datetime.utcnow().isoformat(),
            "resources": resources,
            "containers": containers,
            "agent_version": self.agent_version
        }

    def get_node_resources(self) -> dict:
        """Get current node resource usage."""
        try:
            import psutil
            cpu_count = psutil.cpu_count()
            cpu_percent = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage("/")

            containers_running = 0
            containers_total = 0
            if self.docker_client:
                containers_total = len(self.docker_client.containers.list(all=True))
                containers_running = len(self.docker_client.containers.list())

            # Network I/O - try host /proc/net/dev first, fallback to psutil
            net_rx_mb, net_tx_mb = self._get_host_network()

            return {
                "cpu_cores": cpu_count,
                "cpu_used_percent": round(cpu_percent, 1),
                "memory_total_mb": int(mem.total / 1024 / 1024),
                "memory_used_mb": int(mem.used / 1024 / 1024),
                "disk_total_gb": round(disk.total / 1024 / 1024 / 1024, 1),
                "disk_used_gb": round(disk.used / 1024 / 1024 / 1024, 1),
                "net_rx_mb": net_rx_mb,
                "net_tx_mb": net_tx_mb,
                "containers_running": containers_running,
                "containers_total": containers_total
            }
        except ImportError:
            # psutil not available, use basic info
            import shutil
            disk = shutil.disk_usage("/")

            containers_running = 0
            containers_total = 0
            if self.docker_client:
                containers_total = len(self.docker_client.containers.list(all=True))
                containers_running = len(self.docker_client.containers.list())

            return {
                "cpu_cores": os.cpu_count() or 1,
                "cpu_used_percent": 0.0,
                "memory_total_mb": 0,
                "memory_used_mb": 0,
                "disk_total_gb": round(disk.total / 1024 / 1024 / 1024, 1),
                "disk_used_gb": round(disk.used / 1024 / 1024 / 1024, 1),
                "net_rx_mb": 0.0,
                "net_tx_mb": 0.0,
                "containers_running": containers_running,
                "containers_total": containers_total
            }

    def _get_host_network(self) -> tuple:
        """Read host network stats from /host/net/dev (populated by host cron)
        or via docker API system info."""
        # Method 1: Host-written file
        try:
            with open("/host/net/dev", "r") as f:
                lines = f.readlines()
            rx_total = 0
            tx_total = 0
            for line in lines[2:]:
                parts = line.strip().split()
                if len(parts) < 10:
                    continue
                iface = parts[0].rstrip(":")
                if iface.startswith(("ens", "eth", "em", "bond", "enp")):
                    rx_total += int(parts[1])
                    tx_total += int(parts[9])
            if rx_total > 0 or tx_total > 0:
                return round(rx_total / 1024 / 1024, 2), round(tx_total / 1024 / 1024, 2)
        except Exception:
            pass
        # Method 2: Docker host exec (slow but works)
        try:
            if self.docker_client:
                import subprocess
                result = subprocess.run(
                    ["nsenter", "--target", "1", "--net", "cat", "/proc/net/dev"],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    rx_total = 0
                    tx_total = 0
                    for line in result.stdout.strip().split("\n")[2:]:
                        parts = line.strip().split()
                        if len(parts) < 10:
                            continue
                        iface = parts[0].rstrip(":")
                        if iface.startswith(("ens", "eth", "em", "bond", "enp")):
                            rx_total += int(parts[1])
                            tx_total += int(parts[9])
                    if rx_total > 0 or tx_total > 0:
                        return round(rx_total / 1024 / 1024, 2), round(tx_total / 1024 / 1024, 2)
        except Exception:
            pass
        return 0.0, 0.0

    def _get_container_stats(self, container) -> dict:
        """Get CPU/MEM stats for a single running container."""
        try:
            raw = container.stats(stream=False)
            # CPU
            cpu_delta = (raw.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
                         - raw.get("precpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0))
            sys_delta = (raw.get("cpu_stats", {}).get("system_cpu_usage", 0)
                         - raw.get("precpu_stats", {}).get("system_cpu_usage", 0))
            ncpu = len(raw.get("cpu_stats", {}).get("cpu_usage", {}).get("percpu_usage", []) or [1])
            cpu_pct = round((cpu_delta / sys_delta) * ncpu * 100, 2) if sys_delta > 0 and cpu_delta > 0 else 0.0
            # Memory
            mem = raw.get("memory_stats", {})
            mem_usage = round((mem.get("usage", 0) or 0) / 1024 / 1024, 1)
            mem_limit = round((mem.get("limit", 0) or 0) / 1024 / 1024, 1)
            # Network
            nets = raw.get("networks", {})
            net_rx = sum(v.get("rx_bytes", 0) for v in nets.values())
            net_tx = sum(v.get("tx_bytes", 0) for v in nets.values())
            return {
                "cpu_percent": cpu_pct,
                "memory_mb": mem_usage,
                "memory_limit_mb": mem_limit,
                "net_rx_mb": round(net_rx / 1024 / 1024, 2),
                "net_tx_mb": round(net_tx / 1024 / 1024, 2),
            }
        except Exception:
            return {"cpu_percent": 0.0, "memory_mb": 0.0, "memory_limit_mb": 0.0, "net_rx_mb": 0.0, "net_tx_mb": 0.0}

    def get_managed_containers(self) -> List[dict]:
        """Get list of ALL containers on this node with resource stats."""
        if not self.docker_client:
            return []

        from concurrent.futures import ThreadPoolExecutor, as_completed

        result = []
        try:
            containers = self.docker_client.containers.list(all=True)
            # Collect stats in parallel for running containers
            stats_map = {}
            running = [c for c in containers if c.status == "running"]
            if running:
                with ThreadPoolExecutor(max_workers=min(5, len(running))) as ex:
                    futures = {ex.submit(self._get_container_stats, c): c.id[:12] for c in running}
                    for fut in as_completed(futures):
                        try:
                            stats_map[futures[fut]] = fut.result()
                        except Exception:
                            pass

            for c in containers:
                labels = c.labels or {}
                service_name = labels.get("ai.orchestrator.service", "")
                if not service_name:
                    name = (c.name or "").strip("/")
                    if name.startswith("orch-") and "-" in name[5:]:
                        parts = name[5:].rsplit("-", 1)
                        if parts[-1].isdigit():
                            service_name = parts[0]
                        else:
                            service_name = name
                    else:
                        service_name = name
                cid = c.id[:12]
                st = stats_map.get(cid, {})

                # Extract port info: host bindings + exposed ports
                host_port = ""
                internal_port = ""
                try:
                    ports_map = c.attrs.get("NetworkSettings", {}).get("Ports") or {}
                    for container_port, bindings in ports_map.items():
                        cp = container_port.split("/")[0]  # e.g. "80/tcp" -> "80"
                        if bindings:
                            # Has host binding: take the first one
                            host_port = bindings[0].get("HostPort", "")
                            internal_port = cp
                            break
                        elif not internal_port:
                            internal_port = cp
                    if not internal_port:
                        # Fallback: check image EXPOSE
                        exposed = c.attrs.get("Config", {}).get("ExposedPorts") or {}
                        for ep in exposed:
                            internal_port = ep.split("/")[0]
                            break
                except Exception:
                    pass

                result.append({
                    "container_id": cid,
                    "container_name": (c.name or "").strip("/"),
                    "service_name": service_name,
                    "image": c.image.tags[0] if c.image.tags else str(c.image.id)[:12],
                    "node_name": self.node_name,
                    "status": c.status,
                    "cpu_percent": st.get("cpu_percent", 0.0),
                    "memory_mb": st.get("memory_mb", 0.0),
                    "memory_limit_mb": st.get("memory_limit_mb", 0.0),
                    "net_rx_mb": st.get("net_rx_mb", 0.0),
                    "net_tx_mb": st.get("net_tx_mb", 0.0),
                    "host_port": host_port,
                    "internal_port": internal_port,
                })
        except Exception as e:
            logger.error(f"Error listing containers: {e}")

        return result

    def export_container(self, container_id: str) -> bytes:
        """
        Export a container as a tar archive (docker commit + docker save).
        Returns the image tar bytes.
        """
        if not self.docker_client:
            raise RuntimeError("Docker client not available")

        container = self.docker_client.containers.get(container_id)

        # Commit the container to an image
        commit_tag = f"migration-{container_id[:12]}:latest"
        logger.info(f"Committing container {container_id} as {commit_tag}")
        image = container.commit(repository=f"migration-{container_id[:12]}", tag="latest")

        # Save the image as tar
        logger.info(f"Saving image {commit_tag}")
        chunks = []
        for chunk in image.save(named=True):
            chunks.append(chunk)
        tar_data = b"".join(chunks)

        # Get container config for recreation
        config = {
            "name": container.name,
            "image": commit_tag,
            "labels": dict(container.labels),
            "environment": container.attrs.get("Config", {}).get("Env", []),
            "ports": container.attrs.get("NetworkSettings", {}).get("Ports", {}),
            "status": container.status
        }

        logger.info(f"Exported container {container_id}: {len(tar_data)} bytes")
        return tar_data, config

    def export_container_to_file(self, container_id: str) -> tuple:
        """
        Export container to a temp file instead of memory (for large images).
        Returns (tar_file_path, config_dict, file_size).
        """
        import tempfile
        if not self.docker_client:
            raise RuntimeError("Docker client not available")

        container = self.docker_client.containers.get(container_id)

        # Commit the container to an image
        commit_tag = f"migration-{container_id[:12]}:latest"
        logger.info(f"Committing container {container_id} as {commit_tag}")
        image = container.commit(repository=f"migration-{container_id[:12]}", tag="latest")

        # Save image to temp file (streaming — no full-image memory load)
        tar_path = tempfile.mktemp(suffix=".tar", prefix="migrate-")
        logger.info(f"Saving image {commit_tag} to {tar_path}")
        total = 0
        with open(tar_path, "wb") as f:
            for chunk in image.save(named=True):
                f.write(chunk)
                total += len(chunk)

        config = {
            "name": container.name,
            "image": commit_tag,
            "labels": dict(container.labels),
            "environment": container.attrs.get("Config", {}).get("Env", []),
            "ports": container.attrs.get("NetworkSettings", {}).get("Ports", {}),
            "status": container.status,
        }
        logger.info(f"Exported container {container_id} to file: {tar_path} ({total} bytes)")
        return tar_path, config, total

    def import_container_from_file(self, tar_path: str, config: dict) -> str:
        """
        Import a container from a tar file on disk.
        Returns the new container ID.
        """
        if not self.docker_client:
            raise RuntimeError("Docker client not available")

        logger.info(f"Loading migration image from {tar_path}")
        with open(tar_path, "rb") as f:
            images = self.docker_client.images.load(f)
        if not images:
            raise RuntimeError("Failed to load image from tar")

        loaded_image = images[0]
        image_name = loaded_image.tags[0] if loaded_image.tags else loaded_image.id
        return self._run_migrated(image_name, config)

    def import_container(self, tar_data: bytes, config: dict) -> str:
        """
        Import a container from migration tar data (in-memory).
        Returns the new container ID.
        """
        if not self.docker_client:
            raise RuntimeError("Docker client not available")

        logger.info(f"Loading migration image ({len(tar_data)} bytes)")
        images = self.docker_client.images.load(tar_data)
        if not images:
            raise RuntimeError("Failed to load image from tar")

        loaded_image = images[0]
        image_name = loaded_image.tags[0] if loaded_image.tags else loaded_image.id
        return self._run_migrated(image_name, config)

    def _run_migrated(self, image_name: str, config: dict) -> str:
        """Run a container from a migrated image with the given config."""
        name = config.get("name", "migrated")
        labels = config.get("labels", {})
        labels["ai.orchestrator.managed"] = "true"
        labels["ai.orchestrator.migrated"] = "true"
        env_list = config.get("environment", [])

        # Remove existing container with same name if exists
        try:
            existing = self.docker_client.containers.get(name)
            logger.info(f"Removing existing container with name: {name}")
            existing.stop(timeout=5)
            existing.remove(force=True)
        except Exception:
            pass

        logger.info(f"Starting migrated container: {name} from {image_name}")
        container = self.docker_client.containers.run(
            image_name,
            name=name,
            labels=labels,
            environment=env_list,
            detach=True,
            remove=False,
        )
        logger.info(f"Migrated container started: {container.id[:12]}")
        return container.id[:12]

    def stop_container(self, container_id: str):
        """Stop and remove a container."""
        if not self.docker_client:
            raise RuntimeError("Docker client not available")

        container = self.docker_client.containers.get(container_id)
        container.stop(timeout=10)
        container.remove()
        logger.info(f"Container {container_id} stopped and removed")

    def _handle_master_command(self, cmd: dict):
        """Handle a command from the master node."""
        action = cmd.get("action")
        logger.info(f"Received master command: {action}")

        if action == "stop_container":
            container_id = cmd.get("container_id")
            if container_id:
                try:
                    self.stop_container(container_id)
                except Exception as e:
                    logger.error(f"Failed to stop container {container_id}: {e}")
        elif action == "run_container":
            # Delegate to local runtime
            pass
        else:
            logger.warning(f"Unknown master command: {action}")
