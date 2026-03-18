"""
Service Discovery Registry.
Tracks service endpoints across the cluster for cross-node service resolution.

Services are registered by name and tracked across all nodes.
Each service has a list of endpoints (node_ip:port) where it can be reached.

DNS integration: Updates the DNS server configuration to resolve service names
to the appropriate node IPs.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class ServiceEndpoint:
    def __init__(self, node_name: str, node_address: str, container_id: str,
                 container_name: str, port: int = 0, status: str = "running"):
        self.node_name = node_name
        self.node_address = node_address  # IP:port of the node
        self.container_id = container_id
        self.container_name = container_name
        self.port = port
        self.status = status
        self.last_seen = datetime.utcnow().isoformat()

    def to_dict(self) -> dict:
        return {
            "node_name": self.node_name,
            "node_address": self.node_address,
            "container_id": self.container_id,
            "container_name": self.container_name,
            "port": self.port,
            "status": self.status,
            "last_seen": self.last_seen
        }


class ServiceRegistry:
    """In-memory service discovery registry backed by cluster state."""

    def __init__(self, cluster_state):
        self.cluster_state = cluster_state
        self._services: Dict[str, List[ServiceEndpoint]] = {}

    def refresh(self):
        """Rebuild service registry from cluster state placements."""
        self._services.clear()

        nodes = {n.name: n for n in self.cluster_state.list_nodes()}
        placements = self.cluster_state.get_placements()

        for p in placements:
            if not p.service_name or p.status != "running":
                continue

            node = nodes.get(p.node_name)
            if not node:
                continue

            endpoint = ServiceEndpoint(
                node_name=p.node_name,
                node_address=node.address,
                container_id=p.container_id,
                container_name=p.container_name,
                status=p.status
            )

            if p.service_name not in self._services:
                self._services[p.service_name] = []
            self._services[p.service_name].append(endpoint)

    def get_service(self, service_name: str) -> List[dict]:
        """Get all endpoints for a service."""
        self.refresh()
        endpoints = self._services.get(service_name, [])
        return [ep.to_dict() for ep in endpoints]

    def list_services(self) -> Dict[str, dict]:
        """List all services with endpoint counts."""
        self.refresh()
        result = {}
        for name, endpoints in self._services.items():
            running = [ep for ep in endpoints if ep.status == "running"]
            nodes = list(set(ep.node_name for ep in endpoints))
            result[name] = {
                "total_endpoints": len(endpoints),
                "running_endpoints": len(running),
                "nodes": nodes,
                "endpoints": [ep.to_dict() for ep in endpoints]
            }
        return result

    def get_service_nodes(self, service_name: str) -> List[str]:
        """Get list of node names where a service is running."""
        self.refresh()
        endpoints = self._services.get(service_name, [])
        return list(set(ep.node_name for ep in endpoints if ep.status == "running"))

    def generate_dns_config(self) -> str:
        """Generate dnsmasq-compatible DNS config for service discovery.

        Maps: {service_name}.svc.local -> node IPs
        """
        self.refresh()
        lines = ["# Auto-generated service discovery DNS config"]

        nodes = {n.name: n for n in self.cluster_state.list_nodes()}

        for service_name, endpoints in self._services.items():
            running = [ep for ep in endpoints if ep.status == "running"]
            seen_ips = set()
            for ep in running:
                # Extract IP from node address (address format: IP:port)
                ip = ep.node_address.split(":")[0] if ":" in ep.node_address else ep.node_address
                if ip not in seen_ips:
                    lines.append(f"address=/{service_name}.svc.local/{ip}")
                    seen_ips.add(ip)

        return "\n".join(lines)
