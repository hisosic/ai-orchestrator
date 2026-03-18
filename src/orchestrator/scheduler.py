"""
Multi-node container scheduler.
Decides container placement across cluster nodes.

Strategies:
- least-loaded: Place on node with most available memory (default)
- spread: Distribute replicas across maximum number of distinct nodes
- binpack: Pack containers onto fewest nodes possible
"""

import logging
from typing import List, Optional, Dict, Tuple

logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self, cluster_state):
        """cluster_state is a ClusterStateManager instance."""
        self.cluster_state = cluster_state

    def schedule(self, service_name: str, image: str, replicas: int,
                 constraints=None, strategy: str = "spread") -> List[dict]:
        """
        Decide placement for `replicas` containers of a service.

        Returns list of: {"node_name": str, "count": int}
        representing how many replicas to place on each node.

        Algorithm:
        1. Get all schedulable nodes (healthy, not cordoned/draining)
        2. Filter by affinity/anti-affinity constraints
        3. Check resource requirements
        4. Apply strategy to distribute replicas
        """
        nodes = self.cluster_state.list_nodes()

        # Filter schedulable nodes
        schedulable = [n for n in nodes if n.status.value in ("healthy",)]

        if not schedulable:
            raise SchedulerError("No schedulable nodes available")

        # Apply affinity constraints
        if constraints:
            if constraints.node_affinity:
                schedulable = [n for n in schedulable if n.name in constraints.node_affinity]
            if constraints.node_anti_affinity:
                schedulable = [n for n in schedulable if n.name not in constraints.node_anti_affinity]

            # Filter by resource requirements
            if constraints.memory_required_mb:
                schedulable = [n for n in schedulable
                             if n.resources and (n.resources.memory_total_mb - n.resources.memory_used_mb) >= constraints.memory_required_mb]
            if constraints.cpu_required:
                schedulable = [n for n in schedulable
                             if n.resources and n.resources.cpu_cores * (100 - n.resources.cpu_used_percent) / 100 >= constraints.cpu_required]

        if not schedulable:
            raise SchedulerError("No nodes satisfy scheduling constraints")

        # Get existing placements for anti-affinity (spread replicas of same service)
        existing_placements = self.cluster_state.get_placements(service_name=service_name)
        existing_per_node = {}
        for p in existing_placements:
            existing_per_node[p.node_name] = existing_per_node.get(p.node_name, 0) + 1

        if strategy == "spread":
            return self._strategy_spread(schedulable, replicas, existing_per_node)
        elif strategy == "binpack":
            return self._strategy_binpack(schedulable, replicas)
        else:  # least-loaded
            return self._strategy_least_loaded(schedulable, replicas)

    def _strategy_spread(self, nodes, replicas: int, existing_per_node: Dict[str, int]) -> List[dict]:
        """Spread replicas across as many nodes as possible, considering existing placement."""
        result = []
        # Sort nodes by existing replica count (ascending) then by available memory (descending)
        scored = []
        for n in nodes:
            existing = existing_per_node.get(n.name, 0)
            avail_mem = (n.resources.memory_total_mb - n.resources.memory_used_mb) if n.resources else 0
            scored.append((n, existing, avail_mem))
        scored.sort(key=lambda x: (x[1], -x[2]))

        # Round-robin distribute
        placement = {}
        for i in range(replicas):
            node = scored[i % len(scored)][0]
            placement[node.name] = placement.get(node.name, 0) + 1

        return [{"node_name": name, "count": count} for name, count in placement.items()]

    def _strategy_least_loaded(self, nodes, replicas: int) -> List[dict]:
        """Place all replicas on the least loaded node(s)."""
        scored = []
        for n in nodes:
            used_pct = n.resources.memory_used_mb / max(n.resources.memory_total_mb, 1) * 100 if n.resources else 100
            scored.append((n, used_pct))
        scored.sort(key=lambda x: x[1])

        # Place on least loaded, spreading if single node would be overloaded
        placement = {}
        for i in range(replicas):
            node = scored[i % len(scored)][0]
            placement[node.name] = placement.get(node.name, 0) + 1

        return [{"node_name": name, "count": count} for name, count in placement.items()]

    def _strategy_binpack(self, nodes, replicas: int) -> List[dict]:
        """Pack onto fewest nodes. Fill most-loaded first."""
        scored = []
        for n in nodes:
            avail_mem = (n.resources.memory_total_mb - n.resources.memory_used_mb) if n.resources else 0
            scored.append((n, avail_mem))
        scored.sort(key=lambda x: x[1])  # least available first (most packed)

        placement = {}
        remaining = replicas
        for node, avail in scored:
            if remaining <= 0:
                break
            # Estimate capacity: at least 1, or based on typical container size (256MB default)
            capacity = max(1, avail // 256)
            assign = min(remaining, capacity)
            if assign > 0:
                placement[node.name] = assign
                remaining -= assign

        # If still remaining, distribute to first node
        if remaining > 0:
            first_node = scored[0][0].name
            placement[first_node] = placement.get(first_node, 0) + remaining

        return [{"node_name": name, "count": count} for name, count in placement.items()]

    def find_best_node_for_migration(self, container_id: str, exclude_node: str) -> Optional[str]:
        """Find the best target node for migrating a container away from exclude_node."""
        nodes = self.cluster_state.list_nodes()
        schedulable = [n for n in nodes if n.status.value == "healthy" and n.name != exclude_node]

        if not schedulable:
            return None

        # Pick least loaded
        schedulable.sort(key=lambda n: n.resources.memory_used_mb / max(n.resources.memory_total_mb, 1) if n.resources else 1.0)
        return schedulable[0].name


class SchedulerError(Exception):
    pass
