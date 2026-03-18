"""
Alert Rules Engine.
Monitors cluster node health metrics from heartbeat data and generates alerts.

Alert conditions:
- Node CPU > 90% for sustained period -> critical
- Node CPU > 80% -> warning
- Node memory > 90% -> critical
- Node memory > 80% -> warning
- Node disk > 90% -> critical
- Node disk > 80% -> warning
- Node offline (heartbeat timeout) -> critical
- Node degraded -> warning
- Container count on single node > threshold -> warning
"""

import logging
import threading
import time
from typing import List
from datetime import datetime

logger = logging.getLogger(__name__)

DEFAULT_RULES = [
    {"id": "cpu_critical", "condition": "cpu_used_percent > 90", "severity": "critical", "message": "CPU usage above 90%"},
    {"id": "cpu_warning", "condition": "cpu_used_percent > 80", "severity": "warning", "message": "CPU usage above 80%"},
    {"id": "mem_critical", "condition": "memory_percent > 90", "severity": "critical", "message": "Memory usage above 90%"},
    {"id": "mem_warning", "condition": "memory_percent > 80", "severity": "warning", "message": "Memory usage above 80%"},
    {"id": "disk_critical", "condition": "disk_percent > 90", "severity": "critical", "message": "Disk usage above 90%"},
    {"id": "disk_warning", "condition": "disk_percent > 80", "severity": "warning", "message": "Disk usage above 80%"},
    {"id": "node_offline", "condition": "status == offline", "severity": "critical", "message": "Node is offline"},
    {"id": "node_degraded", "condition": "status == degraded", "severity": "warning", "message": "Node is degraded (missed heartbeats)"},
]


class AlertEngine:
    """Monitors cluster state and generates alerts."""

    def __init__(self, cluster_state):
        self.cluster_state = cluster_state
        self.rules = list(DEFAULT_RULES)
        self._running = False
        self._thread = None
        self.check_interval = 15  # seconds
        self._recent_alerts = set()  # Track recent alert keys to avoid duplicates

    def start(self):
        """Start the alert monitoring loop."""
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info("Alert engine started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _monitor_loop(self):
        while self._running:
            try:
                self._check_all_rules()
            except Exception as e:
                logger.error(f"Alert check error: {e}")
            time.sleep(self.check_interval)

    def _check_all_rules(self):
        """Check all alert rules against current cluster state."""
        from .cluster_models import AlertSeverity, NodeStatus

        # First update node health based on heartbeat timeouts
        self.cluster_state.check_node_health()

        nodes = self.cluster_state.list_nodes()

        for node in nodes:
            # Check node status alerts
            if node.status == NodeStatus.OFFLINE:
                self._fire_alert(node.name, "critical", "status == offline", f"Node {node.name} is offline")
            elif node.status == NodeStatus.DEGRADED:
                self._fire_alert(node.name, "warning", "status == degraded", f"Node {node.name} is degraded")

            # Check resource alerts if resources available
            if node.resources:
                r = node.resources

                # CPU
                if r.cpu_used_percent > 90:
                    self._fire_alert(node.name, "critical", f"cpu={r.cpu_used_percent}%", f"Node {node.name}: CPU at {r.cpu_used_percent}%")
                elif r.cpu_used_percent > 80:
                    self._fire_alert(node.name, "warning", f"cpu={r.cpu_used_percent}%", f"Node {node.name}: CPU at {r.cpu_used_percent}%")

                # Memory
                mem_percent = (r.memory_used_mb / max(r.memory_total_mb, 1)) * 100
                if mem_percent > 90:
                    self._fire_alert(node.name, "critical", f"memory={mem_percent:.0f}%", f"Node {node.name}: Memory at {mem_percent:.0f}%")
                elif mem_percent > 80:
                    self._fire_alert(node.name, "warning", f"memory={mem_percent:.0f}%", f"Node {node.name}: Memory at {mem_percent:.0f}%")

                # Disk
                disk_percent = (r.disk_used_gb / max(r.disk_total_gb, 0.1)) * 100
                if disk_percent > 90:
                    self._fire_alert(node.name, "critical", f"disk={disk_percent:.0f}%", f"Node {node.name}: Disk at {disk_percent:.0f}%")
                elif disk_percent > 80:
                    self._fire_alert(node.name, "warning", f"disk={disk_percent:.0f}%", f"Node {node.name}: Disk at {disk_percent:.0f}%")

    def _fire_alert(self, node_name: str, severity: str, condition: str, message: str):
        """Create an alert if not recently fired for the same condition."""
        from .cluster_models import AlertSeverity

        alert_key = f"{node_name}:{condition}"
        if alert_key in self._recent_alerts:
            return  # Don't duplicate

        self._recent_alerts.add(alert_key)
        # Clean old keys periodically (keep last 1000)
        if len(self._recent_alerts) > 1000:
            self._recent_alerts = set(list(self._recent_alerts)[-500:])

        self.cluster_state.create_alert(
            node_name=node_name,
            severity=AlertSeverity(severity),
            condition=condition,
            message=message
        )
        logger.warning(f"ALERT [{severity}] {node_name}: {message}")

    def add_rule(self, rule: dict):
        self.rules.append(rule)

    def clear_recent(self):
        """Clear recent alert tracking to allow re-firing."""
        self._recent_alerts.clear()
