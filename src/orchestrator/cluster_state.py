import sqlite3
import json
import os
import threading
import uuid
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from .cluster_models import (
    NodeInfo, NodeStatus, NodeResources, ContainerPlacement,
    MigrationInfo, MigrationStatus, AlertInfo, AlertSeverity,
    ClusterStatus
)


class ClusterStateManager:
    """SQLite-based state manager for cluster master node."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.environ.get("ORCHESTRATOR_STATE_DIR", "/data") + "/cluster.db"
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS nodes (
                    name TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    token TEXT DEFAULT '',
                    status TEXT DEFAULT 'healthy',
                    role TEXT DEFAULT 'worker',
                    labels TEXT DEFAULT '{}',
                    resources TEXT,
                    last_heartbeat TEXT,
                    registered_at TEXT NOT NULL,
                    container_count INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS container_placements (
                    container_id TEXT PRIMARY KEY,
                    container_name TEXT,
                    service_name TEXT,
                    image TEXT,
                    node_name TEXT NOT NULL,
                    status TEXT,
                    cpu_percent REAL DEFAULT 0,
                    memory_mb REAL DEFAULT 0,
                    updated_at TEXT,
                    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS services (
                    name TEXT PRIMARY KEY,
                    image TEXT NOT NULL,
                    desired_replicas INTEGER DEFAULT 1,
                    memory_limit TEXT,
                    cpu_limit TEXT,
                    environment TEXT DEFAULT '[]',
                    volumes TEXT DEFAULT '[]',
                    ports TEXT DEFAULT '[]',
                    user_spec TEXT,
                    volume_mode TEXT DEFAULT 'shared',
                    schedule_constraints TEXT
                );

                CREATE TABLE IF NOT EXISTS migrations (
                    id TEXT PRIMARY KEY,
                    container_id TEXT,
                    container_name TEXT DEFAULT '',
                    source_node TEXT,
                    destination_node TEXT,
                    status TEXT DEFAULT 'pending',
                    started_at TEXT,
                    completed_at TEXT,
                    error TEXT,
                    progress INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    node_name TEXT,
                    severity TEXT DEFAULT 'warning',
                    condition TEXT,
                    message TEXT,
                    created_at TEXT,
                    acknowledged INTEGER DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_placements_node ON container_placements(node_name);
                CREATE INDEX IF NOT EXISTS idx_placements_service ON container_placements(service_name);
                CREATE INDEX IF NOT EXISTS idx_migrations_status ON migrations(status);
                CREATE INDEX IF NOT EXISTS idx_alerts_ack ON alerts(acknowledged);
            """)

    # --- Node Management ---

    def register_node(self, node: NodeInfo) -> NodeInfo:
        with self._lock, self._get_conn() as conn:
            now = datetime.utcnow().isoformat()
            conn.execute("""
                INSERT INTO nodes (name, address, token, status, role, labels, resources, last_heartbeat, registered_at, container_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    address=excluded.address,
                    token=excluded.token,
                    status=excluded.status,
                    role=excluded.role,
                    labels=excluded.labels,
                    resources=excluded.resources,
                    last_heartbeat=excluded.last_heartbeat
            """, (
                node.name, node.address, node.token, node.status.value,
                node.role, json.dumps(node.labels),
                node.resources.model_dump_json() if node.resources else None,
                now, node.registered_at or now, node.container_count
            ))
        node.registered_at = node.registered_at or now
        node.last_heartbeat = now
        return node

    def remove_node(self, name: str) -> bool:
        with self._lock, self._get_conn() as conn:
            cursor = conn.execute("DELETE FROM nodes WHERE name=?", (name,))
            return cursor.rowcount > 0

    def get_node(self, name: str) -> Optional[NodeInfo]:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE name=?", (name,)).fetchone()
            if not row:
                return None
            return self._row_to_node(row)

    def list_nodes(self, status: Optional[NodeStatus] = None) -> List[NodeInfo]:
        with self._get_conn() as conn:
            if status:
                rows = conn.execute("SELECT * FROM nodes WHERE status=?", (status.value,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM nodes").fetchall()
            return [self._row_to_node(r) for r in rows]

    def update_node_status(self, name: str, status: NodeStatus):
        with self._lock, self._get_conn() as conn:
            conn.execute("UPDATE nodes SET status=? WHERE name=?", (status.value, name))

    def _row_to_node(self, row) -> NodeInfo:
        resources = None
        if row["resources"]:
            resources = NodeResources.model_validate_json(row["resources"])
        return NodeInfo(
            name=row["name"],
            address=row["address"],
            token=row["token"],
            status=NodeStatus(row["status"]),
            role=row["role"],
            labels=json.loads(row["labels"]) if row["labels"] else {},
            resources=resources,
            last_heartbeat=row["last_heartbeat"],
            registered_at=row["registered_at"],
            container_count=row["container_count"]
        )

    # --- Heartbeat ---

    def process_heartbeat(self, node_name: str, resources: NodeResources, containers: List[ContainerPlacement]):
        now = datetime.utcnow().isoformat()
        with self._lock, self._get_conn() as conn:
            # Update node
            conn.execute("""
                UPDATE nodes SET
                    status='healthy',
                    resources=?,
                    last_heartbeat=?,
                    container_count=?
                WHERE name=?
            """, (resources.model_dump_json(), now, len(containers), node_name))

            # Update placements - remove old ones for this node
            conn.execute("DELETE FROM container_placements WHERE node_name=?", (node_name,))

            # Insert current placements
            for c in containers:
                conn.execute("""
                    INSERT INTO container_placements (container_id, container_name, service_name, image, node_name, status, cpu_percent, memory_mb, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (c.container_id, c.container_name, c.service_name, c.image, node_name, c.status, c.cpu_percent, c.memory_mb, now))

    def check_node_health(self, timeout_degraded: int = 30, timeout_offline: int = 90):
        """Check all nodes and update status based on heartbeat timeout.
        Master role nodes are excluded from heartbeat timeout checks since
        they don't send heartbeats to themselves."""
        now = datetime.utcnow()
        with self._lock, self._get_conn() as conn:
            rows = conn.execute(
                "SELECT name, status, role, last_heartbeat FROM nodes WHERE status NOT IN ('cordoned', 'draining')"
            ).fetchall()
            for row in rows:
                # Master nodes maintain their own heartbeat via self-update loop
                if row["role"] == "master":
                    continue
                if not row["last_heartbeat"]:
                    continue
                last = datetime.fromisoformat(row["last_heartbeat"])
                delta = (now - last).total_seconds()
                if delta > timeout_offline and row["status"] != "offline":
                    conn.execute("UPDATE nodes SET status='offline' WHERE name=?", (row["name"],))
                    self._create_alert_internal(conn, row["name"], "critical", f"heartbeat_timeout>{timeout_offline}s", f"Node {row['name']} is offline (no heartbeat for {int(delta)}s)")
                elif delta > timeout_degraded and row["status"] not in ("offline", "degraded"):
                    conn.execute("UPDATE nodes SET status='degraded' WHERE name=?", (row["name"],))

    # --- Container Placements ---

    def get_placements(self, service_name: Optional[str] = None, node_name: Optional[str] = None) -> List[ContainerPlacement]:
        with self._get_conn() as conn:
            query = "SELECT * FROM container_placements WHERE 1=1"
            params = []
            if service_name:
                query += " AND service_name=?"
                params.append(service_name)
            if node_name:
                query += " AND node_name=?"
                params.append(node_name)
            rows = conn.execute(query, params).fetchall()
            return [ContainerPlacement(
                container_id=r["container_id"],
                container_name=r["container_name"],
                service_name=r["service_name"],
                image=r["image"],
                node_name=r["node_name"],
                status=r["status"],
                cpu_percent=r["cpu_percent"],
                memory_mb=r["memory_mb"]
            ) for r in rows]

    def get_service_placement_summary(self) -> Dict[str, dict]:
        """Get per-service, per-node container counts."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT service_name, node_name, COUNT(*) as cnt,
                       SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) as running
                FROM container_placements
                WHERE service_name IS NOT NULL
                GROUP BY service_name, node_name
            """).fetchall()
            result = {}
            for r in rows:
                svc = r["service_name"]
                if svc not in result:
                    result[svc] = {"total": 0, "running": 0, "nodes": {}}
                result[svc]["total"] += r["cnt"]
                result[svc]["running"] += r["running"]
                result[svc]["nodes"][r["node_name"]] = {"total": r["cnt"], "running": r["running"]}
            return result

    # --- Migrations ---

    def create_migration(self, migration: MigrationInfo) -> MigrationInfo:
        with self._lock, self._get_conn() as conn:
            conn.execute("""
                INSERT INTO migrations (id, container_id, container_name, source_node, destination_node, status, started_at, progress)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (migration.id, migration.container_id, migration.container_name,
                  migration.source_node, migration.destination_node,
                  migration.status.value, migration.started_at, migration.progress))
        return migration

    def update_migration(self, migration_id: str, status: MigrationStatus, progress: int = 0, error: str = None):
        with self._lock, self._get_conn() as conn:
            completed_at = datetime.utcnow().isoformat() if status in (MigrationStatus.COMPLETED, MigrationStatus.FAILED, MigrationStatus.ROLLED_BACK) else None
            conn.execute("""
                UPDATE migrations SET status=?, progress=?, error=?, completed_at=?
                WHERE id=?
            """, (status.value, progress, error, completed_at, migration_id))

    def get_migration(self, migration_id: str) -> Optional[MigrationInfo]:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM migrations WHERE id=?", (migration_id,)).fetchone()
            if not row:
                return None
            return self._row_to_migration(row)

    def list_migrations(self, active_only: bool = False) -> List[MigrationInfo]:
        with self._get_conn() as conn:
            if active_only:
                rows = conn.execute("SELECT * FROM migrations WHERE status NOT IN ('completed', 'failed', 'rolled_back') ORDER BY started_at DESC").fetchall()
            else:
                rows = conn.execute("SELECT * FROM migrations ORDER BY started_at DESC LIMIT 50").fetchall()
            return [self._row_to_migration(r) for r in rows]

    def _row_to_migration(self, row) -> MigrationInfo:
        return MigrationInfo(
            id=row["id"],
            container_id=row["container_id"],
            container_name=row["container_name"] or "",
            source_node=row["source_node"],
            destination_node=row["destination_node"],
            status=MigrationStatus(row["status"]),
            started_at=row["started_at"] or "",
            completed_at=row["completed_at"],
            error=row["error"],
            progress=row["progress"] or 0
        )

    # --- Alerts ---

    def _create_alert_internal(self, conn, node_name: str, severity: str, condition: str, message: str):
        alert_id = str(uuid.uuid4())[:8]
        now = datetime.utcnow().isoformat()
        conn.execute("""
            INSERT INTO alerts (id, node_name, severity, condition, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (alert_id, node_name, severity, condition, message, now))

    def create_alert(self, node_name: str, severity: AlertSeverity, condition: str, message: str) -> AlertInfo:
        alert_id = str(uuid.uuid4())[:8]
        now = datetime.utcnow().isoformat()
        with self._lock, self._get_conn() as conn:
            conn.execute("""
                INSERT INTO alerts (id, node_name, severity, condition, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (alert_id, node_name, severity.value, condition, message, now))
        return AlertInfo(id=alert_id, node_name=node_name, severity=severity, condition=condition, message=message, created_at=now)

    def acknowledge_alert(self, alert_id: str) -> bool:
        with self._lock, self._get_conn() as conn:
            cursor = conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,))
            return cursor.rowcount > 0

    def list_alerts(self, unacknowledged_only: bool = True) -> List[AlertInfo]:
        with self._get_conn() as conn:
            if unacknowledged_only:
                rows = conn.execute("SELECT * FROM alerts WHERE acknowledged=0 ORDER BY created_at DESC").fetchall()
            else:
                rows = conn.execute("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 100").fetchall()
            return [AlertInfo(
                id=r["id"], node_name=r["node_name"],
                severity=AlertSeverity(r["severity"]),
                condition=r["condition"], message=r["message"],
                created_at=r["created_at"],
                acknowledged=bool(r["acknowledged"])
            ) for r in rows]

    # --- Cluster Status ---

    def get_cluster_status(self) -> ClusterStatus:
        nodes = self.list_nodes()
        healthy = sum(1 for n in nodes if n.status == NodeStatus.HEALTHY)
        total_cpu = sum(n.resources.cpu_cores for n in nodes if n.resources)
        total_mem = sum(n.resources.memory_total_mb for n in nodes if n.resources)
        used_mem = sum(n.resources.memory_used_mb for n in nodes if n.resources)
        total_containers = sum(n.container_count for n in nodes)

        cpu_percents = [n.resources.cpu_used_percent for n in nodes if n.resources]
        avg_cpu = sum(cpu_percents) / len(cpu_percents) if cpu_percents else 0.0

        services = self.get_service_placement_summary()
        active_migrations = self.list_migrations(active_only=True)
        alerts = self.list_alerts(unacknowledged_only=True)

        return ClusterStatus(
            total_nodes=len(nodes),
            healthy_nodes=healthy,
            total_containers=total_containers,
            total_cpu_cores=total_cpu,
            total_memory_mb=total_mem,
            used_memory_mb=used_mem,
            avg_cpu_percent=round(avg_cpu, 1),
            nodes=nodes,
            services=services,
            active_migrations=active_migrations,
            alerts=alerts
        )

    # --- Services (cluster-level) ---

    def save_service(self, name: str, image: str, desired_replicas: int = 1, **kwargs):
        with self._lock, self._get_conn() as conn:
            conn.execute("""
                INSERT INTO services (name, image, desired_replicas, memory_limit, cpu_limit, environment, volumes, ports, user_spec, volume_mode, schedule_constraints)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    image=excluded.image,
                    desired_replicas=excluded.desired_replicas,
                    memory_limit=COALESCE(excluded.memory_limit, services.memory_limit),
                    cpu_limit=COALESCE(excluded.cpu_limit, services.cpu_limit),
                    environment=COALESCE(excluded.environment, services.environment),
                    volumes=COALESCE(excluded.volumes, services.volumes),
                    ports=COALESCE(excluded.ports, services.ports)
            """, (
                name, image, desired_replicas,
                kwargs.get("memory_limit"), kwargs.get("cpu_limit"),
                json.dumps(kwargs.get("environment", [])),
                json.dumps(kwargs.get("volumes", [])),
                json.dumps(kwargs.get("ports", [])),
                kwargs.get("user_spec"),
                kwargs.get("volume_mode", "shared"),
                json.dumps(kwargs.get("schedule_constraints")) if kwargs.get("schedule_constraints") else None
            ))

    def get_service(self, name: str) -> Optional[dict]:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM services WHERE name=?", (name,)).fetchone()
            if not row:
                return None
            return dict(row)

    def list_services(self) -> List[dict]:
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM services").fetchall()
            return [dict(r) for r in rows]
