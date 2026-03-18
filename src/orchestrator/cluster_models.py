from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class NodeStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    CORDONED = "cordoned"
    DRAINING = "draining"

class NodeResources(BaseModel):
    cpu_cores: int = 0
    cpu_used_percent: float = 0.0
    memory_total_mb: int = 0
    memory_used_mb: int = 0
    disk_total_gb: float = 0.0
    disk_used_gb: float = 0.0
    net_rx_mb: float = 0.0
    net_tx_mb: float = 0.0
    containers_running: int = 0
    containers_total: int = 0

class NodeInfo(BaseModel):
    name: str
    address: str  # IP:port
    token: str = ""
    status: NodeStatus = NodeStatus.HEALTHY
    role: str = "worker"  # master | worker
    labels: Dict[str, str] = {}
    resources: Optional[NodeResources] = None
    last_heartbeat: Optional[str] = None
    registered_at: str = ""
    container_count: int = 0

class ContainerPlacement(BaseModel):
    container_id: str
    container_name: str
    service_name: Optional[str] = None
    image: str
    node_name: str
    status: str
    cpu_percent: float = 0.0
    memory_mb: float = 0.0

class HeartbeatPayload(BaseModel):
    node_name: str
    timestamp: str
    resources: NodeResources
    containers: List[ContainerPlacement] = []
    agent_version: str = "0.1.0"

class HeartbeatResponse(BaseModel):
    ack: bool = True
    commands: List[dict] = []  # commands for the agent to execute

class ScheduleConstraints(BaseModel):
    node_affinity: Optional[List[str]] = None
    node_anti_affinity: Optional[List[str]] = None
    spread: bool = True
    memory_required_mb: Optional[int] = None
    cpu_required: Optional[float] = None

class ScheduleRequest(BaseModel):
    service_name: str
    image: str
    replicas: int = 1
    memory_limit: Optional[str] = None
    cpu_limit: Optional[str] = None
    environment: List[str] = []
    volumes: List[str] = []
    ports: List[str] = []
    constraints: Optional[ScheduleConstraints] = None

class ScheduleDecision(BaseModel):
    node_name: str
    count: int  # how many replicas on this node

class MigrationRequest(BaseModel):
    container_id: str
    source_node: str
    destination_node: str
    service_name: Optional[str] = None

class MigrationStatus(str, Enum):
    PENDING = "pending"
    EXPORTING = "exporting"
    TRANSFERRING = "transferring"
    IMPORTING = "importing"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"

class MigrationInfo(BaseModel):
    id: str
    container_id: str
    container_name: str = ""
    source_node: str
    destination_node: str
    status: MigrationStatus = MigrationStatus.PENDING
    started_at: str = ""
    completed_at: Optional[str] = None
    error: Optional[str] = None
    progress: int = 0  # 0-100

class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class AlertInfo(BaseModel):
    id: str
    node_name: str
    severity: AlertSeverity
    condition: str
    message: str
    created_at: str
    acknowledged: bool = False

class ClusterStatus(BaseModel):
    total_nodes: int = 0
    healthy_nodes: int = 0
    total_containers: int = 0
    total_cpu_cores: int = 0
    total_memory_mb: int = 0
    used_memory_mb: int = 0
    avg_cpu_percent: float = 0.0
    nodes: List[NodeInfo] = []
    services: Dict[str, dict] = {}
    active_migrations: List[MigrationInfo] = []
    alerts: List[AlertInfo] = []
