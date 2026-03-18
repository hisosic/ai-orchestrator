"""Pydantic models for API and intents."""
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class IntentAction(str, Enum):
    SCALE = "scale"
    DEPLOY = "deploy"
    RESOURCE = "resource"
    STOP = "stop"
    LIST = "list"
    MIGRATE = "migrate"
    DRAIN = "drain"
    CLUSTER_STATUS = "cluster_status"
    NODE_LIST = "node_list"
    UNKNOWN = "unknown"


class ParsedIntent(BaseModel):
    action: IntentAction
    service_name: Optional[str] = None
    replicas: Optional[int] = None
    image: Optional[str] = None
    memory: Optional[str] = None  # e.g. "512m", "1g"
    cpu: Optional[str] = None    # e.g. "0.5", "1"
    target_node: Optional[str] = None  # for migrate/drain commands
    raw: str = ""


class CommandRequest(BaseModel):
    command: str = Field(..., description="Natural language command")
    dry_run: bool = Field(False, description="If true, only return planned actions")


class CommandResponse(BaseModel):
    success: bool
    intent: Optional[ParsedIntent] = None
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class ServiceInfo(BaseModel):
    name: str
    image: str
    replicas: int
    memory_limit: Optional[str] = None
    cpu_limit: Optional[str] = None
    status: str
    container_ids: List[str] = Field(default_factory=list)


class RunContainerRequest(BaseModel):
    image: str = Field(..., description="Image name or id")
    name: Optional[str] = None
    memory: Optional[str] = None
    cpu: Optional[str] = None
    replicas: Optional[int] = Field(1, description="Number of containers to run")
    use_internal_network: bool = True
    environment: Optional[List[str]] = Field(None, description='Env vars e.g. ["KEY=value"]')
    volumes: Optional[List[str]] = Field(None, description='Mounts e.g. ["/host:/container", "/host2:/cont2:ro"]')
    ports: Optional[List[str]] = Field(None, description='Ports e.g. ["8080:80", "8443:443"]')
    user: Optional[str] = Field(None, description="User e.g. uid:gid or username")
    volume_mode: Optional[str] = Field("shared", description="Volume mode: shared or per_replica")


class ScaleServiceRequest(BaseModel):
    service_name: str
    replicas: int


class ActionExecuteRequest(BaseModel):
    """Drag-and-drop builder: execute by action + target + params."""
    action: str = Field(..., description="deploy|scale|resource|stop|run_image|container_stop|container_remove|image_remove|list")
    service_name: Optional[str] = None
    image: Optional[str] = None
    replicas: Optional[int] = None
    memory: Optional[str] = None
    cpu: Optional[str] = None
    container_id: Optional[str] = None
    image_id: Optional[str] = None
