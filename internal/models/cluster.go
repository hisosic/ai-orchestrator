package models

// NodeStatus enum as string type
type NodeStatus string

const (
	NodeHealthy  NodeStatus = "healthy"
	NodeDegraded NodeStatus = "degraded"
	NodeOffline  NodeStatus = "offline"
	NodeCordoned NodeStatus = "cordoned"
	NodeDraining NodeStatus = "draining"
)

type NodeResources struct {
	CPUCores          int     `json:"cpu_cores"`
	CPUUsedPercent    float64 `json:"cpu_used_percent"`
	MemoryTotalMB     int     `json:"memory_total_mb"`
	MemoryUsedMB      int     `json:"memory_used_mb"`
	DiskTotalGB       float64 `json:"disk_total_gb"`
	DiskUsedGB        float64 `json:"disk_used_gb"`
	NetRxMB           float64 `json:"net_rx_mb"`
	NetTxMB           float64 `json:"net_tx_mb"`
	ContainersRunning int     `json:"containers_running"`
	ContainersTotal   int     `json:"containers_total"`
}

type NodeInfo struct {
	Name           string            `json:"name"`
	Address        string            `json:"address"`
	Token          string            `json:"token,omitempty"`
	Status         NodeStatus        `json:"status"`
	Role           string            `json:"role"`
	Labels         map[string]string `json:"labels,omitempty"`
	Resources      *NodeResources    `json:"resources,omitempty"`
	LastHeartbeat  string            `json:"last_heartbeat,omitempty"`
	RegisteredAt   string            `json:"registered_at,omitempty"`
	ContainerCount int               `json:"container_count"`
}

type ContainerPlacement struct {
	ContainerID   string  `json:"container_id"`
	ContainerName string  `json:"container_name"`
	ServiceName   string  `json:"service_name,omitempty"`
	Image         string  `json:"image"`
	NodeName      string  `json:"node_name"`
	Status        string  `json:"status"`
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryMB      float64 `json:"memory_mb"`
	MemoryLimitMB float64 `json:"memory_limit_mb"`
	NetRxMB       float64 `json:"net_rx_mb"`
	NetTxMB       float64 `json:"net_tx_mb"`
}

type HeartbeatPayload struct {
	NodeName     string               `json:"node_name"`
	Timestamp    string               `json:"timestamp"`
	Resources    NodeResources        `json:"resources"`
	Containers   []ContainerPlacement `json:"containers"`
	AgentVersion string               `json:"agent_version"`
}

type HeartbeatResponse struct {
	Ack      bool             `json:"ack"`
	Commands []map[string]any `json:"commands"`
}

type ScheduleConstraints struct {
	NodeAffinity     []string `json:"node_affinity,omitempty"`
	NodeAntiAffinity []string `json:"node_anti_affinity,omitempty"`
	Spread           bool     `json:"spread"`
	MemoryRequiredMB *int     `json:"memory_required_mb,omitempty"`
	CPURequired      *float64 `json:"cpu_required,omitempty"`
}

type ScheduleRequest struct {
	ServiceName string               `json:"service_name"`
	Image       string               `json:"image"`
	Replicas    int                  `json:"replicas"`
	MemoryLimit string               `json:"memory_limit,omitempty"`
	CPULimit    string               `json:"cpu_limit,omitempty"`
	Environment []string             `json:"environment,omitempty"`
	Volumes     []string             `json:"volumes,omitempty"`
	Ports       []string             `json:"ports,omitempty"`
	Constraints *ScheduleConstraints `json:"constraints,omitempty"`
}

type ScheduleDecision struct {
	NodeName string `json:"node_name"`
	Count    int    `json:"count"`
}

type MigrationRequest struct {
	ContainerID     string `json:"container_id"`
	SourceNode      string `json:"source_node"`
	DestinationNode string `json:"destination_node"`
	ServiceName     string `json:"service_name,omitempty"`
}

// MigrationStatus enum as string type
type MigrationStatus string

const (
	MigrationPending      MigrationStatus = "pending"
	MigrationExporting    MigrationStatus = "exporting"
	MigrationTransferring MigrationStatus = "transferring"
	MigrationImporting    MigrationStatus = "importing"
	MigrationVerifying    MigrationStatus = "verifying"
	MigrationCompleted    MigrationStatus = "completed"
	MigrationFailed       MigrationStatus = "failed"
	MigrationRolledBack   MigrationStatus = "rolled_back"
)

type MigrationInfo struct {
	ID              string          `json:"id"`
	ContainerID     string          `json:"container_id"`
	ContainerName   string          `json:"container_name"`
	SourceNode      string          `json:"source_node"`
	DestinationNode string          `json:"destination_node"`
	Status          MigrationStatus `json:"status"`
	StartedAt       string          `json:"started_at"`
	CompletedAt     string          `json:"completed_at,omitempty"`
	Error           string          `json:"error,omitempty"`
	Progress        int             `json:"progress"`
}

// AlertSeverity enum as string type
type AlertSeverity string

const (
	SeverityInfo     AlertSeverity = "info"
	SeverityWarning  AlertSeverity = "warning"
	SeverityCritical AlertSeverity = "critical"
)

type AlertInfo struct {
	ID           string        `json:"id"`
	NodeName     string        `json:"node_name"`
	Severity     AlertSeverity `json:"severity"`
	Condition    string        `json:"condition"`
	Message      string        `json:"message"`
	CreatedAt    string        `json:"created_at"`
	Acknowledged bool          `json:"acknowledged"`
}

type ClusterStatus struct {
	TotalNodes       int                `json:"total_nodes"`
	HealthyNodes     int                `json:"healthy_nodes"`
	TotalContainers  int                `json:"total_containers"`
	TotalCPUCores    int                `json:"total_cpu_cores"`
	TotalMemoryMB    int                `json:"total_memory_mb"`
	UsedMemoryMB     int                `json:"used_memory_mb"`
	AvgCPUPercent    float64            `json:"avg_cpu_percent"`
	Nodes            []NodeInfo         `json:"nodes"`
	Services         map[string]any     `json:"services"`
	ActiveMigrations []MigrationInfo    `json:"active_migrations"`
	Alerts           []AlertInfo        `json:"alerts"`
}
