package models

// IntentAction enum as string type
type IntentAction string

const (
	IntentScale         IntentAction = "scale"
	IntentDeploy        IntentAction = "deploy"
	IntentResource      IntentAction = "resource"
	IntentStop          IntentAction = "stop"
	IntentList          IntentAction = "list"
	IntentMigrate       IntentAction = "migrate"
	IntentDrain         IntentAction = "drain"
	IntentClusterStatus IntentAction = "cluster_status"
	IntentNodeList      IntentAction = "node_list"
	IntentUnknown       IntentAction = "unknown"
)

type ParsedIntent struct {
	Action      IntentAction `json:"action"`
	ServiceName string       `json:"service_name,omitempty"`
	Replicas    *int         `json:"replicas,omitempty"`
	Image       string       `json:"image,omitempty"`
	Memory      string       `json:"memory,omitempty"`
	CPU         string       `json:"cpu,omitempty"`
	TargetNode  string       `json:"target_node,omitempty"`
	Raw         string       `json:"raw"`
}

type CommandRequest struct {
	Command string `json:"command"`
	DryRun  bool   `json:"dry_run"`
}

type CommandResponse struct {
	Success bool           `json:"success"`
	Intent  *ParsedIntent  `json:"intent,omitempty"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type ServiceInfo struct {
	Name         string   `json:"name"`
	Image        string   `json:"image"`
	Replicas     int      `json:"replicas"`
	MemoryLimit  string   `json:"memory_limit,omitempty"`
	CPULimit     string   `json:"cpu_limit,omitempty"`
	Status       string   `json:"status"`
	ContainerIDs []string `json:"container_ids"`
}

type RunContainerRequest struct {
	Image              string   `json:"image"`
	Name               string   `json:"name,omitempty"`
	Memory             string   `json:"memory,omitempty"`
	CPU                string   `json:"cpu,omitempty"`
	Replicas           int      `json:"replicas,omitempty"`
	UseInternalNetwork bool     `json:"use_internal_network"`
	Environment        []string `json:"environment,omitempty"`
	Volumes            []string `json:"volumes,omitempty"`
	Ports              []string `json:"ports,omitempty"`
	User               string   `json:"user,omitempty"`
	VolumeMode         string   `json:"volume_mode,omitempty"`
}

type ScaleServiceRequest struct {
	ServiceName string `json:"service_name"`
	Replicas    int    `json:"replicas"`
}

type ActionExecuteRequest struct {
	Action      string `json:"action"`
	ServiceName string `json:"service_name,omitempty"`
	Image       string `json:"image,omitempty"`
	Replicas    *int   `json:"replicas,omitempty"`
	Memory      string `json:"memory,omitempty"`
	CPU         string `json:"cpu,omitempty"`
	ContainerID string `json:"container_id,omitempty"`
	ImageID     string `json:"image_id,omitempty"`
}
