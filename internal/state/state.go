// Package state provides a simple file-based JSON state store for managed services and cluster nodes.
package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"ai-container-go/internal/models"
)

var mu sync.Mutex

// ClusterNode represents a registered cluster node.
type ClusterNode struct {
	Name    string `json:"name"`
	BaseURL string `json:"base_url"`
	Token   string `json:"token"`
}

// UpsertOption is a functional option for UpsertService.
type UpsertOption func(*upsertConfig)

type upsertConfig struct {
	memoryLimit  *string
	cpuLimit     *string
	containerIDs *[]string
	environment  *[]string
	volumes      *[]string
	ports        *[]string
	user         *string
	volumeMode   *string
}

// WithMemoryLimit sets the memory limit option.
func WithMemoryLimit(v string) UpsertOption {
	return func(c *upsertConfig) { c.memoryLimit = &v }
}

// WithCPULimit sets the CPU limit option.
func WithCPULimit(v string) UpsertOption {
	return func(c *upsertConfig) { c.cpuLimit = &v }
}

// WithContainerIDs sets the container IDs option.
func WithContainerIDs(v []string) UpsertOption {
	return func(c *upsertConfig) { c.containerIDs = &v }
}

// WithEnvironment sets the environment variables option.
func WithEnvironment(v []string) UpsertOption {
	return func(c *upsertConfig) { c.environment = &v }
}

// WithVolumes sets the volumes option.
func WithVolumes(v []string) UpsertOption {
	return func(c *upsertConfig) { c.volumes = &v }
}

// WithPorts sets the ports option.
func WithPorts(v []string) UpsertOption {
	return func(c *upsertConfig) { c.ports = &v }
}

// WithUser sets the user option.
func WithUser(v string) UpsertOption {
	return func(c *upsertConfig) { c.user = &v }
}

// WithVolumeMode sets the volume mode option.
func WithVolumeMode(v string) UpsertOption {
	return func(c *upsertConfig) { c.volumeMode = &v }
}

// baseDir returns the state directory, respecting ORCHESTRATOR_STATE_DIR env var.
// Falls back to ".state" relative to the executable if /data is not available.
func baseDir() string {
	base := os.Getenv("ORCHESTRATOR_STATE_DIR")
	if base == "" {
		base = "/data"
	}
	if base == "/data" {
		if err := os.MkdirAll("/data", 0o755); err != nil {
			exe, _ := os.Executable()
			base = filepath.Join(filepath.Dir(exe), "..", "..", ".state")
		}
	}
	return base
}

// statePath returns the path to services.json.
func statePath() string {
	return filepath.Join(baseDir(), "services.json")
}

// nodesPath returns the path to nodes.json.
func nodesPath() string {
	return filepath.Join(baseDir(), "nodes.json")
}

// ensureDir ensures the parent directory of the state file exists.
func ensureDir() {
	dir := filepath.Dir(statePath())
	if dir != "" {
		os.MkdirAll(dir, 0o755)
	}
}

// LoadServices reads and returns all services from the state file.
func LoadServices() map[string]map[string]any {
	mu.Lock()
	defer mu.Unlock()
	return loadServicesLocked()
}

func loadServicesLocked() map[string]map[string]any {
	ensureDir()
	path := statePath()
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]map[string]any)
	}
	var result map[string]map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return make(map[string]map[string]any)
	}
	return result
}

// SaveServices writes the services map to the state file.
func SaveServices(services map[string]map[string]any) {
	mu.Lock()
	defer mu.Unlock()
	saveServicesLocked(services)
}

func saveServicesLocked(services map[string]map[string]any) {
	ensureDir()
	path := statePath()
	data, err := json.MarshalIndent(services, "", "  ")
	if err != nil {
		return
	}
	os.WriteFile(path, data, 0o644)
}

// LoadNodes reads and returns all nodes from the nodes file.
func LoadNodes() map[string]ClusterNode {
	mu.Lock()
	defer mu.Unlock()
	return loadNodesLocked()
}

func loadNodesLocked() map[string]ClusterNode {
	ensureDir()
	path := nodesPath()
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]ClusterNode)
	}
	var result map[string]ClusterNode
	if err := json.Unmarshal(data, &result); err != nil {
		return make(map[string]ClusterNode)
	}
	if result == nil {
		return make(map[string]ClusterNode)
	}
	return result
}

// SaveNodes writes the nodes map to the nodes file.
func SaveNodes(nodes map[string]ClusterNode) {
	mu.Lock()
	defer mu.Unlock()
	saveNodesLocked(nodes)
}

func saveNodesLocked(nodes map[string]ClusterNode) {
	ensureDir()
	path := nodesPath()
	data, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		return
	}
	os.WriteFile(path, data, 0o644)
}

// UpsertNode adds or updates a cluster node.
func UpsertNode(name, baseURL, token string) {
	mu.Lock()
	defer mu.Unlock()
	nodes := loadNodesLocked()
	nodes[name] = ClusterNode{Name: name, BaseURL: baseURL, Token: token}
	saveNodesLocked(nodes)
}

// DeleteNode removes a cluster node by name.
func DeleteNode(name string) {
	mu.Lock()
	defer mu.Unlock()
	nodes := loadNodesLocked()
	delete(nodes, name)
	saveNodesLocked(nodes)
}

// ListNodes returns all cluster nodes sorted by name.
func ListNodes() []ClusterNode {
	mu.Lock()
	defer mu.Unlock()
	nodes := loadNodesLocked()
	result := make([]ClusterNode, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, n)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// GetService returns a service by name, or nil if not found.
func GetService(name string) map[string]any {
	mu.Lock()
	defer mu.Unlock()
	services := loadServicesLocked()
	svc, ok := services[name]
	if !ok {
		return nil
	}
	return svc
}

// UpsertService adds or updates a service with the given name, image, replicas, and optional fields.
func UpsertService(name, image string, replicas int, opts ...UpsertOption) {
	cfg := &upsertConfig{}
	for _, o := range opts {
		o(cfg)
	}

	mu.Lock()
	defer mu.Unlock()

	services := loadServicesLocked()
	existing, _ := services[name]
	if existing == nil {
		existing = make(map[string]any)
	}

	svc := map[string]any{
		"name":     name,
		"image":    image,
		"replicas": replicas,
	}

	// memory_limit
	if cfg.memoryLimit != nil {
		svc["memory_limit"] = *cfg.memoryLimit
	} else {
		svc["memory_limit"] = existing["memory_limit"]
	}

	// cpu_limit
	if cfg.cpuLimit != nil {
		svc["cpu_limit"] = *cfg.cpuLimit
	} else {
		svc["cpu_limit"] = existing["cpu_limit"]
	}

	// container_ids
	if cfg.containerIDs != nil {
		svc["container_ids"] = *cfg.containerIDs
	} else if ids, ok := existing["container_ids"]; ok {
		svc["container_ids"] = ids
	} else {
		svc["container_ids"] = []string{}
	}

	// environment
	if cfg.environment != nil {
		svc["environment"] = *cfg.environment
	} else {
		svc["environment"] = existing["environment"]
	}

	// volumes
	if cfg.volumes != nil {
		svc["volumes"] = *cfg.volumes
	} else {
		svc["volumes"] = existing["volumes"]
	}

	// ports
	if cfg.ports != nil {
		svc["ports"] = *cfg.ports
	} else {
		svc["ports"] = existing["ports"]
	}

	// user
	if cfg.user != nil {
		svc["user"] = *cfg.user
	} else {
		svc["user"] = existing["user"]
	}

	// volume_mode
	if cfg.volumeMode != nil {
		svc["volume_mode"] = *cfg.volumeMode
	} else if vm, ok := existing["volume_mode"]; ok {
		svc["volume_mode"] = vm
	} else {
		svc["volume_mode"] = "shared"
	}

	services[name] = svc
	saveServicesLocked(services)
}

// UpdateServiceContainers updates the container IDs for an existing service.
func UpdateServiceContainers(name string, containerIDs []string) {
	mu.Lock()
	defer mu.Unlock()
	services := loadServicesLocked()
	svc, ok := services[name]
	if !ok {
		return
	}
	svc["container_ids"] = containerIDs
	services[name] = svc
	saveServicesLocked(services)
}

// DeleteService removes a service by name.
func DeleteService(name string) {
	mu.Lock()
	defer mu.Unlock()
	services := loadServicesLocked()
	delete(services, name)
	saveServicesLocked(services)
}

// ListServices returns all services as ServiceInfo structs.
func ListServices() []models.ServiceInfo {
	mu.Lock()
	defer mu.Unlock()
	raw := loadServicesLocked()
	result := make([]models.ServiceInfo, 0, len(raw))
	for _, s := range raw {
		name, _ := s["name"].(string)
		image, _ := s["image"].(string)

		replicas := 1
		if r, ok := s["replicas"].(float64); ok {
			replicas = int(r)
		}

		memoryLimit, _ := s["memory_limit"].(string)
		cpuLimit, _ := s["cpu_limit"].(string)

		var containerIDs []string
		if ids, ok := s["container_ids"].([]any); ok {
			for _, id := range ids {
				if sid, ok := id.(string); ok {
					containerIDs = append(containerIDs, sid)
				}
			}
		}

		status := "stopped"
		if len(containerIDs) > 0 {
			status = "running"
		}

		result = append(result, models.ServiceInfo{
			Name:         name,
			Image:        image,
			Replicas:     replicas,
			MemoryLimit:  memoryLimit,
			CPULimit:     cpuLimit,
			Status:       status,
			ContainerIDs: containerIDs,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}
