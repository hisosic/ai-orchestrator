// Package discovery provides service discovery registry backed by cluster state.
// It tracks service endpoints across the cluster for cross-node service resolution
// and generates DNS configuration for dnsmasq-based service name resolution.
package discovery

import (
	"fmt"
	"strings"
	"time"

	"ai-container-go/internal/models"
)

// DiscoveryStateProvider is the interface for querying cluster state
// needed by the service registry.
type DiscoveryStateProvider interface {
	ListNodes(status ...models.NodeStatus) []models.NodeInfo
	GetPlacements(serviceName, nodeName string) []models.ContainerPlacement
}

// ServiceEndpoint represents a single reachable instance of a service.
type ServiceEndpoint struct {
	NodeName      string `json:"node_name"`
	NodeAddress   string `json:"node_address"`
	ContainerID   string `json:"container_id"`
	ContainerName string `json:"container_name"`
	Port          int    `json:"port"`
	Status        string `json:"status"`
	LastSeen      string `json:"last_seen"`
}

// ServiceRegistry is an in-memory service discovery registry that rebuilds
// its endpoint map from cluster state placements on every query.
type ServiceRegistry struct {
	state    DiscoveryStateProvider
	services map[string][]ServiceEndpoint
}

// NewServiceRegistry creates a ServiceRegistry backed by the given state provider.
func NewServiceRegistry(state DiscoveryStateProvider) *ServiceRegistry {
	return &ServiceRegistry{
		state:    state,
		services: make(map[string][]ServiceEndpoint),
	}
}

// Refresh rebuilds the service registry from cluster state placements.
// Only running placements that have a service name are included.
func (r *ServiceRegistry) Refresh() {
	r.services = make(map[string][]ServiceEndpoint)

	nodes := r.state.ListNodes()
	nodeMap := make(map[string]models.NodeInfo, len(nodes))
	for _, n := range nodes {
		nodeMap[n.Name] = n
	}

	placements := r.state.GetPlacements("", "")

	for _, p := range placements {
		if p.ServiceName == "" || p.Status != "running" {
			continue
		}

		node, ok := nodeMap[p.NodeName]
		if !ok {
			continue
		}

		ep := ServiceEndpoint{
			NodeName:      p.NodeName,
			NodeAddress:   node.Address,
			ContainerID:   p.ContainerID,
			ContainerName: p.ContainerName,
			Port:          0,
			Status:        p.Status,
			LastSeen:      time.Now().UTC().Format(time.RFC3339),
		}

		r.services[p.ServiceName] = append(r.services[p.ServiceName], ep)
	}
}

// GetService returns all endpoints for the named service.
func (r *ServiceRegistry) GetService(serviceName string) []ServiceEndpoint {
	r.Refresh()
	eps, ok := r.services[serviceName]
	if !ok {
		return []ServiceEndpoint{}
	}
	return eps
}

// ListServices returns a summary for every registered service.
// Each entry contains total_endpoints, running_endpoints, nodes, and endpoints.
func (r *ServiceRegistry) ListServices() map[string]map[string]any {
	r.Refresh()

	result := make(map[string]map[string]any, len(r.services))
	for name, endpoints := range r.services {
		running := 0
		nodeSet := make(map[string]struct{})
		for _, ep := range endpoints {
			if ep.Status == "running" {
				running++
			}
			nodeSet[ep.NodeName] = struct{}{}
		}

		nodes := make([]string, 0, len(nodeSet))
		for n := range nodeSet {
			nodes = append(nodes, n)
		}

		result[name] = map[string]any{
			"total_endpoints":   len(endpoints),
			"running_endpoints": running,
			"nodes":             nodes,
			"endpoints":         endpoints,
		}
	}
	return result
}

// GetServiceNodes returns the unique node names where the named service
// has running endpoints.
func (r *ServiceRegistry) GetServiceNodes(serviceName string) []string {
	r.Refresh()

	endpoints := r.services[serviceName]
	nodeSet := make(map[string]struct{})
	for _, ep := range endpoints {
		if ep.Status == "running" {
			nodeSet[ep.NodeName] = struct{}{}
		}
	}

	nodes := make([]string, 0, len(nodeSet))
	for n := range nodeSet {
		nodes = append(nodes, n)
	}
	return nodes
}

// GenerateDNSConfig produces a dnsmasq-compatible DNS configuration string
// that maps each service to its node IPs under the .svc.local domain.
func (r *ServiceRegistry) GenerateDNSConfig() string {
	r.Refresh()

	lines := []string{"# Auto-generated service discovery DNS config"}

	for serviceName, endpoints := range r.services {
		seenIPs := make(map[string]struct{})
		for _, ep := range endpoints {
			if ep.Status != "running" {
				continue
			}
			ip := ep.NodeAddress
			if idx := strings.Index(ip, ":"); idx != -1 {
				ip = ip[:idx]
			}
			if _, seen := seenIPs[ip]; !seen {
				lines = append(lines, fmt.Sprintf("address=/%s.svc.local/%s", serviceName, ip))
				seenIPs[ip] = struct{}{}
			}
		}
	}

	return strings.Join(lines, "\n")
}
