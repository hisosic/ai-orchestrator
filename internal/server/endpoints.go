package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"

	"ai-container-go/internal/runtime"
)

// ServiceEndpoint describes a reachable endpoint for a service.
type ServiceEndpoint struct {
	Service       string `json:"service"`
	URL           string `json:"url"`
	HostPort      string `json:"host_port"`
	ContainerPort string `json:"container_port"`
	NodeName      string `json:"node_name"`
	NodeIP        string `json:"node_ip"`
	Reachable     bool   `json:"reachable"`
	ResponseMs    int64  `json:"response_ms"`
}

// handleServiceEndpoints returns live, verified endpoints for all services.
// GET /v1/services/endpoints?service=xxx (optional filter)
func handleServiceEndpoints(w http.ResponseWriter, r *http.Request) {
	filterSvc := r.URL.Query().Get("service")

	// Determine master IP for building URLs
	masterNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if masterNodeName == "" {
		masterNodeName = "master"
	}

	// Collect endpoints from local Docker
	localEndpoints := getLocalEndpoints(filterSvc)

	// If cluster mode, also get endpoints from worker nodes
	var clusterEndpoints []ServiceEndpoint
	if clusterState != nil {
		nodes := clusterState.ListNodes()
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, node := range nodes {
			if node.Name == masterNodeName || node.Role == "master" {
				continue
			}
			if node.Status == "offline" {
				continue
			}
			wg.Add(1)
			go func(nodeName, nodeAddr string) {
				defer wg.Done()
				eps := getRemoteEndpoints(nodeAddr, filterSvc, nodeName)
				mu.Lock()
				clusterEndpoints = append(clusterEndpoints, eps...)
				mu.Unlock()
			}(node.Name, node.Address)
		}
		wg.Wait()
	}

	all := append(localEndpoints, clusterEndpoints...)

	// Health-check all endpoints concurrently
	var wg sync.WaitGroup
	for i := range all {
		wg.Add(1)
		go func(ep *ServiceEndpoint) {
			defer wg.Done()
			ep.Reachable, ep.ResponseMs = checkEndpoint(ep.URL)
		}(&all[i])
	}
	wg.Wait()

	// Deduplicate by URL and group by service
	seen := make(map[string]bool)
	grouped := make(map[string][]ServiceEndpoint)
	for _, ep := range all {
		if seen[ep.URL] {
			continue
		}
		seen[ep.URL] = true
		grouped[ep.Service] = append(grouped[ep.Service], ep)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"endpoints": grouped,
	})
}

// getLocalEndpoints inspects Docker containers on this node for port mappings.
func getLocalEndpoints(filterSvc string) []ServiceEndpoint {
	cli := runtime.DockerClient()
	if cli == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filterArgs := filters.NewArgs(
		filters.Arg("label", runtime.LabelOrchestrator+"=true"),
		filters.Arg("status", "running"),
	)
	if filterSvc != "" {
		filterArgs.Add("label", fmt.Sprintf("%s=%s", runtime.LabelService, filterSvc))
	}

	containers, err := cli.ContainerList(ctx, container.ListOptions{
		Filters: filterArgs,
	})
	if err != nil {
		return nil
	}

	// Determine this node's host IP
	nodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if nodeName == "" {
		nodeName = "master"
	}
	hostIP := detectHostIPOnly()

	var endpoints []ServiceEndpoint
	for _, c := range containers {
		svcName := c.Labels[runtime.LabelService]
		if svcName == "" || systemServices[svcName] {
			continue
		}

		for _, p := range c.Ports {
			if p.PublicPort == 0 {
				continue
			}
			ep := ServiceEndpoint{
				Service:       svcName,
				HostPort:      fmt.Sprintf("%d", p.PublicPort),
				ContainerPort: fmt.Sprintf("%d", p.PrivatePort),
				NodeName:      nodeName,
				NodeIP:        hostIP,
				URL:           fmt.Sprintf("http://%s:%d", hostIP, p.PublicPort),
			}
			endpoints = append(endpoints, ep)
		}

		// If no direct port mapping, check Traefik path route
		if !hasPublicPort(c.Ports) {
			ep := ServiceEndpoint{
				Service:       svcName,
				HostPort:      "80",
				ContainerPort: "80",
				NodeName:      nodeName,
				NodeIP:        hostIP,
				URL:           fmt.Sprintf("http://%s/%s/", hostIP, svcName),
			}
			endpoints = append(endpoints, ep)
		}
	}

	return endpoints
}

// getRemoteEndpoints queries a worker node for container port mappings.
// Uses both container API data and cluster placement info for service matching.
func getRemoteEndpoints(nodeAddr, filterSvc, nodeName string) []ServiceEndpoint {
	nodeIP, _ := splitAddress(nodeAddr)
	if nodeIP == "" {
		return nil
	}

	apiURL := fmt.Sprintf("http://%s/v1/containers", nodeAddr)
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil
	}
	token := os.Getenv("ORCHESTRATOR_API_TOKEN")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var wrapper struct {
		Containers []map[string]any `json:"containers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
		return nil
	}

	// Build container-name -> service-name map from cluster placements
	placementMap := make(map[string]string)
	if clusterState != nil {
		for _, p := range clusterState.GetPlacements("", nodeName) {
			if p.ServiceName != "" {
				placementMap[p.ContainerName] = p.ServiceName
			}
		}
	}

	// Resolve master IP for Traefik-routed services
	masterIP := getMasterIP()

	var endpoints []ServiceEndpoint
	for _, c := range wrapper.Containers {
		stateStr, _ := c["state"].(string)
		if stateStr != "running" {
			continue
		}

		cName, _ := c["name"].(string)

		// Determine service name: from label, from API field, or from placement
		svcName, _ := c["service"].(string)
		if svcName == "" {
			svcName = placementMap[cName]
		}
		if svcName == "" || systemServices[svcName] {
			continue
		}
		if filterSvc != "" && svcName != filterSvc {
			continue
		}

		// Parse ports: ["80/tcp->8080", "3000/tcp", ...]
		portsRaw, _ := c["ports"].([]any)
		hasPublic := false
		for _, pRaw := range portsRaw {
			ps, ok := pRaw.(string)
			if !ok || ps == "" {
				continue
			}
			if strings.Contains(ps, "->") {
				parts := strings.SplitN(ps, "->", 2)
				hostPort := strings.TrimSpace(parts[1])
				containerPort := strings.SplitN(strings.TrimSpace(parts[0]), "/", 2)[0]
				if hostPort != "" {
					hasPublic = true
					endpoints = append(endpoints, ServiceEndpoint{
						Service:       svcName,
						HostPort:      hostPort,
						ContainerPort: containerPort,
						NodeName:      nodeName,
						NodeIP:        nodeIP,
						URL:           fmt.Sprintf("http://%s:%s", nodeIP, hostPort),
					})
				}
			}
		}

		// No direct port mapping → Traefik path route via master
		if !hasPublic {
			ep := masterIP
			if ep == "" {
				ep = nodeIP
			}
			endpoints = append(endpoints, ServiceEndpoint{
				Service:       svcName,
				HostPort:      "80",
				ContainerPort: "80",
				NodeName:      nodeName,
				NodeIP:        ep,
				URL:           fmt.Sprintf("http://%s/%s/", ep, svcName),
			})
		}
	}

	return endpoints
}

// getMasterIP returns the master node's host IP.
func getMasterIP() string {
	if clusterState == nil {
		return ""
	}
	masterNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if masterNodeName == "" {
		masterNodeName = "master"
	}
	for _, n := range clusterState.ListNodes() {
		if n.Role == "master" || n.Name == masterNodeName {
			ip, _ := splitAddress(n.Address)
			return ip
		}
	}
	return ""
}

// checkEndpoint performs an HTTP GET with a short timeout and returns reachability + latency.
func checkEndpoint(url string) (bool, int64) {
	client := &http.Client{
		Timeout: 3 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // don't follow redirects
		},
	}
	start := time.Now()
	resp, err := client.Get(url)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		// Try TCP connect as fallback (some services don't respond to HTTP)
		conn, err := net.DialTimeout("tcp", extractHostPort(url), 2*time.Second)
		if err != nil {
			return false, elapsed
		}
		conn.Close()
		return true, time.Since(start).Milliseconds()
	}
	resp.Body.Close()
	return true, elapsed
}

// extractHostPort extracts host:port from a URL string.
func extractHostPort(url string) string {
	url = strings.TrimPrefix(url, "http://")
	url = strings.TrimPrefix(url, "https://")
	parts := strings.SplitN(url, "/", 2)
	hp := parts[0]
	if !strings.Contains(hp, ":") {
		hp += ":80"
	}
	return hp
}

func hasPublicPort(ports []types.Port) bool {
	for _, p := range ports {
		if p.PublicPort > 0 {
			return true
		}
	}
	return false
}


// detectHostIPOnly returns just the IP portion without port.
func detectHostIPOnly() string {
	addr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	if addr != "" {
		ip, _ := splitAddress(addr)
		if ip != "" {
			return ip
		}
	}
	full := detectHostIP()
	ip, _ := splitAddress(full)
	if ip != "" {
		return ip
	}
	return "127.0.0.1"
}

func toInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return 0
}
