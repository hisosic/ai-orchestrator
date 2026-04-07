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

	dtypes "github.com/docker/docker/api/types"
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
				// Master endpoints already collected locally
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

	// Group by service
	grouped := make(map[string][]ServiceEndpoint)
	for _, ep := range all {
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

// getRemoteEndpoints queries a worker node's agent for its container port mappings.
func getRemoteEndpoints(nodeAddr, filterSvc, nodeName string) []ServiceEndpoint {
	nodeIP, _ := splitAddress(nodeAddr)
	if nodeIP == "" {
		return nil
	}

	url := fmt.Sprintf("http://%s/v1/containers", nodeAddr)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	token := os.Getenv("ORCHESTRATOR_API_TOKEN")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var data []map[string]any
	if err := readJSONBody(resp, &data); err != nil {
		return nil
	}

	var endpoints []ServiceEndpoint
	for _, c := range data {
		stateStr, _ := c["state"].(string)
		if stateStr != "running" {
			continue
		}
		labels, _ := c["labels"].(map[string]any)
		if labels == nil {
			continue
		}
		managed, _ := labels[runtime.LabelOrchestrator].(string)
		if managed != "true" {
			continue
		}
		svcName, _ := labels[runtime.LabelService].(string)
		if svcName == "" || systemServices[svcName] {
			continue
		}
		if filterSvc != "" && svcName != filterSvc {
			continue
		}

		portsRaw, _ := c["ports"].([]any)
		hasPublicPort := false
		for _, pRaw := range portsRaw {
			pMap, _ := pRaw.(map[string]any)
			if pMap == nil {
				continue
			}
			pubPort := toInt(pMap["public_port"])
			privPort := toInt(pMap["private_port"])
			if pubPort > 0 {
				hasPublicPort = true
				endpoints = append(endpoints, ServiceEndpoint{
					Service:       svcName,
					HostPort:      fmt.Sprintf("%d", pubPort),
					ContainerPort: fmt.Sprintf("%d", privPort),
					NodeName:      nodeName,
					NodeIP:        nodeIP,
					URL:           fmt.Sprintf("http://%s:%d", nodeIP, pubPort),
				})
			}
		}

		if !hasPublicPort {
			endpoints = append(endpoints, ServiceEndpoint{
				Service:       svcName,
				HostPort:      "80",
				ContainerPort: "80",
				NodeName:      nodeName,
				NodeIP:        nodeIP,
				URL:           fmt.Sprintf("http://%s/%s/", nodeIP, svcName),
			})
		}
	}

	return endpoints
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

func hasPublicPort(ports []dtypes.Port) bool {
	for _, p := range ports {
		if p.PublicPort > 0 {
			return true
		}
	}
	return false
}

func readJSONBody(resp *http.Response, v any) error {
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
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
