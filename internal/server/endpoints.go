package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
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

	// Determine the user-facing base URL (scheme + host[:port]).
	// Priority:
	//   1. The URL the client used to reach us — derived from headers set by
	//      the reverse proxy (X-Forwarded-Proto / X-Forwarded-Host) or the
	//      Host header itself. This lets endpoint URLs follow whatever
	//      domain/port the user accessed (https://itda.24x365.online,
	//      http://20.20.6.248, http://localhost:8000, etc.).
	//   2. ORCHESTRATOR_PUBLIC_URL env (full URL) — fallback.
	//   3. ORCHESTRATOR_PUBLIC_HOST env — fallback.
	//   4. master node IP.
	pubScheme, pubHost, forceRewrite := detectRequestBase(r)
	if pubHost == "" {
		pubScheme, pubHost, forceRewrite = parsePublicURL()
	}
	if pubHost == "" {
		pubHost = strings.TrimSpace(os.Getenv("ORCHESTRATOR_PUBLIC_HOST"))
	}
	if pubHost == "" {
		pubHost = getMasterIP()
	}
	if pubHost == "" {
		pubHost = detectHostIPOnly()
	}
	if pubScheme == "" {
		pubScheme = "http"
	}

	// Ensure every service has a user-facing endpoint on the public base URL.
	all = ensurePublicEndpoints(all, pubScheme, pubHost, forceRewrite)

	// Health-check endpoints concurrently.
	// Skip the probe for external public URLs (https domain) — internal
	// containers can't reach them by design. Mark them reachable so the
	// UI doesn't show false negatives.
	externalBase := ""
	if forceRewrite {
		externalBase = pubHost
	}
	var wg sync.WaitGroup
	for i := range all {
		wg.Add(1)
		go func(ep *ServiceEndpoint) {
			defer wg.Done()
			if externalBase != "" && strings.Contains(ep.URL, externalBase) {
				ep.Reachable = true
				ep.ResponseMs = 0
				return
			}
			ep.Reachable, ep.ResponseMs = checkEndpoint(ep.URL)
		}(&all[i])
	}
	wg.Wait()

	// Deduplicate by URL and group by service; sort so user-facing URLs come first.
	seen := make(map[string]bool)
	grouped := make(map[string][]ServiceEndpoint)
	for _, ep := range all {
		if seen[ep.URL] {
			continue
		}
		seen[ep.URL] = true
		grouped[ep.Service] = append(grouped[ep.Service], ep)
	}
	for svc, eps := range grouped {
		sort.SliceStable(eps, func(i, j int) bool {
			pi := strings.Contains(eps[i].URL, pubHost)
			pj := strings.Contains(eps[j].URL, pubHost)
			if pi != pj {
				return pi
			}
			return eps[i].Reachable && !eps[j].Reachable
		})
		grouped[svc] = eps
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"endpoints":  grouped,
		"public_url": strings.TrimRight(fmt.Sprintf("%s://%s", pubScheme, pubHost), "/"),
	})
}

// detectRequestBase extracts the user-facing scheme + host from the incoming request.
//
// Scheme detection (Traefik and some proxies overwrite X-Forwarded-Proto to the
// entrypoint's scheme, losing the original TLS info). Priority:
//  1. X-Forwarded-Proto header (if set)
//  2. r.TLS (direct TLS connection)
//  3. Heuristic: Host is a public FQDN (has a dot, not an IP) → assume "https".
//     Production domains are almost always served over HTTPS; this avoids
//     returning http:// URLs that browsers then block as mixed content.
//  4. Default: "http"
//
// Host is container-internal IP (172.x) or loopback → returns empty so callers
// fall back to env/master IP.
func detectRequestBase(r *http.Request) (scheme, host string, ok bool) {
	host = strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	if host == "" {
		return "", "", false
	}
	if i := strings.Index(host, ","); i >= 0 {
		host = strings.TrimSpace(host[:i])
	}

	hostOnly := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		hostOnly = h
	}
	if hostOnly == "localhost" || hostOnly == "127.0.0.1" || hostOnly == "::1" {
		return "", "", false
	}
	if isDockerInternalIP(hostOnly) {
		return "", "", false
	}

	// When Host is a public domain, assume https. Traefik and similar proxies
	// often rewrite X-Forwarded-Proto to their entrypoint scheme (http on :80),
	// which would cause the orchestrator to emit http:// URLs that browsers
	// block as mixed content on an HTTPS dashboard. Production domains are
	// virtually always served over HTTPS, so default to that.
	if isPublicFQDN(hostOnly) {
		scheme = "https"
	} else {
		scheme = strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
		if scheme == "" {
			if r.TLS != nil {
				scheme = "https"
			} else {
				scheme = "http"
			}
		}
	}

	return scheme, host, true
}

// isPublicFQDN returns true if host is a domain name (contains a dot and is not an IP).
func isPublicFQDN(host string) bool {
	if host == "" || !strings.Contains(host, ".") {
		return false
	}
	if net.ParseIP(host) != nil {
		return false
	}
	return true
}

// parsePublicURL reads ORCHESTRATOR_PUBLIC_URL and splits it into scheme and host[:port].
// When set, forceRewrite=true signals that ALL endpoint URLs should be rewritten under this base.
func parsePublicURL() (scheme, host string, forceRewrite bool) {
	raw := strings.TrimSpace(os.Getenv("ORCHESTRATOR_PUBLIC_URL"))
	if raw == "" {
		return "", "", false
	}
	raw = strings.TrimRight(raw, "/")
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		s := u.Scheme
		if s == "" {
			s = "http"
		}
		return s, u.Host, true
	}
	// Bare hostname without scheme — treat as http host.
	return "http", raw, true
}

// ensurePublicEndpoints makes sure every service has a user-facing endpoint
// served via the public base URL (scheme://host).
//
// Rules:
//   - If forceRewrite is true (request uses an external URL or PUBLIC_URL env
//     is set): REPLACE per-node entries with a single primary public URL.
//     This avoids mixed-content issues (HTTPS page + HTTP alt URL) and
//     duplicate cards in the dashboard.
//   - Otherwise: add a single Traefik path URL via the public host as the
//     primary URL, keeping per-node direct URLs as alternates for debugging.
func ensurePublicEndpoints(all []ServiceEndpoint, pubScheme, pubHost string, forceRewrite bool) []ServiceEndpoint {
	if pubHost == "" {
		return all
	}

	bySvc := map[string][]ServiceEndpoint{}
	for _, ep := range all {
		bySvc[ep.Service] = append(bySvc[ep.Service], ep)
	}

	// forceRewrite=true: one primary URL per service (hides internal alternates).
	if forceRewrite {
		out := make([]ServiceEndpoint, 0, len(bySvc))
		for svc, eps := range bySvc {
			if svc == "" {
				continue
			}
			if systemServices[svc] {
				out = append(out, eps...)
				continue
			}
			publicURL := fmt.Sprintf("%s://%s/%s/", pubScheme, pubHost, svc)
			out = append(out, ServiceEndpoint{
				Service:       svc,
				HostPort:      "80",
				ContainerPort: "80",
				NodeName:      eps[0].NodeName,
				NodeIP:        pubHost,
				URL:           publicURL,
			})
		}
		return out
	}

	// forceRewrite=false: append a primary public URL if missing, keep alternates.
	out := make([]ServiceEndpoint, 0, len(all)+len(bySvc))
	out = append(out, all...)
	for svc, eps := range bySvc {
		if svc == "" || systemServices[svc] {
			continue
		}
		hasPublic := false
		for _, ep := range eps {
			if strings.Contains(ep.URL, pubHost) && strings.HasPrefix(ep.URL, pubScheme+"://") {
				hasPublic = true
				break
			}
		}
		if hasPublic {
			continue
		}
		out = append(out, ServiceEndpoint{
			Service:       svc,
			HostPort:      "80",
			ContainerPort: "80",
			NodeName:      eps[0].NodeName,
			NodeIP:        pubHost,
			URL:           fmt.Sprintf("%s://%s/%s/", pubScheme, pubHost, svc),
		})
	}
	return out
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
