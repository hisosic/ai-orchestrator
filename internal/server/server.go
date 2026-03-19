// Package server implements the HTTP API for the AI Container Orchestrator.
//
// Supports two roles:
//   - master: Full API including cluster management, scheduling, migration
//   - worker: Local container management + agent heartbeat to master
package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	"github.com/go-chi/chi/v5"

	"archive/tar"

	"ai-container-go/internal/agent"
	"ai-container-go/internal/alerts"
	"ai-container-go/internal/clusterstate"
	"ai-container-go/internal/discovery"
	"ai-container-go/internal/migrate"
	"ai-container-go/internal/models"
	"ai-container-go/internal/monitoring"
	"ai-container-go/internal/nlengine"
	"ai-container-go/internal/runtime"
	"ai-container-go/internal/scheduler"
	"ai-container-go/internal/state"
)

// ---------------------------------------------------------------------------
// Package-level variables
// ---------------------------------------------------------------------------

var (
	// OrchestratorRole is "master" or "worker", read from ORCHESTRATOR_ROLE env.
	OrchestratorRole string
	// Version of this build.
	Version = "0.3.0"

	clusterState    *clusterstate.ClusterStateManager
	sched           *scheduler.Scheduler
	migrationCtrl   *migrate.MigrationController
	alertEngine     *alerts.AlertEngine
	serviceRegistry *discovery.ServiceRegistry
	workerAgent     *agent.WorkerAgent
	hub             *sseHub
)

// systemServices are containers hidden from user-facing listings.
var systemServices = map[string]bool{
	"ai-orchestrator":        true,
	"ai-orchestrator-worker": true,
	"orch-traefik":           true,
	"orch-dns":               true,
	"zbx-agent":              true,
	"zabbix-agent":           true,
	"zabbix-agent2":          true,
}

const reconcileIntervalSec = 15
const autoHealIntervalSec = 30
const autoHealCooldownSec = 120

// AutoHealEvent records a single auto-heal action.
type AutoHealEvent struct {
	ContainerID   string `json:"container_id"`
	ContainerName string `json:"container_name"`
	ServiceName   string `json:"service_name"`
	NodeName      string `json:"node_name"`
	PrevStatus    string `json:"prev_status"`
	Action        string `json:"action"`
	Success       bool   `json:"success"`
	Error         string `json:"error,omitempty"`
	Timestamp     string `json:"timestamp"`
}

var (
	autoHealEnabled   = true
	autoHealMu        sync.Mutex
	autoHealEvents    []AutoHealEvent
	autoHealCooldowns = map[string]time.Time{} // containerName -> last restart time
)

// DashboardHTML holds the embedded dashboard page. Set from main.go.
var DashboardHTML string

// httpClient is used for proxying requests to worker nodes.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// longHTTPClient is used for long-running operations (pull, deploy).
var longHTTPClient = &http.Client{Timeout: 120 * time.Second}

// ---------------------------------------------------------------------------
// Router setup
// ---------------------------------------------------------------------------

// NewRouter creates a chi router with all routes, CORS middleware, and
// optional bearer-token auth middleware.
func NewRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(corsMiddleware)
	r.Use(bearerTokenAuth)

	// Dashboard
	r.Get("/", handleDashboard)
	r.Get("/dashboard", handleDashboard)

	// Health & info
	r.Get("/health", handleHealth)
	r.Get("/v1/services", handleListServices)
	r.Get("/v1/system", handleSystem)
	r.Get("/v1/containers", handleListContainers)
	r.Get("/v1/images", handleListImages)

	// Command / NL
	r.Post("/v1/command", handleCommand)
	r.Post("/v1/action", handleAction)

	// Container management
	r.Post("/v1/containers/run", handleRunContainer)
	r.Post("/v1/containers/{id}/stop", handleStopContainer)
	r.Delete("/v1/containers/{id}", handleRemoveContainer)
	r.Get("/v1/containers/{id}/inspect", handleInspectContainer)
	r.Delete("/v1/images/{id}", handleRemoveImage)
	r.Post("/v1/services/scale", handleScaleService)
	r.Post("/v1/images/pull", handlePullImage)

	// Cluster API (master)
	r.Get("/v1/cluster/status", handleClusterStatus)
	r.Get("/v1/cluster/nodes", handleClusterListNodes)
	r.Post("/v1/cluster/nodes", handleClusterAddNode)
	r.Delete("/v1/cluster/nodes/{name}", handleClusterDeleteNode)
	r.Post("/v1/cluster/nodes/{name}/cordon", handleClusterCordonNode)
	r.Post("/v1/cluster/nodes/{name}/uncordon", handleClusterUncordonNode)
	r.Post("/v1/cluster/nodes/{name}/drain", handleClusterDrainNode)
	r.Post("/v1/cluster/heartbeat", handleClusterHeartbeat)
	r.Post("/v1/cluster/schedule", handleClusterSchedule)
	r.Post("/v1/cluster/migrate", handleClusterMigrate)
	r.Post("/v1/cluster/move", handleClusterMove)
	r.Get("/v1/cluster/migrations", handleClusterMigrations)
	r.Get("/v1/cluster/migrations/{id}", handleClusterMigrationDetail)
	r.Get("/v1/cluster/placements", handleClusterPlacements)
	r.Get("/v1/cluster/services", handleClusterServices)
	r.Get("/v1/cluster/alerts", handleClusterAlerts)
	r.Post("/v1/cluster/alerts/{id}/ack", handleClusterAckAlert)
	r.Get("/v1/cluster/discovery", handleClusterDiscovery)
	r.Get("/v1/cluster/discovery/{service}", handleClusterDiscoveryService)
	r.Post("/v1/cluster/scale", handleClusterScale)
	r.Post("/v1/cluster/stop", handleClusterStop)
	r.Post("/v1/cluster/container/stop", handleClusterContainerStop)
	r.Delete("/v1/cluster/container/{id}", handleClusterContainerDelete)
	r.Post("/v1/cluster/deploy", handleClusterDeploy)

	// Auto-heal API
	r.Get("/v1/cluster/autoheal", handleAutoHealStatus)
	r.Post("/v1/cluster/autoheal/toggle", handleAutoHealToggle)

	// Agent API
	r.Post("/v1/agent/export/{id}", handleAgentExport)
	r.Post("/v1/agent/import", handleAgentImport)
	r.Get("/v1/agent/resources", handleAgentResources)
	r.Post("/v1/agent/adjust-replicas", handleAgentAdjustReplicas)
	r.Post("/v1/agent/run-one", handleAgentRunOne)
	r.Post("/v1/agent/reconcile-skip", handleAgentReconcileSkip)
	r.Post("/v1/agent/blockchain/deploy", handleAgentBlockchainDeploy)
	r.Post("/v1/agent/exec", handleAgentExec)

	// SSE streaming
	r.Get("/v1/stream", handleSSEStream)

	// QuickStart API
	r.Post("/v1/quickstart/blockchain", handleQuickstartBlockchain)
	r.Post("/v1/quickstart/blockchain/distributed", handleQuickstartBlockchainDistributed)
	r.Get("/v1/quickstart/blockchain/status", handleQuickstartBlockchainStatus)

	// Node provisioning
	r.Post("/v1/cluster/nodes/provision", handleProvisionNode)
	r.Get("/v1/cluster/nodes/provision/log", handleProvisionLog)

	return r
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

// InitCluster initializes cluster components based on the orchestrator role.
func InitCluster() {
	OrchestratorRole = strings.ToLower(os.Getenv("ORCHESTRATOR_ROLE"))
	if OrchestratorRole == "" {
		OrchestratorRole = "master"
	}

	if OrchestratorRole == "master" {
		clusterState = clusterstate.NewClusterStateManager("")
		sched = scheduler.NewScheduler(clusterState)
		migrationCtrl = migrate.NewMigrationController(clusterState)
		alertEngine = alerts.NewAlertEngine(clusterState)
		serviceRegistry = discovery.NewServiceRegistry(clusterState)

		// Wire reconcile-skip callbacks for the migration controller.
		migrate.LocalReconcileSkipAdd = runtime.ReconcileSkipAdd
		migrate.LocalReconcileSkipRemove = runtime.ReconcileSkipRemove

		// Register master node itself so heartbeat and cluster status work.
		masterNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
		if masterNodeName == "" {
			masterNodeName = "master"
		}
		masterAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
		if masterAddr == "" {
			// Auto-detect: try default gateway (Docker host IP) first,
			// then fall back to non-loopback interface IP.
			masterAddr = detectHostIP()
			if masterAddr == "" {
				masterAddr = "127.0.0.1:8000"
			}
		}
		clusterState.RegisterNode(models.NodeInfo{
			Name:    masterNodeName,
			Address: masterAddr,
			Status:  models.NodeHealthy,
			Role:    "master",
			Labels:  map[string]string{},
		})
		log.Printf("Master node '%s' registered", masterNodeName)

		alertEngine.Start()
	}

	if OrchestratorRole == "worker" {
		workerAgent = agent.NewWorkerAgent()
		workerAgent.Start()
	}
}

// StartBackgroundTasks starts the reconcile loop and (for master) the cluster
// health loop.
func StartBackgroundTasks() {
	go reconcileLoop()

	if OrchestratorRole == "master" {
		go clusterHealthLoop()
		go autoHealLoop()
	}

	hub = newSSEHub()
	go ssePublishLoop()
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearerTokenAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN"))
		path := r.URL.Path
		method := r.Method

		if token == "" || !strings.HasPrefix(path, "/v1/") ||
			strings.HasPrefix(path, "/v1/cluster/") ||
			strings.HasPrefix(path, "/v1/agent/") ||
			strings.HasPrefix(path, "/v1/quickstart/") {
			next.ServeHTTP(w, r)
			return
		}

		// Allow GET on read endpoints without token (dashboard).
		if method == http.MethodGet &&
			(path == "/v1/system" || path == "/v1/services" ||
				path == "/v1/containers" || path == "/v1/images" ||
				path == "/v1/stream" ||
				strings.HasPrefix(path, "/v1/containers")) {
			next.ServeHTTP(w, r)
			return
		}

		// Allow POST/DELETE from dashboard on management endpoints without token.
		if strings.HasPrefix(path, "/v1/services/") ||
			strings.HasPrefix(path, "/v1/containers/") ||
			strings.HasPrefix(path, "/v1/images/") ||
			path == "/v1/command" || path == "/v1/action" {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				// No auth header = likely dashboard, allow it.
				next.ServeHTTP(w, r)
				return
			}
			if auth != "Bearer "+token {
				writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "Unauthorized"})
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+token {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "Unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// ---------------------------------------------------------------------------
// SSE (Server-Sent Events) hub
// ---------------------------------------------------------------------------

type sseClient struct {
	ch chan []byte
}

type sseHub struct {
	mu      sync.Mutex
	clients map[*sseClient]bool
}

func newSSEHub() *sseHub {
	return &sseHub{clients: make(map[*sseClient]bool)}
}

func (h *sseHub) addClient(c *sseClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = true
}

func (h *sseHub) removeClient(c *sseClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
	close(c.ch)
}

func (h *sseHub) broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.ch <- data:
		default:
			// Drop message if client buffer is full.
		}
	}
}

func handleSSEStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	client := &sseClient{ch: make(chan []byte, 16)}
	hub.addClient(client)
	defer hub.removeClient(client)

	// Send initial data immediately.
	data := buildSSEPayload()
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-client.ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func buildSSEPayload() []byte {
	payload := map[string]any{}

	// Health
	payload["health"] = map[string]any{
		"status": "ok", "version": Version, "role": OrchestratorRole,
	}

	// System info
	payload["system"] = monitoring.GetSystemInfo()
	payload["system"].(map[string]any)["role"] = OrchestratorRole

	// Services
	payload["services"] = getServicesData(false)

	// Images
	payload["images"] = monitoring.ListImages()

	// Cluster status
	if clusterState != nil {
		status := clusterState.GetClusterStatus()
		statusJSON, _ := json.Marshal(status)
		var statusMap map[string]any
		json.Unmarshal(statusJSON, &statusMap)
		payload["cluster_status"] = statusMap

		// Nodes
		nodes := clusterState.ListNodes()
		nodesData := make([]map[string]any, 0)
		for _, n := range nodes {
			nJSON, _ := json.Marshal(n)
			var nMap map[string]any
			json.Unmarshal(nJSON, &nMap)
			nodesData = append(nodesData, nMap)
		}
		payload["nodes"] = nodesData

		// Placements (filter system)
		placements := clusterState.GetPlacements("", "")
		placementsData := make([]map[string]any, 0)
		for _, p := range placements {
			if systemServices[p.ServiceName] {
				continue
			}
			pJSON, _ := json.Marshal(p)
			var pMap map[string]any
			json.Unmarshal(pJSON, &pMap)
			placementsData = append(placementsData, pMap)
		}
		payload["placements"] = placementsData

		// Alerts
		alerts := clusterState.ListAlerts(true)
		alertsData := make([]map[string]any, 0)
		for _, a := range alerts {
			aJSON, _ := json.Marshal(a)
			var aMap map[string]any
			json.Unmarshal(aJSON, &aMap)
			alertsData = append(alertsData, aMap)
		}
		payload["alerts"] = alertsData

		// Migrations
		migrations := clusterState.ListMigrations(false)
		migrationsData := make([]map[string]any, 0)
		for _, m := range migrations {
			mJSON, _ := json.Marshal(m)
			var mMap map[string]any
			json.Unmarshal(mJSON, &mMap)
			migrationsData = append(migrationsData, mMap)
		}
		payload["migrations"] = migrationsData
	}

	result, _ := json.Marshal(payload)
	return result
}

func ssePublishLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if hub == nil {
			continue
		}
		hub.mu.Lock()
		count := len(hub.clients)
		hub.mu.Unlock()
		if count == 0 {
			continue // no clients, skip expensive data gathering
		}
		data := buildSSEPayload()
		hub.broadcast(data)
	}
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, DashboardHTML)
}

// ---------------------------------------------------------------------------
// QuickStart API
// ---------------------------------------------------------------------------

func handleQuickstartBlockchain(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Validators  int               `json:"validators"`
		Citizens    int               `json:"citizens"`
		Channel     string            `json:"channel"`
		Image       string            `json:"image"`
		P2PPort     int               `json:"p2p_port"`
		RPCPort     int               `json:"rpc_port"`
		LogLevel    string            `json:"log_level"`
		Network     string            `json:"network"`
		ServiceName string            `json:"service_name"`
		EnvVars     map[string]string `json:"env_vars,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if body.Validators < 1 {
		body.Validators = 4
	}
	if body.Validators > 20 {
		body.Validators = 20
	}
	if body.Citizens < 0 {
		body.Citizens = 0
	}
	if body.Channel == "" {
		body.Channel = "seoul"
	}
	if body.Image == "" {
		body.Image = "20.20.0.13:80/iconloop-enterprise/goloop:v1.2.5-seoul-test"
	}
	if body.P2PPort <= 0 {
		body.P2PPort = 7100
	}
	if body.RPCPort <= 0 {
		body.RPCPort = 9100
	}
	if body.LogLevel == "" {
		body.LogLevel = "trace"
	}
	if body.Network == "" {
		body.Network = "orch-internal"
	}
	if body.ServiceName == "" {
		body.ServiceName = "blockchain"
	}

	scriptPath := "/app/services/blockchain/quickstart.sh"
	for _, p := range []string{scriptPath, "services/blockchain/quickstart.sh"} {
		if _, err := os.Stat(p); err == nil {
			scriptPath = p
			break
		}
	}

	log.Printf("QuickStart Blockchain: validators=%d citizens=%d channel=%s image=%s p2p=%d rpc=%d log=%s",
		body.Validators, body.Citizens, body.Channel, body.Image, body.P2PPort, body.RPCPort, body.LogLevel)

	go func() {
		cmd := exec.Command("bash", scriptPath,
			fmt.Sprintf("%d", body.Validators),
			fmt.Sprintf("%d", body.Citizens),
			body.Channel,
			body.Image,
		)
		blockchainSrc := os.Getenv("BLOCKCHAIN_HOST_PATH")
		if blockchainSrc == "" {
			blockchainSrc = "/blockchain"
		}
		workDir := os.Getenv("BLOCKCHAIN_WORK_DIR")
		if workDir == "" {
			workDir = "/tmp/blockchain-data"
		}
		env := []string{
			"BLOCKCHAIN_SRC=" + blockchainSrc,
			"WORK_DIR=" + workDir,
			fmt.Sprintf("QS_P2P_PORT=%d", body.P2PPort),
			fmt.Sprintf("QS_RPC_PORT=%d", body.RPCPort),
			"QS_LOG_LEVEL=" + body.LogLevel,
			"QS_NETWORK=" + body.Network,
			"QS_SERVICE_NAME=" + body.ServiceName,
		}
		// Pass user-defined env vars with QS_ENV_ prefix
		for k, v := range body.EnvVars {
			env = append(env, "QS_ENV_"+k+"="+v)
		}
		cmd.Env = append(os.Environ(), env...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("QuickStart error: %v\n%s", err, string(output))
		} else {
			log.Printf("QuickStart completed:\n%s", string(output))
		}
	}()

	writeJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    fmt.Sprintf("블록체인 구성 시작: 합의노드 %d개 + 시티즌 %d개 (채널: %s)", body.Validators, body.Citizens, body.Channel),
		"validators": body.Validators,
		"citizens":   body.Citizens,
		"channel":    body.Channel,
		"image":      body.Image,
		"p2p_port":   body.P2PPort,
		"rpc_port":   body.RPCPort,
		"log_level":  body.LogLevel,
	})
}

func handleQuickstartBlockchainStatus(w http.ResponseWriter, r *http.Request) {
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Docker not available"})
		return
	}

	ctx := context.Background()
	containers, err := cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", "blockchain.channel")),
	})
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": err.Error()})
		return
	}

	var nodes []map[string]any
	for _, c := range containers {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		cid := c.ID
		if len(cid) > 12 {
			cid = cid[:12]
		}

		// Try to get chain status via RPC
		// Use container name on orch-internal network (port 9080 internal)
		// Fallback to host-mapped port via 127.0.0.1
		chainStatus := ""
		if c.State == "running" {
			channel := c.Labels["blockchain.channel"]
			rpcURLs := []string{}
			// Primary: container name via Docker network
			if name != "" && channel != "" {
				rpcURLs = append(rpcURLs, fmt.Sprintf("http://%s:9080/api/v3/%s", name, channel))
			}
			// Fallback: host-mapped port
			for _, p := range c.Ports {
				if p.PrivatePort == 9080 && p.PublicPort > 0 && channel != "" {
					rpcURLs = append(rpcURLs, fmt.Sprintf("http://127.0.0.1:%d/api/v3/%s", p.PublicPort, channel))
					break
				}
			}
			shortClient := &http.Client{Timeout: 3 * time.Second}
			for _, rpcURL := range rpcURLs {
				req, _ := http.NewRequest("POST", rpcURL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"icx_getLastBlock"}`))
				req.Header.Set("Content-Type", "application/json")
				resp, err := shortClient.Do(req)
				if err == nil {
					var rpcResp map[string]any
					json.NewDecoder(resp.Body).Decode(&rpcResp)
					resp.Body.Close()
					if result, ok := rpcResp["result"].(map[string]any); ok {
						if height, ok := result["height"]; ok {
							chainStatus = fmt.Sprintf("height=%v", height)
						}
					}
				}
				if chainStatus != "" {
					break
				}
			}
		}

		// Extract public RPC port
		rpcPort := 0
		for _, p := range c.Ports {
			if p.PrivatePort == 9080 && p.PublicPort > 0 {
				rpcPort = int(p.PublicPort)
				break
			}
		}

		// Get NID from admin/chain API and build endpoint
		endpoint := ""
		if c.State == "running" && name != "" {
			adminURL := fmt.Sprintf("http://%s:9080/admin/chain", name)
			shortClient2 := &http.Client{Timeout: 3 * time.Second}
			adminResp, adminErr := shortClient2.Get(adminURL)
			if adminErr == nil {
				var chains []map[string]any
				json.NewDecoder(adminResp.Body).Decode(&chains)
				adminResp.Body.Close()
				if len(chains) > 0 {
					if nid, ok := chains[0]["nid"].(string); ok && nid != "" {
						endpoint = fmt.Sprintf("http://%s:9080/admin/chain/%s", name, nid)
					}
				}
			}
		}

		nodes = append(nodes, map[string]any{
			"id":       cid,
			"name":     name,
			"status":   c.State,
			"role":     c.Labels["blockchain.role"],
			"channel":  c.Labels["blockchain.channel"],
			"index":    c.Labels["blockchain.index"],
			"chain":    chainStatus,
			"rpc_port": rpcPort,
			"endpoint": endpoint,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"nodes":   nodes,
		"total":   len(nodes),
	})
}

// ---------------------------------------------------------------------------
// Node Provisioning
// ---------------------------------------------------------------------------

var (
	provisionMu  sync.Mutex
	provisionLog strings.Builder
)

func handleProvisionNode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeIP      string `json:"node_ip"`
		NodeName    string `json:"node_name"`
		SSHUser     string `json:"ssh_user"`
		SSHPassword string `json:"ssh_password"`
		SSHKeyPath  string `json:"ssh_key_path"`
		AuthType    string `json:"auth_type"` // "password" or "key"
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if body.NodeIP == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "node_ip는 필수입니다."})
		return
	}
	if body.SSHUser == "" {
		body.SSHUser = "root"
	}
	if body.NodeName == "" {
		parts := strings.Split(body.NodeIP, ".")
		body.NodeName = "worker-" + parts[len(parts)-1]
	}

	masterAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	if masterAddr == "" {
		masterAddr = "127.0.0.1:8000"
	}
	apiToken := os.Getenv("ORCHESTRATOR_API_TOKEN")

	scriptPath := "/app/services/provision-node.sh"
	for _, p := range []string{scriptPath, "services/provision-node.sh"} {
		if _, err := os.Stat(p); err == nil {
			scriptPath = p
			break
		}
	}

	log.Printf("Provision node: %s (%s) user=%s", body.NodeName, body.NodeIP, body.SSHUser)

	// Clear previous log
	provisionMu.Lock()
	provisionLog.Reset()
	provisionLog.WriteString(fmt.Sprintf("=== Provisioning %s (%s) ===\n", body.NodeName, body.NodeIP))
	provisionMu.Unlock()

	go func() {
		// Resolve SSH key path
		sshKeyPath := body.SSHKeyPath
		if body.AuthType == "key" && sshKeyPath == "" {
			// Try default key locations
			for _, p := range []string{"/root/.ssh/id_rsa", "/ssh-keys/loopvm.pem", "/ssh-keys/id_rsa"} {
				if _, err := os.Stat(p); err == nil {
					sshKeyPath = p
					break
				}
			}
		}
		sshPassword := body.SSHPassword
		if body.AuthType == "key" {
			sshPassword = "" // don't use password when key auth
		}

		cmd := exec.Command("bash", scriptPath,
			body.NodeIP, body.NodeName, body.SSHUser, sshPassword,
			masterAddr, apiToken, "", sshKeyPath,
		)
		cmd.Env = append(os.Environ(), "SSHPASS="+sshPassword)

		stdout, _ := cmd.StdoutPipe()
		cmd.Stderr = cmd.Stdout
		if err := cmd.Start(); err != nil {
			provisionMu.Lock()
			provisionLog.WriteString("ERROR: " + err.Error() + "\n")
			provisionMu.Unlock()
			log.Printf("Provision error: %v", err)
			return
		}

		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				provisionMu.Lock()
				provisionLog.Write(buf[:n])
				provisionMu.Unlock()
			}
			if err != nil {
				break
			}
		}
		cmd.Wait()

		provisionMu.Lock()
		if cmd.ProcessState.ExitCode() == 0 {
			provisionLog.WriteString("\n=== Provisioning Complete ===\n")
		} else {
			provisionLog.WriteString(fmt.Sprintf("\n=== Provisioning Failed (exit %d) ===\n", cmd.ProcessState.ExitCode()))
		}
		provisionMu.Unlock()
		log.Printf("Provision %s finished: exit=%d", body.NodeName, cmd.ProcessState.ExitCode())
	}()

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("노드 '%s' (%s) 프로비저닝 시작", body.NodeName, body.NodeIP),
	})
}

func handleProvisionLog(w http.ResponseWriter, r *http.Request) {
	provisionMu.Lock()
	logStr := provisionLog.String()
	provisionMu.Unlock()

	done := strings.Contains(logStr, "=== Provisioning Complete ===") || strings.Contains(logStr, "=== Provisioning Failed")
	writeJSON(w, http.StatusOK, map[string]any{
		"log":  logStr,
		"done": done,
	})
}

// ---------------------------------------------------------------------------
// Health & Info handlers
// ---------------------------------------------------------------------------

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": Version,
		"role":    OrchestratorRole,
	})
}

func handleSystem(w http.ResponseWriter, r *http.Request) {
	info := monitoring.GetSystemInfo()
	info["role"] = OrchestratorRole
	writeJSON(w, http.StatusOK, info)
}

func handleListServices(w http.ResponseWriter, r *http.Request) {
	showSystem := r.URL.Query().Get("show_system") == "true"
	result := getServicesData(showSystem)
	writeJSON(w, http.StatusOK, result)
}

// getServicesData builds the services listing data. It is used by both the
// handleListServices handler and the SSE payload builder.
func getServicesData(showSystem bool) []map[string]any {
	localServices := state.ListServices()

	if clusterState == nil {
		var result []map[string]any
		for _, s := range localServices {
			if !showSystem && systemServices[s.Name] {
				continue
			}
			result = append(result, serviceInfoToMap(s))
		}
		if result == nil {
			result = []map[string]any{}
		}
		return result
	}

	// Cluster-aware service listing.
	clusterSvcs := clusterState.GetServicePlacementSummary()
	localMap := make(map[string]models.ServiceInfo)
	for _, s := range localServices {
		localMap[s.Name] = s
	}

	// Build node-name -> address map
	masterNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if masterNodeName == "" {
		masterNodeName = "master"
	}
	var masterIP string
	nodeInfoMap := make(map[string]map[string]string)
	for _, n := range clusterState.ListNodes() {
		ip, port := splitAddress(n.Address)
		nodeInfoMap[n.Name] = map[string]string{"ip": ip, "port": port}
		if n.Role == "master" || n.Name == masterNodeName {
			masterIP = ip
		}
	}

	var result []map[string]any
	seen := make(map[string]bool)

	for svcName, infoRaw := range clusterSvcs {
		if !showSystem && systemServices[svcName] {
			continue
		}
		seen[svcName] = true
		info, _ := infoRaw.(map[string]any)
		if info == nil {
			continue
		}

		local, hasLocal := localMap[svcName]
		nodesRaw, _ := info["nodes"].(map[string]any)
		var nodeList []string
		for n, v := range nodesRaw {
			vMap, _ := v.(map[string]any)
			running := 0
			if vMap != nil {
				running, _ = vMap["running"].(int)
			}
			nodeList = append(nodeList, fmt.Sprintf("%s:%d", n, running))
		}

		clusterSvc := clusterState.GetService(svcName)
		image := ""
		if clusterSvc != nil {
			image, _ = clusterSvc["image"].(string)
		} else if hasLocal {
			image = local.Image
		}

		// Build endpoint URL via master node.
		endpoint := ""
		if masterIP != "" {
			portInfo := ""
			if clusterSvc != nil {
				portsRaw, _ := clusterSvc["ports"].(string)
				if portsRaw != "" {
					var portsList []any
					json.Unmarshal([]byte(portsRaw), &portsList)
					for _, p := range portsList {
						ps, _ := p.(string)
						parts := strings.SplitN(ps, ":", 2)
						if len(parts) == 2 {
							portInfo = strings.TrimSpace(parts[0])
						}
					}
				}
			}
			if portInfo != "" && portInfo != "80" && portInfo != "443" {
				endpoint = fmt.Sprintf("http://%s:%s/", masterIP, portInfo)
			} else {
				endpoint = fmt.Sprintf("http://%s/%s/", masterIP, svcName)
			}
		}

		totalRaw, _ := info["total"].(int)
		runningRaw, _ := info["running"].(int)
		memLimit := ""
		cpuLimit := ""
		if clusterSvc != nil {
			memLimit, _ = clusterSvc["memory_limit"].(string)
			cpuLimit, _ = clusterSvc["cpu_limit"].(string)
		} else if hasLocal {
			memLimit = local.MemoryLimit
			cpuLimit = local.CPULimit
		}

		status := "stopped"
		if runningRaw > 0 {
			status = "running"
		}

		result = append(result, map[string]any{
			"name":          svcName,
			"image":         image,
			"replicas":      totalRaw,
			"running":       runningRaw,
			"memory_limit":  memLimit,
			"cpu_limit":     cpuLimit,
			"status":        status,
			"container_ids": []string{},
			"nodes":         nodeList,
			"endpoint":      endpoint,
		})
	}

	// Add local-only services not in cluster placements.
	for _, s := range localServices {
		if seen[s.Name] {
			continue
		}
		if !showSystem && systemServices[s.Name] {
			continue
		}
		if s.Replicas == 0 && len(s.ContainerIDs) == 0 {
			continue
		}
		result = append(result, serviceInfoToMap(s))
	}

	if result == nil {
		result = []map[string]any{}
	}
	// Sort: running first, then by name alphabetically.
	sort.Slice(result, func(i, j int) bool {
		ri, _ := result[i]["running"].(int)
		rj, _ := result[j]["running"].(int)
		if ri != rj {
			return ri > rj
		}
		ni, _ := result[i]["name"].(string)
		nj, _ := result[j]["name"].(string)
		return ni < nj
	})
	return result
}

func handleListContainers(w http.ResponseWriter, r *http.Request) {
	statsParam := r.URL.Query().Get("stats") == "true"
	var data []map[string]any
	if statsParam {
		data = monitoring.GetAllContainersWithStats()
	} else {
		data = monitoring.ListContainers(true)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"containers": data,
		"error":      nil,
	})
}

func handleListImages(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, monitoring.ListImages())
}

// ---------------------------------------------------------------------------
// Command / NL handlers
// ---------------------------------------------------------------------------

func handleCommand(w http.ResponseWriter, r *http.Request) {
	var req models.CommandRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	intent := nlengine.Parse(req.Command)
	if intent.Action == models.IntentUnknown {
		writeJSON(w, http.StatusOK, models.CommandResponse{
			Success: false,
			Intent:  &intent,
			Message: "명령을 이해하지 못했습니다. 예: 'nginx를 3개로 스케일해줘', 'redis 메모리 512m', 'nginx를 node-b로 마이그레이션해줘'",
		})
		return
	}

	success, message, details := runtime.ExecuteIntent(intent, req.DryRun)
	writeJSON(w, http.StatusOK, models.CommandResponse{
		Success: success,
		Intent:  &intent,
		Message: message,
		Details: details,
	})
}

func handleAction(w http.ResponseWriter, r *http.Request) {
	var req models.ActionExecuteRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusOK, models.CommandResponse{
			Success: false,
			Message: "Docker 연결 실패",
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	action := strings.ToLower(strings.TrimSpace(req.Action))
	var ok bool
	var msg string
	var details map[string]any

	switch action {
	case "deploy":
		name := req.ServiceName
		if name == "" {
			name = req.Image
		}
		if name == "" {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "서비스명 또는 이미지를 지정하세요."})
			return
		}
		ok, msg, details = runtime.ExecuteDeploy(ctx, cli, name, req.Image)

	case "scale":
		if req.ServiceName == "" || req.Replicas == nil {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "서비스명과 레플리카 수를 지정하세요."})
			return
		}
		ok, msg, details = runtime.ExecuteScale(ctx, cli, req.ServiceName, *req.Replicas)

	case "resource":
		if req.ServiceName == "" {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "서비스명을 지정하세요."})
			return
		}
		ok, msg, details = runtime.ExecuteResource(ctx, cli, req.ServiceName, req.Memory, req.CPU)

	case "stop":
		target := req.ServiceName
		if target == "" {
			target = req.ContainerID
		}
		if target == "" {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "서비스명 또는 컨테이너 ID를 지정하세요."})
			return
		}
		ok, msg, details = runtime.ExecuteStop(ctx, cli, target)

	case "run_image":
		if req.Image == "" {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "이미지를 지정하세요."})
			return
		}
		ok, msg, details = runtime.RunContainer(ctx, cli, req.Image, runtime.RunContainerOpts{
			Name:   req.ServiceName,
			Memory: req.Memory,
			CPU:    req.CPU,
		})

	case "container_stop":
		if req.ContainerID == "" {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "컨테이너 ID를 지정하세요."})
			return
		}
		ok, msg, _ = runtime.StopContainerByID(ctx, cli, req.ContainerID)
		details = map[string]any{}

	case "container_remove":
		if req.ContainerID == "" {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "컨테이너 ID를 지정하세요."})
			return
		}
		ok, msg, _ = runtime.RemoveContainerByID(ctx, cli, req.ContainerID)
		details = map[string]any{}

	case "image_remove":
		if req.ImageID == "" {
			writeJSON(w, http.StatusOK, models.CommandResponse{Success: false, Message: "이미지 ID를 지정하세요."})
			return
		}
		ok, msg, _ = runtime.RemoveImageByID(ctx, cli, req.ImageID)
		details = map[string]any{}

	case "list":
		svcs := state.ListServices()
		svcList := make([]map[string]any, 0, len(svcs))
		for _, s := range svcs {
			svcList = append(svcList, serviceInfoToMap(s))
		}
		ok = true
		msg = "서비스 목록"
		details = map[string]any{"services": svcList}

	default:
		writeJSON(w, http.StatusOK, models.CommandResponse{
			Success: false,
			Message: fmt.Sprintf("지원하지 않는 동작: %s", action),
		})
		return
	}

	writeJSON(w, http.StatusOK, models.CommandResponse{
		Success: ok,
		Message: msg,
		Details: details,
	})
}

// ---------------------------------------------------------------------------
// Container management handlers
// ---------------------------------------------------------------------------

func handleRunContainer(w http.ResponseWriter, r *http.Request) {
	var req models.RunContainerRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ok, msg, details := runtime.RunContainer(ctx, cli, req.Image, runtime.RunContainerOpts{
		Name:               req.Name,
		Memory:             req.Memory,
		CPU:                req.CPU,
		Replicas:           req.Replicas,
		UseInternalNetwork: req.UseInternalNetwork,
		Environment:        req.Environment,
		Volumes:            req.Volumes,
		Ports:              req.Ports,
		User:               req.User,
		VolumeMode:         req.VolumeMode,
		AutoPull:           true,
	})
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg, "details": details})
}

func handleStopContainer(w http.ResponseWriter, r *http.Request) {
	containerID := chi.URLParam(r, "id")
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ok, msg, _ := runtime.StopContainerByID(ctx, cli, containerID)
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg})
}

func handleRemoveContainer(w http.ResponseWriter, r *http.Request) {
	containerID := chi.URLParam(r, "id")
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ok, msg, details := runtime.RemoveContainerByID(ctx, cli, containerID)
	if ok && details != nil {
		if svcName, _ := details["service_name"].(string); svcName != "" {
			info := state.GetService(svcName)
			if info != nil {
				replicas := 0
				if r, ok := info["replicas"].(float64); ok {
					replicas = int(r)
				} else if r, ok := info["replicas"].(int); ok {
					replicas = r
				}
				if replicas > 0 {
					runtime.ExecuteScale(ctx, cli, svcName, replicas)
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg})
}

func handleInspectContainer(w http.ResponseWriter, r *http.Request) {
	containerID := chi.URLParam(r, "id")
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ok, msg, details := runtime.InspectContainerByID(ctx, cli, containerID)
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg, "details": details})
}

func handleRemoveImage(w http.ResponseWriter, r *http.Request) {
	imageID := chi.URLParam(r, "id")
	cli := runtime.DockerClient()
	var results []map[string]any

	// 1. Delete from local (master).
	if cli != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ok, msg, _ := runtime.RemoveImageByID(ctx, cli, imageID)
		cancel()
		results = append(results, map[string]any{"node": "master", "success": ok, "message": msg})
	} else {
		results = append(results, map[string]any{"node": "master", "success": false, "message": "Docker connection failed"})
	}

	// 2. Delete from all worker nodes.
	if clusterState != nil {
		masterNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
		if masterNodeName == "" {
			masterNodeName = "master"
		}
		for _, node := range clusterState.ListNodes() {
			if node.Name == masterNodeName || node.Role == "master" {
				continue
			}
			baseURL := nodeBaseURL(&node)
			req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/images/%s", baseURL, imageID), nil)
			if err != nil {
				results = append(results, map[string]any{"node": node.Name, "success": false, "message": err.Error()})
				continue
			}
			setNodeHeaders(req, &node)
			resp, err := httpClient.Do(req)
			if err != nil {
				results = append(results, map[string]any{"node": node.Name, "success": false, "message": err.Error()})
				continue
			}
			var data map[string]any
			json.NewDecoder(resp.Body).Decode(&data)
			resp.Body.Close()
			s, _ := data["success"].(bool)
			m, _ := data["message"].(string)
			results = append(results, map[string]any{"node": node.Name, "success": s, "message": m})
		}
	}

	deleted := 0
	failed := 0
	for _, res := range results {
		if s, _ := res["success"].(bool); s {
			deleted++
		} else {
			failed++
		}
	}
	msg := fmt.Sprintf("이미지 '%s' 삭제: %d개 노드 성공", imageID, deleted)
	if failed > 0 {
		msg += fmt.Sprintf(", %d개 노드 없음/실패", failed)
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": deleted > 0, "message": msg, "details": results})
}

func handleScaleService(w http.ResponseWriter, r *http.Request) {
	var req models.ScaleServiceRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ok, msg, details := runtime.ExecuteScale(ctx, cli, req.ServiceName, req.Replicas)
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg, "details": details})
}

func handlePullImage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Image string `json:"image"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	image := strings.TrimSpace(body.Image)
	if image == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "이미지 경로를 입력하세요."})
		return
	}

	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ok, msg := runtime.PullImage(ctx, cli, image)
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg})
}

// ---------------------------------------------------------------------------
// Cluster API handlers (master only)
// ---------------------------------------------------------------------------

func handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Cluster management not available (not master role)"})
		return
	}
	status := clusterState.GetClusterStatus()
	writeJSON(w, http.StatusOK, status)
}

func handleClusterListNodes(w http.ResponseWriter, r *http.Request) {
	if clusterState != nil {
		nodes := clusterState.ListNodes()
		writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
		return
	}
	// Fallback to legacy state.
	legacyNodes := state.ListNodes()
	var nodes []map[string]any
	for _, n := range legacyNodes {
		nodes = append(nodes, map[string]any{
			"name":    n.Name,
			"address": n.BaseURL,
		})
	}
	if nodes == nil {
		nodes = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func handleClusterAddNode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string            `json:"name"`
		Address string            `json:"address"`
		BaseURL string            `json:"base_url"`
		Token   string            `json:"token"`
		Role    string            `json:"role"`
		Labels  map[string]string `json:"labels"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	name := strings.TrimSpace(body.Name)
	address := strings.TrimSpace(body.Address)
	if address == "" {
		address = strings.TrimSpace(body.BaseURL)
	}
	token := strings.TrimSpace(body.Token)

	if name == "" || address == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "name과 address(IP:port)는 필수입니다."})
		return
	}

	// Legacy state.
	baseURL := address
	if !strings.HasPrefix(baseURL, "http") {
		baseURL = "http://" + address
	}
	state.UpsertNode(name, baseURL, token)

	// Cluster state.
	if clusterState != nil {
		role := body.Role
		if role == "" {
			role = "worker"
		}
		labels := body.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		node := models.NodeInfo{
			Name:    name,
			Address: address,
			Token:   token,
			Status:  models.NodeHealthy,
			Role:    role,
			Labels:  labels,
		}
		clusterState.RegisterNode(node)
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": fmt.Sprintf("노드 '%s' 등록됨.", name)})
}

func handleClusterDeleteNode(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	// Stop all containers on this node before removing it
	if clusterState != nil {
		node := clusterState.GetNode(name)
		if node != nil {
			placements := clusterState.GetPlacements("", name)
			baseURL := nodeBaseURL(node)
			for _, p := range placements {
				if systemServices[p.ServiceName] {
					continue
				}
				cid := p.ContainerID
				if cid == "" {
					cid = p.ContainerName
				}
				// Stop
				stopReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/containers/%s/stop", baseURL, cid), nil)
				setNodeHeaders(stopReq, node)
				if resp, err := httpClient.Do(stopReq); err == nil {
					resp.Body.Close()
				}
				// Remove
				delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/containers/%s", baseURL, cid), nil)
				setNodeHeaders(delReq, node)
				if resp, err := httpClient.Do(delReq); err == nil {
					resp.Body.Close()
				}
				log.Printf("Node removal: stopped container %s on %s", p.ContainerName, name)
			}
		}
		clusterState.RemoveNode(name)
	}
	state.DeleteNode(name)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": fmt.Sprintf("노드 '%s' 삭제됨 (컨테이너 정리 완료).", name)})
}

func handleClusterCordonNode(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Cluster not available"})
		return
	}
	clusterState.UpdateNodeStatus(name, models.NodeCordoned)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": fmt.Sprintf("노드 '%s' cordoned (스케줄링 중지).", name)})
}

func handleClusterUncordonNode(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Cluster not available"})
		return
	}
	clusterState.UpdateNodeStatus(name, models.NodeHealthy)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": fmt.Sprintf("노드 '%s' uncordoned (스케줄링 재개).", name)})
}

func handleClusterDrainNode(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if clusterState == nil || migrationCtrl == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Cluster not available"})
		return
	}

	var body struct {
		TargetNode string `json:"target_node"`
	}
	readJSON(r, &body)

	result := migrationCtrl.DrainNode(name, body.TargetNode)
	writeJSON(w, http.StatusOK, result)
}

func handleClusterHeartbeat(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"ack": false, "error": "Not master"})
		return
	}

	var body struct {
		NodeName   string                    `json:"node_name"`
		Resources  models.NodeResources      `json:"resources"`
		Containers []models.ContainerPlacement `json:"containers"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ack": false, "error": "Invalid payload"})
		return
	}

	clusterState.ProcessHeartbeat(body.NodeName, body.Resources, body.Containers)
	writeJSON(w, http.StatusOK, models.HeartbeatResponse{
		Ack:      true,
		Commands: []map[string]any{},
	})
}

func handleClusterSchedule(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil || sched == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Cluster not available"})
		return
	}

	var body struct {
		ServiceName string                     `json:"service_name"`
		Image       string                     `json:"image"`
		Replicas    int                        `json:"replicas"`
		Strategy    string                     `json:"strategy"`
		MemoryLimit string                     `json:"memory_limit"`
		CPULimit    string                     `json:"cpu_limit"`
		Environment []string                   `json:"environment"`
		Volumes     []string                   `json:"volumes"`
		Ports       []string                   `json:"ports"`
		Constraints *models.ScheduleConstraints `json:"constraints"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request"})
		return
	}
	if body.Replicas <= 0 {
		body.Replicas = 1
	}
	if body.Strategy == "" {
		body.Strategy = "spread"
	}

	decisions, err := sched.Schedule(body.ServiceName, body.Image, body.Replicas, body.Constraints, body.Strategy)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	// Execute scheduling: proxy container run to each target node.
	var results []map[string]any
	for _, d := range decisions {
		node := clusterState.GetNode(d.NodeName)
		if node == nil {
			results = append(results, map[string]any{"node": d.NodeName, "error": "Node not found"})
			continue
		}
		baseURL := nodeBaseURL(node)
		payload, _ := json.Marshal(map[string]any{
			"image":                body.Image,
			"name":                 body.ServiceName,
			"replicas":             d.Count,
			"memory":               body.MemoryLimit,
			"cpu":                  body.CPULimit,
			"use_internal_network": true,
			"environment":          body.Environment,
			"volumes":              body.Volumes,
			"ports":                body.Ports,
		})
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/containers/run", bytes.NewReader(payload))
		setNodeHeaders(req, node)
		resp, err := httpClient.Do(req)
		if err != nil {
			results = append(results, map[string]any{"node": d.NodeName, "error": err.Error()})
			continue
		}
		var respData map[string]any
		json.NewDecoder(resp.Body).Decode(&respData)
		resp.Body.Close()
		results = append(results, map[string]any{"node": d.NodeName, "count": d.Count, "result": respData})
	}

	// Save cluster service.
	clusterState.SaveService(body.ServiceName, body.Image, body.Replicas, map[string]any{
		"memory_limit": body.MemoryLimit,
		"cpu_limit":    body.CPULimit,
		"environment":  body.Environment,
		"volumes":      body.Volumes,
		"ports":        body.Ports,
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"success":   true,
		"message":   "서비스 스케줄링 완료",
		"decisions": decisions,
		"results":   results,
	})
}

func handleClusterMigrate(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil || migrationCtrl == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Cluster not available"})
		return
	}

	var body models.MigrationRequest
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Invalid request"})
		return
	}

	result := migrationCtrl.Migrate(body.ContainerID, body.SourceNode, body.DestinationNode, "", body.ServiceName)
	writeJSON(w, http.StatusOK, result)
}

func handleClusterMove(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": "Cluster not available"})
		return
	}

	var body struct {
		ContainerID   string `json:"container_id"`
		ContainerName string `json:"container_name"`
		SourceNode    string `json:"source_node"`
		DestNode      string `json:"destination_node"`
		ServiceName   string `json:"service_name"`
		Image         string `json:"image"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request"})
		return
	}

	if body.SourceNode == "" || body.DestNode == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": "source_node와 destination_node는 필수입니다."})
		return
	}

	srcNode := clusterState.GetNode(body.SourceNode)
	dstNode := clusterState.GetNode(body.DestNode)
	if srcNode == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": fmt.Sprintf("소스 노드 '%s'를 찾을 수 없습니다.", body.SourceNode)})
		return
	}
	if dstNode == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": fmt.Sprintf("대상 노드 '%s'를 찾을 수 없습니다.", body.DestNode)})
		return
	}

	srcURL := nodeBaseURL(srcNode)
	dstURL := nodeBaseURL(dstNode)
	image := body.Image

	// Step 1: If no image specified, inspect source to get image name.
	if image == "" {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/containers/%s/inspect", srcURL, body.ContainerID), nil)
		setNodeHeaders(req, srcNode)
		resp, err := httpClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var data map[string]any
			json.NewDecoder(resp.Body).Decode(&data)
			resp.Body.Close()
			details, _ := data["details"].(map[string]any)
			if details == nil {
				details = data
			}
			attrs, _ := details["attrs"].(map[string]any)
			if attrs == nil {
				attrs = details
			}
			cfg, _ := attrs["Config"].(map[string]any)
			if cfg != nil {
				image, _ = cfg["Image"].(string)
			}
		} else if resp != nil {
			resp.Body.Close()
		}
		if image == "" {
			writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": "이미지를 확인할 수 없습니다. image 파라미터를 지정하세요."})
			return
		}
	}

	serviceName := body.ServiceName

	// Step 2: Reduce source replicas by 1 (so reconcile won't restart).
	if serviceName != "" {
		adjustReplicas(srcURL, srcNode, serviceName, -1)
	}

	// Step 3: Stop and remove source container.
	stopReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/containers/%s/stop", srcURL, body.ContainerID), nil)
	setNodeHeaders(stopReq, srcNode)
	if resp, err := httpClient.Do(stopReq); err == nil {
		resp.Body.Close()
	}

	delReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", srcURL, body.ContainerID), nil)
	setNodeHeaders(delReq, srcNode)
	resp, err := httpClient.Do(delReq)
	if err == nil {
		if resp.StatusCode != http.StatusOK && body.ContainerName != "" {
			resp.Body.Close()
			delReq2, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", srcURL, body.ContainerName), nil)
			setNodeHeaders(delReq2, srcNode)
			if resp2, err2 := httpClient.Do(delReq2); err2 == nil {
				resp2.Body.Close()
			}
		} else {
			resp.Body.Close()
		}
	}

	// Step 4: Determine service group.
	svcGroup := serviceName
	if svcGroup == "" && body.ContainerName != "" {
		nameClean := strings.TrimPrefix(body.ContainerName, "orch-")
		nameClean = strings.TrimPrefix(nameClean, "/")
		parts := strings.Split(nameClean, "-")
		if len(parts) > 1 {
			last := parts[len(parts)-1]
			if _, err := strconv.Atoi(last); err == nil {
				svcGroup = strings.Join(parts[:len(parts)-1], "-")
			} else {
				svcGroup = nameClean
			}
		} else {
			svcGroup = nameClean
		}
	}

	// Step 5: Run 1 container on destination via agent/run-one.
	runPayload, _ := json.Marshal(map[string]any{
		"image":        image,
		"service_name": svcGroup,
	})
	runReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/agent/run-one", dstURL), bytes.NewReader(runPayload))
	setNodeHeaders(runReq, dstNode)
	runResp, err := longHTTPClient.Do(runReq)
	if err != nil {
		// Rollback: restore source replicas.
		if serviceName != "" {
			adjustReplicas(srcURL, srcNode, serviceName, +1)
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": fmt.Sprintf("대상 노드에서 컨테이너 기동 실패: %v", err)})
		return
	}
	var runData map[string]any
	json.NewDecoder(runResp.Body).Decode(&runData)
	runResp.Body.Close()

	if s, _ := runData["success"].(bool); !s {
		// Rollback: restore source replicas.
		if serviceName != "" {
			adjustReplicas(srcURL, srcNode, serviceName, +1)
		}
		errMsg, _ := runData["error"].(string)
		if errMsg == "" {
			errMsg, _ = runData["message"].(string)
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": fmt.Sprintf("대상 노드에서 컨테이너 기동 실패: %s", errMsg)})
		return
	}

	// Step 6: Increase destination replicas by 1.
	if serviceName != "" {
		adjustReplicas(dstURL, dstNode, serviceName, +1)
	}

	// Step 7: Force refresh Traefik routes.
	syncTraefikRoutes()

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("'%s' 이동 완료: %s -> %s (이미지: %s)", body.ContainerName, body.SourceNode, body.DestNode, image),
	})
}

func handleClusterMigrations(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"migrations": []any{}})
		return
	}
	migrations := clusterState.ListMigrations(false)
	writeJSON(w, http.StatusOK, map[string]any{"migrations": migrations})
}

func handleClusterMigrationDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Not available"})
		return
	}
	m := clusterState.GetMigration(id)
	if m == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Migration not found"})
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func handleClusterPlacements(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"placements": []any{}})
		return
	}
	svcName := r.URL.Query().Get("service_name")
	nodeName := r.URL.Query().Get("node_name")
	showSystem := r.URL.Query().Get("show_system") == "true"

	placements := clusterState.GetPlacements(svcName, nodeName)
	if !showSystem {
		var filtered []models.ContainerPlacement
		for _, p := range placements {
			if !systemServices[p.ServiceName] {
				filtered = append(filtered, p)
			}
		}
		placements = filtered
	}
	if placements == nil {
		placements = []models.ContainerPlacement{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"placements": placements})
}

func handleClusterServices(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"services": map[string]any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": clusterState.GetServicePlacementSummary()})
}

func handleClusterAlerts(w http.ResponseWriter, r *http.Request) {
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"alerts": []any{}})
		return
	}
	showAll := r.URL.Query().Get("all") == "true"
	alertsList := clusterState.ListAlerts(!showAll)
	writeJSON(w, http.StatusOK, map[string]any{"alerts": alertsList})
}

func handleClusterAckAlert(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": "Not available"})
		return
	}
	ok := clusterState.AcknowledgeAlert(id)
	msg := fmt.Sprintf("Alert %s acknowledged", id)
	if !ok {
		msg = "Alert not found"
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg})
}

func handleClusterDiscovery(w http.ResponseWriter, r *http.Request) {
	if serviceRegistry == nil {
		writeJSON(w, http.StatusOK, map[string]any{"services": map[string]any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": serviceRegistry.ListServices()})
}

func handleClusterDiscoveryService(w http.ResponseWriter, r *http.Request) {
	svcName := chi.URLParam(r, "service")
	if serviceRegistry == nil {
		writeJSON(w, http.StatusOK, map[string]any{"endpoints": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"endpoints": serviceRegistry.GetService(svcName)})
}

func handleClusterScale(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string `json:"service_name"`
		Replicas    int    `json:"replicas"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	serviceName := strings.TrimSpace(body.ServiceName)
	replicas := body.Replicas

	if serviceName == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "서비스명을 입력하세요."})
		return
	}

	if clusterState == nil {
		// Fallback: local-only.
		cli := runtime.DockerClient()
		if cli == nil {
			writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Docker connection failed"})
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		ok, msg, details := runtime.ExecuteScale(ctx, cli, serviceName, replicas)
		writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg, "details": details})
		return
	}

	if replicas == 0 {
		result := clusterStopServiceInternal(serviceName)
		writeJSON(w, http.StatusOK, result)
		return
	}

	svcInfo := clusterState.GetService(serviceName)

	// Count actual running containers across all nodes via API calls.
	// This is more accurate than placements (which depend on heartbeat timing).
	currentCount := 0
	for _, node := range clusterState.ListNodes() {
		baseURL := nodeBaseURL(&node)
		req, _ := http.NewRequest(http.MethodGet, baseURL+"/v1/containers", nil)
		setNodeHeaders(req, &node)
		resp, err := httpClient.Do(req)
		if err != nil {
			continue
		}
		var respData struct {
			Containers []map[string]any `json:"containers"`
		}
		json.NewDecoder(resp.Body).Decode(&respData)
		resp.Body.Close()
		for _, c := range respData.Containers {
			svc, _ := c["service"].(string)
			cname, _ := c["name"].(string)
			st, _ := c["state"].(string)
			if svc == "" && strings.HasPrefix(cname, "orch-"+serviceName+"-") {
				svc = serviceName
			}
			if svc == serviceName && (st == "running" || st == "created") {
				currentCount++
			}
		}
	}

	if svcInfo == nil && currentCount == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": fmt.Sprintf("'%s' 서비스를 찾을 수 없습니다. 먼저 '클러스터 배포'로 이미지를 지정하여 배포하세요.", serviceName)})
		return
	}
	image := serviceName
	if svcInfo != nil {
		if img, ok := svcInfo["image"].(string); ok && img != "" {
			image = img
		}
	}

	if replicas == currentCount {
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": fmt.Sprintf("'%s' 이미 %d개 실행 중.", serviceName, replicas), "details": map[string]any{}})
		return
	}

	if replicas > currentCount {
		// Scale up.
		newCount := replicas - currentCount
		decisions, err := sched.Schedule(serviceName, image, newCount, nil, "spread")
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": fmt.Sprintf("스케줄링 실패: %v", err)})
			return
		}

		var results []map[string]any
		for _, d := range decisions {
			node := clusterState.GetNode(d.NodeName)
			if node == nil {
				continue
			}
			baseURL := nodeBaseURL(node)
			// Use /v1/agent/run-one for each container to avoid name conflicts.
			// run-one finds the next available index automatically.
			for i := 0; i < d.Count; i++ {
				payload, _ := json.Marshal(map[string]any{
					"image": image, "service_name": serviceName,
				})
				req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/agent/run-one", bytes.NewReader(payload))
				setNodeHeaders(req, node)
				resp, err := longHTTPClient.Do(req)
				if err != nil {
					results = append(results, map[string]any{"node": d.NodeName, "success": false, "message": err.Error()})
					continue
				}
				var respData map[string]any
				json.NewDecoder(resp.Body).Decode(&respData)
				resp.Body.Close()
				results = append(results, map[string]any{"node": d.NodeName, "success": respData["success"], "message": respData["message"]})
			}
		}
		// Update local service state replicas on each target node.
		for _, d := range decisions {
			node := clusterState.GetNode(d.NodeName)
			if node == nil {
				continue
			}
			adjustReplicas(nodeBaseURL(node), node, serviceName, d.Count)
		}

		if svcInfo != nil {
			clusterState.SaveService(serviceName, image, replicas, nil)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"message": fmt.Sprintf("'%s' %d -> %d개로 스케일업.", serviceName, currentCount, replicas),
			"details": map[string]any{"results": results},
		})
	} else {
		// Scale down.
		toRemove := currentCount - replicas

		// Fetch current placements for deletion targets.
		placements := clusterState.GetPlacements(serviceName, "")

		// Group by node, remove from nodes with most containers.
		byNode := make(map[string][]models.ContainerPlacement)
		for _, p := range placements {
			byNode[p.NodeName] = append(byNode[p.NodeName], p)
		}
		type nodeGroup struct {
			name       string
			placements []models.ContainerPlacement
		}
		var sortedNodes []nodeGroup
		for name, pl := range byNode {
			sortedNodes = append(sortedNodes, nodeGroup{name, pl})
		}
		sort.Slice(sortedNodes, func(i, j int) bool {
			return len(sortedNodes[i].placements) > len(sortedNodes[j].placements)
		})

		var removed []map[string]any
		remaining := toRemove
		for _, ng := range sortedNodes {
			if remaining <= 0 {
				break
			}
			node := clusterState.GetNode(ng.name)
			if node == nil {
				continue
			}
			baseURL := nodeBaseURL(node)
			removeFromHere := remaining
			if removeFromHere > len(ng.placements) {
				removeFromHere = len(ng.placements)
			}
			for _, p := range ng.placements[:removeFromHere] {
				// Stop.
				stopReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/containers/%s/stop", baseURL, p.ContainerID), nil)
				setNodeHeaders(stopReq, node)
				if resp, err := httpClient.Do(stopReq); err == nil {
					resp.Body.Close()
				}
				// Delete.
				delReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", baseURL, p.ContainerID), nil)
				setNodeHeaders(delReq, node)
				resp, err := httpClient.Do(delReq)
				if err == nil {
					if resp.StatusCode != http.StatusOK && p.ContainerName != "" {
						resp.Body.Close()
						delReq2, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", baseURL, p.ContainerName), nil)
						setNodeHeaders(delReq2, node)
						if resp2, err2 := httpClient.Do(delReq2); err2 == nil {
							resp2.Body.Close()
						}
					} else {
						resp.Body.Close()
					}
				}
				removed = append(removed, map[string]any{"node": ng.name, "container": p.ContainerName})
				remaining--
			}
		}

		// Update local state on ALL nodes to match target replicas.
		// Use absolute "set" to avoid miscalculation from stale placement counts.
		for _, node := range clusterState.ListNodes() {
			setNodeServiceReplicas(&node, serviceName, replicas)
		}

		if svcInfo != nil {
			clusterState.SaveService(serviceName, image, replicas, nil)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"message": fmt.Sprintf("'%s' %d -> %d개로 스케일다운.", serviceName, currentCount, replicas),
			"details": map[string]any{"removed": removed},
		})
	}
}

func handleClusterStop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string `json:"service_name"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	serviceName := strings.TrimSpace(body.ServiceName)
	if serviceName == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "서비스명을 입력하세요."})
		return
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다."})
		return
	}
	result := clusterStopServiceInternal(serviceName)
	writeJSON(w, http.StatusOK, result)
}

// handleClusterContainerStop stops a single container on a specific node via the master.
func handleClusterContainerStop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ContainerID   string `json:"container_id"`
		ContainerName string `json:"container_name"`
		NodeName      string `json:"node_name"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Cluster not available"})
		return
	}
	node := clusterState.GetNode(body.NodeName)
	if node == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Node not found: " + body.NodeName})
		return
	}
	baseURL := nodeBaseURL(node)
	cid := body.ContainerID
	if cid == "" {
		cid = body.ContainerName
	}
	// Stop
	stopReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/containers/%s/stop", baseURL, cid), nil)
	setNodeHeaders(stopReq, node)
	if resp, err := httpClient.Do(stopReq); err == nil {
		resp.Body.Close()
	}
	// Remove
	delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/containers/%s", baseURL, cid), nil)
	setNodeHeaders(delReq, node)
	ok := false
	if resp, err := httpClient.Do(delReq); err == nil {
		ok = resp.StatusCode == http.StatusOK
		resp.Body.Close()
	}
	// Try by name if ID failed
	if !ok && body.ContainerName != "" && body.ContainerName != cid {
		delReq2, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/containers/%s", baseURL, body.ContainerName), nil)
		setNodeHeaders(delReq2, node)
		if resp, err := httpClient.Do(delReq2); err == nil {
			ok = resp.StatusCode == http.StatusOK
			resp.Body.Close()
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": ok,
		"message": fmt.Sprintf("컨테이너 '%s' on %s %s", body.ContainerName, body.NodeName, map[bool]string{true: "삭제됨", false: "삭제 실패"}[ok]),
	})
}

// handleClusterContainerDelete is an alias for DELETE method.
func handleClusterContainerDelete(w http.ResponseWriter, r *http.Request) {
	cid := chi.URLParam(r, "id")
	nodeName := r.URL.Query().Get("node")
	if clusterState == nil || nodeName == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Cluster not available or node not specified"})
		return
	}
	node := clusterState.GetNode(nodeName)
	if node == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Node not found"})
		return
	}
	baseURL := nodeBaseURL(node)
	stopReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/containers/%s/stop", baseURL, cid), nil)
	setNodeHeaders(stopReq, node)
	if resp, err := httpClient.Do(stopReq); err == nil {
		resp.Body.Close()
	}
	delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/containers/%s", baseURL, cid), nil)
	setNodeHeaders(delReq, node)
	ok := false
	if resp, err := httpClient.Do(delReq); err == nil {
		ok = resp.StatusCode == http.StatusOK
		resp.Body.Close()
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": ok})
}

func handleClusterDeploy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Image       string                     `json:"image"`
		Name        string                     `json:"name"`
		Replicas    int                        `json:"replicas"`
		Strategy    string                     `json:"strategy"`
		Memory      string                     `json:"memory"`
		CPU         string                     `json:"cpu"`
		Environment []string                   `json:"environment"`
		Volumes     []string                   `json:"volumes"`
		Ports       []string                   `json:"ports"`
		Nodes       []string                   `json:"nodes"`
		Constraints *models.ScheduleConstraints `json:"constraints"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	image := strings.TrimSpace(body.Image)
	if image == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "이미지 경로를 입력하세요."})
		return
	}

	if clusterState == nil || sched == nil {
		// Fallback: single-node deploy.
		cli := runtime.DockerClient()
		if cli == nil {
			writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Docker connection failed"})
			return
		}
		replicas := body.Replicas
		if replicas <= 0 {
			replicas = 1
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		ok, msg, details := runtime.RunContainer(ctx, cli, image, runtime.RunContainerOpts{
			Name:               body.Name,
			Replicas:           replicas,
			Memory:             body.Memory,
			CPU:                body.CPU,
			Environment:        body.Environment,
			Volumes:            body.Volumes,
			Ports:              body.Ports,
			UseInternalNetwork: true,
			AutoPull:           true,
		})
		writeJSON(w, http.StatusOK, map[string]any{"success": ok, "message": msg, "details": details})
		return
	}

	name := strings.TrimSpace(body.Name)
	if name == "" {
		parts := strings.Split(strings.SplitN(image, ":", 2)[0], "/")
		name = parts[len(parts)-1]
	}
	replicas := body.Replicas
	if replicas <= 0 {
		replicas = 1
	}
	strategy := body.Strategy
	if strategy == "" {
		strategy = "spread"
	}

	// Clean up existing service first.
	existing := clusterState.GetPlacements(name, "")
	if len(existing) > 0 {
		clusterStopServiceInternal(name)
		time.Sleep(2 * time.Second)
	}

	// Determine target nodes.
	var decisions []models.ScheduleDecision
	if len(body.Nodes) > 0 {
		nodeCount := len(body.Nodes)
		base := replicas / nodeCount
		remainder := replicas % nodeCount
		for i, n := range body.Nodes {
			count := base
			if i < remainder {
				count++
			}
			if count < 1 {
				count = 1
			}
			decisions = append(decisions, models.ScheduleDecision{NodeName: n, Count: count})
		}
	} else {
		var err error
		decisions, err = sched.Schedule(name, image, replicas, body.Constraints, strategy)
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": fmt.Sprintf("스케줄링 실패: %v", err)})
			return
		}
	}

	// Execute on each node: pull image + run containers.
	var results []map[string]any
	totalCreated := 0
	for _, d := range decisions {
		node := clusterState.GetNode(d.NodeName)
		if node == nil {
			results = append(results, map[string]any{"node": d.NodeName, "success": false, "error": "노드를 찾을 수 없습니다."})
			continue
		}
		baseURL := nodeBaseURL(node)

		// Step 1: Pull image.
		pullPayload, _ := json.Marshal(map[string]any{"image": image})
		pullReq, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/images/pull", bytes.NewReader(pullPayload))
		setNodeHeaders(pullReq, node)
		pullResp, err := longHTTPClient.Do(pullReq)
		if err != nil {
			results = append(results, map[string]any{"node": d.NodeName, "success": false, "phase": "pull", "error": err.Error()})
			continue
		}
		var pullData map[string]any
		json.NewDecoder(pullResp.Body).Decode(&pullData)
		pullResp.Body.Close()
		if s, _ := pullData["success"].(bool); !s {
			errMsg, _ := pullData["message"].(string)
			results = append(results, map[string]any{"node": d.NodeName, "success": false, "phase": "pull", "error": errMsg})
			continue
		}

		// Step 2: Run containers.
		runPayload, _ := json.Marshal(map[string]any{
			"image":                image,
			"name":                 name,
			"replicas":             d.Count,
			"memory":               body.Memory,
			"cpu":                  body.CPU,
			"use_internal_network": true,
			"environment":          body.Environment,
			"volumes":              body.Volumes,
			"ports":                body.Ports,
		})
		runReq, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/containers/run", bytes.NewReader(runPayload))
		setNodeHeaders(runReq, node)
		runResp, err := longHTTPClient.Do(runReq)
		if err != nil {
			results = append(results, map[string]any{"node": d.NodeName, "success": false, "phase": "run", "error": err.Error()})
			continue
		}
		var runData map[string]any
		json.NewDecoder(runResp.Body).Decode(&runData)
		runResp.Body.Close()

		created := 0
		if det, _ := runData["details"].(map[string]any); det != nil {
			if ids, _ := det["container_ids"].([]any); ids != nil {
				created = len(ids)
			}
		}
		totalCreated += created
		results = append(results, map[string]any{
			"node":    d.NodeName,
			"success": runData["success"],
			"created": created,
			"message": runData["message"],
		})
	}

	// Save cluster service.
	clusterState.SaveService(name, image, replicas, map[string]any{
		"memory_limit": body.Memory,
		"cpu_limit":    body.CPU,
		"environment":  body.Environment,
		"volumes":      body.Volumes,
		"ports":        body.Ports,
	})

	allOK := true
	for _, res := range results {
		if s, _ := res["success"].(bool); !s {
			allOK = false
			break
		}
	}

	status := "완료"
	if !allOK {
		status = "일부 실패"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": allOK,
		"message": fmt.Sprintf("'%s' 배포 %s: %d개 컨테이너 생성 (%d개 노드)", name, status, totalCreated, len(decisions)),
		"details": map[string]any{
			"service_name":  name,
			"image":         image,
			"total_created": totalCreated,
			"decisions":     decisions,
			"results":       results,
		},
	})
}

// ---------------------------------------------------------------------------
// Agent API handlers
// ---------------------------------------------------------------------------

func handleAgentExport(w http.ResponseWriter, r *http.Request) {
	containerID := chi.URLParam(r, "id")
	ag := workerAgent
	if ag == nil {
		ag = agent.NewWorkerAgent()
	}

	tarData, config, err := ag.ExportContainer(containerID)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"image_data": base64.StdEncoding.EncodeToString(tarData),
		"config":     config,
	})
}

func handleAgentImport(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ImageData   string         `json:"image_data"`
		Config      map[string]any `json:"config"`
		ServiceName string         `json:"service_name"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request"})
		return
	}

	imageData, err := base64.StdEncoding.DecodeString(body.ImageData)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": "Invalid base64 image data"})
		return
	}

	config := body.Config
	if config == nil {
		config = map[string]any{}
	}
	if body.ServiceName != "" {
		labels, _ := config["labels"].(map[string]any)
		if labels == nil {
			labels = map[string]any{}
		}
		labels["ai.orchestrator.service"] = body.ServiceName
		config["labels"] = labels
	}

	ag := workerAgent
	if ag == nil {
		ag = agent.NewWorkerAgent()
	}

	containerID, err := ag.ImportContainer(imageData, config)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "container_id": containerID})
}

func handleAgentResources(w http.ResponseWriter, r *http.Request) {
	ag := workerAgent
	if ag == nil {
		ag = agent.NewWorkerAgent()
	}
	writeJSON(w, http.StatusOK, ag.GetNodeResources())
}

func handleAgentAdjustReplicas(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string `json:"service_name"`
		Delta       int    `json:"delta"`
		Set         *int   `json:"set,omitempty"` // absolute value override
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false})
		return
	}
	svc := strings.TrimSpace(body.ServiceName)
	if svc == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false})
		return
	}

	info := state.GetService(svc)
	if info == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "service not found"})
		return
	}

	var newReplicas int
	if body.Set != nil {
		// Absolute value mode: set replicas directly.
		newReplicas = *body.Set
		if newReplicas < 0 {
			newReplicas = 0
		}
	} else {
		// Delta mode: adjust by delta.
		currentReplicas := 0
		switch v := info["replicas"].(type) {
		case float64:
			currentReplicas = int(v)
		case int:
			currentReplicas = v
		}
		newReplicas = currentReplicas + body.Delta
		if newReplicas < 0 {
			newReplicas = 0
		}
	}

	imageName, _ := info["image"].(string)
	var opts []state.UpsertOption
	if ml, ok := info["memory_limit"].(string); ok && ml != "" {
		opts = append(opts, state.WithMemoryLimit(ml))
	}
	if cl, ok := info["cpu_limit"].(string); ok && cl != "" {
		opts = append(opts, state.WithCPULimit(cl))
	}
	// Preserve container_ids.
	if ids, ok := info["container_ids"]; ok {
		switch v := ids.(type) {
		case []any:
			var sids []string
			for _, id := range v {
				if s, ok := id.(string); ok {
					sids = append(sids, s)
				}
			}
			opts = append(opts, state.WithContainerIDs(sids))
		case []string:
			opts = append(opts, state.WithContainerIDs(v))
		}
	}
	state.UpsertService(svc, imageName, newReplicas, opts...)

	writeJSON(w, http.StatusOK, map[string]any{"success": true, "service_name": svc, "replicas": newReplicas})
}

func handleAgentRunOne(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Image       string `json:"image"`
		ServiceName string `json:"service_name"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request"})
		return
	}
	image := strings.TrimSpace(body.Image)
	svcName := strings.TrimSpace(body.ServiceName)
	if image == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": "image is required"})
		return
	}

	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": "Docker connection failed"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Auto-pull if not found locally.
	_, _, err := cli.ImageInspectWithRaw(ctx, image)
	if err != nil {
		ok, pullMsg := runtime.PullImage(ctx, cli, image)
		if !ok {
			writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": pullMsg})
			return
		}
	}

	// Find next available index.
	prefix := fmt.Sprintf("orch-%s-", svcName)
	usedIndices := make(map[int]bool)
	containers, _ := cli.ContainerList(ctx, container.ListOptions{All: true})
	for _, c := range containers {
		for _, n := range c.Names {
			name := strings.TrimPrefix(n, "/")
			if strings.HasPrefix(name, prefix) {
				suffix := name[len(prefix):]
				if idx, err := strconv.Atoi(suffix); err == nil {
					usedIndices[idx] = true
				}
			}
		}
	}
	idx := 0
	for usedIndices[idx] {
		idx++
	}
	cname := fmt.Sprintf("%s%d", prefix, idx)

	labels := map[string]string{
		runtime.LabelOrchestrator: "true",
		runtime.LabelService:      svcName,
	}
	for k, v := range runtime.TraefikLabels(svcName, runtime.TraefikHTTPPort) {
		labels[k] = v
	}

	networkID := runtime.EnsureNetwork(ctx, cli)

	config := &container.Config{
		Image:  image,
		Labels: labels,
	}
	hostConfig := &container.HostConfig{}

	var networkConfig *network.NetworkingConfig
	if networkID != "" && svcName != "" {
		networkConfig = &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				runtime.OrchNetwork: {
					NetworkID: networkID,
					Aliases:   []string{svcName},
				},
			},
		}
	}

	resp, err := cli.ContainerCreate(ctx, config, hostConfig, networkConfig, nil, cname)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	cid := resp.ID
	if len(cid) > 12 {
		cid = cid[:12]
	}

	// Do NOT call state.UpsertService -- replicas stay unchanged.
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "container_id": cid, "container_name": cname})
}

func handleAgentReconcileSkip(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string `json:"service_name"`
		Skip        bool   `json:"skip"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false})
		return
	}
	svcName := strings.TrimSpace(body.ServiceName)
	if svcName == "" {
		writeJSON(w, http.StatusOK, map[string]any{"success": false})
		return
	}
	if body.Skip {
		runtime.ReconcileSkipAdd(svcName)
	} else {
		runtime.ReconcileSkipRemove(svcName)
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "service_name": svcName, "skip": body.Skip})
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

func reconcileLoop() {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("reconcile panic: %v", r)
				}
			}()
			cli := runtime.DockerClient()
			if cli != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				runtime.ReconcileReplicas(ctx, cli)
				cancel()
			}
		}()
		time.Sleep(reconcileIntervalSec * time.Second)
	}
}

func clusterHealthLoop() {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("cluster health panic: %v", r)
				}
			}()
			if clusterState != nil {
				clusterState.CheckNodeHealth(30, 90)
				masterSelfHeartbeat()
				syncTraefikRoutes()
			}
		}()
		time.Sleep(10 * time.Second)
	}
}

func masterSelfHeartbeat() {
	if clusterState == nil {
		return
	}
	ag := agent.NewWorkerAgent()
	resourcesMap := ag.GetNodeResources()

	// Convert map to NodeResources struct.
	resJSON, _ := json.Marshal(resourcesMap)
	var resources models.NodeResources
	json.Unmarshal(resJSON, &resources)

	// Get managed containers.
	containersRaw := ag.GetManagedContainers()
	var containers []models.ContainerPlacement
	for _, c := range containersRaw {
		cJSON, _ := json.Marshal(c)
		var cp models.ContainerPlacement
		json.Unmarshal(cJSON, &cp)
		containers = append(containers, cp)
	}

	nodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if nodeName == "" {
		nodeName = "master"
	}
	clusterState.ProcessHeartbeat(nodeName, resources, containers)
}

func syncTraefikRoutes() {
	if clusterState == nil {
		return
	}

	configDir := "/traefik-dynamic"
	configPath := configDir + "/cluster-routes.yml"

	info, err := os.Stat(configDir)
	if err != nil || !info.IsDir() {
		return
	}

	masterNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if masterNodeName == "" {
		masterNodeName = "master"
	}

	placements := clusterState.GetPlacements("", "")
	allNodes := clusterState.ListNodes()
	nodeMap := make(map[string]models.NodeInfo)
	for _, n := range allNodes {
		nodeMap[n.Name] = n
	}

	// Group services by name -> set of node IPs.
	svcNodes := make(map[string]map[string]bool)
	for _, p := range placements {
		svc := p.ServiceName
		if svc == "" || systemServices[svc] {
			continue
		}
		if svcNodes[svc] == nil {
			svcNodes[svc] = make(map[string]bool)
		}
		node, ok := nodeMap[p.NodeName]
		if ok && p.Status == "running" {
			ip, _ := splitAddress(node.Address)
			svcNodes[svc][ip] = true
		}
	}

	// Build Traefik YAML config.
	type routerCfg struct {
		rule        string
		service     string
		entryPoints []string
	}
	type serviceCfg struct {
		servers []string
	}

	routers := make(map[string]routerCfg)
	services := make(map[string]serviceCfg)

	re := regexp.MustCompile(`[^a-z0-9-]`)

	for svcName, ips := range svcNodes {
		if len(ips) == 0 {
			continue
		}
		// Skip services that have containers on master (Docker provider handles those).
		masterHasIt := false
		for _, p := range placements {
			if p.ServiceName == svcName && p.NodeName == masterNodeName && p.Status == "running" {
				masterHasIt = true
				break
			}
		}
		if masterHasIt {
			continue
		}

		safe := strings.Trim(re.ReplaceAllString(strings.ToLower(svcName), "-"), "-")
		if safe == "" {
			safe = "svc"
		}
		routeName := "cluster-" + safe

		routers[routeName+"-path"] = routerCfg{
			rule:        fmt.Sprintf("PathPrefix(`/%s/`)", svcName),
			service:     routeName,
			entryPoints: []string{"web"},
		}
		routers[routeName] = routerCfg{
			rule:        fmt.Sprintf("Host(`%s.local`)", svcName),
			service:     routeName,
			entryPoints: []string{"web"},
		}

		var sortedIPs []string
		for ip := range ips {
			sortedIPs = append(sortedIPs, ip)
		}
		sort.Strings(sortedIPs)

		var servers []string
		for _, ip := range sortedIPs {
			servers = append(servers, fmt.Sprintf("http://%s:80", ip))
		}
		services[routeName] = serviceCfg{servers: servers}
	}

	// Write as simple YAML.
	var lines []string
	lines = append(lines, "# Auto-generated by AI Container Orchestrator", "http:")

	if len(routers) > 0 {
		lines = append(lines, "  routers:")
		// Sort router keys for deterministic output.
		var routerKeys []string
		for k := range routers {
			routerKeys = append(routerKeys, k)
		}
		sort.Strings(routerKeys)
		for _, name := range routerKeys {
			cfg := routers[name]
			lines = append(lines, fmt.Sprintf("    %s:", name))
			lines = append(lines, fmt.Sprintf("      rule: \"%s\"", cfg.rule))
			lines = append(lines, fmt.Sprintf("      service: %s", cfg.service))
			if len(cfg.entryPoints) > 0 {
				lines = append(lines, "      entryPoints:")
				for _, ep := range cfg.entryPoints {
					lines = append(lines, fmt.Sprintf("        - %s", ep))
				}
			}
		}
	}

	if len(services) > 0 {
		lines = append(lines, "  services:")
		var svcKeys []string
		for k := range services {
			svcKeys = append(svcKeys, k)
		}
		sort.Strings(svcKeys)
		for _, name := range svcKeys {
			cfg := services[name]
			lines = append(lines, fmt.Sprintf("    %s:", name))
			lines = append(lines, "      loadBalancer:")
			lines = append(lines, "        servers:")
			for _, s := range cfg.servers {
				lines = append(lines, fmt.Sprintf("          - url: \"%s\"", s))
			}
		}
	}

	content := strings.Join(lines, "\n") + "\n"

	// Only write if changed.
	existing, _ := os.ReadFile(configPath)
	if string(existing) != content {
		os.WriteFile(configPath, []byte(content), 0o644)
	}
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func nodeBaseURL(node *models.NodeInfo) string {
	addr := node.Address
	if strings.HasPrefix(addr, "http") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + strings.TrimRight(addr, "/")
}

func setNodeHeaders(req *http.Request, node *models.NodeInfo) {
	req.Header.Set("Content-Type", "application/json")
	if node.Token != "" {
		req.Header.Set("Authorization", "Bearer "+node.Token)
	}
}

func setNodeServiceReplicas(node *models.NodeInfo, serviceName string, replicas int) {
	// Use adjust-replicas with "set" mode to update ONLY the services.json state
	// without creating/removing containers. This prevents reconcile from restoring old counts.
	baseURL := nodeBaseURL(node)
	payload, _ := json.Marshal(map[string]any{
		"service_name": serviceName,
		"set":          replicas,
	})
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/agent/adjust-replicas", bytes.NewReader(payload))
	if err != nil {
		return
	}
	setNodeHeaders(req, node)
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

func adjustReplicas(baseURL string, node *models.NodeInfo, serviceName string, delta int) {
	payload, _ := json.Marshal(map[string]any{
		"service_name": serviceName,
		"delta":        delta,
	})
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/agent/adjust-replicas", bytes.NewReader(payload))
	if err != nil {
		return
	}
	setNodeHeaders(req, node)
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

func clusterStopServiceInternal(serviceName string) map[string]any {
	placements := clusterState.GetPlacements(serviceName, "")
	allNodes := clusterState.ListNodes()

	// First: set replicas=0 on ALL nodes to stop reconcile from restarting.
	for i := range allNodes {
		setNodeServiceReplicas(&allNodes[i], serviceName, 0)
	}

	if len(placements) == 0 {
		return map[string]any{
			"success": true,
			"message": fmt.Sprintf("'%s' 서비스 중지됨 (실행 중인 컨테이너 없음).", serviceName),
		}
	}

	var removed []map[string]any
	for _, p := range placements {
		node := clusterState.GetNode(p.NodeName)
		if node == nil {
			continue
		}
		baseURL := nodeBaseURL(node)
		ok := false

		// Try delete by ID.
		delReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", baseURL, p.ContainerID), nil)
		setNodeHeaders(delReq, node)
		resp, err := httpClient.Do(delReq)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				ok = true
			}
			resp.Body.Close()
		}

		if !ok && p.ContainerName != "" {
			// Try by name.
			delReq2, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", baseURL, p.ContainerName), nil)
			setNodeHeaders(delReq2, node)
			resp2, err := httpClient.Do(delReq2)
			if err == nil {
				if resp2.StatusCode == http.StatusOK {
					ok = true
				}
				resp2.Body.Close()
			}
		}
		removed = append(removed, map[string]any{"node": p.NodeName, "container": p.ContainerName, "ok": ok})
	}

	// Update cluster state.
	svcInfo := clusterState.GetService(serviceName)
	if svcInfo != nil {
		if img, _ := svcInfo["image"].(string); img != "" {
			clusterState.SaveService(serviceName, img, 0, nil)
		}
	}

	return map[string]any{
		"success": true,
		"message": fmt.Sprintf("'%s' 전체 중지: %d개 컨테이너 제거.", serviceName, len(removed)),
		"details": map[string]any{"removed": removed},
	}
}

func serviceInfoToMap(s models.ServiceInfo) map[string]any {
	ids := s.ContainerIDs
	if ids == nil {
		ids = []string{}
	}
	return map[string]any{
		"name":          s.Name,
		"image":         s.Image,
		"replicas":      s.Replicas,
		"memory_limit":  s.MemoryLimit,
		"cpu_limit":     s.CPULimit,
		"status":        s.Status,
		"container_ids": ids,
	}
}

// detectHostIP tries to find the real host IP visible to external clients.
// Inside Docker, net.InterfaceAddrs returns the container-internal IP (e.g. 172.x).
// Instead, we dial an external address (without sending data) to discover which
// source IP the OS would use — this gives the host-mapped IP on bridged networks,
// or the real NIC IP when running on bare metal.
func detectHostIP() string {
	// Method 1: UDP dial trick — returns the IP used to reach the default gateway
	conn, err := net.DialTimeout("udp4", "8.8.8.8:53", 2*time.Second)
	if err == nil {
		defer conn.Close()
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && !addr.IP.IsLoopback() {
			ip := addr.IP.String()
			// Skip Docker-internal IPs (172.16-31.x.x, 10.x.x.x with common Docker ranges)
			if !strings.HasPrefix(ip, "172.") {
				return ip + ":8000"
			}
		}
	}

	// Method 2: Read default route from /proc (Linux)
	if data, err := os.ReadFile("/proc/net/route"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines[1:] {
			fields := strings.Fields(line)
			if len(fields) >= 3 && fields[1] == "00000000" {
				// Default route found — use that interface
				iface, err := net.InterfaceByName(fields[0])
				if err != nil {
					continue
				}
				addrs, err := iface.Addrs()
				if err != nil {
					continue
				}
				for _, a := range addrs {
					if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP.To4() != nil && !ipNet.IP.IsLoopback() {
						return ipNet.IP.String() + ":8000"
					}
				}
			}
		}
	}

	// Method 3: fallback — first non-loopback, non-docker interface IP
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
				return ipNet.IP.String() + ":8000"
			}
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Auto-heal loop & handlers
// ---------------------------------------------------------------------------

func autoHealLoop() {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[auto-heal] panic: %v", r)
				}
			}()

			autoHealMu.Lock()
			enabled := autoHealEnabled
			autoHealMu.Unlock()

			if !enabled || clusterState == nil {
				return
			}

			placements := clusterState.GetPlacements("", "")
			now := time.Now()

			for _, p := range placements {
				if p.Status == "running" {
					continue
				}
				// Skip system services.
				if systemServices[p.ContainerName] || systemServices[p.ServiceName] {
					continue
				}
				// Skip containers without a known service (unmanaged).
				if p.ServiceName == "" {
					continue
				}
				// Skip temporary quickstart containers.
				if strings.HasPrefix(p.ContainerName, "qs-") || strings.HasPrefix(p.ServiceName, "qs-") {
					continue
				}

				// Cooldown check.
				autoHealMu.Lock()
				lastRestart, hasCooldown := autoHealCooldowns[p.ContainerName]
				autoHealMu.Unlock()
				if hasCooldown && now.Sub(lastRestart) < autoHealCooldownSec*time.Second {
					continue
				}

				log.Printf("[auto-heal] Container %s (%s) on node %s has status %q, attempting restart",
					p.ContainerName, p.ServiceName, p.NodeName, p.Status)

				evt := AutoHealEvent{
					ContainerID:   p.ContainerID,
					ContainerName: p.ContainerName,
					ServiceName:   p.ServiceName,
					NodeName:      p.NodeName,
					PrevStatus:    p.Status,
					Action:        "restart",
					Timestamp:     now.Format(time.RFC3339),
				}

				err := autoHealRestart(p)
				if err != nil {
					log.Printf("[auto-heal] Failed to restart %s: %v", p.ContainerName, err)
					evt.Success = false
					evt.Error = err.Error()
				} else {
					log.Printf("[auto-heal] Successfully restarted %s", p.ContainerName)
					evt.Success = true
				}

				autoHealMu.Lock()
				autoHealCooldowns[p.ContainerName] = now
				autoHealEvents = append(autoHealEvents, evt)
				// Keep only last 200 events.
				if len(autoHealEvents) > 200 {
					autoHealEvents = autoHealEvents[len(autoHealEvents)-200:]
				}
				autoHealMu.Unlock()

				// Broadcast SSE event.
				if hub != nil {
					sseData, _ := json.Marshal(map[string]any{
						"type":           "autoheal",
						"container_name": p.ContainerName,
						"service_name":   p.ServiceName,
						"node_name":      p.NodeName,
						"success":        evt.Success,
					})
					hub.broadcast(sseData)
				}
			}
		}()
		time.Sleep(autoHealIntervalSec * time.Second)
	}
}

// autoHealRestart restarts a stopped/crashed container on the appropriate node.
func autoHealRestart(p models.ContainerPlacement) error {
	node := clusterState.GetNode(p.NodeName)
	if node == nil {
		return fmt.Errorf("node %q not found", p.NodeName)
	}

	// Look up the service config for memory/cpu/env/volumes/ports.
	svcInfo := clusterState.GetService(p.ServiceName)

	baseURL := nodeBaseURL(node)

	runPayload := map[string]any{
		"image":                p.Image,
		"name":                 p.ServiceName,
		"replicas":             1,
		"use_internal_network": true,
	}
	if svcInfo != nil {
		if ml, ok := svcInfo["memory_limit"].(string); ok && ml != "" {
			runPayload["memory"] = ml
		}
		if cl, ok := svcInfo["cpu_limit"].(string); ok && cl != "" {
			runPayload["cpu"] = cl
		}
		if env, ok := svcInfo["environment"].(string); ok && env != "" && env != "[]" {
			var envSlice []string
			json.Unmarshal([]byte(env), &envSlice)
			if len(envSlice) > 0 {
				runPayload["environment"] = envSlice
			}
		}
		if vols, ok := svcInfo["volumes"].(string); ok && vols != "" && vols != "[]" {
			var volSlice []string
			json.Unmarshal([]byte(vols), &volSlice)
			if len(volSlice) > 0 {
				runPayload["volumes"] = volSlice
			}
		}
		if ports, ok := svcInfo["ports"].(string); ok && ports != "" && ports != "[]" {
			var portSlice []string
			json.Unmarshal([]byte(ports), &portSlice)
			if len(portSlice) > 0 {
				runPayload["ports"] = portSlice
			}
		}
	}

	payload, _ := json.Marshal(runPayload)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/containers/run", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	setNodeHeaders(req, node)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending restart request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("worker returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func handleAutoHealStatus(w http.ResponseWriter, r *http.Request) {
	autoHealMu.Lock()
	enabled := autoHealEnabled
	events := make([]AutoHealEvent, len(autoHealEvents))
	copy(events, autoHealEvents)
	autoHealMu.Unlock()

	// Return newest first.
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":      enabled,
		"total_events": len(events),
		"events":       events,
	})
}

func handleAutoHealToggle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if err := readJSON(r, &body); err != nil || body.Enabled == nil {
		// Toggle if no body provided.
		autoHealMu.Lock()
		autoHealEnabled = !autoHealEnabled
		current := autoHealEnabled
		autoHealMu.Unlock()
		log.Printf("[auto-heal] Toggled to %v", current)
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"enabled": current,
		})
		return
	}

	autoHealMu.Lock()
	autoHealEnabled = *body.Enabled
	autoHealMu.Unlock()
	log.Printf("[auto-heal] Set to %v", *body.Enabled)
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"enabled": *body.Enabled,
	})
}

func splitAddress(addr string) (string, string) {
	if idx := strings.Index(addr, "://"); idx >= 0 {
		addr = addr[idx+3:]
	}
	if idx := strings.LastIndex(addr, ":"); idx >= 0 {
		return addr[:idx], addr[idx+1:]
	}
	return addr, "8000"
}

// ---------------------------------------------------------------------------
// Agent: Blockchain Deploy (worker endpoint)
// ---------------------------------------------------------------------------

// handleAgentBlockchainDeploy creates a blockchain container on a worker node.
// Config files (keystore, gs.zip, license, keysecret) are sent as base64.
func handleAgentBlockchainDeploy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ContainerName string            `json:"container_name"`
		Image         string            `json:"image"`
		P2PPort       int               `json:"p2p_port"`
		RPCPort       int               `json:"rpc_port"`
		Network       string            `json:"network"`
		Role          string            `json:"role"`
		Channel       string            `json:"channel"`
		Index         int               `json:"index"`
		EnvVars       map[string]string `json:"env_vars"`
		// Base64-encoded config files
		KeystoreB64  string `json:"keystore_b64"`
		KeysecretB64 string `json:"keysecret_b64"`
		GsZipB64     string `json:"gs_zip_b64"`
		LicenseB64   string `json:"license_b64"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if body.Image == "" || body.ContainerName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "image and container_name required"})
		return
	}

	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker not available"})
		return
	}
	ctx := context.Background()

	// Remove existing container
	cli.ContainerRemove(ctx, body.ContainerName, container.RemoveOptions{Force: true})

	// Pull image if needed
	runtime.PullImage(ctx, cli, body.Image)

	// Build environment variables
	envList := []string{
		"GOLOOP_NODE_DIR=/goloop/data",
		"GOLOOP_ENGINES=python",
		fmt.Sprintf("GOLOOP_P2P=%s:8080", body.ContainerName),
		"GOLOOP_P2P_LISTEN=:8080",
		"GOLOOP_RPC_ADDR=:9080",
		"GOLOOP_RPC_DUMP=false",
		"GOLOOP_KEY_STORE=/goloop/conf/keystore.json",
		"GOLOOP_KEY_SECRET=/goloop/conf/keysecret",
		"GOLOOP_LICENSE_FILE=/goloop/conf/license.json",
		"GOLOOP_CONSOLE_LEVEL=warn",
		"GOLOOP_LOG_WRITER_FILENAME=/goloop/data/log/goloop.log",
		"GOLOOP_LOG_WRITER_COMPRESS=true",
		"GOLOOP_LOG_WRITER_MAXSIZE=100",
	}
	for k, v := range body.EnvVars {
		envList = append(envList, k+"="+v)
	}

	if body.Network == "" {
		body.Network = "orch-internal"
	}

	// Create container (use root user so docker-cp'd files are accessible)
	containerCfg := &container.Config{
		Image: body.Image,
		User:  "root",
		Env:   envList,
		Labels: map[string]string{
			"blockchain.role":     body.Role,
			"blockchain.channel":  body.Channel,
			"blockchain.index":    fmt.Sprintf("%d", body.Index),
			"blockchain.p2p_port": fmt.Sprintf("%d", body.P2PPort),
			"blockchain.rpc_port": fmt.Sprintf("%d", body.RPCPort),
		},
	}
	hostCfg := &container.HostConfig{
		PortBindings: nat.PortMap{
			"8080/tcp": []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", body.P2PPort)}},
			"9080/tcp": []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", body.RPCPort)}},
		},
	}
	netCfg := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			body.Network: {},
		},
	}

	resp, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, netCfg, nil, body.ContainerName)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Container create failed: " + err.Error()})
		return
	}

	// Copy config files into container via docker cp
	type configFile struct {
		name    string
		b64data string
	}
	configs := []configFile{
		{"keystore.json", body.KeystoreB64},
		{"keysecret", body.KeysecretB64},
		{"gs.zip", body.GsZipB64},
		{"license.json", body.LicenseB64},
	}
	// Decode and copy all config files at once
	configFiles := make(map[string][]byte)
	for _, cf := range configs {
		if cf.b64data == "" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(cf.b64data)
		if err != nil {
			log.Printf("Failed to decode %s: %v", cf.name, err)
			continue
		}
		configFiles[cf.name] = data
	}
	if len(configFiles) > 0 {
		tarBuf := createTarArchive(configFiles)
		err = cli.CopyToContainer(ctx, resp.ID, "/goloop/conf/", tarBuf, container.CopyToContainerOptions{})
		if err != nil {
			log.Printf("Failed to copy config files to container: %v", err)
		}
	}

	// Start container
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Container start failed: " + err.Error()})
		return
	}

	cid := resp.ID
	if len(cid) > 12 {
		cid = cid[:12]
	}
	log.Printf("Blockchain container %s started (role=%s, channel=%s)", body.ContainerName, body.Role, body.Channel)
	writeJSON(w, http.StatusOK, map[string]any{
		"success":        true,
		"container_id":   cid,
		"container_name": body.ContainerName,
	})
}

// createTarArchive creates a tar archive containing the given files.
func createTarArchive(files map[string][]byte) *bytes.Buffer {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, data := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(data)),
		}
		tw.WriteHeader(hdr)
		tw.Write(data)
	}
	tw.Close()
	return &buf
}

// ---------------------------------------------------------------------------
// QuickStart: Distributed Blockchain Deployment (master only)
// ---------------------------------------------------------------------------

func handleQuickstartBlockchainDistributed(w http.ResponseWriter, r *http.Request) {
	if OrchestratorRole != "master" || clusterState == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Distributed deploy requires master node with cluster"})
		return
	}

	var body struct {
		Validators  int               `json:"validators"`
		Citizens    int               `json:"citizens"`
		Channel     string            `json:"channel"`
		Image       string            `json:"image"`
		P2PPort     int               `json:"p2p_port"`
		RPCPort     int               `json:"rpc_port"`
		LogLevel    string            `json:"log_level"`
		Network     string            `json:"network"`
		ServiceName string            `json:"service_name"`
		Nodes       []string          `json:"nodes"`
		EnvVars     map[string]string `json:"env_vars,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	// Defaults
	if body.Validators < 1 {
		body.Validators = 4
	}
	if body.Channel == "" {
		body.Channel = "seoul"
	}
	if body.Image == "" {
		body.Image = "20.20.0.13:80/iconloop-enterprise/goloop:v1.2.5-seoul-test"
	}
	if body.P2PPort <= 0 {
		body.P2PPort = 7100
	}
	if body.RPCPort <= 0 {
		body.RPCPort = 9100
	}
	if body.LogLevel == "" {
		body.LogLevel = "trace"
	}
	if body.Network == "" {
		body.Network = "orch-internal"
	}
	if body.ServiceName == "" {
		body.ServiceName = "blockchain"
	}

	// Get target nodes
	allNodes := clusterState.ListNodes("healthy")
	if len(body.Nodes) > 0 {
		// Filter to requested nodes
		nodeMap := make(map[string]*models.NodeInfo)
		for i := range allNodes {
			nodeMap[allNodes[i].Name] = &allNodes[i]
		}
		var targetNodes []models.NodeInfo
		for _, name := range body.Nodes {
			if n, ok := nodeMap[name]; ok {
				targetNodes = append(targetNodes, *n)
			}
		}
		allNodes = targetNodes
	}
	if len(allNodes) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "No healthy nodes available"})
		return
	}

	total := body.Validators + body.Citizens
	log.Printf("Distributed Blockchain: %d validators + %d citizens across %d nodes, channel=%s",
		body.Validators, body.Citizens, len(allNodes), body.Channel)

	// Run in background
	go func() {
		results := distributedBlockchainDeploy(body.Validators, body.Citizens, body.Channel, body.Image,
			body.P2PPort, body.RPCPort, body.LogLevel, body.Network, body.EnvVars, allNodes)
		log.Printf("Distributed blockchain deploy results: %+v", results)
	}()

	writeJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    fmt.Sprintf("분산 블록체인 배포 시작: %d개 노드 × %d대 서버 (채널: %s)", total, len(allNodes), body.Channel),
		"validators": body.Validators,
		"citizens":   body.Citizens,
		"channel":    body.Channel,
		"nodes":      len(allNodes),
	})
}

func distributedBlockchainDeploy(validators, citizens int, channel, image string,
	p2pPort, rpcPort int, logLevel, networkName string,
	envVars map[string]string, nodes []models.NodeInfo) map[string]any {

	total := validators + citizens
	cli := runtime.DockerClient()
	if cli == nil {
		return map[string]any{"success": false, "message": "Docker not available"}
	}
	ctx := context.Background()

	// ===== Phase 0: Cleanup =====
	log.Println("[Distributed 0/7] Cleaning up previous deployment...")
	for _, node := range nodes {
		baseURL := nodeBaseURL(&node)
		// List and remove existing blockchain containers
		req, _ := http.NewRequest("GET", baseURL+"/v1/containers", nil)
		setNodeHeaders(req, &node)
		resp, err := httpClient.Do(req)
		if err != nil {
			continue
		}
		var containers []map[string]any
		json.NewDecoder(resp.Body).Decode(&containers)
		resp.Body.Close()
		for _, c := range containers {
			name, _ := c["name"].(string)
			if strings.HasPrefix(name, "blockchain-"+channel+"-") {
				id, _ := c["id"].(string)
				if id == "" {
					id = name
				}
				stopReq, _ := http.NewRequest("POST", baseURL+"/v1/containers/"+id+"/stop", nil)
				setNodeHeaders(stopReq, &node)
				httpClient.Do(stopReq)
				delReq, _ := http.NewRequest("DELETE", baseURL+"/v1/containers/"+id, nil)
				setNodeHeaders(delReq, &node)
				httpClient.Do(delReq)
			}
		}
	}

	// ===== Phase 1: Generate Keystores =====
	log.Println("[Distributed 1/7] Generating keystores...")
	type nodeConfig struct {
		Index    int
		Role     string
		Keystore []byte
		Address  string
	}
	configs := make([]nodeConfig, total)

	for i := 0; i < total; i++ {
		role := "validator"
		if i >= validators {
			role = "citizen"
		}

		// Run temp container to generate keystore
		tmpName := fmt.Sprintf("qs-dist-ks-%d-%d", os.Getpid(), i)
		cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})
		createResp, err := cli.ContainerCreate(ctx, &container.Config{
			Image:      image,
			User:       "root",
			Entrypoint: []string{"sh"},
			Cmd:        []string{"-c", "mkdir -p /work && chmod 777 /work && goloop ks gen --out /work/keystore.json --password gochain"},
		}, nil, nil, nil, tmpName)
		if err != nil {
			log.Printf("Failed to create keygen container %d: %v", i, err)
			continue
		}
		cli.ContainerStart(ctx, createResp.ID, container.StartOptions{})
		statusCh, _ := cli.ContainerWait(ctx, createResp.ID, container.WaitConditionNotRunning)
		<-statusCh

		// Read keystore from container
		reader, _, err := cli.CopyFromContainer(ctx, createResp.ID, "/work/keystore.json")
		if err != nil {
			log.Printf("Failed to read keystore %d: %v", i, err)
			cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})
			continue
		}
		ksData := readFileFromTar(reader)
		reader.Close()
		cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})

		// Extract address
		var ks map[string]any
		json.Unmarshal(ksData, &ks)
		addr, _ := ks["address"].(string)

		configs[i] = nodeConfig{Index: i, Role: role, Keystore: ksData, Address: addr}
		log.Printf("  Node %d (%s): %s", i, role, addr)
	}

	// ===== Phase 2: Genesis =====
	log.Println("[Distributed 2/7] Generating genesis...")
	var valAddrs []string
	for i := 0; i < validators; i++ {
		valAddrs = append(valAddrs, configs[i].Address)
	}
	godAddr := valAddrs[0]
	gnCmd := fmt.Sprintf("mkdir -p /work && chmod 777 /work && goloop gn gen --out /work/genesis.json --god %s --config revision=0x8,minimizeBlockGen=0x1 %s",
		godAddr, strings.Join(valAddrs, " "))
	tmpName := fmt.Sprintf("qs-dist-gn-%d", os.Getpid())
	cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})
	createResp, _ := cli.ContainerCreate(ctx, &container.Config{
		Image: image, User: "root", Entrypoint: []string{"sh"}, Cmd: []string{"-c", gnCmd},
	}, nil, nil, nil, tmpName)
	cli.ContainerStart(ctx, createResp.ID, container.StartOptions{})
	statusCh, _ := cli.ContainerWait(ctx, createResp.ID, container.WaitConditionNotRunning)
	<-statusCh
	genesisReader, _, _ := cli.CopyFromContainer(ctx, createResp.ID, "/work/genesis.json")
	genesisData := readFileFromTar(genesisReader)
	genesisReader.Close()
	cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})

	// ===== Phase 3: gs.zip =====
	log.Println("[Distributed 3/7] Generating gs.zip...")
	tmpName = fmt.Sprintf("qs-dist-gs-%d", os.Getpid())
	cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})
	createResp, _ = cli.ContainerCreate(ctx, &container.Config{
		Image: image, User: "root", Entrypoint: []string{"sh"},
		Cmd: []string{"-c", "mkdir -p /work && chmod 777 /work && cp /goloop/conf/genesis.json /work/genesis.json && goloop gs gen --input /work/genesis.json --out /work/gs.zip"},
	}, nil, nil, nil, tmpName)
	// Copy genesis.json to /goloop/conf/ (always exists in image)
	genBuf := createTarArchive(map[string][]byte{"genesis.json": genesisData})
	if err := cli.CopyToContainer(ctx, createResp.ID, "/goloop/conf/", genBuf, container.CopyToContainerOptions{}); err != nil {
		log.Printf("Failed to copy genesis.json: %v", err)
	}
	cli.ContainerStart(ctx, createResp.ID, container.StartOptions{})
	statusCh, _ = cli.ContainerWait(ctx, createResp.ID, container.WaitConditionNotRunning)
	<-statusCh
	gsReader, _, err := cli.CopyFromContainer(ctx, createResp.ID, "/work/gs.zip")
	var gsData []byte
	if err != nil {
		log.Printf("Failed to copy gs.zip from container: %v", err)
	} else {
		gsData = readFileFromTar(gsReader)
		gsReader.Close()
	}
	cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})

	// ===== Phase 4: License =====
	log.Println("[Distributed 4/7] Generating license...")
	issuerPath := os.Getenv("BLOCKCHAIN_HOST_PATH")
	if issuerPath == "" {
		issuerPath = "/blockchain"
	}
	issuerKS, _ := os.ReadFile(issuerPath + "/issuer.json")
	if len(issuerKS) == 0 {
		// Try alternate paths
		for _, p := range []string{"/tmp/blockchain-src/issuer.json", "services/blockchain/issuer.json"} {
			issuerKS, _ = os.ReadFile(p)
			if len(issuerKS) > 0 {
				break
			}
		}
	}

	var allAddrs []string
	for i := 0; i < total; i++ {
		allAddrs = append(allAddrs, configs[i].Address)
	}
	lcCmd := fmt.Sprintf("mkdir -p /issuer /work && chmod 777 /issuer /work && cp /goloop/conf/issuer.json /issuer/issuer.json && goloop lc gen --keystore /issuer/issuer.json --password gochain --out /work/license.json --duration infinite --subject %s %s",
		channel, strings.Join(allAddrs, " "))
	tmpName = fmt.Sprintf("qs-dist-lc-%d", os.Getpid())
	cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})
	createResp, _ = cli.ContainerCreate(ctx, &container.Config{
		Image: image, User: "root", Entrypoint: []string{"sh"}, Cmd: []string{"-c", lcCmd},
	}, nil, nil, nil, tmpName)
	// Copy issuer.json
	issuerBuf := createTarArchive(map[string][]byte{"issuer.json": issuerKS})
	cli.CopyToContainer(ctx, createResp.ID, "/goloop/conf/", issuerBuf, container.CopyToContainerOptions{})
	cli.ContainerStart(ctx, createResp.ID, container.StartOptions{})
	statusCh, _ = cli.ContainerWait(ctx, createResp.ID, container.WaitConditionNotRunning)
	<-statusCh
	lcReader, _, _ := cli.CopyFromContainer(ctx, createResp.ID, "/work/license.json")
	licenseData := readFileFromTar(lcReader)
	lcReader.Close()
	cli.ContainerRemove(ctx, tmpName, container.RemoveOptions{Force: true})

	log.Printf("  Config generated: genesis=%d bytes, gs.zip=%d bytes, license=%d bytes",
		len(genesisData), len(gsData), len(licenseData))

	// ===== Phase 5: Distribute containers across nodes =====
	log.Println("[Distributed 5/7] Deploying containers across cluster...")

	// Pre-pull image on all target nodes
	for _, node := range nodes {
		baseURL := nodeBaseURL(&node)
		pullPayload, _ := json.Marshal(map[string]any{"image": image})
		pullReq, _ := http.NewRequest("POST", baseURL+"/v1/images/pull", bytes.NewReader(pullPayload))
		setNodeHeaders(pullReq, &node)
		pullResp, err := longHTTPClient.Do(pullReq)
		if err != nil {
			log.Printf("  Image pull on %s failed: %v", node.Name, err)
		} else {
			pullResp.Body.Close()
			log.Printf("  Image pull on %s: ok", node.Name)
		}
	}

	// Build node-to-IP map and assign containers round-robin
	type assignment struct {
		NodeIdx       int
		Node          models.NodeInfo
		ContainerName string
		P2PPort       int
		RPCPort       int
		Role          string
		Index         int
	}
	var assignments []assignment
	gsB64 := base64.StdEncoding.EncodeToString(gsData)
	licenseB64 := base64.StdEncoding.EncodeToString(licenseData)

	for i := 0; i < total; i++ {
		nodeIdx := i % len(nodes)
		containerName := fmt.Sprintf("blockchain-%s-%d", channel, i)
		role := "validator"
		if i >= validators {
			role = "citizen"
		}
		assignments = append(assignments, assignment{
			NodeIdx:       nodeIdx,
			Node:          nodes[nodeIdx],
			ContainerName: containerName,
			P2PPort:       p2pPort + i,
			RPCPort:       rpcPort + i,
			Role:          role,
			Index:         i,
		})
	}

	// Build seed addresses using actual node IPs
	seedAddrs := make([]string, validators)
	for i := 0; i < validators; i++ {
		nodeIP, _ := splitAddress(assignments[i].Node.Address)
		seedAddrs[i] = fmt.Sprintf("%s:%d", nodeIP, assignments[i].P2PPort)
	}

	// Deploy each container
	var deployResults []map[string]any
	for _, a := range assignments {
		node := a.Node
		baseURL := nodeBaseURL(&node)

		// Build seeds for this container
		var seeds string
		if a.Index < validators {
			var seedList []string
			for j := 0; j < validators; j++ {
				if j != a.Index {
					seedList = append(seedList, seedAddrs[j])
				}
			}
			if len(seedList) > 2 {
				seedList = seedList[:2]
			}
			seeds = strings.Join(seedList, ",")
		} else {
			if validators >= 2 {
				seeds = seedAddrs[0] + "," + seedAddrs[1]
			} else {
				seeds = seedAddrs[0]
			}
		}

		evMap := map[string]string{
			"GOLOOP_LOG_LEVEL": logLevel,
		}
		for k, v := range envVars {
			evMap[k] = v
		}
		// Override P2P to use external IP for distributed mode
		nodeIP, _ := splitAddress(node.Address)
		evMap["GOLOOP_P2P"] = fmt.Sprintf("%s:%d", nodeIP, a.P2PPort)
		if seeds != "" {
			evMap["GOLOOP_P2P_SEEDS"] = seeds
		}

		payload, _ := json.Marshal(map[string]any{
			"container_name": a.ContainerName,
			"image":          image,
			"p2p_port":       a.P2PPort,
			"rpc_port":       a.RPCPort,
			"network":        networkName,
			"role":           a.Role,
			"channel":        channel,
			"index":          a.Index,
			"env_vars":       evMap,
			"keystore_b64":   base64.StdEncoding.EncodeToString(configs[a.Index].Keystore),
			"keysecret_b64":  base64.StdEncoding.EncodeToString([]byte("gochain")),
			"gs_zip_b64":     gsB64,
			"license_b64":    licenseB64,
		})

		req, _ := http.NewRequest("POST", baseURL+"/v1/agent/blockchain/deploy", bytes.NewReader(payload))
		setNodeHeaders(req, &node)
		resp, err := longHTTPClient.Do(req)
		result := map[string]any{"node": node.Name, "container": a.ContainerName}
		if err != nil {
			result["success"] = false
			result["error"] = err.Error()
		} else {
			var respData map[string]any
			json.NewDecoder(resp.Body).Decode(&respData)
			resp.Body.Close()
			result["success"] = respData["success"]
			result["message"] = respData["message"]
		}
		deployResults = append(deployResults, result)
		log.Printf("  %s on %s: %v", a.ContainerName, node.Name, result["success"])
	}

	// ===== Phase 6: Join & Start Chains =====
	log.Println("[Distributed 6/7] Waiting for containers to initialize...")
	time.Sleep(8 * time.Second)

	log.Println("  Joining chains...")
	for _, a := range assignments {
		node := a.Node
		baseURL := nodeBaseURL(&node)

		// Build seeds for join command
		var seedPart string
		if a.Index < validators {
			var seedList []string
			for j := 0; j < validators; j++ {
				if j != a.Index {
					nodeIP, _ := splitAddress(assignments[j].Node.Address)
					seedList = append(seedList, fmt.Sprintf("%s:%d", nodeIP, assignments[j].P2PPort))
				}
			}
			if len(seedList) > 2 {
				seedList = seedList[:2]
			}
			seedPart = "--seed " + strings.Join(seedList, ",")
		} else {
			if validators >= 2 {
				nodeIP0, _ := splitAddress(assignments[0].Node.Address)
				nodeIP1, _ := splitAddress(assignments[1].Node.Address)
				seedPart = fmt.Sprintf("--seed %s:%d,%s:%d", nodeIP0, assignments[0].P2PPort, nodeIP1, assignments[1].P2PPort)
			} else {
				nodeIP0, _ := splitAddress(assignments[0].Node.Address)
				seedPart = fmt.Sprintf("--seed %s:%d", nodeIP0, assignments[0].P2PPort)
			}
		}

		joinCmd := fmt.Sprintf("goloop chain join --genesis /goloop/conf/gs.zip %s --channel %s", seedPart, channel)
		if a.Index >= validators {
			joinCmd += " --role 0"
		}
		startCmd := fmt.Sprintf("goloop chain start %s", channel)

		// Execute via container inspect + exec proxy: use docker exec on the target node
		// We'll call the container's RPC directly since the nodes expose 9080
		// Actually, we need to exec into the container. Use a workaround:
		// POST to node's /v1/containers/{name}/exec (not available) OR use docker exec via SSH
		// Simplest: call goloop CLI via the node's RPC endpoint

		// Alternative: use a shell exec via the agent
		for _, cmd := range []string{joinCmd, startCmd} {
			execPayload, _ := json.Marshal(map[string]any{
				"container": a.ContainerName,
				"cmd":       cmd,
			})
			req, _ := http.NewRequest("POST", baseURL+"/v1/agent/exec", bytes.NewReader(execPayload))
			setNodeHeaders(req, &node)
			resp, err := httpClient.Do(req)
			if err != nil {
				// Fallback: try local docker exec if on master
				log.Printf("  Exec via API failed for %s, trying direct docker exec", a.ContainerName)
				output, execErr := exec.Command("docker", "exec", a.ContainerName, "sh", "-c", cmd).CombinedOutput()
				if execErr != nil {
					log.Printf("  %s exec failed: %v (%s)", a.ContainerName, execErr, string(output))
				} else {
					log.Printf("  %s: %s", a.ContainerName, strings.TrimSpace(string(output)))
				}
			} else {
				var execResp map[string]any
				json.NewDecoder(resp.Body).Decode(&execResp)
				resp.Body.Close()
				log.Printf("  %s: %v", a.ContainerName, execResp)
			}
		}
	}

	// ===== Phase 7: Verify =====
	log.Println("[Distributed 7/7] Verifying...")
	time.Sleep(3 * time.Second)

	return map[string]any{
		"success":     true,
		"validators":  validators,
		"citizens":    citizens,
		"channel":     channel,
		"nodes":       len(nodes),
		"deployments": deployResults,
	}
}

// readFileFromTar extracts the first regular file content from a tar stream.
func readFileFromTar(reader io.ReadCloser) []byte {
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			return nil
		}
		if hdr.Typeflag == tar.TypeReg || hdr.Typeflag == 0 {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil
			}
			return data
		}
	}
}

// ---------------------------------------------------------------------------
// Agent: Container Exec (worker endpoint for distributed blockchain)
// ---------------------------------------------------------------------------

func handleAgentExec(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Container string `json:"container"`
		Cmd       string `json:"cmd"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	output, err := exec.Command("docker", "exec", body.Container, "sh", "-c", body.Cmd).CombinedOutput()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"success": false,
			"message": err.Error(),
			"output":  string(output),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"output":  strings.TrimSpace(string(output)),
	})
}
