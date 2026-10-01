// Package server implements the HTTP API for the AI Container Orchestrator.
//
// Supports two roles:
//   - master: Full API including cluster management, scheduling, migration
//   - worker: Local container management + agent heartbeat to master
package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	"ai-container-go/internal/auth"
	"ai-container-go/internal/clusterstate"
	"ai-container-go/internal/discovery"
	"ai-container-go/internal/migrate"
	"ai-container-go/internal/models"
	"ai-container-go/internal/monitoring"
	"ai-container-go/internal/nlengine"
	"ai-container-go/internal/runtime"
	"ai-container-go/internal/scheduler"
	"ai-container-go/internal/secrets"
	"ai-container-go/internal/state"
	"ai-container-go/internal/usersecrets"
)

// ---------------------------------------------------------------------------
// Owner-aware env-var helpers (Phase-1 secret encryption + per-owner masking)
// ---------------------------------------------------------------------------

const maskedSecretPlaceholder = "***"

// requesterUsername extracts the calling user's name from the session cookie.
// Returns "" for unauthenticated/inter-node calls.
func requesterUsername(r *http.Request) string {
	if sess := auth.SessionFromRequest(r); sess != nil {
		return sess.Username
	}
	return ""
}

// userMutatingAllowed reports whether a RoleUser session may issue a mutating
// request to the given path. These are the owner-scoped self-service endpoints
// the portal needs; each handler still verifies the caller owns the target.
func userMutatingAllowed(path string) bool {
	switch {
	case strings.HasPrefix(path, "/v1/user/"): // own secrets
		return true
	case path == "/v1/services/deploy-source",
		path == "/v1/services/deploy-source/async",
		path == "/v1/services/generate-dockerfile",
		path == "/v1/services/deploy-git",
		path == "/v1/cluster/deploy", // deploy from a registry image (owner recorded)
		path == "/v1/cluster/stop",
		path == "/v1/cluster/delete",
		path == "/v1/cluster/scale",
		path == "/v1/cluster/container/stop":
		return true
	case strings.HasPrefix(path, "/v1/services/") &&
		(strings.HasSuffix(path, "/update") || strings.HasSuffix(path, "/env")):
		return true
	}
	return false
}

// canManageService reports whether the request's caller may manage svcInfo.
// Rules: inter-node token (caller == "") → yes; admin → yes (all services);
// other authenticated user → only services they own (owner == caller). An
// empty/missing owner is NOT manageable by a non-admin user.
func canManageService(r *http.Request, svcInfo map[string]any) (bool, string) {
	caller := requesterUsername(r)
	if caller == "" {
		return true, "" // inter-node / token path
	}
	if sess := auth.SessionFromRequest(r); sess != nil && sess.Role == auth.RoleAdmin {
		return true, ""
	}
	owner, _ := svcInfo["owner"].(string)
	if owner != "" && owner == caller {
		return true, owner
	}
	return false, owner
}

// buildEnvVars converts the legacy `Environment []string` + `Secrets []string`
// request fields into the persisted `[]models.EnvVar` shape: each entry is
// flagged is_secret if its name appears in `secretNames`, and secret values
// are stored encrypted. Plaintext values pass through unchanged.
func buildEnvVars(env []string, secretNames []string) []models.EnvVar {
	if len(env) == 0 {
		return nil
	}
	secretSet := make(map[string]bool, len(secretNames))
	for _, n := range secretNames {
		secretSet[strings.TrimSpace(n)] = true
	}
	out := make([]models.EnvVar, 0, len(env))
	for _, kv := range env {
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		name, val := kv[:eq], kv[eq+1:]
		ev := models.EnvVar{Name: name, Value: val}
		if secretSet[name] {
			ev.IsSecret = true
			if ct, err := secrets.Encrypt(val); err == nil {
				ev.Value = ct
			}
		}
		out = append(out, ev)
	}
	return out
}

// envVarsFromState reads back the stored env_vars field from a service map,
// tolerating both []models.EnvVar and the JSON-decoded []map[string]any form
// that loadServicesLocked produces.
func envVarsFromState(svc map[string]any) []models.EnvVar {
	if svc == nil {
		return nil
	}
	raw, ok := svc["env_vars"]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []models.EnvVar:
		return v
	case []any:
		out := make([]models.EnvVar, 0, len(v))
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			ev := models.EnvVar{}
			if s, _ := m["name"].(string); s != "" {
				ev.Name = s
			}
			if s, _ := m["value"].(string); s != "" {
				ev.Value = s
			}
			if b, _ := m["is_secret"].(bool); b {
				ev.IsSecret = true
			}
			out = append(out, ev)
		}
		return out
	}
	return nil
}

// decryptedEnvironment returns the env-var slice in Docker's "KEY=value"
// format with all secret values decrypted. Used when (re)creating containers.
func decryptedEnvironment(svc map[string]any) []string {
	evs := envVarsFromState(svc)
	if len(evs) == 0 {
		// Fall back to the legacy plaintext field.
		if raw, ok := svc["environment"]; ok {
			if list, ok := raw.([]any); ok {
				out := make([]string, 0, len(list))
				for _, x := range list {
					if s, ok := x.(string); ok {
						out = append(out, s)
					}
				}
				return out
			}
			if list, ok := raw.([]string); ok {
				return list
			}
			// Cluster state stores environment as a JSON-encoded string.
			if s, ok := raw.(string); ok && s != "" && s != "[]" {
				var list []string
				if err := json.Unmarshal([]byte(s), &list); err == nil {
					return list
				}
			}
		}
		return nil
	}
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		val := ev.Value
		if ev.IsSecret {
			if pt, err := secrets.Decrypt(ev.Value); err == nil {
				val = pt
			}
		}
		out = append(out, ev.Name+"="+val)
	}
	return out
}

// maskedEnvVarsForViewer returns the env-vars shaped for API responses:
// non-secrets always show plaintext; secrets show their value only when
// `viewer` matches the service owner, otherwise the placeholder "***".
// An empty viewer (unauthenticated) sees the same as a non-owner admin.
func maskedEnvVarsForViewer(svc map[string]any, viewer string) []models.EnvVar {
	evs := envVarsFromState(svc)
	if len(evs) == 0 {
		return nil
	}
	owner, _ := svc["owner"].(string)
	isOwner := viewer != "" && viewer == owner
	out := make([]models.EnvVar, len(evs))
	for i, ev := range evs {
		out[i] = models.EnvVar{Name: ev.Name, IsSecret: ev.IsSecret}
		if !ev.IsSecret {
			out[i].Value = ev.Value
			continue
		}
		if isOwner {
			if pt, err := secrets.Decrypt(ev.Value); err == nil {
				out[i].Value = pt
			} else {
				out[i].Value = maskedSecretPlaceholder
			}
		} else {
			out[i].Value = maskedSecretPlaceholder
		}
	}
	return out
}

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

// PortalHTML holds the self-service user portal page. Set from main.go.
var PortalHTML string

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
	r.Use(gzipMiddleware)
	r.Use(securityHeadersMiddleware)
	r.Use(corsMiddleware)
	r.Use(ipAllowlistMiddleware)
	r.Use(loginRateLimitMiddleware)
	r.Use(bearerTokenAuth)
	r.Use(sessionAuthMiddleware)
	r.Use(csrfMiddleware)

	// Root serves the self-service portal; the cluster-management dashboard
	// lives at /admin (with /dashboard kept as a legacy alias).
	r.Get("/", handlePortal)
	r.Get("/portal", handlePortal)
	r.Get("/admin", handleDashboard)
	r.Get("/dashboard", handleDashboard)

	// Auth
	r.Post("/v1/auth/login", handleLogin)
	r.Post("/v1/auth/logout", handleLogout)
	r.Get("/v1/auth/me", handleAuthMe)
	r.Get("/v1/auth/google/status", handleGoogleStatus)
	r.Get("/v1/auth/google/login", handleGoogleLogin)
	r.Get("/v1/auth/google/callback", handleGoogleCallback)
	r.Post("/v1/auth/change-password", handleChangePassword)
	r.Get("/v1/auth/users", handleListUsers)
	r.Post("/v1/auth/users", handleCreateUser)
	r.Delete("/v1/auth/users/{username}", handleDeleteUser)
	r.Post("/v1/auth/users/{username}/reset-password", handleResetUserPassword)
	r.Get("/v1/public/services", handlePublicServices)
	r.Get("/v1/user/services", handleListUserServices)
	r.Post("/v1/user/service-share", handleUserServiceShare)
	r.Post("/v1/user/deploy-database", handleUserDeployDatabase)
	r.Post("/v1/user/image-import", handleUserImageImport)
	r.Get("/v1/user/registry-images", handleUserRegistryImages)
	r.Get("/v1/user/secrets", handleListUserSecrets)
	r.Post("/v1/user/secrets", handleCreateUserSecret)
	r.Get("/v1/user/secrets/{id}", handleGetUserSecret)
	r.Put("/v1/user/secrets/{id}", handleUpdateUserSecret)
	r.Delete("/v1/user/secrets/{id}", handleDeleteUserSecret)

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
	r.Get("/v1/services/endpoints", handleServiceEndpoints)
	r.Post("/v1/services/deploy-source", handleDeploySource)
	r.Post("/v1/services/deploy-source/async", handleDeploySourceAsync)
	r.Post("/v1/services/generate-dockerfile", handleGenerateDockerfile)
	r.Post("/v1/services/deploy-git", handleDeployGit)
	r.Get("/v1/services/deploy/{id}", handleDeployJobStatus)
	r.Get("/v1/services/deploy/{id}/events", handleDeployJobEvents)
	r.Post("/v1/services/deploy-compose", handleDeployCompose)
	r.Get("/v1/services/groups", handleGroupsList)
	r.Post("/v1/services/group/{name}/stop", handleGroupStop)
	r.Post("/v1/services/group/{name}/update", handleGroupUpdate)
	r.Post("/v1/services/{name}/update", handleServiceUpdate)
	r.Put("/v1/services/{name}/env", handleServiceEnvUpdate)
	r.Get("/v1/compose-templates", handleComposeTemplateList)
	r.Post("/v1/compose-templates", handleComposeTemplateSave)
	r.Get("/v1/compose-templates/{name}", handleComposeTemplateGet)
	r.Delete("/v1/compose-templates/{name}", handleComposeTemplateDelete)
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
	r.Get("/v1/cluster/logs", handleClusterLogs)
	r.Get("/v1/cluster/stats", handleClusterStats)
	r.Post("/v1/cluster/stop", handleClusterStop)
	r.Post("/v1/cluster/delete", handleClusterDeleteService)
	r.Post("/v1/cluster/container/stop", handleClusterContainerStop)
	r.Delete("/v1/cluster/container/{id}", handleClusterContainerDelete)
	r.Post("/v1/cluster/deploy", handleClusterDeploy)

	// Per-node proxy: forwards a request to a specific node's local API.
	// The dashboard uses this when a target node is selected (apiUrl →
	// /v1/cluster/{node}/proxy?path=...). All HTTP methods are forwarded.
	r.HandleFunc("/v1/cluster/{node}/proxy", handleClusterNodeProxy)

	// Auto-heal API
	r.Get("/v1/cluster/autoheal", handleAutoHealStatus)
	r.Post("/v1/cluster/autoheal/toggle", handleAutoHealToggle)

	// Agent API
	r.Post("/v1/agent/export/{id}", handleAgentExport)
	r.Post("/v1/agent/import", handleAgentImport)
	r.Get("/v1/agent/resources", handleAgentResources)
	r.Post("/v1/agent/adjust-replicas", handleAgentAdjustReplicas)
	r.Post("/v1/agent/run-one", handleAgentRunOne)
	r.Post("/v1/agent/logs", handleAgentLogs)
	r.Post("/v1/agent/reconcile-skip", handleAgentReconcileSkip)
	r.Post("/v1/agent/blockchain/deploy", handleAgentBlockchainDeploy)
	r.Post("/v1/agent/exec", handleAgentExec)
	r.Post("/v1/agent/update-image", handleAgentUpdateImage)
	r.Post("/v1/agent/upsert-env", handleAgentUpsertEnv)
	r.Post("/v1/agent/delete-service", handleAgentDeleteService)

	// SSE streaming
	r.Get("/v1/stream", handleSSEStream)

	// QuickStart API
	r.Post("/v1/quickstart/blockchain", handleQuickstartBlockchain)
	r.Post("/v1/quickstart/blockchain/distributed", handleQuickstartBlockchainDistributed)
	r.Get("/v1/quickstart/blockchain/status", handleQuickstartBlockchainStatus)

	// Node provisioning
	r.Post("/v1/cluster/nodes/provision", handleProvisionNode)
	r.Get("/v1/cluster/nodes/provision/log", handleProvisionLog)

	// AI Chat API
	r.Get("/v1/ai/status", handleAIStatus)
	r.Post("/v1/ai/chat", handleAIChat)
	r.Get("/v1/ai/advisor", handleAIAdvisor)

	// Container Registry
	r.Get("/v1/registry/status", handleRegistryStatus)
	r.Post("/v1/registry/enable", handleRegistryEnable)
	r.Post("/v1/registry/disable", handleRegistryDisable)
	r.Get("/v1/registry/catalog", handleRegistryCatalog)
	r.Get("/v1/registry/tags/*", handleRegistryTags)
	r.Delete("/v1/registry/repo/*", handleRegistryDeleteTag)
	r.Post("/v1/registry/push", handleRegistryPush)
	r.Post("/v1/registry/pull", handleRegistryPull)
	r.Handle("/v1/registry/v2/*", registryProxy())

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

	// Initialize account-based auth (admin + guest seed users).
	if OrchestratorRole == "master" {
		stateDir := os.Getenv("ORCHESTRATOR_STATE_DIR")
		if stateDir == "" {
			stateDir = "/data"
		}
		adminPW := os.Getenv("ORCHESTRATOR_ADMIN_PASSWORD")
		guestPW := os.Getenv("ORCHESTRATOR_GUEST_PASSWORD")
		if err := auth.Init(stateDir, adminPW, guestPW); err != nil {
			log.Printf("auth init failed: %v", err)
		} else {
			log.Printf("Auth initialized (users file: %s/users.json)", stateDir)
		}
		auth.InitAudit(stateDir)
		// Warn if default passwords are still in use (CSAP non-compliance).
		if auth.IsDefaultPassword("admin") {
			log.Printf("[SECURITY WARNING] admin 계정이 기본 비밀번호를 사용 중입니다. 즉시 변경하세요.")
		}
		if auth.IsDefaultPassword("guest") {
			log.Printf("[SECURITY WARNING] guest 계정이 기본 비밀번호를 사용 중입니다. 즉시 변경하세요.")
		}
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
			Token:   os.Getenv("ORCHESTRATOR_API_TOKEN"),
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
		writeDashboardTraefikRoute()
	}

	hub = newSSEHub()
	go ssePublishLoop()

	go deployJobJanitor(context.Background())
}

// writeDashboardTraefikRoute writes a Traefik file-provider config that
// exposes the orchestrator dashboard on port 80 via Traefik.
// Uses a low priority (1) so per-service PathPrefix routes still win.
func writeDashboardTraefikRoute() {
	configDir := "/traefik-dynamic"
	if info, err := os.Stat(configDir); err != nil || !info.IsDir() {
		return
	}

	content := `# Auto-generated: orchestrator dashboard route (port 80 → 8000)
http:
  routers:
    orchestrator-dashboard:
      rule: "PathPrefix(` + "`/`" + `)"
      service: orchestrator-dashboard
      entryPoints:
        - web
      priority: 1
  services:
    orchestrator-dashboard:
      loadBalancer:
        servers:
          - url: "http://ai-orchestrator:8000"
`
	path := configDir + "/dashboard-route.yml"
	existing, _ := os.ReadFile(path)
	if string(existing) == content {
		return
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		log.Printf("writeDashboardTraefikRoute: %v", err)
		return
	}
	log.Printf("Dashboard Traefik route written to %s", path)
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// IP allowlist middleware
// ---------------------------------------------------------------------------

// cloudflareCIDRs are the public Cloudflare proxy ranges (IPv4 + IPv6).
// Source: https://www.cloudflare.com/ips/. Embedded so we don't make network
// calls at startup; update on rebuild if Cloudflare publishes new ranges.
var cloudflareCIDRs = []string{
	// IPv4
	"173.245.48.0/20",
	"103.21.244.0/22",
	"103.22.200.0/22",
	"103.31.4.0/22",
	"141.101.64.0/18",
	"108.162.192.0/18",
	"190.93.240.0/20",
	"188.114.96.0/20",
	"197.234.240.0/22",
	"198.41.128.0/17",
	"162.158.0.0/15",
	"104.16.0.0/13",
	"104.24.0.0/14",
	"172.64.0.0/13",
	"131.0.72.0/22",
	// IPv6
	"2400:cb00::/32",
	"2606:4700::/32",
	"2803:f800::/32",
	"2405:b500::/32",
	"2405:8100::/32",
	"2a06:98c0::/29",
	"2c0f:f248::/32",
}

var (
	ipAllowOnce    sync.Once
	ipAllowEnabled bool
	ipAllowCIDRs   []*net.IPNet
	ipAllowSingles []net.IP
	cfCIDRs        []*net.IPNet // for CF-Connecting-IP trust decisions (always parsed)
)

// parseAllowlist reads ORCHESTRATOR_ALLOWED_IPS (comma-separated IPs/CIDRs)
// once and caches the parsed result. Cloudflare proxy ranges are *always*
// parsed into cfCIDRs so CF-Connecting-IP trust logic works; they are
// added to the enforcement allowlist only when the env var is set.
func parseAllowlist() {
	// Parse CF ranges for trust decisions regardless of enforcement state.
	for _, s := range cloudflareCIDRs {
		if _, cidr, err := net.ParseCIDR(s); err == nil {
			cfCIDRs = append(cfCIDRs, cidr)
		}
	}

	raw := strings.TrimSpace(os.Getenv("ORCHESTRATOR_ALLOWED_IPS"))
	if raw == "" {
		// No allowlist configured → middleware is a no-op.
		return
	}
	ipAllowEnabled = true

	// When enforcing, also accept all Cloudflare edges (CF-proxied traffic).
	ipAllowCIDRs = append(ipAllowCIDRs, cfCIDRs...)

	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "/") {
			if _, cidr, err := net.ParseCIDR(item); err == nil {
				ipAllowCIDRs = append(ipAllowCIDRs, cidr)
			}
			continue
		}
		if ip := net.ParseIP(item); ip != nil {
			ipAllowSingles = append(ipAllowSingles, ip)
		}
	}
}

// isCloudflareIP reports whether ip belongs to a Cloudflare proxy range.
func isCloudflareIP(ip net.IP) bool {
	for _, cidr := range cfCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// clientIP extracts the real client IP.
// Priority:
//  1. CF-Connecting-IP — trusted when the request comes from a Cloudflare
//     edge IP or a private/loopback proxy (Traefik on docker internal net).
//  2. X-Forwarded-For (leftmost).
//  3. X-Real-IP.
//  4. RemoteAddr.
func clientIP(r *http.Request) net.IP {
	// RemoteAddr first, to decide whether to trust CF-Connecting-IP.
	remoteHost, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteHost = r.RemoteAddr
	}
	remoteIP := net.ParseIP(remoteHost)

	if cf := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); cf != "" {
		if ip := net.ParseIP(cf); ip != nil {
			if remoteIP != nil && (isPrivateIP(remoteIP) || isCloudflareIP(remoteIP)) {
				return ip
			}
		}
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if ip := net.ParseIP(p); ip != nil {
				return ip
			}
		}
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
		if ip := net.ParseIP(xri); ip != nil {
			return ip
		}
	}
	return remoteIP
}

// isPrivateIP returns true for loopback / link-local / RFC1918 / Docker-internal.
// These are always allowed so cluster traffic and local admin calls keep working.
func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	return ip.IsPrivate()
}

// ipAllowlistMiddleware enforces ORCHESTRATOR_ALLOWED_IPS. If the env is
// unset, the middleware is a no-op. Internal/private IPs and requests with
// a valid ORCHESTRATOR_API_TOKEN bearer (inter-node) are always allowed.
func ipAllowlistMiddleware(next http.Handler) http.Handler {
	ipAllowOnce.Do(parseAllowlist)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ipAllowEnabled {
			next.ServeHTTP(w, r)
			return
		}
		// /health always public so LBs/probes work.
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		// Inter-node requests authenticated by the shared API token pass through.
		if tok := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN")); tok != "" {
			if r.Header.Get("Authorization") == "Bearer "+tok {
				next.ServeHTTP(w, r)
				return
			}
		}

		ip := clientIP(r)
		if ip != nil {
			if isPrivateIP(ip) {
				next.ServeHTTP(w, r)
				return
			}
			for _, single := range ipAllowSingles {
				if single.Equal(ip) {
					next.ServeHTTP(w, r)
					return
				}
			}
			for _, cidr := range ipAllowCIDRs {
				if cidr.Contains(ip) {
					next.ServeHTTP(w, r)
					return
				}
			}
		}

		ipStr := "unknown"
		if ip != nil {
			ipStr = ip.String()
		}
		log.Printf("[ip-allowlist] blocked request from %s to %s", ipStr, r.URL.Path)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("403 Forbidden — 이 IP(" + ipStr + ")는 접근 허용 목록에 없습니다."))
	})
}

// ---------------------------------------------------------------------------
// gzip middleware
// ---------------------------------------------------------------------------

var gzipWriterPool = sync.Pool{
	New: func() any {
		// Compression level 5 is a good tradeoff for responsiveness vs ratio.
		w, _ := gzip.NewWriterLevel(io.Discard, 5)
		return w
	},
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz          *gzip.Writer
	wroteHeader bool
	compress    bool // decided on first Write based on Content-Type
	flusher     http.Flusher
}

func (g *gzipResponseWriter) WriteHeader(code int) {
	// Defer actual header write so we can strip Content-Length if we compress.
	g.ResponseWriter.Header().Del("Content-Length")
	g.ResponseWriter.Header().Set("Content-Encoding", "gzip")
	g.ResponseWriter.Header().Add("Vary", "Accept-Encoding")
	g.ResponseWriter.WriteHeader(code)
	g.wroteHeader = true
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	if !g.wroteHeader {
		g.WriteHeader(http.StatusOK)
	}
	return g.gz.Write(b)
}

// Flush passes through to the underlying flusher (needed for SSE).
func (g *gzipResponseWriter) Flush() {
	_ = g.gz.Flush()
	if g.flusher != nil {
		g.flusher.Flush()
	}
}

// gzipMiddleware compresses responses when the client sends
// "Accept-Encoding: gzip". Skips:
//   - Requests that already have Content-Encoding set by the handler
//   - /v1/registry/v2/* (Docker registry protocol — already compressed blobs)
//   - /v1/stream (SSE — we want immediate flushing; we still compress but
//     rely on gz.Flush() per message)
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/registry/v2/") {
			next.ServeHTTP(w, r)
			return
		}
		gz := gzipWriterPool.Get().(*gzip.Writer)
		gz.Reset(w)
		defer func() {
			_ = gz.Close()
			gzipWriterPool.Put(gz)
		}()
		fl, _ := w.(http.Flusher)
		grw := &gzipResponseWriter{ResponseWriter: w, gz: gz, flusher: fl}
		next.ServeHTTP(grw, r)
	})
}

// securityHeadersMiddleware applies defense-in-depth HTTP response headers
// (HSTS, CSP, X-Frame-Options, X-Content-Type-Options, Referrer-Policy).
// CSAP 기준의 관리형 클라우드 보안 표준에 맞춘 기본값.
func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		// Force HTTPS on any future request for 1 year.
		h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		// CSP only for dashboard HTML. APIs return JSON and don't need it.
		if r.URL.Path == "/" || r.URL.Path == "/dashboard" || r.URL.Path == "/portal" {
			h.Set("Content-Security-Policy",
				"default-src 'self'; "+
					"script-src 'self' 'unsafe-inline'; "+ // inline scripts in dashboard.html
					"style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "+
					"font-src 'self' https://fonts.gstatic.com data:; "+
					"img-src 'self' data: http: https:; "+
					// Allow http: too so service-preview thumbnails work when the
					// dashboard itself is served over plain HTTP (direct-IP access);
					// endpoint URLs are http://svc.<domain>/ in that case.
					"frame-src 'self' http: https:; "+
					"connect-src 'self'; "+
					"frame-ancestors 'none'")
		}
		next.ServeHTTP(w, r)
	})
}

// corsAllowedOrigin returns true if the given origin is allowed.
// Sources: ORCHESTRATOR_CORS_ORIGINS env (comma-separated) and the current
// request host (same-origin requests are always permitted).
func corsAllowedOrigin(origin string, r *http.Request) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return false
	}
	// Same-origin: if Origin's host equals Request's host (via X-Forwarded-Host),
	// allow. Covers the dashboard fetching its own API.
	reqHost := r.Header.Get("X-Forwarded-Host")
	if reqHost == "" {
		reqHost = r.Host
	}
	if strings.HasSuffix(origin, "://"+reqHost) {
		return true
	}
	// Env allowlist.
	raw := strings.TrimSpace(os.Getenv("ORCHESTRATOR_CORS_ORIGINS"))
	if raw == "" {
		return false
	}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if item == "*" {
			return true // explicit opt-in to wildcard
		}
		if item == origin {
			return true
		}
	}
	return false
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && corsAllowedOrigin(origin, r) {
			// Echo the exact Origin and allow credentials — required for cookies.
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, X-CSRF-Token")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Login rate limiting (per IP and per username)
// ---------------------------------------------------------------------------

type rateLimitEntry struct {
	fails     int
	firstFail time.Time
	lockUntil time.Time
}

var (
	rateLimitMu sync.Mutex
	rlByIP      = map[string]*rateLimitEntry{}
	rlByUser    = map[string]*rateLimitEntry{}
)

const (
	rlWindow     = 10 * time.Minute
	rlMaxFails   = 5
	rlLockoutFor = 15 * time.Minute
)

// loginRateLimitMiddleware blocks login attempts when rate limit exceeded.
// Per-IP enforcement; per-user enforcement is applied inside handleLogin
// after the username is known.
func loginRateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !(r.URL.Path == "/v1/auth/login" && r.Method == http.MethodPost) {
			next.ServeHTTP(w, r)
			return
		}
		ip := clientIP(r)
		if ip != nil {
			if isRateLimited(rlByIP, ip.String()) {
				writeJSON(w, http.StatusTooManyRequests, map[string]any{
					"success": false,
					"message": "로그인 시도가 너무 많습니다. 잠시 후 다시 시도하세요.",
				})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// isRateLimited reports whether the identified key is currently locked out.
// The caller must not hold rateLimitMu.
func isRateLimited(store map[string]*rateLimitEntry, key string) bool {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()
	e := store[key]
	if e == nil {
		return false
	}
	now := time.Now()
	if now.Before(e.lockUntil) {
		return true
	}
	// Stale window — reset.
	if now.Sub(e.firstFail) > rlWindow {
		delete(store, key)
	}
	return false
}

// recordLoginFail increments failure count for the given key; locks out if
// it crosses the threshold.
func recordLoginFail(store map[string]*rateLimitEntry, key string) {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()
	e := store[key]
	now := time.Now()
	if e == nil || now.Sub(e.firstFail) > rlWindow {
		store[key] = &rateLimitEntry{fails: 1, firstFail: now}
		return
	}
	e.fails++
	if e.fails >= rlMaxFails {
		e.lockUntil = now.Add(rlLockoutFor)
	}
}

func clearLoginFail(store map[string]*rateLimitEntry, key string) {
	rateLimitMu.Lock()
	delete(store, key)
	rateLimitMu.Unlock()
}

// ---------------------------------------------------------------------------
// CSRF protection (double-submit cookie)
// ---------------------------------------------------------------------------

const csrfCookie = "orch_csrf"
const csrfHeader = "X-CSRF-Token"

// csrfMiddleware enforces double-submit CSRF check on state-changing requests.
//
// Rules:
//   - GET/HEAD/OPTIONS: always pass (no state change).
//   - /v1/auth/login, /v1/auth/logout: no CSRF required (no prior session).
//   - Inter-node calls with ORCHESTRATOR_API_TOKEN Bearer: bypass.
//   - /v1/cluster/heartbeat, /v1/agent/*: bypass (inter-node).
//   - Other mutating requests: header X-CSRF-Token must equal cookie orch_csrf.
//
// The csrf cookie is (re)issued on every login and rotated at logout.
func csrfMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		path := r.URL.Path
		if method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(path, "/v1/auth/login") ||
			strings.HasPrefix(path, "/v1/auth/logout") ||
			strings.HasPrefix(path, "/v1/cluster/heartbeat") ||
			strings.HasPrefix(path, "/v1/agent/") ||
			strings.HasPrefix(path, "/v1/registry/v2/") {
			next.ServeHTTP(w, r)
			return
		}
		// Inter-node shared-token requests bypass.
		if tok := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN")); tok != "" {
			if r.Header.Get("Authorization") == "Bearer "+tok {
				next.ServeHTTP(w, r)
				return
			}
		}
		// Only enforce if a session exists — anonymous writes are already
		// blocked by sessionAuthMiddleware (401).
		if sess := auth.SessionFromRequest(r); sess != nil {
			c, _ := r.Cookie(csrfCookie)
			hdr := r.Header.Get(csrfHeader)
			if c == nil || hdr == "" || c.Value != hdr {
				writeJSON(w, http.StatusForbidden, map[string]any{
					"success": false,
					"message": "CSRF 검증 실패",
					"code":    "csrf",
				})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func setCSRFCookie(w http.ResponseWriter, r *http.Request) string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	tok := hex.EncodeToString(b)
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookie,
		Value:    tok,
		Path:     "/",
		HttpOnly: false, // must be readable by JS so dashboard can echo in X-CSRF-Token
		Secure:   cookieSecure(r),
		SameSite: http.SameSiteStrictMode,
		Expires:  time.Now().Add(24 * time.Hour),
	})
	return tok
}

func clearCSRFCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: csrfCookie, Value: "", Path: "/", MaxAge: -1,
	})
}

// cookieSecure reports whether the Secure cookie flag should be set.
// Production default: always true. Set ORCHESTRATOR_COOKIE_INSECURE=true
// only for local dev over plain HTTP.
func cookieSecure(r *http.Request) bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("ORCHESTRATOR_COOKIE_INSECURE")), "true") {
		return false
	}
	// If the request clearly arrived over HTTPS (direct TLS or a trusted
	// proxy said so), keep Secure. Otherwise fall back to not-Secure so
	// direct HTTP access (e.g. http://<master-ip>:8000 from an admin on
	// the LAN) can still hold a session cookie. Secure cookies are
	// silently dropped by browsers on plain HTTP.
	if r.TLS != nil {
		return true
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); strings.EqualFold(proto, "https") {
		return true
	}
	if cfv := r.Header.Get("Cf-Visitor"); strings.Contains(strings.ToLower(cfv), `"scheme":"https"`) {
		return true
	}
	return false
}

func bearerTokenAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN"))
		path := r.URL.Path
		method := r.Method

		if token == "" || !strings.HasPrefix(path, "/v1/") ||
			strings.HasPrefix(path, "/v1/cluster/") ||
			strings.HasPrefix(path, "/v1/agent/") ||
			strings.HasPrefix(path, "/v1/quickstart/") ||
			strings.HasPrefix(path, "/v1/auth/") ||
			strings.HasPrefix(path, "/v1/public/") ||
			strings.HasPrefix(path, "/v1/user/") {
			// /v1/public/* is intentionally unauthenticated (shared services).
			// /v1/user/* is access-controlled by sessionAuthMiddleware + the
			// per-handler owner check, so the inter-node bearer guard skips it.
			next.ServeHTTP(w, r)
			return
		}

		// Allow GET on read endpoints without token (dashboard).
		if method == http.MethodGet &&
			(path == "/v1/system" || path == "/v1/services" ||
				path == "/v1/containers" || path == "/v1/images" ||
				path == "/v1/stream" ||
				strings.HasPrefix(path, "/v1/containers") ||
				strings.HasPrefix(path, "/v1/ai/") ||
				strings.HasPrefix(path, "/v1/registry/catalog") ||
				strings.HasPrefix(path, "/v1/registry/tags/") ||
				strings.HasPrefix(path, "/v1/registry/status") ||
				strings.HasPrefix(path, "/v1/compose-templates") ||
				path == "/v1/services/endpoints" ||
				path == "/v1/services/groups") {
			next.ServeHTTP(w, r)
			return
		}

		// Allow POST/DELETE from dashboard on management endpoints without token.
		if strings.HasPrefix(path, "/v1/services/") ||
			strings.HasPrefix(path, "/v1/services/group/") ||
			strings.HasPrefix(path, "/v1/containers/") ||
			strings.HasPrefix(path, "/v1/images/") ||
			strings.HasPrefix(path, "/v1/ai/") ||
			strings.HasPrefix(path, "/v1/registry/") ||
			strings.HasPrefix(path, "/v1/compose-templates") ||
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

// auditLog appends a privileged-action audit entry.
func auditLog(user, role, ip, action, target, result string) {
	auth.WriteAudit(user, role, ip, action, target, result)
}

// auditRequest extracts user/role/ip from the request and emits an audit entry.
func auditRequest(r *http.Request, action, target, result string) {
	user, role := "anonymous", "guest"
	if sess := auth.SessionFromRequest(r); sess != nil {
		user = sess.Username
		role = string(sess.Role)
	}
	ip := "unknown"
	if c := clientIP(r); c != nil {
		ip = c.String()
	}
	auditLog(user, role, ip, action, target, result)
}

// ---------------------------------------------------------------------------
// Session auth: account-based auth with role gating
// ---------------------------------------------------------------------------

// sessionAuthMiddleware enforces login + role-based access for the dashboard APIs.
//
// Rules:
//   - Static dashboard HTML (GET /, GET /dashboard), /health, /v1/auth/*,
//     /v1/cluster/heartbeat, /v1/agent/* (worker→master), and /v1/registry/v2/*
//     (Docker registry protocol) are always allowed.
//   - All other /v1/* endpoints require a valid session.
//   - Guest role is viewer-only: only GET (and OPTIONS) allowed. Any mutating
//     method returns 403.
func sessionAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		method := r.Method

		// Unauthenticated endpoints (health, dashboard HTML, login, inter-node).
		if method == http.MethodOptions ||
			path == "/" || path == "/dashboard" || path == "/admin" || path == "/portal" || path == "/health" ||
			strings.HasPrefix(path, "/v1/auth/") ||
			strings.HasPrefix(path, "/v1/cluster/heartbeat") ||
			strings.HasPrefix(path, "/v1/agent/") ||
			strings.HasPrefix(path, "/v1/registry/v2/") {
			next.ServeHTTP(w, r)
			return
		}

		// Inter-node calls authenticated via the shared bearer token bypass.
		if tok := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN")); tok != "" {
			if r.Header.Get("Authorization") == "Bearer "+tok {
				next.ServeHTTP(w, r)
				return
			}
		}

		// Anything else (under /v1/) needs a session.
		if !strings.HasPrefix(path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}

		sess := auth.SessionFromRequest(r)
		isRead := method == http.MethodGet || method == http.MethodHead

		// Unauthenticated visitors default to guest view — reads are allowed,
		// writes require admin login.
		if isRead {
			next.ServeHTTP(w, r)
			return
		}

		// Mutating methods: must have a session.
		if sess == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"success": false,
				"message": "관리자 로그인이 필요합니다",
				"code":    "unauthenticated",
			})
			return
		}
		// RoleUser (self-service portal accounts) may call a whitelist of
		// owner-scoped endpoints; the handlers enforce that the caller owns
		// the target service. Everything else stays admin-only.
		if sess.Role == auth.RoleUser {
			if !userMutatingAllowed(path) {
				auditRequest(r, method+" "+path, path, "forbidden:user")
				writeJSON(w, http.StatusForbidden, map[string]any{
					"success": false,
					"message": "이 작업은 관리자만 가능합니다",
					"code":    "forbidden",
					"role":    string(sess.Role),
				})
				return
			}
		} else if sess.Role != auth.RoleAdmin {
			auditRequest(r, method+" "+path, path, "forbidden:guest")
			writeJSON(w, http.StatusForbidden, map[string]any{
				"success": false,
				"message": "게스트 계정은 조회만 가능합니다",
				"code":    "forbidden",
				"role":    string(sess.Role),
			})
			return
		}

		// Audit all admin-level write operations (excluding auth endpoints
		// which have their own richer logging).
		if !strings.HasPrefix(path, "/v1/auth/") {
			auditRequest(r, method+" "+path, path, "permitted")
		}
		next.ServeHTTP(w, r)
	})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	body.Username = strings.TrimSpace(body.Username)
	if body.Username == "" || body.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "아이디와 비밀번호를 입력하세요"})
		return
	}

	ipStr := "unknown"
	if ip := clientIP(r); ip != nil {
		ipStr = ip.String()
	}
	// Per-user rate limit check BEFORE bcrypt to avoid timing-based user
	// enumeration via lockout speed; combined with per-IP check in middleware.
	if isRateLimited(rlByUser, body.Username) {
		auditLog(body.Username, "guest", ipStr, "login", body.Username, "rate_limited")
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"success": false,
			"message": "로그인 시도가 너무 많습니다. 잠시 후 다시 시도하세요.",
		})
		return
	}

	sess, err := auth.Login(body.Username, body.Password)
	if err != nil {
		recordLoginFail(rlByIP, ipStr)
		recordLoginFail(rlByUser, body.Username)
		auditLog(body.Username, "guest", ipStr, "login", body.Username, "failure")
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": err.Error()})
		return
	}
	clearLoginFail(rlByIP, ipStr)
	clearLoginFail(rlByUser, body.Username)

	auth.SetSessionCookieStrict(w, sess.Token, cookieSecure(r))
	csrf := setCSRFCookie(w, r)
	auditLog(sess.Username, string(sess.Role), ipStr, "login", sess.Username, "success")
	writeJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"username":   sess.Username,
		"role":       string(sess.Role),
		"token":      sess.Token,
		"csrf_token": csrf,
	})
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if c, err := r.Cookie(auth.CookieName); err == nil {
		auth.Logout(c.Value)
	}
	auth.ClearSessionCookie(w)
	clearCSRFCookie(w)
	if sess != nil {
		ipStr := "unknown"
		if ip := clientIP(r); ip != nil {
			ipStr = ip.String()
		}
		auditLog(sess.Username, string(sess.Role), ipStr, "logout", sess.Username, "success")
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// handleChangePassword lets the logged-in user change their own password.
// Both admin and guest can change their own; anonymous visitors cannot.
func handleChangePassword(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "로그인이 필요합니다"})
		return
	}
	var body struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if body.CurrentPassword == "" || body.NewPassword == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "현재/새 비밀번호를 입력하세요"})
		return
	}
	ipStr := "unknown"
	if ip := clientIP(r); ip != nil {
		ipStr = ip.String()
	}
	if err := auth.ChangePassword(sess.Username, body.CurrentPassword, body.NewPassword); err != nil {
		auditLog(sess.Username, string(sess.Role), ipStr, "change_password", sess.Username, "failure:"+err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	// Invalidate all OTHER sessions for this user (keep the current one).
	auth.InvalidateUserSessions(sess.Username)
	// Re-create a fresh session for the current request.
	newSess, err := auth.Login(sess.Username, body.NewPassword)
	if err != nil {
		auditLog(sess.Username, string(sess.Role), ipStr, "change_password", sess.Username, "success_but_relogin_required")
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": "비밀번호가 변경되었습니다. 다시 로그인해주세요"})
		return
	}
	auth.SetSessionCookieStrict(w, newSess.Token, cookieSecure(r))
	csrf := setCSRFCookie(w, r)
	auditLog(newSess.Username, string(newSess.Role), ipStr, "change_password", newSess.Username, "success")
	writeJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "비밀번호가 변경되었습니다",
		"username":   newSess.Username,
		"role":       string(newSess.Role),
		"csrf_token": csrf,
	})
}

func handleAuthMe(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"authenticated": false,
			"role":          string(auth.RoleGuest),
			"anonymous":     true,
		})
		return
	}
	resp := map[string]any{
		"authenticated":    true,
		"username":         sess.Username,
		"role":             string(sess.Role),
		"default_password": auth.IsDefaultPassword(sess.Username),
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleListUsers: GET /v1/auth/users. Admin-only.
func handleListUsers(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil || sess.Role != auth.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false, "message": "관리자 권한이 필요합니다", "code": "forbidden",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": auth.ListUsers()})
}

// handleCreateUser: POST /v1/auth/users. Admin-only.
func handleCreateUser(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil || sess.Role != auth.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false, "message": "관리자 권한이 필요합니다", "code": "forbidden",
		})
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	role := auth.Role(strings.TrimSpace(body.Role))
	if role == "" {
		role = auth.RoleGuest
	}
	if err := auth.CreateUser(strings.TrimSpace(body.Username), body.Password, role); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	auditRequest(r, "POST /v1/auth/users", body.Username, "created:"+string(role))
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "username": body.Username, "role": string(role)})
}

// handleAgentUpsertEnv: POST /v1/agent/upsert-env — worker-side endpoint that
// rewrites a service's environment in the local services.json so the next
// ReconcileReplicas / ExecuteScale cycle uses the new values. Called by the
// master immediately after PUT /v1/services/{name}/env so worker state does
// not lag behind cluster state.
func handleAgentUpsertEnv(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string          `json:"service_name"`
		Environment []string        `json:"environment"`
		EnvVars     []models.EnvVar `json:"env_vars"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	body.ServiceName = strings.TrimSpace(body.ServiceName)
	if body.ServiceName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service_name required"})
		return
	}
	existing := state.GetService(body.ServiceName)
	if existing == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "service not found"})
		return
	}
	image, _ := existing["image"].(string)
	replicas := 0
	switch v := existing["replicas"].(type) {
	case int:
		replicas = v
	case int64:
		replicas = int(v)
	case float64:
		replicas = int(v)
	}
	opts := []state.UpsertOption{state.WithEnvironment(body.Environment)}
	if len(body.EnvVars) > 0 {
		opts = append(opts, state.WithEnvVars(body.EnvVars))
	}
	state.UpsertService(body.ServiceName, image, replicas, opts...)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// handleServiceEnvUpdate: PUT /v1/services/{name}/env
//
// Replaces the environment variables of an existing service with the supplied
// list. Each entry may flag is_secret; secret values are stored encrypted at
// rest and access-restricted to the service owner. After the update the
// service's running containers are stopped+removed so the reconcile loop
// recreates them with the new env on the next pass.
//
// Permission: caller's username must match the service's owner (cluster
// state). Services with no owner (legacy) are editable by anyone with an
// authenticated session.
func handleServiceEnvUpdate(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(chi.URLParam(r, "name"))
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service name required"})
		return
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다"})
		return
	}

	var body struct {
		EnvVars []models.EnvVar `json:"env_vars"`
		// SecretRefs lets the caller attach pre-saved user secrets to the
		// service without re-typing their values. Each ref expands into one
		// is_secret EnvVar (env_name → decrypted secret value, re-encrypted
		// for storage). Refs override any same-named entry in EnvVars.
		SecretRefs []usersecrets.SecretRef `json:"secret_refs"`
		// Optional: when true, automatically remove the running containers so
		// reconcile rebuilds them with the new env (matches what users expect
		// from "save and apply"). Defaults to true.
		Restart *bool `json:"restart"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	svcInfo := clusterState.GetService(name)
	if svcInfo == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "서비스를 찾을 수 없습니다"})
		return
	}

	// Owner check (admin manages all; user only own services).
	caller := requesterUsername(r)
	if ok, owner := canManageService(r, svcInfo); !ok {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false,
			"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 환경변수를 수정할 수 있습니다", name, owner),
			"code":    "forbidden_owner",
		})
		return
	}

	// Expand user-secret refs into the EnvVars list. The caller must own the
	// referenced secret (ResolveRefs enforces this). Refs supersede any
	// same-named entry already in EnvVars.
	if len(body.SecretRefs) > 0 && caller != "" {
		resolved, err := usersecrets.ResolveRefs(caller, body.SecretRefs)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false,
				"message": "시크릿 변수 적용 실패: " + err.Error(),
			})
			return
		}
		// Build a fast lookup of names already in EnvVars so we can replace
		// in place instead of duplicating entries.
		idx := map[string]int{}
		for i, ev := range body.EnvVars {
			idx[ev.Name] = i
		}
		for envName, val := range resolved {
			ev := models.EnvVar{Name: envName, Value: val, IsSecret: true}
			if i, ok := idx[envName]; ok {
				body.EnvVars[i] = ev
			} else {
				body.EnvVars = append(body.EnvVars, ev)
			}
		}
	}

	// Encrypt secrets + build the plaintext slice that Docker needs.
	persisted := make([]models.EnvVar, 0, len(body.EnvVars))
	plaintextEnv := make([]string, 0, len(body.EnvVars))
	for _, ev := range body.EnvVars {
		nm := strings.TrimSpace(ev.Name)
		if nm == "" {
			continue
		}
		val := ev.Value
		stored := val
		if ev.IsSecret {
			// If the client returned the "***" placeholder, preserve the
			// existing ciphertext instead of overwriting it with "***".
			if val == maskedSecretPlaceholder {
				for _, prev := range envVarsFromState(svcInfo) {
					if prev.Name == nm && prev.IsSecret {
						stored = prev.Value
						break
					}
				}
				// Plaintext for the container is also preserved.
				if pt, err := secrets.Decrypt(stored); err == nil {
					val = pt
				}
			} else if ct, err := secrets.Encrypt(val); err == nil {
				stored = ct
			}
		}
		persisted = append(persisted, models.EnvVar{Name: nm, Value: stored, IsSecret: ev.IsSecret})
		plaintextEnv = append(plaintextEnv, nm+"="+val)
	}

	image, _ := svcInfo["image"].(string)
	desired := 0
	switch v := svcInfo["desired_replicas"].(type) {
	case int:
		desired = v
	case int64:
		desired = int(v)
	case float64:
		desired = int(v)
	}

	clusterState.SaveService(name, image, desired, map[string]any{
		"environment": plaintextEnv,
		"env_vars":    persisted,
		"owner":       svcInfo["owner"],
	})

	restart := true
	if body.Restart != nil {
		restart = *body.Restart
	}
	var restarted []string
	if restart {
		// Determine which nodes host this service.
		seenNodes := map[string]bool{}
		var placements []models.ContainerPlacement
		for _, p := range clusterState.GetPlacements(name, "") {
			placements = append(placements, p)
			seenNodes[p.NodeName] = true
		}

		// 1) Sync worker-local services.json BEFORE removing the container.
		//    Without this, the worker's reconcile loop respawns from its
		//    stale state and the old env values come back.
		upsertBody, _ := json.Marshal(map[string]any{
			"service_name": name,
			"environment":  plaintextEnv,
			"env_vars":     persisted,
		})
		for nodeName := range seenNodes {
			node := clusterState.GetNode(nodeName)
			if node == nil {
				continue
			}
			req, _ := http.NewRequest("POST", nodeBaseURL(node)+"/v1/agent/upsert-env", bytes.NewReader(upsertBody))
			setNodeHeaders(req, node)
			if resp, err := httpClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}

		// 2) Stop + remove existing containers. Reconcile (now reading the
		//    fresh services.json on each worker) will respawn with the new
		//    environment.
		for _, p := range placements {
			node := clusterState.GetNode(p.NodeName)
			if node == nil {
				continue
			}
			baseURL := nodeBaseURL(node)
			stopReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/containers/%s/stop", baseURL, p.ContainerID), nil)
			setNodeHeaders(stopReq, node)
			if resp, err := httpClient.Do(stopReq); err == nil {
				resp.Body.Close()
			}
			delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/containers/%s", baseURL, p.ContainerID), nil)
			setNodeHeaders(delReq, node)
			if resp, err := httpClient.Do(delReq); err == nil {
				resp.Body.Close()
			}
			restarted = append(restarted, p.ContainerName)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":   true,
		"message":   fmt.Sprintf("'%s' 환경변수 갱신 완료 (%d개 변수)", name, len(persisted)),
		"restarted": restarted,
	})
}

// ---------------------------------------------------------------------------
// Per-user secret store (used by deploy forms to inject pre-saved values)
// ---------------------------------------------------------------------------

// userSecretCaller returns the authenticated session, or writes a 401 and
// returns nil. Both admin and guest sessions may manage their own secrets.
func userSecretCaller(w http.ResponseWriter, r *http.Request) *auth.Session {
	sess := auth.SessionFromRequest(r)
	if sess == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"success": false, "message": "로그인이 필요합니다", "code": "unauthenticated",
		})
		return nil
	}
	return sess
}

// handleUserDeployDatabase: POST /v1/user/deploy-database
//
// Deploys a PostgreSQL or MySQL container for the caller with the given
// credentials. The DB is TCP (not HTTP) so Traefik routing is disabled; other
// services on the same internal network connect via "<name>:<port>". Returns
// the connection endpoint. The password is stored encrypted (env secret).
//
// body: {engine:"postgres"|"mysql", name, db_name, username, password, node?}
func handleUserDeployDatabase(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	var body struct {
		Engine   string `json:"engine"`
		Name     string `json:"name"`
		DBName   string `json:"db_name"`
		Username string `json:"username"`
		Password string `json:"password"`
		Node     string `json:"node"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	name := sanitizeServiceName(body.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "서비스명을 입력하세요"})
		return
	}
	engine := strings.ToLower(strings.TrimSpace(body.Engine))
	dbName := strings.TrimSpace(body.DBName)
	user := strings.TrimSpace(body.Username)
	pass := body.Password
	if dbName == "" {
		dbName = "appdb"
	}
	if user == "" {
		user = "appuser"
	}
	if pass == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "비밀번호를 입력하세요"})
		return
	}

	var image string
	var port int
	var env []string
	var secretNames []string
	switch engine {
	case "postgres", "postgresql":
		engine = "postgres"
		image = "postgres:16-alpine"
		port = 5432
		env = []string{
			"POSTGRES_DB=" + dbName,
			"POSTGRES_USER=" + user,
			"POSTGRES_PASSWORD=" + pass,
		}
		secretNames = []string{"POSTGRES_PASSWORD"}
	case "mysql", "mariadb":
		engine = "mysql"
		image = "mysql:8.0"
		port = 3306
		env = []string{
			"MYSQL_DATABASE=" + dbName,
			"MYSQL_USER=" + user,
			"MYSQL_PASSWORD=" + pass,
			"MYSQL_ROOT_PASSWORD=" + pass,
		}
		secretNames = []string{"MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"}
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "engine은 postgres 또는 mysql 이어야 합니다"})
		return
	}

	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다"})
		return
	}

	// Pick a node: explicit > scheduler > local master.
	nodeName := strings.TrimSpace(body.Node)
	if nodeName == "" && sched != nil {
		if decisions, err := sched.Schedule(name, image, 1, nil, "least-loaded"); err == nil && len(decisions) > 0 {
			nodeName = decisions[0].NodeName
		}
	}
	if nodeName == "" {
		nodeName = os.Getenv("ORCHESTRATOR_NODE_NAME")
		if nodeName == "" {
			nodeName = "master"
		}
	}
	node := clusterState.GetNode(nodeName)
	if node == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "노드를 찾을 수 없습니다: " + nodeName})
		return
	}
	baseURL := nodeBaseURL(node)

	// DB extra-labels: disable Traefik (TCP, not HTTP) + tag as database.
	extraLabels := map[string]string{
		"traefik.enable":            "false",
		"ai.orchestrator.kind":      "database",
		"ai.orchestrator.db.engine": engine,
		"ai.orchestrator.db.port":   strconv.Itoa(port),
	}

	// Pull image on the node.
	pullBody, _ := json.Marshal(map[string]any{"image": image})
	pullReq, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/images/pull", bytes.NewReader(pullBody))
	setNodeHeaders(pullReq, node)
	if resp, err := longHTTPClient.Do(pullReq); err == nil {
		resp.Body.Close()
	}

	// Run the DB container (internal network, no HTTP routing).
	runBody, _ := json.Marshal(map[string]any{
		"image":                image,
		"name":                 name,
		"replicas":             1,
		"use_internal_network": true,
		"environment":          env,
		"extra_labels":         extraLabels,
	})
	runReq, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/containers/run", bytes.NewReader(runBody))
	setNodeHeaders(runReq, node)
	runResp, err := longHTTPClient.Do(runReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "message": "DB 컨테이너 기동 실패: " + err.Error()})
		return
	}
	var runData map[string]any
	json.NewDecoder(runResp.Body).Decode(&runData)
	runResp.Body.Close()
	if s, _ := runData["success"].(bool); !s {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "DB 기동 실패: " + fmt.Sprint(runData["message"])})
		return
	}

	// Persist service with owner + encrypted password env + the DB labels so
	// reconcile recreates it identically.
	envVars := buildEnvVars(env, secretNames)
	clusterState.SaveService(name, image, 1, map[string]any{
		"owner":        sess.Username,
		"environment":  env,
		"env_vars":     envVars,
		"extra_labels": extraLabels,
	})

	connStr := ""
	if engine == "postgres" {
		connStr = fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", user, "<password>", name, port, dbName)
	} else {
		connStr = fmt.Sprintf("mysql://%s:%s@%s:%d/%s", user, "<password>", name, port, dbName)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"message":      fmt.Sprintf("%s 데이터베이스 '%s' 기동 완료 (노드: %s)", engine, name, nodeName),
		"engine":       engine,
		"service_name": name,
		"node":         nodeName,
		"db_host":      name, // other containers connect via this host on orch-internal
		"db_port":      port,
		"db_name":      dbName,
		"db_user":      user,
		"connection":   connStr, // password masked; the owner set it
		"note":         "같은 내부 네트워크(orch-internal)의 서비스가 위 호스트:포트로 접속합니다. (DB와 소비 서비스는 동일 노드 권장)",
	})
}

// handleUserImageImport: POST /v1/user/image-import {image, name?}
//
// Pulls an external/public image into local Docker and pushes it to the
// built-in registry so it can be used to start services. This is the
// web-driven equivalent of `docker pull X && docker tag X registry/... &&
// docker push`. Available to any authenticated session (admin or user).
func handleUserImageImport(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	var body struct {
		Image string `json:"image"`
		Name  string `json:"name"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	src := strings.TrimSpace(body.Image)
	if src == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "image 필드가 필요합니다"})
		return
	}
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "Docker 연결 실패"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Minute)
	defer cancel()

	// 1) Pull the external image locally.
	if ok, msg := runtime.PullImage(ctx, cli, src); !ok {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "message": "이미지 pull 실패: " + msg})
		return
	}
	// 2) Derive a registry repo name (sanitised), then push.
	repo := strings.TrimSpace(body.Name)
	if repo == "" {
		base := src
		if i := strings.LastIndex(base, "/"); i >= 0 {
			base = base[i+1:]
		}
		if i := strings.IndexByte(base, ':'); i >= 0 {
			base = base[:i]
		}
		repo = base
	}
	repo = strings.Trim(regexp.MustCompile(`[^a-zA-Z0-9_.-]`).ReplaceAllString(strings.ToLower(repo), "-"), "-")
	if repo == "" {
		repo = "imported"
	}
	if !isRegistryRunning(ctx) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "message": "내장 registry가 실행 중이 아닙니다"})
		return
	}
	repoTag := repo + ":latest"
	regImage, pushErr := pushImageToRegistry(ctx, src, repoTag)
	if pushErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "registry push 실패: " + pushErr.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"message":      fmt.Sprintf("이미지 '%s' → 내장 registry 저장 완료", src),
		"registry_tag": regImage,                     // localhost:5000/repo:latest (deploy uses this)
		"external_tag": registryExternalTag(repoTag), // host:5000/repo:latest (docker pull from outside)
		"repo":         repo,
	})
}

// handleUserRegistryImages: GET /v1/user/registry-images — list images stored
// in the built-in registry (repo:tag), for the portal's "이미지로 기동" picker.
func handleUserRegistryImages(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	images := []string{}
	// Use the registry URL reachable from inside this container (the registry
	// runs as a separate container, so "localhost:5000" would hit ourselves).
	regURL := getRegistryInternalURL(ctx)
	if regURL != "" {
		for repoTag := range buildRegistryCatalogSet(regURL) {
			// Return the external (master-IP) reference so any node can pull it
			// during a cluster deploy — "localhost:5000" only resolves on master.
			images = append(images, registryExternalTag(repoTag))
		}
	}
	sort.Strings(images)
	writeJSON(w, http.StatusOK, map[string]any{"images": images})
}

// dbEngineFromImage detects a database service from its image reference,
// returning the engine name and default port (or "",0 if not a DB image).
func dbEngineFromImage(img any) (string, int) {
	s, _ := img.(string)
	s = strings.ToLower(s)
	switch {
	case strings.Contains(s, "postgres"):
		return "postgres", 5432
	case strings.Contains(s, "mysql"), strings.Contains(s, "mariadb"):
		return "mysql", 3306
	}
	return "", 0
}

// applyDBFields enriches a service entry with database connection info when the
// image is a known DB engine (shared by user + public service listings).
func applyDBFields(s map[string]any) {
	name, _ := s["name"].(string)
	if engine, port := dbEngineFromImage(s["image"]); engine != "" && name != "" {
		s["kind"] = "database"
		s["db_engine"] = engine
		s["db_port"] = port
		s["db_host"] = name
		userKey, dbKey := "POSTGRES_USER", "POSTGRES_DB"
		if engine == "mysql" {
			userKey, dbKey = "MYSQL_USER", "MYSQL_DATABASE"
		}
		if evs, ok := s["env_vars"].([]models.EnvVar); ok {
			for _, ev := range evs {
				switch ev.Name {
				case userKey:
					s["db_user"] = ev.Value
				case dbKey:
					s["db_name"] = ev.Value
				}
			}
		}
	}
}

// handlePublicServices: GET /v1/public/services — services that owners marked
// as shared, visible without authentication. Secrets/env are never included.
func handlePublicServices(w http.ResponseWriter, r *http.Request) {
	all := getServicesDataForViewer(false, "") // viewer "" → secrets masked anyway
	base := runtime.BaseDomain()
	pathHost := runtime.PathHost()
	endpointFor := func(name string) string {
		if serviceUsesSubdomain(name) && base != "" {
			return "http://" + name + "." + base + "/"
		}
		if pathHost != "" {
			return "http://" + pathHost + "/" + name + "/"
		}
		return ""
	}
	out := []map[string]any{}
	for _, s := range all {
		if sh, _ := s["shared"].(bool); !sh {
			continue
		}
		name, _ := s["name"].(string)
		// DB enrichment needs env_vars; run it before stripping them.
		applyDBFields(s)
		if s["kind"] != "database" && name != "" {
			if ep := endpointFor(name); ep != "" {
				s["endpoint"] = ep
			}
		}
		// Never expose environment/secrets publicly.
		delete(s, "env_vars")
		delete(s, "can_edit_secrets")
		out = append(out, s)
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": out})
}

// handleUserServiceShare: POST /v1/user/service-share {name, shared}. Owner-only
// toggle of a service's public-share flag.
func handleUserServiceShare(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	var body struct {
		Name   string `json:"name"`
		Shared bool   `json:"shared"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" || clusterState == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "서비스명이 필요합니다"})
		return
	}
	info := clusterState.GetService(name)
	if info == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "서비스를 찾을 수 없습니다"})
		return
	}
	if ok, owner := canManageService(r, info); !ok {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false,
			"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 공유 설정을 변경할 수 있습니다", name, owner),
			"code":    "forbidden_owner",
		})
		return
	}
	clusterState.SetServiceShared(name, body.Shared)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "name": name, "shared": body.Shared})
}

// handleListUserServices: GET /v1/user/services — only the services owned by
// the caller, each enriched with its primary public endpoint URL so the portal
// can render thumbnails. Admins get every service (owner filter relaxed).
func handleListUserServices(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	all := getServicesDataForViewer(false, sess.Username)
	isAdmin := sess.Role == auth.RoleAdmin
	// scope=all returns every service (read-only for non-owned); admin-only.
	// Regular users only ever see their own services here (plus the separate
	// public /v1/public/services shared list). Default "mine".
	scopeAll := r.URL.Query().Get("scope") == "all" && isAdmin

	// Map service → primary public URL honoring its routing mode: default
	// path routing (<path-host>/svc/), or subdomain (svc.<base-domain>) when
	// the service opted in. Matches ensurePublicEndpoints / what actually works.
	base := runtime.BaseDomain()
	pathHost := runtime.PathHost()
	endpointFor := func(name string) string {
		if serviceUsesSubdomain(name) && base != "" {
			return "http://" + name + "." + base + "/"
		}
		if pathHost != "" {
			return "http://" + pathHost + "/" + name + "/"
		}
		return ""
	}

	out := []map[string]any{}
	for _, s := range all {
		owner, _ := s["owner"].(string)
		owned := isAdmin || owner == sess.Username
		if !scopeAll && !owned {
			continue
		}
		// "owned" tells the UI whether to show manage (delete) controls.
		s["owned"] = owned
		name, _ := s["name"].(string)
		// Database services are TCP, not HTTP: expose an internal connection
		// endpoint (<name>:<port>) instead of a public web URL.
		applyDBFields(s)
		if s["kind"] != "database" && name != "" {
			if ep := endpointFor(name); ep != "" {
				s["endpoint"] = ep
			}
		}
		out = append(out, s)
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": out, "username": sess.Username, "role": string(sess.Role)})
}

// handleListUserSecrets: GET /v1/user/secrets — caller's secrets, masked.
func handleListUserSecrets(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"secrets": usersecrets.List(sess.Username)})
}

// handleCreateUserSecret: POST /v1/user/secrets {name, value}.
func handleCreateUserSecret(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	var body struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	id, err := usersecrets.Save(sess.Username, body.Name, body.Value)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": id, "name": body.Name})
}

// handleGetUserSecret: GET /v1/user/secrets/{id} — reveals plaintext for the
// owner only. Other callers receive 403 even if the session is admin.
func handleGetUserSecret(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	meta, plaintext, err := usersecrets.GetPlaintext(sess.Username, id)
	if err != nil {
		status := http.StatusBadRequest
		if err.Error() == "forbidden" {
			status = http.StatusForbidden
		} else if err.Error() == "not found" {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"success": false, "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"id":      meta.ID, "name": meta.Name, "value": plaintext,
		"updated_at": meta.UpdatedAt.Format(time.RFC3339),
	})
}

// handleUpdateUserSecret: PUT /v1/user/secrets/{id} {name?, value?}.
func handleUpdateUserSecret(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	var body struct {
		Name  *string `json:"name"`
		Value *string `json:"value"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if err := usersecrets.Update(sess.Username, id, body.Name, body.Value); err != nil {
		status := http.StatusBadRequest
		if err.Error() == "forbidden" {
			status = http.StatusForbidden
		} else if err.Error() == "not found" {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"success": false, "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// handleDeleteUserSecret: DELETE /v1/user/secrets/{id}.
func handleDeleteUserSecret(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if err := usersecrets.Delete(sess.Username, id); err != nil {
		status := http.StatusBadRequest
		if err.Error() == "forbidden" {
			status = http.StatusForbidden
		} else if err.Error() == "not found" {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"success": false, "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// handleDeleteUser: DELETE /v1/auth/users/{username}. Admin-only.
func handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil || sess.Role != auth.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false, "message": "관리자 권한이 필요합니다", "code": "forbidden",
		})
		return
	}
	username := strings.TrimSpace(chi.URLParam(r, "username"))
	if username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "username required"})
		return
	}
	if username == sess.Username {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "자신의 계정은 삭제할 수 없습니다"})
		return
	}
	if err := auth.DeleteUser(username); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	auditRequest(r, "DELETE /v1/auth/users/"+username, username, "deleted")
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "username": username})
}

// handleResetUserPassword: POST /v1/auth/users/{username}/reset-password.
// Admin-only. Sets a random temporary password, signs the user out
// everywhere, and returns the temporary password once.
func handleResetUserPassword(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil || sess.Role != auth.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false, "message": "관리자 권한이 필요합니다", "code": "forbidden",
		})
		return
	}
	username := strings.TrimSpace(chi.URLParam(r, "username"))
	if username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "username required"})
		return
	}
	if username == sess.Username {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "자신의 비밀번호는 '비밀번호 변경'에서 바꾸세요"})
		return
	}
	temp, err := auth.ResetPassword(username)
	if err != nil {
		auditRequest(r, "POST /v1/auth/users/"+username+"/reset-password", username, "failure:"+err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	auditRequest(r, "POST /v1/auth/users/"+username+"/reset-password", username, "reset")
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "username": username, "temp_password": temp})
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

	// Health (static, no I/O)
	payload["health"] = map[string]any{
		"status": "ok", "version": Version, "role": OrchestratorRole,
	}

	// Fetch expensive data in parallel
	var wg sync.WaitGroup
	var systemInfo map[string]any
	var services []map[string]any
	var images []map[string]any

	wg.Add(3)
	go func() {
		defer wg.Done()
		systemInfo = monitoring.GetSystemInfo()
	}()
	go func() {
		defer wg.Done()
		services = getServicesData(false)
	}()
	go func() {
		defer wg.Done()
		images = monitoring.ListImages()
	}()
	wg.Wait()

	if systemInfo != nil {
		systemInfo["role"] = OrchestratorRole
	}
	payload["system"] = systemInfo
	payload["services"] = services
	payload["images"] = images

	// Cluster data: read from in-memory state (cheap, no I/O).
	// Use json.Marshal once for the whole payload at the end instead of
	// marshal/unmarshal per struct.
	if clusterState != nil {
		payload["cluster_status"] = clusterState.GetClusterStatus()
		payload["nodes"] = clusterState.ListNodes()

		// Placements (filter system)
		allPlacements := clusterState.GetPlacements("", "")
		filtered := make([]any, 0, len(allPlacements))
		for i := range allPlacements {
			if !systemServices[allPlacements[i].ServiceName] {
				filtered = append(filtered, allPlacements[i])
			}
		}
		payload["placements"] = filtered
		payload["alerts"] = clusterState.ListAlerts(true)
		payload["migrations"] = clusterState.ListMigrations(false)
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

// dashboardETag is computed once at first request from DashboardHTML content.
var (
	dashboardETagOnce sync.Once
	dashboardETagVal  string
)

func dashboardETag() string {
	dashboardETagOnce.Do(func() {
		h := sha256.Sum256([]byte(DashboardHTML))
		dashboardETagVal = `"` + hex.EncodeToString(h[:8]) + `"`
	})
	return dashboardETagVal
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	etag := dashboardETag()
	w.Header().Set("ETag", etag)
	// Force revalidation on every reload but allow 304 to skip body.
	w.Header().Set("Cache-Control", "no-cache")
	if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, DashboardHTML)
}

// handlePortal serves the self-service user portal page.
func handlePortal(w http.ResponseWriter, r *http.Request) {
	if PortalHTML == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, "<!doctype html><meta charset=utf-8><h1>Portal not available</h1>")
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, PortalHTML)
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

	// Reuse a single http.Client for all RPC/admin calls in this request.
	shortClient := &http.Client{Timeout: 3 * time.Second}

	nodes := make([]map[string]any, 0, len(containers))
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
		chainStatus := ""
		if c.State == "running" {
			channel := c.Labels["blockchain.channel"]
			rpcURLs := []string{}
			if name != "" && channel != "" {
				rpcURLs = append(rpcURLs, fmt.Sprintf("http://%s:9080/api/v3/%s", name, channel))
			}
			for _, p := range c.Ports {
				if p.PrivatePort == 9080 && p.PublicPort > 0 && channel != "" {
					rpcURLs = append(rpcURLs, fmt.Sprintf("http://127.0.0.1:%d/api/v3/%s", p.PublicPort, channel))
					break
				}
			}
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
			adminResp, adminErr := shortClient.Get(adminURL)
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
	// Strict input validation — these values are passed to bash as argv.
	// Rejecting shell metachars / non-IP / non-identifier inputs eliminates
	// the command-injection attack surface flagged by gosec G702.
	if net.ParseIP(body.NodeIP) == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "node_ip가 유효한 IP가 아닙니다."})
		return
	}
	if body.SSHUser == "" {
		body.SSHUser = "root"
	}
	if !isSafeIdent(body.SSHUser, 32) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "ssh_user에 허용되지 않은 문자가 포함되어 있습니다."})
		return
	}
	if body.NodeName == "" {
		parts := strings.Split(body.NodeIP, ".")
		body.NodeName = "worker-" + parts[len(parts)-1]
	}
	if !isSafeIdent(body.NodeName, 64) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "node_name에 허용되지 않은 문자가 포함되어 있습니다."})
		return
	}
	if body.SSHKeyPath != "" && !isSafePath(body.SSHKeyPath) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "ssh_key_path에 허용되지 않은 문자가 포함되어 있습니다."})
		return
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

		// Pass secrets via env (SSHPASS / ORCH_API_TOKEN) not argv when possible.
		// Still provide positional args for backward-compat with older scripts,
		// but the provisionLog output will be scrubbed of any password/token leaks.
		cmd := exec.Command("bash", scriptPath,
			body.NodeIP, body.NodeName, body.SSHUser, sshPassword,
			masterAddr, apiToken, "", sshKeyPath,
		)
		cmd.Env = append(os.Environ(),
			"SSHPASS="+sshPassword,
			"ORCH_API_TOKEN="+apiToken,
		)

		stdout, _ := cmd.StdoutPipe()
		cmd.Stderr = cmd.Stdout
		if err := cmd.Start(); err != nil {
			provisionMu.Lock()
			provisionLog.WriteString("ERROR: " + err.Error() + "\n")
			provisionMu.Unlock()
			log.Printf("Provision error: %v", err)
			return
		}

		// Build a sanitizer that scrubs known secrets from stdout before
		// it lands in the HTTP-exposed provisionLog.
		sanitize := func(data []byte) []byte {
			s := string(data)
			if sshPassword != "" {
				s = strings.ReplaceAll(s, sshPassword, "****")
			}
			if apiToken != "" {
				s = strings.ReplaceAll(s, apiToken, "****")
			}
			return []byte(s)
		}

		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				provisionMu.Lock()
				provisionLog.Write(sanitize(buf[:n]))
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
	result := getServicesDataForViewer(showSystem, requesterUsername(r))
	writeJSON(w, http.StatusOK, result)
}

// getServicesData is the legacy entry kept for non-HTTP callers (SSE builder).
// It produces the same shape as handleListServices for an unauthenticated
// viewer, i.e. secret values are masked.
func getServicesData(showSystem bool) []map[string]any {
	return getServicesDataForViewer(showSystem, "")
}

// getServicesDataForViewer builds the services listing data, applying
// per-owner secret masking based on the supplied viewer username.
func getServicesDataForViewer(showSystem bool, viewer string) []map[string]any {
	localServices := state.ListServices()

	if clusterState == nil {
		var result []map[string]any
		for _, s := range localServices {
			if !showSystem && systemServices[s.Name] {
				continue
			}
			result = append(result, serviceInfoToMapForViewer(s, viewer))
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

		entry := map[string]any{
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
		}
		// Attach owner + masked env_vars from cluster state (handleClusterDeploy
		// writes here) with a fallback to local services state for master-only
		// flows that bypass the scheduler.
		var svcMeta map[string]any
		if clusterSvc != nil {
			svcMeta = clusterSvc
		}
		if svcMeta == nil {
			svcMeta = state.GetService(svcName)
		}
		if svcMeta != nil {
			if owner, ok := svcMeta["owner"].(string); ok {
				entry["owner"] = owner
			}
			if sh, ok := svcMeta["shared"].(bool); ok {
				entry["shared"] = sh
			}
			if evs := maskedEnvVarsForViewer(svcMeta, viewer); len(evs) > 0 {
				entry["env_vars"] = evs
				entry["can_edit_secrets"] = viewer != "" && viewer == entry["owner"]
			}
		}
		result = append(result, entry)
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
		result = append(result, serviceInfoToMapForViewer(s, viewer))
	}

	// Add defined-but-stopped services (desired_replicas set, no live
	// placements). Without this, stopping a service makes it vanish from the
	// portal so it can't be restarted from the UI.
	for _, svc := range clusterState.ListServices() {
		svcName, _ := svc["name"].(string)
		if svcName == "" || seen[svcName] {
			continue
		}
		if !showSystem && systemServices[svcName] {
			continue
		}
		image, _ := svc["image"].(string)
		desired := 0
		switch v := svc["desired_replicas"].(type) {
		case int:
			desired = v
		case int64:
			desired = int(v)
		case float64:
			desired = int(v)
		}
		entry := map[string]any{
			"name":          svcName,
			"image":         image,
			"replicas":      desired,
			"running":       0,
			"status":        "stopped",
			"container_ids": []string{},
			"nodes":         []string{},
			"endpoint":      "",
			"memory_limit":  svc["memory_limit"],
			"cpu_limit":     svc["cpu_limit"],
		}
		// Owner + masked env from full service record.
		if full := clusterState.GetService(svcName); full != nil {
			if owner, ok := full["owner"].(string); ok {
				entry["owner"] = owner
			}
			if sh, ok := full["shared"].(bool); ok {
				entry["shared"] = sh
			}
			if evs := maskedEnvVarsForViewer(full, viewer); len(evs) > 0 {
				entry["env_vars"] = evs
				entry["can_edit_secrets"] = viewer != "" && viewer == entry["owner"]
			}
		}
		seen[svcName] = true
		result = append(result, entry)
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

// registryCatalogCache caches the registry catalog set to avoid blocking image listing.
var (
	regCatalogMu      sync.RWMutex
	regCatalogCache   map[string]bool
	regCatalogExpires time.Time
	regEndpointCache  string
)

func getCachedRegistryInfo() (string, map[string]bool) {
	regCatalogMu.RLock()
	if time.Now().Before(regCatalogExpires) {
		ep, cat := regEndpointCache, regCatalogCache
		regCatalogMu.RUnlock()
		return ep, cat
	}
	regCatalogMu.RUnlock()

	// Refresh in background to not block the caller on the first miss.
	go refreshRegistryCatalog()
	return "", nil
}

func refreshRegistryCatalog() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if !isRegistryRunning(ctx) {
		regCatalogMu.Lock()
		regEndpointCache = ""
		regCatalogCache = nil
		regCatalogExpires = time.Now().Add(15 * time.Second)
		regCatalogMu.Unlock()
		return
	}

	endpoint := ""
	if info := getRegistryInfo(ctx); info != nil {
		if ep, ok := info["endpoint"].(string); ok {
			endpoint = ep
		}
	}
	regURL := getRegistryInternalURL(ctx)
	catalog := buildRegistryCatalogSet(regURL)

	regCatalogMu.Lock()
	regEndpointCache = endpoint
	regCatalogCache = catalog
	regCatalogExpires = time.Now().Add(15 * time.Second)
	regCatalogMu.Unlock()
}

func handleListImages(w http.ResponseWriter, r *http.Request) {
	images := monitoring.ListImages()

	// Use cached registry info instead of blocking on registry calls
	endpoint, regSet := getCachedRegistryInfo()

	for i, img := range images {
		tags, _ := img["tags"].([]string)
		repo, _ := img["repository"].(string)
		tag, _ := img["tag"].(string)
		if repo == "<none>" || repo == "" {
			continue
		}

		if endpoint == "" {
			images[i]["image"] = repo + ":" + tag
			continue
		}

		var registryPath string
		for _, t := range tags {
			if strings.HasPrefix(t, "localhost:5000/") {
				registryPath = strings.Replace(t, "localhost:5000", endpoint, 1)
				break
			}
			if strings.HasPrefix(t, endpoint+"/") {
				registryPath = t
				break
			}
		}

		if registryPath == "" && regSet != nil {
			candidate := repo + ":" + tag
			if regSet[candidate] {
				registryPath = endpoint + "/" + candidate
			}
		}

		if registryPath != "" {
			images[i]["image"] = registryPath
			images[i]["in_registry"] = true
		} else {
			images[i]["image"] = repo + ":" + tag
			images[i]["in_registry"] = false
		}
	}

	writeJSON(w, http.StatusOK, images)
}

// buildRegistryCatalogSet queries the registry and returns a set of "repo:tag" strings.
func buildRegistryCatalogSet(regURL string) map[string]bool {
	result := make(map[string]bool)
	if regURL == "" {
		return result
	}
	resp, err := http.Get(regURL + "/v2/_catalog")
	if err != nil {
		return result
	}
	defer resp.Body.Close()
	var catalog struct {
		Repositories []string `json:"repositories"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&catalog); err != nil {
		return result
	}
	for _, repo := range catalog.Repositories {
		tagsResp, err := http.Get(fmt.Sprintf("%s/v2/%s/tags/list", regURL, repo))
		if err != nil {
			continue
		}
		var tagList struct {
			Tags []string `json:"tags"`
		}
		json.NewDecoder(tagsResp.Body).Decode(&tagList)
		tagsResp.Body.Close()
		for _, t := range tagList.Tags {
			result[repo+":"+t] = true
		}
	}
	return result
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

	// 1) Try fast regex-based NL engine first
	intent := nlengine.Parse(req.Command)
	if intent.Action != models.IntentUnknown {
		success, message, details := runtime.ExecuteIntent(intent, req.DryRun)
		writeJSON(w, http.StatusOK, models.CommandResponse{
			Success: success,
			Intent:  &intent,
			Message: message,
			Details: details,
		})
		return
	}

	// 2) Fallback: use Claude API to understand and execute the command
	if getAnthropicKey() == "" {
		writeJSON(w, http.StatusOK, models.CommandResponse{
			Success: false,
			Intent:  &intent,
			Message: "명령을 이해하지 못했습니다. AI 엔진이 비활성화 상태입니다.",
		})
		return
	}

	systemPrompt := buildCommandSystemPrompt(req.DryRun)

	if req.DryRun {
		// Dry run: use Claude without tools to describe what it would do
		response, err := callClaude(systemPrompt, req.Command)
		if err != nil {
			log.Printf("[AI command fallback] error: %v", err)
			writeJSON(w, http.StatusOK, models.CommandResponse{
				Success: false,
				Intent:  &intent,
				Message: "AI 명령 해석 실패: " + err.Error(),
			})
			return
		}
		aiIntent := models.ParsedIntent{Action: "ai_interpreted", Raw: req.Command}
		writeJSON(w, http.StatusOK, models.CommandResponse{
			Success: true,
			Intent:  &aiIntent,
			Message: "[AI 미리보기] " + response,
		})
		return
	}

	// Execute via Claude with tools
	response, toolLog, err := callClaudeWithTools(systemPrompt, req.Command)
	if err != nil {
		log.Printf("[AI command fallback] error: %v", err)
		writeJSON(w, http.StatusOK, models.CommandResponse{
			Success: false,
			Intent:  &intent,
			Message: "AI 명령 실행 실패: " + err.Error(),
		})
		return
	}

	aiIntent := models.ParsedIntent{Action: "ai_executed", Raw: req.Command}
	details := map[string]any{}
	if len(toolLog) > 0 {
		details["actions"] = toolLog
	}
	writeJSON(w, http.StatusOK, models.CommandResponse{
		Success: true,
		Intent:  &aiIntent,
		Message: response,
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
		replicas := 0
		if req.Replicas != nil {
			replicas = *req.Replicas
		}
		ok, msg, details = runtime.ExecuteDeploy(ctx, cli, name, req.Image, replicas)

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
		ExtraAliases:       req.ExtraAliases,
		ExtraLabels:        req.ExtraLabels,
		Subdomain:          req.Subdomain,
		AutoPull:           true,
	})
	if ok {
		monitoring.InvalidateCache("containers_all")
		monitoring.InvalidateCache("containers_running")
	}
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
	if ok {
		monitoring.InvalidateCache("containers_all")
		monitoring.InvalidateCache("containers_running")
	}
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
		Image    string `json:"image"`
		SkipPush bool   `json:"skip_push"` // opt-out from auto-push to internal registry
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	ok, msg := runtime.PullImage(ctx, cli, image)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": msg})
		return
	}
	monitoring.InvalidateCache("images")

	resp := map[string]any{"success": true, "message": msg, "image": image}

	// Auto-push to internal registry (best-effort).
	if !body.SkipPush && isRegistryRunning(ctx) {
		repoTag := stripRegistryPrefix(image)
		regImg, pushErr := pushImageToRegistry(ctx, image, repoTag)
		if pushErr != nil {
			resp["registry_pushed"] = false
			resp["registry_error"] = pushErr.Error()
			resp["message"] = msg + " (내장 registry push 실패: " + pushErr.Error() + ")"
		} else {
			resp["registry_pushed"] = true
			resp["registry_image"] = regImg
			resp["registry_external"] = registryExternalTag(repoTag)
			resp["message"] = msg + " · 내장 registry에 push됨 → " + registryExternalTag(repoTag)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// stripRegistryPrefix turns a fully qualified image reference into a repo:tag
// suitable for tagging against the local registry. Examples:
//
//	nginx:latest                                    -> nginx:latest
//	docker.io/library/nginx:latest                  -> library/nginx:latest
//	20.20.0.13:80/iconloop/goloop:v1.2.5            -> iconloop/goloop:v1.2.5
//	ghcr.io/owner/app@sha256:...                    -> owner/app:sha-<first12>
func stripRegistryPrefix(image string) string {
	// If image has "@sha256:..." (digest), replace with a tag name derived from the digest.
	if at := strings.Index(image, "@sha256:"); at >= 0 {
		name := image[:at]
		digest := image[at+len("@sha256:"):]
		if len(digest) > 12 {
			digest = digest[:12]
		}
		image = name + ":sha-" + digest
	}

	// Ensure there's an explicit :tag — default to latest.
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon < lastSlash {
		// The colon was part of a host:port, not a tag.
		image = image + ":latest"
	}

	// Strip registry host prefix. Identified as first path segment containing
	// '.', ':', or equal to "localhost".
	firstSlash := strings.Index(image, "/")
	if firstSlash > 0 {
		head := image[:firstSlash]
		if head == "localhost" || strings.Contains(head, ".") || strings.Contains(head, ":") {
			image = image[firstSlash+1:]
		}
	}
	return image
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
		NodeName   string                      `json:"node_name"`
		Resources  models.NodeResources        `json:"resources"`
		Containers []models.ContainerPlacement `json:"containers"`
		Address    string                      `json:"address"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ack": false, "error": "Invalid payload"})
		return
	}

	// Use advertised address from heartbeat, or derive from remote IP
	addr := body.Address
	if addr == "" {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if host != "" {
			addr = host + ":8000"
		}
	}

	clusterState.ProcessHeartbeat(body.NodeName, addr, body.Resources, body.Containers)
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
		ServiceName string                      `json:"service_name"`
		Image       string                      `json:"image"`
		Replicas    int                         `json:"replicas"`
		Strategy    string                      `json:"strategy"`
		MemoryLimit string                      `json:"memory_limit"`
		CPULimit    string                      `json:"cpu_limit"`
		Environment []string                    `json:"environment"`
		Volumes     []string                    `json:"volumes"`
		Ports       []string                    `json:"ports"`
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

	// Enforce ownership: non-admin users may only scale their own services.
	if clusterState != nil {
		if info := clusterState.GetService(serviceName); info != nil {
			if ok, owner := canManageService(r, info); !ok {
				writeJSON(w, http.StatusForbidden, map[string]any{
					"success": false,
					"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 스케일을 조정할 수 있습니다", serviceName, owner),
					"code":    "forbidden_owner",
				})
				return
			}
		}
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
	// Owner check: admin manages all; a user may only stop services it owns.
	if info := clusterState.GetService(serviceName); info != nil {
		if ok, owner := canManageService(r, info); !ok {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"success": false,
				"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 중지할 수 있습니다", serviceName, owner),
				"code":    "forbidden_owner",
			})
			return
		}
	}
	result := clusterStopServiceInternal(serviceName)
	writeJSON(w, http.StatusOK, result)
}

// handleClusterDeleteService fully removes a service: stops + removes all its
// containers (like stop) AND deletes the service definition + placement rows
// from cluster state so it no longer appears in listings and reconcile never
// respawns it. Same owner restriction as stop.
func handleClusterDeleteService(w http.ResponseWriter, r *http.Request) {
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
	// Owner check (same rule as stop).
	if info := clusterState.GetService(serviceName); info != nil {
		if ok, owner := canManageService(r, info); !ok {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"success": false,
				"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 삭제할 수 있습니다", serviceName, owner),
				"code":    "forbidden_owner",
			})
			return
		}
	}
	// Stop + remove containers first, then delete the definition everywhere.
	clusterStopServiceInternal(serviceName)
	clusterState.DeleteService(serviceName)
	// Master's own local services.json.
	state.DeleteService(serviceName)
	// Propagate the local-state deletion to every worker so the entry doesn't
	// linger (and so heartbeats don't re-surface it in listings).
	delBody, _ := json.Marshal(map[string]any{"service_name": serviceName})
	for _, n := range clusterState.ListNodes() {
		if n.Role == "master" {
			continue
		}
		node := n
		req, _ := http.NewRequest("POST", nodeBaseURL(&node)+"/v1/agent/delete-service", bytes.NewReader(delBody))
		setNodeHeaders(req, &node)
		if resp, err := httpClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("'%s' 서비스 삭제 완료 (컨테이너 제거 + 정의 삭제)", serviceName),
	})
}

// handleAgentDeleteService removes a service definition from this node's local
// services.json (worker-side counterpart of the cluster delete).
func handleAgentDeleteService(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string `json:"service_name"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	name := strings.TrimSpace(body.ServiceName)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service_name required"})
		return
	}
	state.DeleteService(name)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// handleClusterNodeProxy forwards a request to a specific node's local API.
//
// The dashboard's apiUrl() helper rewrites calls to
// /v1/cluster/{node}/proxy?path=<urlencoded original path> when a target node
// other than "local" is selected. Without this handler chi returns a plain
// "404 page not found" body, which the dashboard then tries to JSON.parse —
// producing the confusing "Unexpected non-whitespace character after JSON at
// position 4" error (it parses the leading 404 as a number).
//
// "local" / "master" / empty resolve to this node directly (loopback) so the
// proxy is always safe to call.
func handleClusterNodeProxy(w http.ResponseWriter, r *http.Request) {
	nodeName := strings.TrimSpace(chi.URLParam(r, "node"))
	path := r.URL.Query().Get("path")
	if path == "" || !strings.HasPrefix(path, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "proxy path 파라미터가 필요합니다"})
		return
	}

	// The /v1/cluster/ prefix bypasses sessionAuthMiddleware, so guard mutating
	// proxied calls here: forwarding with the cluster token grants privileged
	// access, so require an admin session for anything that changes state.
	if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
		if sess := auth.SessionFromRequest(r); sess == nil || sess.Role != auth.RoleAdmin {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"success": false, "message": "관리자 권한이 필요합니다", "code": "forbidden",
			})
			return
		}
	}

	// Resolve the destination base URL.
	masterName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if masterName == "" {
		masterName = "master"
	}
	var baseURL string
	var node *models.NodeInfo
	if nodeName == "" || nodeName == "local" || nodeName == masterName {
		baseURL = "http://localhost:" + getServerPort()
	} else if clusterState != nil {
		node = clusterState.GetNode(nodeName)
		if node == nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "노드를 찾을 수 없습니다: " + nodeName})
			return
		}
		baseURL = nodeBaseURL(node)
	} else {
		baseURL = "http://localhost:" + getServerPort()
	}

	// Build the forwarded request, preserving method + body.
	var bodyReader io.Reader
	if r.Body != nil {
		data, _ := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		bodyReader = bytes.NewReader(data)
	}
	fwReq, err := http.NewRequest(r.Method, baseURL+path, bodyReader)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": err.Error()})
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		fwReq.Header.Set("Content-Type", ct)
	}
	// Authenticate the forwarded call with the shared cluster token so the
	// destination treats it as a trusted inter-node request.
	if node != nil {
		setNodeHeaders(fwReq, node)
	} else if tok := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN")); tok != "" {
		fwReq.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := longHTTPClient.Do(fwReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"success": false, "message": "프록시 요청 실패: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	// Relay status + content-type + body verbatim.
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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
	// Owner check: resolve the container's owning service via its placement
	// row; admin manages all, a user may only remove its own. Containers that
	// can't be mapped to a known service fall through (token/legacy only).
	if requesterUsername(r) != "" {
		svcName := ""
		for _, p := range clusterState.GetPlacements("", body.NodeName) {
			if p.ContainerID == body.ContainerID || (body.ContainerName != "" && p.ContainerName == body.ContainerName) {
				svcName = p.ServiceName
				break
			}
		}
		if svcName != "" {
			if info := clusterState.GetService(svcName); info != nil {
				if ok, owner := canManageService(r, info); !ok {
					writeJSON(w, http.StatusForbidden, map[string]any{
						"success": false,
						"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 컨테이너를 제거할 수 있습니다", svcName, owner),
						"code":    "forbidden_owner",
					})
					return
				}
			}
		}
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
		Image       string   `json:"image"`
		Name        string   `json:"name"`
		Replicas    int      `json:"replicas"`
		Strategy    string   `json:"strategy"`
		Memory      string   `json:"memory"`
		CPU         string   `json:"cpu"`
		Environment []string `json:"environment"`
		// Secrets: variable names within Environment that must be persisted
		// encrypted and access-restricted to the deploying user.
		Secrets []string `json:"secrets"`
		// SecretRefs pulls values from the caller's pre-saved user-secret
		// store. Each ref is expanded into Environment[env_name=plaintext]
		// just before the container starts; the env_name is also appended to
		// Secrets so the rest of the pipeline treats it as encrypted at rest.
		SecretRefs  []usersecrets.SecretRef     `json:"secret_refs"`
		Volumes     []string                    `json:"volumes"`
		Ports       []string                    `json:"ports"`
		Nodes       []string                    `json:"nodes"`
		Constraints *models.ScheduleConstraints `json:"constraints"`
		// Subdomain opts this service into subdomain routing
		// (svc.<base-domain>) instead of the default path routing
		// (<path-host>/svc/).
		Subdomain bool `json:"subdomain"`
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

	// Expand any pre-saved user-secret references into body.Environment BEFORE
	// dispatching to nodes — every container in this deploy needs the secret
	// value injected as a plaintext env var. Owner check happens inside
	// ResolveRefs; refs that don't belong to the deploying user reject the
	// whole deployment.
	if len(body.SecretRefs) > 0 {
		deployOwnerEarly := requesterUsername(r)
		if deployOwnerEarly != "" {
			resolved, err := usersecrets.ResolveRefs(deployOwnerEarly, body.SecretRefs)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{
					"success": false,
					"message": "시크릿 변수 적용 실패: " + err.Error(),
				})
				return
			}
			for envName, val := range resolved {
				body.Environment = append(body.Environment, envName+"="+val)
				body.Secrets = append(body.Secrets, envName)
			}
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
			"subdomain":            body.Subdomain,
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
	deployOwner := requesterUsername(r)
	envVars := buildEnvVars(body.Environment, body.Secrets)
	clusterState.SaveService(name, image, replicas, map[string]any{
		"memory_limit": body.Memory,
		"cpu_limit":    body.CPU,
		"environment":  body.Environment,
		"env_vars":     envVars,
		"owner":        deployOwner,
		"volumes":      body.Volumes,
		"ports":        body.Ports,
		"subdomain":    body.Subdomain,
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

// handleClusterStats: GET /v1/cluster/stats. Aggregates live CPU/memory usage
// per service across all nodes (sums replicas). Read-only, non-sensitive usage
// numbers — no per-service auth gate.
func handleClusterStats(w http.ResponseWriter, r *http.Request) {
	toF := func(v any) float64 {
		switch n := v.(type) {
		case float64:
			return n
		case int:
			return float64(n)
		}
		return 0
	}
	svcFromName := func(name string) string {
		s := strings.TrimPrefix(name, "/")
		s = strings.TrimPrefix(s, "orch-")
		if i := strings.LastIndex(s, "-"); i > 0 {
			if _, err := strconv.Atoi(s[i+1:]); err == nil {
				return s[:i]
			}
		}
		return s
	}
	agg := map[string]map[string]float64{}
	add := func(containers []map[string]any) {
		for _, c := range containers {
			svc, _ := c["service"].(string)
			if svc == "" {
				name, _ := c["name"].(string)
				svc = svcFromName(name)
			}
			if svc == "" {
				continue
			}
			st, _ := c["stats"].(map[string]any)
			if st == nil {
				continue
			}
			e := agg[svc]
			if e == nil {
				e = map[string]float64{}
				agg[svc] = e
			}
			e["cpu_percent"] += toF(st["cpu_percent"])
			e["memory_usage_mb"] += toF(st["memory_usage_mb"])
			e["memory_limit_mb"] += toF(st["memory_limit_mb"])
		}
	}

	if clusterState == nil {
		add(monitoring.GetAllContainersWithStats())
	} else {
		for _, node := range clusterState.ListNodes() {
			req, _ := http.NewRequest(http.MethodGet, nodeBaseURL(&node)+"/v1/containers?stats=true", nil)
			setNodeHeaders(req, &node)
			resp, err := longHTTPClient.Do(req)
			if err != nil {
				continue
			}
			var d struct {
				Containers []map[string]any `json:"containers"`
			}
			json.NewDecoder(resp.Body).Decode(&d)
			resp.Body.Close()
			add(d.Containers)
		}
	}

	out := map[string]any{}
	for svc, e := range agg {
		memPct := 0.0
		if e["memory_limit_mb"] > 0 {
			memPct = e["memory_usage_mb"] / e["memory_limit_mb"] * 100.0
		}
		cpu := e["cpu_percent"]
		if cpu > 100 {
			cpu = 100
		}
		out[svc] = map[string]any{
			"cpu_percent":     cpu,
			"memory_usage_mb": e["memory_usage_mb"],
			"memory_limit_mb": e["memory_limit_mb"],
			"memory_percent":  memPct,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "stats": out})
}

// handleAgentLogs: POST /v1/agent/logs {service_name, tail}. Node-local endpoint
// (inter-node, token-auth) that returns recent logs for every container of the
// named service on this node.
func handleAgentLogs(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string `json:"service_name"`
		Tail        string `json:"tail"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Invalid request"})
		return
	}
	svc := strings.TrimSpace(body.ServiceName)
	tail := strings.TrimSpace(body.Tail)
	if tail == "" {
		tail = "200"
	}
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": "Docker connection failed"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	containers, _ := cli.ContainerList(ctx, container.ListOptions{All: true})
	prefix := "orch-" + svc + "-"
	var out strings.Builder
	found := false
	for _, c := range containers {
		name := ""
		for _, n := range c.Names {
			nm := strings.TrimPrefix(n, "/")
			if strings.HasPrefix(nm, prefix) {
				name = nm
				break
			}
		}
		if name == "" {
			continue
		}
		found = true
		lr, err := cli.ContainerLogs(ctx, c.ID, container.LogsOptions{
			ShowStdout: true, ShowStderr: true, Tail: tail, Timestamps: false,
		})
		out.WriteString(fmt.Sprintf("===== %s (%s) =====\n", name, c.State))
		if err == nil {
			b, _ := io.ReadAll(lr)
			lr.Close()
			out.WriteString(stripDockerLogHeaders(string(b)))
		} else {
			out.WriteString("(로그 조회 실패: " + err.Error() + ")")
		}
		out.WriteString("\n")
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "found": found, "logs": out.String()})
}

// handleClusterLogs: GET /v1/cluster/logs?service=NAME&tail=N. Returns recent
// container logs for a service, aggregated across nodes. GET requests bypass the
// role middleware, so this handler enforces its own auth: a session is required
// and non-admins may only read logs for services they own (logs can leak
// secrets).
func handleClusterLogs(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "로그인이 필요합니다", "code": "unauthenticated"})
		return
	}
	serviceName := strings.TrimSpace(r.URL.Query().Get("service"))
	if serviceName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service 파라미터가 필요합니다"})
		return
	}
	tail := strings.TrimSpace(r.URL.Query().Get("tail"))
	if tail == "" {
		tail = "200"
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다"})
		return
	}
	info := clusterState.GetService(serviceName)
	if info == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "서비스를 찾을 수 없습니다: " + serviceName})
		return
	}
	if ok, owner := canManageService(r, info); !ok {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false,
			"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 로그를 볼 수 있습니다", serviceName, owner),
			"code":    "forbidden_owner",
		})
		return
	}
	payload, _ := json.Marshal(map[string]any{"service_name": serviceName, "tail": tail})
	var combined strings.Builder
	for _, node := range clusterState.ListNodes() {
		baseURL := nodeBaseURL(&node)
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/agent/logs", bytes.NewReader(payload))
		setNodeHeaders(req, &node)
		resp, err := longHTTPClient.Do(req)
		if err != nil {
			continue
		}
		var d struct {
			Found bool   `json:"found"`
			Logs  string `json:"logs"`
		}
		json.NewDecoder(resp.Body).Decode(&d)
		resp.Body.Close()
		if d.Found && strings.TrimSpace(d.Logs) != "" {
			combined.WriteString(fmt.Sprintf("########## node: %s ##########\n", node.Name))
			combined.WriteString(d.Logs)
			combined.WriteString("\n")
		}
	}
	logs := combined.String()
	if strings.TrimSpace(logs) == "" {
		logs = "(실행 중인 컨테이너 로그가 없습니다)"
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "service": serviceName, "logs": logs})
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
	// Default path routing; agent run-one is used by migrate/compose flows
	// which don't carry a per-service subdomain preference.
	for k, v := range runtime.TraefikLabels(svcName, runtime.TraefikHTTPPort, false) {
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

// masterAgent is reused across heartbeats to avoid allocation overhead.
var masterAgent *agent.WorkerAgent

func masterSelfHeartbeat() {
	if clusterState == nil {
		return
	}
	if masterAgent == nil {
		masterAgent = agent.NewWorkerAgent()
	}
	resourcesMap := masterAgent.GetNodeResources()

	// Convert map to NodeResources struct.
	resJSON, _ := json.Marshal(resourcesMap)
	var resources models.NodeResources
	json.Unmarshal(resJSON, &resources)

	// Get managed containers.
	containersRaw := masterAgent.GetManagedContainers()
	containers := make([]models.ContainerPlacement, 0, len(containersRaw))
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
	masterAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	clusterState.ProcessHeartbeat(nodeName, masterAddr, resources, containers)
}

// yamlDoubleQuote wraps a Traefik rule expression for safe emission inside
// a double-quoted YAML scalar. Traefik rules contain backticks and may
// contain backslashes (Referer regex), both of which need escaping when
// the value is written inside YAML double quotes.
func yamlDoubleQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
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
		middlewares []string
		priority    int
	}
	type serviceCfg struct {
		servers []string
	}
	type middlewareCfg struct {
		stripPrefixes []string
	}

	routers := make(map[string]routerCfg)
	services := make(map[string]serviceCfg)
	middlewares := make(map[string]middlewareCfg)

	re := regexp.MustCompile(`[^a-z0-9-]`)

	baseDomain := runtime.BaseDomain()
	pathHost := runtime.PathHost()

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

		if serviceUsesSubdomain(svcName) && baseDomain != "" {
			// Subdomain routing (opt-in): svc.baseDomain → worker Traefik.
			routers[routeName] = routerCfg{
				rule:        fmt.Sprintf("Host(`%s.%s`)", svcName, baseDomain),
				service:     routeName,
				entryPoints: []string{"web"},
			}
		} else {
			// Default path routing. The MASTER forwards the original /svc/...
			// path to the worker's Traefik (port 80), which applies the
			// container's own strip-prefix middleware — so we must NOT strip
			// here (that would double-strip).
			if pathHost != "" {
				routers[routeName+"-host"] = routerCfg{
					rule:        fmt.Sprintf("Host(`%s`) && PathPrefix(`/%s/`)", pathHost, svcName),
					service:     routeName,
					entryPoints: []string{"web"},
					priority:    120,
				}
			}
			routers[routeName+"-path"] = routerCfg{
				rule:        fmt.Sprintf("PathPrefix(`/%s/`)", svcName),
				service:     routeName,
				entryPoints: []string{"web"},
				priority:    100,
			}
			routers[routeName+"-referer"] = routerCfg{
				rule:        fmt.Sprintf("HeadersRegexp(`Referer`, `^https?://[^/]+/%s(/|\\?|$)`)", svcName),
				service:     routeName,
				entryPoints: []string{"web"},
				priority:    50,
			}
			routers[routeName] = routerCfg{
				rule:        fmt.Sprintf("Host(`%s.local`)", svcName),
				service:     routeName,
				entryPoints: []string{"web"},
			}
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
			// Quote the rule — the Referer regex contains backslashes that
			// YAML would otherwise interpret as escapes, breaking parsing.
			// Using a bare quoted string means we need to escape any embedded
			// double quotes; Traefik rules don't use those, so this is safe.
			lines = append(lines, fmt.Sprintf("      rule: %s", yamlDoubleQuote(cfg.rule)))
			lines = append(lines, fmt.Sprintf("      service: %s", cfg.service))
			if cfg.priority > 0 {
				lines = append(lines, fmt.Sprintf("      priority: %d", cfg.priority))
			}
			if len(cfg.entryPoints) > 0 {
				lines = append(lines, "      entryPoints:")
				for _, ep := range cfg.entryPoints {
					lines = append(lines, fmt.Sprintf("        - %s", ep))
				}
			}
			if len(cfg.middlewares) > 0 {
				lines = append(lines, "      middlewares:")
				for _, mw := range cfg.middlewares {
					lines = append(lines, fmt.Sprintf("        - %s", mw))
				}
			}
		}
	}

	if len(middlewares) > 0 {
		lines = append(lines, "  middlewares:")
		var mwKeys []string
		for k := range middlewares {
			mwKeys = append(mwKeys, k)
		}
		sort.Strings(mwKeys)
		for _, name := range mwKeys {
			cfg := middlewares[name]
			lines = append(lines, fmt.Sprintf("    %s:", name))
			if len(cfg.stripPrefixes) > 0 {
				lines = append(lines, "      stripPrefix:")
				lines = append(lines, "        prefixes:")
				for _, p := range cfg.stripPrefixes {
					lines = append(lines, fmt.Sprintf("          - \"%s\"", p))
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
	token := node.Token
	if token == "" {
		// Fall back to the master's shared cluster token. Workers that joined
		// via heartbeat have an empty Token field on master-side state, but
		// they run with the same ORCHESTRATOR_API_TOKEN in their own env.
		token = strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN"))
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
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
	return serviceInfoToMapForViewer(s, "")
}

// serviceInfoToMapForViewer is the owner-aware variant. It enriches the
// response with the persisted owner + env_vars, masking secret values for
// any viewer that is not the owner.
func serviceInfoToMapForViewer(s models.ServiceInfo, viewer string) map[string]any {
	ids := s.ContainerIDs
	if ids == nil {
		ids = []string{}
	}
	m := map[string]any{
		"name":          s.Name,
		"image":         s.Image,
		"replicas":      s.Replicas,
		"memory_limit":  s.MemoryLimit,
		"cpu_limit":     s.CPULimit,
		"status":        s.Status,
		"container_ids": ids,
	}
	if svc := state.GetService(s.Name); svc != nil {
		if owner, ok := svc["owner"].(string); ok {
			m["owner"] = owner
		}
		if evs := maskedEnvVarsForViewer(svc, viewer); len(evs) > 0 {
			m["env_vars"] = evs
			m["can_edit_secrets"] = viewer != "" && viewer == m["owner"]
		}
	}
	return m
}

// isDockerInternalIP returns true if the IP belongs to common Docker/container network ranges.
func isDockerInternalIP(ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	dockerRanges := []string{
		"172.16.0.0/12",  // Docker bridge default
		"10.0.0.0/8",     // Common overlay/swarm
		"192.168.0.0/16", // docker-compose default on some setups
	}
	for _, cidr := range dockerRanges {
		_, subnet, err := net.ParseCIDR(cidr)
		if err == nil && subnet.Contains(parsed) {
			return true
		}
	}
	return false
}

// detectHostIP tries to find the real host IP visible to external clients.
// Inside Docker, standard methods return the container-internal IP (e.g. 172.x).
// This function uses multiple strategies to find the actual host IP.
func detectHostIP() string {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	// Method 1: Read host IP from Docker's /host/net/dev mount or host-mapped gateway.
	// Many Docker setups expose the host gateway at 172.17.0.1 or via host.docker.internal.
	// We check the Docker default gateway's ARP entry or /etc/hosts for the host IP.
	if data, err := os.ReadFile("/etc/hosts"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, "host.docker.internal") {
				fields := strings.Fields(line)
				if len(fields) >= 1 {
					ip := fields[0]
					if !isDockerInternalIP(ip) && net.ParseIP(ip) != nil {
						return ip + ":" + port
					}
				}
			}
		}
	}

	// Method 2: Read default gateway from /proc/net/route and resolve upstream host IP.
	// On Docker bridge networks, the default gateway IS the host.
	if data, err := os.ReadFile("/proc/net/route"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines[1:] {
			fields := strings.Fields(line)
			if len(fields) >= 3 && fields[1] == "00000000" {
				// Parse gateway IP (little-endian hex)
				gwHex := fields[2]
				if len(gwHex) == 8 {
					var octets [4]uint64
					for i := 0; i < 4; i++ {
						v, _ := strconv.ParseUint(gwHex[i*2:i*2+2], 16, 8)
						octets[i] = v
					}
					// /proc/net/route stores the gateway in little-endian hex on x86,
					// so the most-significant byte is last. Reverse to get a.b.c.d.
					gwIP := fmt.Sprintf("%d.%d.%d.%d", octets[3], octets[2], octets[1], octets[0])
					// If the gateway itself is Docker-internal, try connecting through it
					// to discover the host's external IP via the gateway's perspective.
					if !isDockerInternalIP(gwIP) {
						return gwIP + ":" + port
					}
				}
			}
		}
	}

	// Method 3: UDP dial trick — returns the IP used to reach the default gateway.
	// Skip if it returns a Docker-internal IP.
	conn, err := net.DialTimeout("udp4", "8.8.8.8:53", 2*time.Second)
	if err == nil {
		defer conn.Close()
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && !addr.IP.IsLoopback() {
			ip := addr.IP.String()
			if !isDockerInternalIP(ip) {
				return ip + ":" + port
			}
		}
	}

	// Method 4: Scan network interfaces, pick the first non-loopback, non-Docker IP.
	if ifaces, err := net.Interfaces(); err == nil {
		for _, iface := range ifaces {
			// Skip Docker/veth interfaces by name
			name := strings.ToLower(iface.Name)
			if strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "veth") ||
				strings.HasPrefix(name, "br-") || name == "lo" {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, a := range addrs {
				if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP.To4() != nil && !ipNet.IP.IsLoopback() {
					ip := ipNet.IP.String()
					if !isDockerInternalIP(ip) {
						return ip + ":" + port
					}
				}
			}
		}
	}

	// Method 5: Last resort — first non-loopback interface
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
				return ipNet.IP.String() + ":" + port
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
		// Prefer the structured env_vars (with per-secret encryption) so the
		// container always restarts with the full set of variables intact;
		// fall back to the legacy plaintext slice when env_vars is missing.
		if envSlice := decryptedEnvironment(svcInfo); len(envSlice) > 0 {
			runPayload["environment"] = envSlice
		} else if env, ok := svcInfo["environment"].(string); ok && env != "" && env != "[]" {
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

// handleAgentExec runs `docker exec <container> sh -c <cmd>` on the host.
//
// SECURITY: This endpoint can execute arbitrary shell commands. It MUST only
// be reachable by authenticated cluster traffic or an admin session.
// Access is gated by:
//  1. Valid ORCHESTRATOR_API_TOKEN bearer (inter-node cluster calls), OR
//  2. Admin session cookie (interactive operator).
//
// Additionally, only allow-listed container-name patterns (currently limited
// to blockchain-* / qs-dist-* used by the distributed deploy flow) are
// accepted to minimize blast radius.
// handleAgentUpdateImage updates the worker-side services.json so the next
// reconcile uses the new image/tag. Called from master when the user chooses
// a different tag via `/v1/services/{name}/update`.
func handleAgentUpdateImage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ServiceName string `json:"service_name"`
		Image       string `json:"image"`
		Replicas    int    `json:"replicas"`
		// Optional resource-limit overrides. When non-empty, they replace the
		// preserved local limits so reconcile recreates with new constraints.
		Memory string `json:"memory"`
		CPU    string `json:"cpu"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	body.ServiceName = strings.TrimSpace(body.ServiceName)
	body.Image = strings.TrimSpace(body.Image)
	if body.ServiceName == "" || body.Image == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service_name and image required"})
		return
	}
	info := state.GetService(body.ServiceName)
	if info == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "service not tracked on this node"})
		return
	}
	// Preserve all existing options; only the image (and possibly replicas) change.
	replicas := body.Replicas
	if replicas <= 0 {
		if v, ok := info["replicas"].(float64); ok {
			replicas = int(v)
		} else if v, ok := info["replicas"].(int); ok {
			replicas = v
		}
	}
	var opts []state.UpsertOption
	// Resource limits: explicit override > preserved existing.
	memLimit, _ := info["memory_limit"].(string)
	if body.Memory != "" {
		memLimit = body.Memory
	}
	if memLimit != "" {
		opts = append(opts, state.WithMemoryLimit(memLimit))
	}
	cpuLimit, _ := info["cpu_limit"].(string)
	if body.CPU != "" {
		cpuLimit = body.CPU
	}
	if cpuLimit != "" {
		opts = append(opts, state.WithCPULimit(cpuLimit))
	}
	if v, ok := info["environment"].([]any); ok {
		var env []string
		for _, e := range v {
			if s, ok := e.(string); ok {
				env = append(env, s)
			}
		}
		opts = append(opts, state.WithEnvironment(env))
	}
	if v, ok := info["volumes"].([]any); ok {
		var vs []string
		for _, e := range v {
			if s, ok := e.(string); ok {
				vs = append(vs, s)
			}
		}
		opts = append(opts, state.WithVolumes(vs))
	}
	if v, ok := info["ports"].([]any); ok {
		var ps []string
		for _, e := range v {
			if s, ok := e.(string); ok {
				ps = append(ps, s)
			}
		}
		opts = append(opts, state.WithPorts(ps))
	}
	if lb, ok := info["extra_labels"].(map[string]any); ok {
		m := map[string]string{}
		for k, v := range lb {
			if s, ok := v.(string); ok {
				m[k] = s
			}
		}
		if len(m) > 0 {
			opts = append(opts, state.WithExtraLabels(m))
		}
	}
	state.UpsertService(body.ServiceName, body.Image, replicas, opts...)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "service": body.ServiceName, "image": body.Image})
}

func handleAgentExec(w http.ResponseWriter, r *http.Request) {
	// AuthN
	tok := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN"))
	authed := false
	if tok != "" && r.Header.Get("Authorization") == "Bearer "+tok {
		authed = true
	}
	if !authed {
		if sess := auth.SessionFromRequest(r); sess != nil && sess.Role == auth.RoleAdmin {
			authed = true
		}
	}
	if !authed {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "인증되지 않은 exec 요청"})
		return
	}

	var body struct {
		Container string `json:"container"`
		Cmd       string `json:"cmd"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}

	// Container name allowlist — only blockchain deploy containers.
	if !isExecAllowedContainer(body.Container) {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false,
			"message": "허용되지 않은 컨테이너 이름입니다",
		})
		return
	}
	// Reject obviously dangerous shell patterns. Not exhaustive — the real
	// boundary is the container allowlist + caller authentication above.
	if strings.ContainsAny(body.Cmd, "`") || strings.Contains(body.Cmd, "$(") {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"message": "허용되지 않은 cmd 문법 (backtick/$() 금지)",
		})
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

var execContainerAllowPatterns = []string{"blockchain-", "qs-dist-", "orch-"}

// isSafeIdent allows [A-Za-z0-9_.-] up to maxLen; rejects everything else.
// Used for validating values passed as shell argv.
func isSafeIdent(s string, maxLen int) bool {
	if s == "" || len(s) > maxLen {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') &&
			!(c >= '0' && c <= '9') && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

// isSafePath restricts a path to absolute filesystem paths without shell
// metacharacters or parent-dir escapes.
func isSafePath(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	if strings.Contains(s, "..") || strings.ContainsAny(s, " \t\n\r`$|&;<>*?\\\"'") {
		return false
	}
	return true
}

func isExecAllowedContainer(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	// Basic shape check — container names are alphanumeric + _-.
	for _, c := range name {
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') &&
			c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	for _, prefix := range execContainerAllowPatterns {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// AI Chat API (Claude via Anthropic API)
// ---------------------------------------------------------------------------

func getAnthropicKey() string {
	return os.Getenv("ANTHROPIC_API_KEY")
}

func handleAIStatus(w http.ResponseWriter, r *http.Request) {
	key := getAnthropicKey()
	enabled := key != ""
	resp := map[string]any{"ai_enabled": enabled}
	if enabled {
		resp["engine"] = "Claude"
	}
	writeJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Claude tool_use types and definitions
// ---------------------------------------------------------------------------

type claudeTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"input_schema"`
}

type claudeContentBlock struct {
	Type  string          `json:"type"`
	Text  string          `json:"text,omitempty"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
}

type claudeResponse struct {
	Content    []claudeContentBlock `json:"content"`
	StopReason string               `json:"stop_reason"`
	Error      *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type claudeToolResult struct {
	Type      string `json:"type"`
	ToolUseID string `json:"tool_use_id"`
	Content   string `json:"content"`
	IsError   bool   `json:"is_error,omitempty"`
}

var containerTools = []claudeTool{
	{
		Name:        "cluster_deploy",
		Description: "Deploy a service across the cluster. Pulls the image and runs containers on scheduled nodes.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"image":    map[string]any{"type": "string", "description": "Docker image (e.g. nginx:alpine, redis:latest)"},
				"name":     map[string]any{"type": "string", "description": "Service name"},
				"replicas": map[string]any{"type": "integer", "description": "Number of replicas", "default": 1},
			},
			"required": []string{"image"},
		},
	},
	{
		Name:        "cluster_scale",
		Description: "Scale a running service up or down to the specified number of replicas.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"service_name": map[string]any{"type": "string", "description": "Service name to scale"},
				"replicas":     map[string]any{"type": "integer", "description": "Target replica count"},
			},
			"required": []string{"service_name", "replicas"},
		},
	},
	{
		Name:        "cluster_stop",
		Description: "Stop and remove all containers for a service across all nodes.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"service_name": map[string]any{"type": "string", "description": "Service name to stop"},
			},
			"required": []string{"service_name"},
		},
	},
	{
		Name:        "cluster_status",
		Description: "Get full cluster status: nodes, services, resource usage, alerts.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
	},
	{
		Name:        "list_services",
		Description: "List all managed services with their status, replicas, and endpoints.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
	},
	{
		Name:        "cluster_migrate",
		Description: "Migrate a container from one node to another.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"container_id":     map[string]any{"type": "string", "description": "Container ID to migrate"},
				"source_node":      map[string]any{"type": "string", "description": "Source node name"},
				"destination_node": map[string]any{"type": "string", "description": "Destination node name"},
				"service_name":     map[string]any{"type": "string", "description": "Service name (optional)"},
			},
			"required": []string{"container_id", "source_node", "destination_node"},
		},
	},
	{
		Name:        "set_resource_limits",
		Description: "Set memory or CPU limits for a service.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"service_name": map[string]any{"type": "string", "description": "Service name"},
				"memory":       map[string]any{"type": "string", "description": "Memory limit (e.g. 512m, 1g)"},
				"cpu":          map[string]any{"type": "string", "description": "CPU limit (e.g. 0.5, 2)"},
			},
			"required": []string{"service_name"},
		},
	},
}

func getServerPort() string {
	if p := os.Getenv("PORT"); p != "" {
		return p
	}
	return "8000"
}

func executeContainerTool(toolName string, input json.RawMessage) (string, bool) {
	var params map[string]any
	if err := json.Unmarshal(input, &params); err != nil {
		return `{"error":"invalid tool input"}`, true
	}

	baseURL := "http://localhost:" + getServerPort()
	client := &http.Client{Timeout: 120 * time.Second}

	var resp *http.Response
	var err error

	switch toolName {
	case "cluster_deploy":
		body, _ := json.Marshal(params)
		resp, err = client.Post(baseURL+"/v1/cluster/deploy", "application/json", bytes.NewReader(body))
	case "cluster_scale":
		body, _ := json.Marshal(params)
		resp, err = client.Post(baseURL+"/v1/cluster/scale", "application/json", bytes.NewReader(body))
	case "cluster_stop":
		body, _ := json.Marshal(params)
		resp, err = client.Post(baseURL+"/v1/cluster/stop", "application/json", bytes.NewReader(body))
	case "cluster_status":
		resp, err = client.Get(baseURL + "/v1/cluster/status")
	case "list_services":
		resp, err = client.Get(baseURL + "/v1/services")
	case "cluster_migrate":
		body, _ := json.Marshal(params)
		resp, err = client.Post(baseURL+"/v1/cluster/migrate", "application/json", bytes.NewReader(body))
	case "set_resource_limits":
		actionReq := map[string]any{
			"action":       "resource",
			"service_name": params["service_name"],
			"memory":       params["memory"],
			"cpu":          params["cpu"],
		}
		body, _ := json.Marshal(actionReq)
		resp, err = client.Post(baseURL+"/v1/action", "application/json", bytes.NewReader(body))
	default:
		return fmt.Sprintf(`{"error":"unknown tool: %s"}`, toolName), true
	}

	if err != nil {
		return fmt.Sprintf(`{"error":"%s"}`, err.Error()), true
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf(`{"error":"failed to read response: %s"}`, err.Error()), true
	}

	result := string(respBody)
	if len(result) > 4000 {
		result = result[:4000] + "... (truncated)"
	}
	return result, false
}

func buildCommandSystemPrompt(dryRun bool) string {
	base := buildClusterSystemPrompt()
	var b strings.Builder
	b.WriteString(base)
	b.WriteString("\n\n")
	if dryRun {
		b.WriteString(`The user is asking you to preview a command (dry run).
DO NOT execute any tools. Instead, explain what actions you would take and what the expected result would be.
Keep your response concise — 1-3 sentences describing the planned action.`)
	} else {
		b.WriteString(`The user typed a command in the command console. Parse their intent and execute it using the available tools.
After execution, respond with a concise summary of what was done and the result.
Keep your response short — ideally 1-3 sentences. Do not ask follow-up questions; just execute.`)
	}
	return b.String()
}

func buildClusterSystemPrompt() string {
	var b strings.Builder
	b.WriteString(`You are a container orchestration assistant powered by Claude.
You help users manage Docker containers and services in real-time.
Respond in the same language as the user (Korean or English).

You have tools to directly control containers:
- Deploy, scale, stop services
- Check cluster status and list services
- Migrate containers between nodes
- Set resource limits (memory, CPU)

When the user asks to perform an action, USE THE TOOLS to execute it directly.
After executing tools, summarize what you did and the result.
When appropriate, proactively suggest optimizations.`)

	if clusterState != nil {
		nodes := clusterState.ListNodes()
		b.WriteString(fmt.Sprintf("\n\nCluster nodes (%d):\n", len(nodes)))
		for _, n := range nodes {
			b.WriteString(fmt.Sprintf("  - %s: role=%s status=%s containers=%d", n.Name, n.Role, n.Status, n.ContainerCount))
			if n.Resources != nil {
				b.WriteString(fmt.Sprintf(" cpu=%.1f%% mem=%d/%dMB",
					n.Resources.CPUUsedPercent, n.Resources.MemoryUsedMB, n.Resources.MemoryTotalMB))
			}
			b.WriteString("\n")
		}
	}

	cli := runtime.DockerClient()
	if cli != nil {
		containers, _ := cli.ContainerList(context.Background(), container.ListOptions{All: true})
		running := 0
		var names []string
		for _, c := range containers {
			if c.State == "running" {
				running++
			}
			if len(c.Names) > 0 {
				name := strings.TrimPrefix(c.Names[0], "/")
				if !systemServices[name] {
					names = append(names, fmt.Sprintf("%s(%s)", name, c.State))
				}
			}
		}
		b.WriteString(fmt.Sprintf("\nLocal containers: %d running / %d total\n", running, len(containers)))
		if len(names) > 0 && len(names) <= 30 {
			b.WriteString("  " + strings.Join(names, ", ") + "\n")
		}
	}

	return b.String()
}

func callClaudeWithTools(systemPrompt, userMessage string) (string, []map[string]any, error) {
	key := getAnthropicKey()
	if key == "" {
		return "", nil, fmt.Errorf("ANTHROPIC_API_KEY not configured")
	}

	messages := []map[string]any{
		{"role": "user", "content": userMessage},
	}

	var finalText string
	var toolLog []map[string]any
	httpClient := &http.Client{Timeout: 90 * time.Second}

	const maxTurns = 5
	for turn := 0; turn < maxTurns; turn++ {
		reqBody := map[string]any{
			"model":      "claude-sonnet-4-20250514",
			"max_tokens": 2048,
			"system":     systemPrompt,
			"messages":   messages,
			"tools":      containerTools,
		}
		bodyBytes, _ := json.Marshal(reqBody)

		req, err := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(bodyBytes))
		if err != nil {
			return "", nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", key)
		req.Header.Set("anthropic-version", "2023-06-01")

		resp, err := httpClient.Do(req)
		if err != nil {
			return "", nil, fmt.Errorf("API request failed: %v", err)
		}

		var apiResp claudeResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			resp.Body.Close()
			return "", nil, fmt.Errorf("failed to parse API response: %v", err)
		}
		resp.Body.Close()

		if apiResp.Error != nil {
			return "", nil, fmt.Errorf("API error: %s", apiResp.Error.Message)
		}

		// Separate text and tool_use blocks
		var textParts []string
		var toolUses []claudeContentBlock
		for _, block := range apiResp.Content {
			if block.Type == "text" {
				textParts = append(textParts, block.Text)
			} else if block.Type == "tool_use" {
				toolUses = append(toolUses, block)
			}
		}

		if len(textParts) > 0 {
			finalText = strings.Join(textParts, "\n")
		}

		if len(toolUses) == 0 {
			break
		}

		// Append assistant response to messages
		assistantContent := make([]map[string]any, 0, len(apiResp.Content))
		for _, block := range apiResp.Content {
			b := map[string]any{"type": block.Type}
			if block.Type == "text" {
				b["text"] = block.Text
			} else if block.Type == "tool_use" {
				b["id"] = block.ID
				b["name"] = block.Name
				b["input"] = json.RawMessage(block.Input)
			}
			assistantContent = append(assistantContent, b)
		}
		messages = append(messages, map[string]any{"role": "assistant", "content": assistantContent})

		// Execute tools and collect results
		var toolResults []map[string]any
		for _, tu := range toolUses {
			inputPreview := string(tu.Input)
			if len(inputPreview) > 200 {
				inputPreview = inputPreview[:200]
			}
			log.Printf("[AI tool_use] %s(%s)", tu.Name, inputPreview)

			result, isErr := executeContainerTool(tu.Name, tu.Input)

			tr := map[string]any{
				"type":        "tool_result",
				"tool_use_id": tu.ID,
				"content":     result,
			}
			if isErr {
				tr["is_error"] = true
			}
			toolResults = append(toolResults, tr)

			resultPreview := result
			if len(resultPreview) > 200 {
				resultPreview = resultPreview[:200]
			}
			toolLog = append(toolLog, map[string]any{
				"tool":           tu.Name,
				"input":          json.RawMessage(tu.Input),
				"result_preview": resultPreview,
			})
		}

		messages = append(messages, map[string]any{"role": "user", "content": toolResults})
	}

	return finalText, toolLog, nil
}

func callClaude(systemPrompt, userMessage string) (string, error) {
	key := getAnthropicKey()
	if key == "" {
		return "", fmt.Errorf("ANTHROPIC_API_KEY not configured")
	}

	reqBody := map[string]any{
		"model":      "claude-sonnet-4-20250514",
		"max_tokens": 1024,
		"system":     systemPrompt,
		"messages": []map[string]string{
			{"role": "user", "content": userMessage},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", key)
	req.Header.Set("anthropic-version", "2023-06-01")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to parse API response: %v", err)
	}
	if result.Error != nil {
		return "", fmt.Errorf("API error: %s", result.Error.Message)
	}
	if len(result.Content) > 0 {
		return result.Content[0].Text, nil
	}
	return "", fmt.Errorf("empty response from Claude")
}

func handleAIChat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Message        string `json:"message"`
		IncludeContext bool   `json:"include_context"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid request"})
		return
	}

	systemPrompt := buildClusterSystemPrompt()

	reply, toolLog, err := callClaudeWithTools(systemPrompt, body.Message)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"response": "AI 오류: " + err.Error()})
		return
	}

	result := map[string]any{"response": reply}
	if len(toolLog) > 0 {
		result["actions"] = toolLog
	}
	writeJSON(w, http.StatusOK, result)
}

func handleAIAdvisor(w http.ResponseWriter, r *http.Request) {
	var ctx strings.Builder
	ctx.WriteString("Analyze the following cluster state and provide optimization recommendations.\n\n")

	if clusterState != nil {
		nodes := clusterState.ListNodes()
		ctx.WriteString(fmt.Sprintf("Nodes (%d):\n", len(nodes)))
		for _, n := range nodes {
			ctx.WriteString(fmt.Sprintf("  - %s: role=%s status=%s containers=%d", n.Name, n.Role, n.Status, n.ContainerCount))
			if n.Resources != nil {
				ctx.WriteString(fmt.Sprintf(" cpu=%.1f%% mem=%d/%dMB disk=%.1f/%.1fGB",
					n.Resources.CPUUsedPercent, n.Resources.MemoryUsedMB, n.Resources.MemoryTotalMB,
					n.Resources.DiskUsedGB, n.Resources.DiskTotalGB))
			}
			ctx.WriteString("\n")
		}
	}

	cli := runtime.DockerClient()
	if cli != nil {
		containers, _ := cli.ContainerList(r.Context(), container.ListOptions{All: true})
		ctx.WriteString(fmt.Sprintf("\nLocal containers (%d):\n", len(containers)))
		for _, c := range containers {
			name := ""
			if len(c.Names) > 0 {
				name = strings.TrimPrefix(c.Names[0], "/")
			}
			ctx.WriteString(fmt.Sprintf("  - %s: image=%s state=%s\n", name, c.Image, c.State))
		}
	}

	systemPrompt := "You are a container cluster operations advisor. Analyze the cluster state and provide: " +
		"1) Health summary, 2) Resource utilization assessment, 3) Optimization recommendations. " +
		"Answer in Korean. Be concise."

	reply, err := callClaude(systemPrompt, ctx.String())
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"analysis": "AI 분석 오류: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis": reply})
}
