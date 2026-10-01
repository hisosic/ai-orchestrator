// Package server — owner-scoped container debugging (inspect + exec).
//
// Master endpoints (session-authenticated, owner-checked):
//
//	GET  /v1/cluster/inspect?service=NAME  → per-replica state across nodes
//	POST /v1/cluster/exec {service_name, command, container?, timeout_sec?}
//
// Each fans out to the node agents, which only accept the cluster token and
// only touch containers named orch-<service>-*, so a caller can never reach a
// container outside the service it was authorized for.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/models"
	"ai-container-go/internal/runtime"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	execDefaultTimeout = 30 * time.Second
	execMaxTimeout     = 120 * time.Second
	execMaxOutput      = 64 << 10
)

// agentTokenOK reports whether an agent request carries the cluster token.
// /v1/agent/* bypasses session auth, so endpoints that act on containers must
// check this themselves.
func agentTokenOK(r *http.Request) bool {
	tok := strings.TrimSpace(os.Getenv("ORCHESTRATOR_API_TOKEN"))
	return tok != "" && r.Header.Get("Authorization") == "Bearer "+tok
}

// serviceContainerPrefix is the naming convention for a service's replicas.
func serviceContainerPrefix(service string) string { return "orch-" + service + "-" }

// lookupManagedService loads a service and enforces the owner check, writing
// the error response itself. Returns false when the caller must stop.
func lookupManagedService(w http.ResponseWriter, r *http.Request, service string) bool {
	// canManageService treats a missing session as the inter-node path, and
	// GETs reach handlers anonymously, so require a session explicitly.
	if auth.SessionFromRequest(r) == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "로그인이 필요합니다", "code": "unauthenticated"})
		return false
	}
	if service == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service가 필요합니다"})
		return false
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다"})
		return false
	}
	info := clusterState.GetService(service)
	if info == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "서비스를 찾을 수 없습니다: " + service})
		return false
	}
	if ok, owner := canManageService(r, info); !ok {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false,
			"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 접근할 수 있습니다", service, owner),
			"code":    "forbidden_owner",
		})
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Inspect
// ---------------------------------------------------------------------------

// replicaInfo is one container's debugging summary.
type replicaInfo struct {
	Node         string `json:"node"`
	Container    string `json:"container"`
	Image        string `json:"image"`
	Status       string `json:"status"`
	Running      bool   `json:"running"`
	RestartCount int    `json:"restart_count"`
	ExitCode     int    `json:"exit_code"`
	OOMKilled    bool   `json:"oom_killed"`
	Error        string `json:"error,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
	FinishedAt   string `json:"finished_at,omitempty"`
	Health       string `json:"health,omitempty"`
	HealthLog    string `json:"health_log,omitempty"`
	Ports        string `json:"ports,omitempty"`
}

func handleClusterInspect(w http.ResponseWriter, r *http.Request) {
	service := strings.TrimSpace(r.URL.Query().Get("service"))
	if !lookupManagedService(w, r, service) {
		return
	}
	replicas := clusterReplicas(service)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "service": service, "replicas": replicas})
}

// clusterReplicas collects replica summaries for a service from every
// reachable node, querying nodes in parallel so one slow node can't stall it.
func clusterReplicas(service string) []replicaInfo {
	payload, _ := json.Marshal(map[string]any{"service_name": service})
	client := &http.Client{Timeout: 10 * time.Second}
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out = []replicaInfo{}
	)
	for _, node := range clusterState.ListNodes() {
		if node.Status == models.NodeOffline {
			continue
		}
		wg.Add(1)
		go func(node models.NodeInfo) {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, nodeBaseURL(&node)+"/v1/agent/service-inspect", bytes.NewReader(payload))
			setNodeHeaders(req, &node)
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var d struct {
				Replicas []replicaInfo `json:"replicas"`
			}
			json.NewDecoder(resp.Body).Decode(&d)
			mu.Lock()
			for _, rep := range d.Replicas {
				rep.Node = node.Name
				out = append(out, rep)
			}
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].Node+out[i].Container < out[j].Node+out[j].Container })
	return out
}

func handleAgentServiceInspect(w http.ResponseWriter, r *http.Request) {
	if !agentTokenOK(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "Unauthorized"})
		return
	}
	var body struct {
		ServiceName string `json:"service_name"`
	}
	if err := readJSON(r, &body); err != nil || strings.TrimSpace(body.ServiceName) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service_name required"})
		return
	}
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	prefix := serviceContainerPrefix(strings.TrimSpace(body.ServiceName))
	list, _ := cli.ContainerList(ctx, container.ListOptions{All: true})
	out := []replicaInfo{}
	for _, c := range list {
		name := containerNameWithPrefix(c.Names, prefix)
		if name == "" {
			continue
		}
		ri := replicaInfo{Container: name, Image: c.Image, Status: c.Status}
		var ports []string
		for _, p := range c.Ports {
			if p.PublicPort > 0 {
				ports = append(ports, fmt.Sprintf("%d->%d/%s", p.PublicPort, p.PrivatePort, p.Type))
			}
		}
		ri.Ports = strings.Join(ports, ", ")
		if ins, err := cli.ContainerInspect(ctx, c.ID); err == nil && ins.State != nil {
			ri.Running = ins.State.Running
			ri.ExitCode = ins.State.ExitCode
			ri.OOMKilled = ins.State.OOMKilled
			ri.Error = ins.State.Error
			ri.StartedAt = ins.State.StartedAt
			ri.FinishedAt = ins.State.FinishedAt
			ri.RestartCount = ins.RestartCount
			if h := ins.State.Health; h != nil {
				ri.Health = h.Status
				if n := len(h.Log); n > 0 {
					ri.HealthLog = truncate(strings.TrimSpace(h.Log[n-1].Output), 500)
				}
			}
		}
		out = append(out, ri)
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "replicas": out})
}

func containerNameWithPrefix(names []string, prefix string) string {
	for _, n := range names {
		if nm := strings.TrimPrefix(n, "/"); strings.HasPrefix(nm, prefix) {
			return nm
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(truncated)"
}

// ---------------------------------------------------------------------------
// Exec
// ---------------------------------------------------------------------------

type execRequest struct {
	ServiceName string `json:"service_name"`
	Container   string `json:"container"`
	Command     string `json:"command"`
	TimeoutSec  int    `json:"timeout_sec"`
}

func (e *execRequest) timeout() time.Duration {
	t := time.Duration(e.TimeoutSec) * time.Second
	if t <= 0 {
		return execDefaultTimeout
	}
	if t > execMaxTimeout {
		return execMaxTimeout
	}
	return t
}

func handleClusterExec(w http.ResponseWriter, r *http.Request) {
	var body execRequest
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	body.ServiceName = strings.TrimSpace(body.ServiceName)
	body.Command = strings.TrimSpace(body.Command)
	if !lookupManagedService(w, r, body.ServiceName) {
		return
	}
	if body.Command == "" || len(body.Command) > 4096 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "command가 비어 있거나 너무 깁니다 (최대 4096자)"})
		return
	}
	// Pick the target replica: the requested one, else the first running.
	var target *replicaInfo
	replicas := clusterReplicas(body.ServiceName)
	for i := range replicas {
		rep := &replicas[i]
		if body.Container != "" && rep.Container != body.Container {
			continue
		}
		if rep.Running {
			target = rep
			break
		}
	}
	if target == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "실행 중인 대상 컨테이너가 없습니다", "replicas": replicas})
		return
	}
	node := clusterState.GetNode(target.Node)
	if node == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "노드를 찾을 수 없습니다: " + target.Node})
		return
	}
	auditRequest(r, "exec "+body.ServiceName, target.Container, "cmd:"+truncate(body.Command, 200))

	body.Container = target.Container
	payload, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, nodeBaseURL(node)+"/v1/agent/service-exec", bytes.NewReader(payload))
	setNodeHeaders(req, node)
	client := &http.Client{Timeout: body.timeout() + 15*time.Second}
	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "노드 exec 호출 실패: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	var d map[string]any
	json.NewDecoder(resp.Body).Decode(&d)
	if d == nil {
		d = map[string]any{"success": false, "message": "노드 응답 파싱 실패"}
	}
	d["node"] = target.Node
	d["container"] = target.Container
	writeJSON(w, http.StatusOK, d)
}

func handleAgentServiceExec(w http.ResponseWriter, r *http.Request) {
	if !agentTokenOK(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "Unauthorized"})
		return
	}
	var body execRequest
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	svc := strings.TrimSpace(body.ServiceName)
	// The container must belong to the service the master authorized.
	if svc == "" || !strings.HasPrefix(body.Container, serviceContainerPrefix(svc)) || strings.TrimSpace(body.Command) == "" {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "message": "허용되지 않은 컨테이너입니다"})
		return
	}
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}
	timeout := body.timeout()
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	ex, err := cli.ContainerExecCreate(ctx, body.Container, container.ExecOptions{
		AttachStdout: true, AttachStderr: true,
		Cmd: []string{"sh", "-c", body.Command},
	})
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "exec 생성 실패: " + err.Error()})
		return
	}
	att, err := cli.ContainerExecAttach(ctx, ex.ID, container.ExecAttachOptions{})
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "exec 연결 실패: " + err.Error()})
		return
	}
	defer att.Close()
	stdout := &cappedBuffer{max: execMaxOutput}
	stderr := &cappedBuffer{max: execMaxOutput}
	done := make(chan error, 1)
	go func() { _, err := stdcopy.StdCopy(stdout, stderr, att.Reader); done <- err }()
	timedOut := false
	select {
	case <-done:
	case <-ctx.Done():
		timedOut = true
	}
	exitCode := -1
	if ins, err := cli.ContainerExecInspect(context.Background(), ex.ID); err == nil && !ins.Running {
		exitCode = ins.ExitCode
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":   !timedOut && exitCode == 0,
		"exit_code": exitCode,
		"timed_out": timedOut,
		"stdout":    stdout.String(),
		"stderr":    stderr.String(),
		"truncated": stdout.truncated || stderr.truncated,
	})
}

// cappedBuffer keeps the first max bytes written and drops the rest.
type cappedBuffer struct {
	bytes.Buffer
	max       int
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.Len(); room < len(p) {
		b.truncated = true
		if room > 0 {
			b.Buffer.Write(p[:room])
		}
		return len(p), nil
	}
	return b.Buffer.Write(p)
}
