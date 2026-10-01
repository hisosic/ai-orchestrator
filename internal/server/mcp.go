// Package server — MCP (Model Context Protocol) endpoint for developers.
//
// POST /mcp speaks JSON-RPC 2.0 over MCP's Streamable HTTP transport in
// stateless mode (one JSON response per request, no server-initiated
// streams). Clients authenticate with a personal API token
// (Authorization: Bearer omcp_...) issued from the portal.
//
// Authorization model — a token can only ever act on its owner's services:
//   - Every tool that names a service first checks that the service's owner
//     is exactly the token's user. Admin tokens get no exemption.
//   - Tools run by replaying the portal's own API calls in-process under a
//     short-lived session whose role is forced to "user", so the existing
//     per-handler owner checks apply a second time.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"time"

	"ai-container-go/internal/auth"

	"github.com/go-chi/chi/v5"
)

// mcpRouter is the application router; tools dispatch internal API calls
// through it so they pass the same middleware and handlers as the portal.
var mcpRouter http.Handler

var mcpProtocolVersions = []string{"2025-06-18", "2025-03-26", "2024-11-05"}

const mcpMaxBody = 1 << 20

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// mcpCaller is the authenticated principal of one MCP request.
type mcpCaller struct {
	username string
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func handleMCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "only POST is supported (stateless Streamable HTTP)"})
		return
	}
	username, _, ok := auth.ResolveAPIToken(bearerToken(r))
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="ai-orchestrator"`)
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "유효한 API 토큰이 필요합니다 (포털 → MCP 연결에서 발급)"})
		return
	}
	caller := &mcpCaller{username: username}

	raw, err := io.ReadAll(io.LimitReader(r.Body, mcpMaxBody))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{-32700, "read error"}})
		return
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '[' {
		var batch []rpcRequest
		if err := json.Unmarshal(raw, &batch); err != nil {
			writeJSON(w, http.StatusOK, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{-32700, "parse error"}})
			return
		}
		var out []rpcResponse
		for i := range batch {
			if resp := caller.dispatch(&batch[i]); resp != nil {
				out = append(out, *resp)
			}
		}
		if len(out) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	}
	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSON(w, http.StatusOK, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{-32700, "parse error"}})
		return
	}
	resp := caller.dispatch(&req)
	if resp == nil { // notification
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// dispatch handles one JSON-RPC message; nil means no response (notification).
func (c *mcpCaller) dispatch(req *rpcRequest) *rpcResponse {
	if len(req.ID) == 0 {
		return nil
	}
	resp := &rpcResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		json.Unmarshal(req.Params, &p)
		ver := mcpProtocolVersions[0]
		for _, v := range mcpProtocolVersions {
			if v == p.ProtocolVersion {
				ver = v
			}
		}
		resp.Result = map[string]any{
			"protocolVersion": ver,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "ai-orchestrator", "version": Version},
			"instructions":    mcpInstructions,
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		resp.Result = map[string]any{"tools": mcpToolList()}
	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			resp.Error = &rpcError{-32602, "invalid params"}
			return resp
		}
		tool := mcpTools[p.Name]
		if tool == nil {
			resp.Error = &rpcError{-32602, "unknown tool: " + p.Name}
			return resp
		}
		if p.Arguments == nil {
			p.Arguments = map[string]any{}
		}
		out, err := tool.run(c, toolArgs(p.Arguments))
		resp.Result = toolResult(out, err)
	default:
		resp.Error = &rpcError{-32601, "method not found: " + req.Method}
	}
	return resp
}

const mcpInstructions = `AI Container Orchestrator — deploy and debug your own services on the internal cluster.
This token can only see and change services owned by its user; requests for other users' services are refused.
Typical flow: get_registry_info (docker push your image) → deploy_image → get_service_status / get_logs / inspect_containers → exec_in_container for debugging → update_image after pushing a new tag.
Or let the cluster build for you: deploy_from_git → get_deploy_status.`

// ---------------------------------------------------------------------------
// Tool plumbing
// ---------------------------------------------------------------------------

type mcpTool struct {
	description string
	schema      map[string]any
	run         func(c *mcpCaller, a toolArgs) (any, error)
}

func mcpToolList() []map[string]any {
	names := make([]string, 0, len(mcpTools))
	for n := range mcpTools {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		t := mcpTools[n]
		out = append(out, map[string]any{"name": n, "description": t.description, "inputSchema": t.schema})
	}
	return out
}

func toolResult(out any, err error) map[string]any {
	if err != nil {
		return map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": err.Error()}}}
	}
	var text string
	if s, ok := out.(string); ok {
		text = s
	} else {
		b, _ := json.MarshalIndent(out, "", "  ")
		text = string(b)
	}
	return map[string]any{"content": []map[string]any{{"type": "text", "text": text}}}
}

type toolArgs map[string]any

func (a toolArgs) str(k string) string {
	s, _ := a[k].(string)
	return strings.TrimSpace(s)
}

func (a toolArgs) num(k string, def int) int {
	switch v := a[k].(type) {
	case float64:
		return int(v)
	case string:
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

func (a toolArgs) boolean(k string) bool {
	b, _ := a[k].(bool)
	return b
}

func (a toolArgs) require(keys ...string) error {
	for _, k := range keys {
		if a.str(k) == "" {
			return fmt.Errorf("'%s' 인자가 필요합니다", k)
		}
	}
	return nil
}

func schema(props map[string]any, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}
func intProp(desc string) map[string]any {
	return map[string]any{"type": "integer", "description": desc}
}

// ownService returns the caller's service or an error. The owner must be the
// caller exactly — no admin exemption.
func (c *mcpCaller) ownService(name string) (map[string]any, error) {
	if name == "" {
		return nil, fmt.Errorf("'name' 인자가 필요합니다")
	}
	if clusterState == nil {
		return nil, fmt.Errorf("클러스터 모드가 아닙니다")
	}
	info := clusterState.GetService(name)
	if info == nil {
		return nil, fmt.Errorf("서비스 '%s'를 찾을 수 없습니다", name)
	}
	if owner, _ := info["owner"].(string); owner != c.username {
		return nil, fmt.Errorf("'%s'는 본인(%s) 소유 서비스가 아니므로 접근할 수 없습니다", name, c.username)
	}
	return info, nil
}

// freeOrOwnName allows a deploy target name that is unused or owned by caller.
func (c *mcpCaller) freeOrOwnName(name string) error {
	if clusterState == nil {
		return fmt.Errorf("클러스터 모드가 아닙니다")
	}
	if info := clusterState.GetService(name); info != nil {
		if owner, _ := info["owner"].(string); owner != c.username {
			return fmt.Errorf("'%s' 이름은 다른 사용자의 서비스가 사용 중입니다. 다른 이름을 사용하세요", name)
		}
	}
	return nil
}

// api replays a portal API call in-process as the caller with role "user".
func (c *mcpCaller) api(method, path string, body any) (map[string]any, error) {
	if mcpRouter == nil {
		return nil, fmt.Errorf("router not ready")
	}
	sess, err := auth.IssueInternalSession(c.username, auth.RoleUser)
	if err != nil {
		return nil, err
	}
	defer auth.EndSession(sess.Token)

	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, "http://orchestrator.internal"+path, rd)
	req.RemoteAddr = "127.0.0.1:0"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	const csrf = "mcp-internal"
	req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: sess.Token})
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
	req.Header.Set(csrfHeader, csrf)

	rec := httptest.NewRecorder()
	mcpRouter.ServeHTTP(rec, req)

	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("API 응답 해석 실패 (HTTP %d): %s", rec.Code, truncate(rec.Body.String(), 300))
	}
	if rec.Code >= 400 {
		return out, fmt.Errorf("%s", apiMessage(out, rec.Code))
	}
	if s, ok := out["success"].(bool); ok && !s {
		return out, fmt.Errorf("%s", apiMessage(out, rec.Code))
	}
	return out, nil
}

func apiMessage(out map[string]any, code int) string {
	for _, k := range []string{"message", "error"} {
		if m, _ := out[k].(string); m != "" {
			return m
		}
	}
	return fmt.Sprintf("요청 실패 (HTTP %d)", code)
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

var mcpTools map[string]*mcpTool

func init() {
	mcpTools = map[string]*mcpTool{
		"whoami": {
			description: "Show the account this token acts as and the cluster's registry endpoint.",
			schema:      schema(map[string]any{}),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				reg := getRegistryInfo(ctx)
				return map[string]any{"username": c.username, "scope": "own services only", "registry": reg["endpoint"]}, nil
			},
		},
		"list_my_services": {
			description: "List services owned by you with status, replicas, image and public URL.",
			schema:      schema(map[string]any{}),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				out, err := c.api("GET", "/v1/user/services", nil)
				if err != nil {
					return nil, err
				}
				return c.filterOwned(out["services"]), nil
			},
		},
		"get_service_status": {
			description: "Status of one of your services: desired/running replicas, image, URL, per-replica container state and CPU/memory.",
			schema:      schema(map[string]any{"name": strProp("service name")}, "name"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				if _, err := c.ownService(name); err != nil {
					return nil, err
				}
				res := map[string]any{"name": name}
				if out, err := c.api("GET", "/v1/user/services", nil); err == nil {
					for _, s := range c.filterOwned(out["services"]) {
						if s["name"] == name {
							res["service"] = s
						}
					}
				}
				if out, err := c.api("GET", "/v1/cluster/inspect?service="+url.QueryEscape(name), nil); err == nil {
					res["replicas"] = out["replicas"]
				}
				if out, err := c.api("GET", "/v1/cluster/stats", nil); err == nil {
					if st, ok := out["stats"].(map[string]any); ok {
						res["stats"] = st[name]
					}
				}
				return res, nil
			},
		},
		"get_registry_info": {
			description: "Internal Docker registry address plus the exact commands to build (linux/amd64) and docker push an image from your machine. Pushing must happen from the company network/VPN.",
			schema:      schema(map[string]any{"image_name": strProp("repository name to push, e.g. myapp (default: <username>-app)"), "tag": strProp("tag (default: latest)")}),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				reg := getRegistryInfo(ctx)
				ep, _ := reg["endpoint"].(string)
				if running, _ := reg["running"].(bool); !running || ep == "" {
					return nil, fmt.Errorf("내장 registry가 실행 중이 아닙니다. 관리자에게 문의하세요")
				}
				repo := strings.ToLower(a.str("image_name"))
				if repo == "" {
					repo = safeRepoPrefix(c.username) + "-app"
				}
				tag := a.str("tag")
				if tag == "" {
					tag = "latest"
				}
				ref := ep + "/" + repo + ":" + tag
				return map[string]any{
					"registry":       ep,
					"image_ref":      ref,
					"one_time_setup": fmt.Sprintf(`Docker daemon must allow the HTTP registry: add {"insecure-registries": ["%s"]} to Docker Desktop → Settings → Docker Engine (or /etc/docker/daemon.json, then restart docker).`, ep),
					"commands": []string{
						"docker build --platform linux/amd64 -t " + ref + " .",
						"docker push " + ref,
					},
					"next":   fmt.Sprintf("deploy_image with image=%q (new service) or update_image (existing service).", ref),
					"naming": "Prefix image names with your account (e.g. " + safeRepoPrefix(c.username) + "-myapp) so they never collide with other users' images. Never bake secrets into images; use secret_refs.",
				}, nil
			},
		},
		"list_registry_images": {
			description: "List image references stored in the internal registry.",
			schema:      schema(map[string]any{}),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				out, err := c.api("GET", "/v1/user/registry-images", nil)
				if err != nil {
					return nil, err
				}
				return out["images"], nil
			},
		},
		"import_image": {
			description: "Pull a public/external image (e.g. nginx:alpine, ghcr.io/org/app:v1) and store it in the internal registry. Returns the internal reference to deploy.",
			schema: schema(map[string]any{
				"source_image": strProp("image to pull"),
				"name":         strProp("registry repository name to save as (default: derived from the image)"),
			}, "source_image"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				if err := a.require("source_image"); err != nil {
					return nil, err
				}
				return c.api("POST", "/v1/user/image-import", map[string]any{"image": a.str("source_image"), "name": a.str("name")})
			},
		},
		"list_my_secrets": {
			description: "List your stored secrets (names and ids only, never values). Use the ids in deploy_image secret_refs.",
			schema:      schema(map[string]any{}),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				out, err := c.api("GET", "/v1/user/secrets", nil)
				if err != nil {
					return nil, err
				}
				return out["secrets"], nil
			},
		},
		"deploy_image": {
			description: "Deploy a container image as a new service you own, or redeploy one of your services (replaces its containers). Use internal registry refs from get_registry_info / import_image.",
			schema: schema(map[string]any{
				"name":     strProp("service name (letters, digits, -)"),
				"image":    strProp("image reference, e.g. 20.20.6.248:5000/me-app:v1"),
				"replicas": intProp("replica count (default 1, max 10)"),
				"env":      map[string]any{"type": "object", "description": "environment variables as {KEY: value}", "additionalProperties": map[string]any{"type": "string"}},
				"secret_refs": map[string]any{"type": "array", "description": "inject stored secrets: [{secret_id, env_name}]", "items": map[string]any{
					"type": "object", "properties": map[string]any{"secret_id": strProp("id from list_my_secrets"), "env_name": strProp("env var name")}, "required": []string{"secret_id"},
				}},
				"memory":    strProp("memory limit, e.g. 512m"),
				"cpu":       strProp("CPU limit, e.g. 0.5"),
				"subdomain": map[string]any{"type": "boolean", "description": "route as <name>.<base-domain> instead of path routing"},
			}, "name", "image"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				if err := a.require("name", "image"); err != nil {
					return nil, err
				}
				name := a.str("name")
				if err := c.freeOrOwnName(name); err != nil {
					return nil, err
				}
				replicas := a.num("replicas", 1)
				if replicas < 1 || replicas > 10 {
					return nil, fmt.Errorf("replicas는 1~10 사이여야 합니다")
				}
				body := map[string]any{"name": name, "image": a.str("image"), "replicas": replicas, "subdomain": a.boolean("subdomain")}
				if env, ok := a["env"].(map[string]any); ok && len(env) > 0 {
					keys := make([]string, 0, len(env))
					for k := range env {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					lines := make([]string, 0, len(keys))
					for _, k := range keys {
						lines = append(lines, fmt.Sprintf("%s=%v", k, env[k]))
					}
					body["environment"] = lines
				}
				if refs, ok := a["secret_refs"].([]any); ok && len(refs) > 0 {
					body["secret_refs"] = refs
				}
				if m := a.str("memory"); m != "" {
					body["memory"] = m
				}
				if cpu := a.str("cpu"); cpu != "" {
					body["cpu"] = cpu
				}
				return c.api("POST", "/v1/cluster/deploy", body)
			},
		},
		"update_image": {
			description: "Roll one of your services to a new image tag, or re-pull the current image when image is omitted (after pushing the same tag again).",
			schema:      schema(map[string]any{"name": strProp("service name"), "image": strProp("new image reference (optional)")}, "name"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				if _, err := c.ownService(name); err != nil {
					return nil, err
				}
				body := map[string]any{}
				if img := a.str("image"); img != "" {
					body["image"] = img
				}
				return c.api("POST", "/v1/services/"+url.PathEscape(name)+"/update", body)
			},
		},
		"scale_service": {
			description: "Set the replica count of one of your services (0 stops it).",
			schema:      schema(map[string]any{"name": strProp("service name"), "replicas": intProp("0-10")}, "name", "replicas"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				if _, err := c.ownService(name); err != nil {
					return nil, err
				}
				n := a.num("replicas", -1)
				if n < 0 || n > 10 {
					return nil, fmt.Errorf("replicas는 0~10 사이여야 합니다")
				}
				return c.api("POST", "/v1/cluster/scale", map[string]any{"service_name": name, "replicas": n})
			},
		},
		"restart_service": {
			description: "Restart one of your services (stop all replicas, then start the same count).",
			schema:      schema(map[string]any{"name": strProp("service name")}, "name"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				info, err := c.ownService(name)
				if err != nil {
					return nil, err
				}
				n := 1
				if v, ok := info["desired_replicas"].(int); ok && v > 0 {
					n = v
				}
				if _, err := c.api("POST", "/v1/cluster/scale", map[string]any{"service_name": name, "replicas": 0}); err != nil {
					return nil, fmt.Errorf("중지 실패: %v", err)
				}
				time.Sleep(2 * time.Second)
				return c.api("POST", "/v1/cluster/scale", map[string]any{"service_name": name, "replicas": n})
			},
		},
		"delete_service": {
			description: "Permanently delete one of your services and its containers. Requires confirm=true.",
			schema:      schema(map[string]any{"name": strProp("service name"), "confirm": map[string]any{"type": "boolean", "description": "must be true"}}, "name", "confirm"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				if _, err := c.ownService(name); err != nil {
					return nil, err
				}
				if !a.boolean("confirm") {
					return nil, fmt.Errorf("삭제하려면 confirm=true가 필요합니다")
				}
				return c.api("POST", "/v1/cluster/delete", map[string]any{"service_name": name})
			},
		},
		"deploy_from_git": {
			description: "Clone an HTTPS git repository, build it on the cluster (Dockerfile detected or generated), push to the registry and deploy as your service. Returns deploy_id; poll get_deploy_status.",
			schema: schema(map[string]any{
				"git_url":   strProp("https URL of the repository"),
				"branch":    strProp("branch (optional)"),
				"name":      strProp("service name (default: from repo name)"),
				"secret_id": strProp("id of a stored secret holding a token for private repos (optional)"),
			}, "git_url"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				if err := a.require("git_url"); err != nil {
					return nil, err
				}
				if name := a.str("name"); name != "" {
					if err := c.freeOrOwnName(sanitizeServiceName(name)); err != nil {
						return nil, err
					}
				}
				body := map[string]any{"git_url": a.str("git_url")}
				for _, k := range []string{"branch", "name", "secret_id"} {
					if v := a.str(k); v != "" {
						body[k] = v
					}
				}
				return c.api("POST", "/v1/services/deploy-git", body)
			},
		},
		"get_deploy_status": {
			description: "Progress and result of a build/deploy job (from deploy_from_git). Optionally waits up to wait_seconds (max 90) for it to finish. Includes recent build log lines.",
			schema:      schema(map[string]any{"deploy_id": strProp("id returned by deploy_from_git"), "wait_seconds": intProp("0-90")}, "deploy_id"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				if err := a.require("deploy_id"); err != nil {
					return nil, err
				}
				job := getDeployJob(a.str("deploy_id"))
				if job == nil {
					return nil, fmt.Errorf("deploy job을 찾을 수 없습니다 (완료 후 일정 시간이 지나면 정리됩니다)")
				}
				if job.Owner != c.username {
					return nil, fmt.Errorf("본인의 배포 작업이 아닙니다")
				}
				wait := a.num("wait_seconds", 0)
				if wait > 90 {
					wait = 90
				}
				deadline := time.Now().Add(time.Duration(wait) * time.Second)
				for {
					job.mu.Lock()
					done := job.done
					job.mu.Unlock()
					if done || time.Now().After(deadline) {
						break
					}
					time.Sleep(2 * time.Second)
				}
				return deployJobSummary(job), nil
			},
		},
		"get_logs": {
			description: "Recent stdout/stderr of all replicas of one of your services, across nodes.",
			schema:      schema(map[string]any{"name": strProp("service name"), "tail": intProp("lines per replica (default 200, max 2000)")}, "name"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				if _, err := c.ownService(name); err != nil {
					return nil, err
				}
				tail := a.num("tail", 200)
				if tail < 1 || tail > 2000 {
					tail = 200
				}
				out, err := c.api("GET", fmt.Sprintf("/v1/cluster/logs?service=%s&tail=%d", url.QueryEscape(name), tail), nil)
				if err != nil {
					return nil, err
				}
				logs, _ := out["logs"].(string)
				return logs, nil
			},
		},
		"inspect_containers": {
			description: "Per-replica container details for one of your services: node, state, exit code, OOM kill, restart count, health check output, ports.",
			schema:      schema(map[string]any{"name": strProp("service name")}, "name"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				if _, err := c.ownService(name); err != nil {
					return nil, err
				}
				out, err := c.api("GET", "/v1/cluster/inspect?service="+url.QueryEscape(name), nil)
				if err != nil {
					return nil, err
				}
				return out["replicas"], nil
			},
		},
		"exec_in_container": {
			description: "Run a shell command (sh -c) inside a running container of one of your services for debugging, e.g. 'env', 'ls /app', 'wget -qO- localhost:8080/health'. Output is capped at 64KB; timeout default 30s, max 120s. Every call is audited.",
			schema: schema(map[string]any{
				"name":        strProp("service name"),
				"command":     strProp("shell command"),
				"container":   strProp("specific container name from inspect_containers (default: first running replica)"),
				"timeout_sec": intProp("1-120"),
			}, "name", "command"),
			run: func(c *mcpCaller, a toolArgs) (any, error) {
				name := a.str("name")
				if _, err := c.ownService(name); err != nil {
					return nil, err
				}
				if err := a.require("command"); err != nil {
					return nil, err
				}
				out, err := c.api("POST", "/v1/cluster/exec", map[string]any{
					"service_name": name, "command": a.str("command"),
					"container": a.str("container"), "timeout_sec": a.num("timeout_sec", 0),
				})
				if err != nil && out == nil {
					return nil, err
				}
				// A non-zero exit is a normal debugging result, not a tool failure.
				return out, nil
			},
		},
	}
}

// filterOwned keeps only services whose owner is the caller (the user-scope
// listing already does this; this is the MCP-side guard).
func (c *mcpCaller) filterOwned(v any) []map[string]any {
	list, _ := v.([]any)
	out := []map[string]any{}
	for _, it := range list {
		s, ok := it.(map[string]any)
		if !ok {
			continue
		}
		if owner, _ := s["owner"].(string); owner != c.username {
			continue
		}
		delete(s, "env_vars")
		out = append(out, s)
	}
	return out
}

func deployJobSummary(job *DeployJob) map[string]any {
	job.mu.Lock()
	defer job.mu.Unlock()
	var lastPhase, lastMsg string
	var tail []string
	for _, e := range job.history {
		switch e.Type {
		case "phase", "error":
			lastPhase, lastMsg = e.Phase, e.Message
			tail = append(tail, "["+e.Type+"] "+e.Message)
		case "log":
			line := e.Line
			if line == "" {
				line = e.Message
			}
			tail = append(tail, line)
		}
	}
	if len(tail) > 60 {
		tail = tail[len(tail)-60:]
	}
	return map[string]any{
		"deploy_id":    job.ID,
		"service":      job.ServiceName,
		"done":         job.done,
		"last_phase":   lastPhase,
		"last_message": lastMsg,
		"result":       job.finalResult,
		"recent_log":   tail,
	}
}

func safeRepoPrefix(username string) string {
	if i := strings.IndexByte(username, '@'); i > 0 {
		username = username[:i]
	}
	return strings.Trim(sanitizeServiceName(strings.ToLower(username)), "-")
}

// ---------------------------------------------------------------------------
// Token management (portal, browser session)
// ---------------------------------------------------------------------------

// handleListAPITokens: GET /v1/user/api-tokens
func handleListAPITokens(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "tokens": auth.ListAPITokens(sess.Username)})
}

// handleCreateAPIToken: POST /v1/user/api-tokens {name} → plaintext once.
func handleCreateAPIToken(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	readJSON(r, &body)
	plain, meta, err := auth.CreateAPIToken(sess.Username, body.Name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	auditRequest(r, "POST /v1/user/api-tokens", meta.ID, "created:"+meta.Name)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "token": plain, "id": meta.ID, "name": meta.Name})
}

// handleRevokeAPIToken: DELETE /v1/user/api-tokens/{id}
func handleRevokeAPIToken(w http.ResponseWriter, r *http.Request) {
	sess := userSecretCaller(w, r)
	if sess == nil {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if err := auth.RevokeAPIToken(sess.Username, id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": err.Error()})
		return
	}
	auditRequest(r, "DELETE /v1/user/api-tokens/"+id, id, "revoked")
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}
