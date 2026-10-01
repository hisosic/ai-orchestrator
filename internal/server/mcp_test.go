package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-container-go/internal/auth"
)

// mcpEnv starts a router with two users: alice (admin) owns "alice-web",
// bob (user) owns "bob-api". Returns the router and alice's API token.
func mcpEnv(t *testing.T) (http.Handler, string) {
	t.Helper()
	// Register the master at a port that refuses connections instantly, so
	// node fan-out in tools doesn't wait on network timeouts.
	t.Setenv("ORCHESTRATOR_ADVERTISE_ADDR", "127.0.0.1:1")
	h := setupTestServer(t)
	if clusterState == nil {
		t.Skip("cluster state unavailable in this environment")
	}
	for name, role := range map[string]auth.Role{"alice@parametacorp.com": auth.RoleAdmin, "bob@parametacorp.com": auth.RoleUser} {
		if _, err := auth.EnsureUserSession(name, role); err != nil {
			t.Fatal(err)
		}
	}
	clusterState.SaveService("alice-web", "reg:5000/alice-web:v1", 1, map[string]any{"owner": "alice@parametacorp.com"})
	clusterState.SaveService("bob-api", "reg:5000/bob-api:v1", 1, map[string]any{"owner": "bob@parametacorp.com"})
	tok, _, err := auth.CreateAPIToken("alice@parametacorp.com", "test")
	if err != nil {
		t.Fatal(err)
	}
	return h, tok
}

func mcpCall(t *testing.T, h http.Handler, token, method string, params any) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var out map[string]any
	json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

// toolText returns the text content and isError flag of a tools/call result.
func toolText(t *testing.T, resp map[string]any) (string, bool) {
	t.Helper()
	res, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("no result: %v", resp)
	}
	content := res["content"].([]any)
	isErr, _ := res["isError"].(bool)
	return content[0].(map[string]any)["text"].(string), isErr
}

func TestMCPRequiresToken(t *testing.T) {
	h, _ := mcpEnv(t)
	for _, tok := range []string{"", "omcp_wrong", testBearerToken} {
		if code, _ := mcpCall(t, h, tok, "tools/list", nil); code != http.StatusUnauthorized {
			t.Errorf("token %q: status %d, want 401", tok, code)
		}
	}
}

func TestMCPInitializeAndList(t *testing.T) {
	h, tok := mcpEnv(t)
	code, out := mcpCall(t, h, tok, "initialize", map[string]any{"protocolVersion": "2025-03-26"})
	if code != 200 || out["result"].(map[string]any)["protocolVersion"] != "2025-03-26" {
		t.Fatalf("initialize: %d %v", code, out)
	}
	_, out = mcpCall(t, h, tok, "tools/list", nil)
	tools := out["result"].(map[string]any)["tools"].([]any)
	have := map[string]bool{}
	for _, x := range tools {
		have[x.(map[string]any)["name"].(string)] = true
	}
	for _, n := range []string{"deploy_image", "update_image", "get_logs", "exec_in_container", "get_registry_info", "deploy_from_git"} {
		if !have[n] {
			t.Errorf("missing tool %s", n)
		}
	}
}

// An admin's token must still be confined to the admin's own services.
func TestMCPRefusesOtherUsersServices(t *testing.T) {
	h, tok := mcpEnv(t)
	calls := []map[string]any{
		{"name": "get_logs", "arguments": map[string]any{"name": "bob-api"}},
		{"name": "get_service_status", "arguments": map[string]any{"name": "bob-api"}},
		{"name": "inspect_containers", "arguments": map[string]any{"name": "bob-api"}},
		{"name": "exec_in_container", "arguments": map[string]any{"name": "bob-api", "command": "id"}},
		{"name": "update_image", "arguments": map[string]any{"name": "bob-api", "image": "evil:1"}},
		{"name": "scale_service", "arguments": map[string]any{"name": "bob-api", "replicas": 0}},
		{"name": "restart_service", "arguments": map[string]any{"name": "bob-api"}},
		{"name": "delete_service", "arguments": map[string]any{"name": "bob-api", "confirm": true}},
		{"name": "deploy_image", "arguments": map[string]any{"name": "bob-api", "image": "evil:1"}},
		{"name": "deploy_from_git", "arguments": map[string]any{"git_url": "https://github.com/x/y", "name": "bob-api"}},
	}
	for _, p := range calls {
		_, out := mcpCall(t, h, tok, "tools/call", p)
		text, isErr := toolText(t, out)
		if !isErr {
			t.Errorf("%s on bob-api succeeded: %s", p["name"], text)
		}
	}
	if info := clusterState.GetService("bob-api"); info == nil || info["owner"] != "bob@parametacorp.com" || info["image"] != "reg:5000/bob-api:v1" {
		t.Fatalf("bob-api was modified: %v", info)
	}
}

func TestMCPListShowsOnlyOwnServices(t *testing.T) {
	h, tok := mcpEnv(t)
	_, out := mcpCall(t, h, tok, "tools/call", map[string]any{"name": "list_my_services"})
	text, isErr := toolText(t, out)
	if isErr || strings.Contains(text, "bob-api") || !strings.Contains(text, "alice-web") {
		t.Fatalf("list_my_services = %s (isErr=%v)", text, isErr)
	}
}

// The portal API itself must refuse to redeploy over someone else's service.
func TestClusterDeployRejectsForeignService(t *testing.T) {
	h, _ := mcpEnv(t)
	sess, err := auth.IssueInternalSession("alice@parametacorp.com", auth.RoleUser)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.NewBufferString(`{"name":"bob-api","image":"evil:1"}`)
	req := httptest.NewRequest("POST", "/v1/cluster/deploy", body)
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: sess.Token})
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: "x"})
	req.Header.Set(csrfHeader, "x")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403: %s", w.Code, w.Body.String())
	}
}

func TestRevokedTokenStopsWorking(t *testing.T) {
	h, tok := mcpEnv(t)
	list := auth.ListAPITokens("alice@parametacorp.com")
	for _, x := range list {
		if err := auth.RevokeAPIToken("alice@parametacorp.com", x.ID); err != nil {
			t.Fatal(err)
		}
	}
	if code, _ := mcpCall(t, h, tok, "tools/list", nil); code != http.StatusUnauthorized {
		t.Fatalf("revoked token accepted: %d", code)
	}
}

func TestPasswordLoginDisabledByDefault(t *testing.T) {
	h := setupTestServer(t)
	req := httptest.NewRequest("POST", "/v1/auth/login", bytes.NewBufferString(`{"username":"admin","password":"admin"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "password_login_disabled") {
		t.Fatalf("login: %d %s", w.Code, w.Body.String())
	}
}

// Own-service tools must reach the real handlers through the in-process call.
func TestMCPOwnServiceToolsReachHandlers(t *testing.T) {
	h, tok := mcpEnv(t)
	for _, p := range []map[string]any{
		{"name": "get_service_status", "arguments": map[string]any{"name": "alice-web"}},
		{"name": "inspect_containers", "arguments": map[string]any{"name": "alice-web"}},
		{"name": "get_logs", "arguments": map[string]any{"name": "alice-web", "tail": 10}},
		{"name": "whoami"},
	} {
		_, out := mcpCall(t, h, tok, "tools/call", p)
		if text, isErr := toolText(t, out); isErr {
			t.Errorf("%s failed: %s", p["name"], text)
		}
	}
}
