package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"ai-container-go/internal/auth"
)

// Paths that could leak secrets (container env, node tokens, images) must be
// closed to anonymous callers and to non-admin accounts.
func TestSecretExposurePathsAreClosed(t *testing.T) {
	h, _ := mcpEnv(t)
	bob, _ := auth.EnsureUserSession("bob@parametacorp.com", auth.RoleUser)
	internal, _ := auth.IssueInternalSession("alice@parametacorp.com", auth.RoleUser)

	do := func(method, path, token, bearer string) int {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(`{"node_name":"node-b","address":"6.6.6.6:8000","service_name":"x"}`))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: token})
			req.AddCookie(&http.Cookie{Name: csrfCookie, Value: "x"})
			req.Header.Set(csrfHeader, "x")
		}
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}

	// Node-to-node APIs: cluster token only (anonymous, users, and even
	// admin sessions are refused).
	for _, p := range []string{"/v1/agent/resources", "/v1/agent/logs", "/v1/agent/export/abc", "/v1/agent/upsert-env", "/v1/agent/run-one", "/v1/agent/delete-service", "/v1/cluster/heartbeat"} {
		method := "POST"
		if p == "/v1/agent/resources" {
			method = "GET"
		}
		for who, tok := range map[string]string{"anonymous": "", "user": bob.Token} {
			if c := do(method, p, tok, ""); c != http.StatusUnauthorized {
				t.Errorf("%s %s as %s: %d, want 401", method, p, who, c)
			}
		}
		if c := do(method, p, "", "wrong-token"); c != http.StatusUnauthorized {
			t.Errorf("%s %s with a wrong token: %d", method, p, c)
		}
	}
	if c := do("GET", "/v1/agent/resources", "", testBearerToken); c != http.StatusOK {
		t.Errorf("agent with cluster token: %d", c)
	}
	// A forged heartbeat must not move a node.
	do("POST", "/v1/cluster/heartbeat", "", "")
	if n := clusterState.GetNode("node-b"); n != nil && n.Address == "6.6.6.6:8000" {
		t.Fatal("unauthenticated heartbeat rewrote node-b's address")
	}

	// Cluster-wide reads are admin-only.
	for _, p := range []string{"/v1/containers", "/v1/containers/abc/inspect", "/v1/cluster/nodes", "/v1/services", "/v1/stream", "/v1/system", "/v1/images", "/v1/registry/v2/_catalog"} {
		for who, tok := range map[string]string{"user": bob.Token, "mcp": internal.Token} {
			if c := do("GET", p, tok, ""); c != http.StatusForbidden && c != http.StatusUnauthorized {
				t.Errorf("GET %s as %s: %d, want 403/401", p, who, c)
			}
		}
	}
	// What the portal and MCP need still works for users.
	for _, p := range []string{"/v1/user/services", "/v1/user/secrets", "/v1/cluster/stats", "/v1/registry/status", "/v1/public/services"} {
		if c := do("GET", p, bob.Token, ""); c == http.StatusForbidden || c == http.StatusUnauthorized {
			t.Errorf("portal read %s blocked: %d", p, c)
		}
	}
}
