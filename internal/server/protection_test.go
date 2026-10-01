package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/models"
)

func postAs(h http.Handler, path, body, sessionToken string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if sessionToken != "" {
		req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: sessionToken})
		req.AddCookie(&http.Cookie{Name: csrfCookie, Value: "x"})
		req.Header.Set(csrfHeader, "x")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestDeleteProtection(t *testing.T) {
	h, tok := mcpEnv(t) // alice (admin) owns alice-web, bob (user) owns bob-api
	bob, _ := auth.EnsureUserSession("bob@parametacorp.com", auth.RoleUser)
	alice, _ := auth.EnsureUserSession("alice@parametacorp.com", auth.RoleAdmin)

	// Only the owner (or an admin) may toggle it.
	carol, _ := auth.EnsureUserSession("carol@parametacorp.com", auth.RoleUser)
	if w := postAs(h, "/v1/services/bob-api/delete-protection", `{"enabled":true}`, carol.Token); w.Code != http.StatusForbidden {
		t.Fatalf("non-owner toggle: %d %s", w.Code, w.Body.String())
	}
	if w := postAs(h, "/v1/services/bob-api/delete-protection", `{"enabled":true}`, bob.Token); w.Code != http.StatusOK {
		t.Fatalf("owner toggle: %d %s", w.Code, w.Body.String())
	}
	if !clusterState.IsDeleteProtected("bob-api") {
		t.Fatal("flag not stored")
	}

	// Nobody can delete it now — not the owner, not an admin.
	for name, s := range map[string]string{"owner": bob.Token, "admin": alice.Token} {
		w := postAs(h, "/v1/cluster/delete", `{"service_name":"bob-api"}`, s)
		if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "delete_protected") {
			t.Errorf("%s delete: %d %s", name, w.Code, w.Body.String())
		}
	}
	// Removing one of its containers by name is refused too.
	req := httptest.NewRequest("DELETE", "/v1/cluster/container/orch-bob-api-0?node=master", nil)
	req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: alice.Token})
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: "x"})
	req.Header.Set(csrfHeader, "x")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("container delete: %d %s", w.Code, w.Body.String())
	}
	// Backstop: the state layer itself refuses.
	if clusterState.DeleteService("bob-api") || clusterState.GetService("bob-api") == nil {
		t.Fatal("DeleteService removed a protected service")
	}
	// Redeploys keep the flag.
	clusterState.SaveService("bob-api", "reg:5000/bob-api:v2", 1, map[string]any{"owner": "bob@parametacorp.com"})
	if !clusterState.IsDeleteProtected("bob-api") {
		t.Fatal("redeploy cleared protection")
	}

	// MCP: delete refused, and API-token sessions cannot switch protection off.
	clusterState.SetDeleteProtection("alice-web", true)
	_, out := mcpCall(t, h, tok, "tools/call", map[string]any{"name": "delete_service", "arguments": map[string]any{"name": "alice-web", "confirm": true}})
	if text, isErr := toolText(t, out); !isErr || !strings.Contains(text, "삭제 방지") {
		t.Errorf("mcp delete: %v %s", isErr, text)
	}
	internal, _ := auth.IssueInternalSession("alice@parametacorp.com", auth.RoleUser)
	if w := postAs(h, "/v1/services/alice-web/delete-protection", `{"enabled":false}`, internal.Token); w.Code != http.StatusUnauthorized {
		t.Errorf("internal session toggle: %d", w.Code)
	}
	if !clusterState.IsDeleteProtected("alice-web") {
		t.Fatal("protection was switched off via API-token session")
	}

	// Turning it off restores normal deletion.
	if w := postAs(h, "/v1/services/bob-api/delete-protection", `{"enabled":false}`, bob.Token); w.Code != http.StatusOK {
		t.Fatalf("owner untoggle: %d", w.Code)
	}
	if w := postAs(h, "/v1/cluster/delete", `{"service_name":"bob-api"}`, bob.Token); w.Code != http.StatusOK || clusterState.GetService("bob-api") != nil {
		t.Errorf("delete after unprotect: %d %s", w.Code, w.Body.String())
	}
}

func TestPostLoginHopCarriesNoRedirect(t *testing.T) {
	w := httptest.NewRecorder()
	writePostLoginHop(w, "/admin")
	body := w.Body.String()
	if w.Code != http.StatusOK || w.Header().Get("Location") != "" || !strings.Contains(body, `location.replace("/admin")`) {
		t.Fatalf("hop page: %d %q", w.Code, body)
	}
	w = httptest.NewRecorder()
	writePostLoginHop(w, "https://evil.example/")
	if strings.Contains(w.Body.String(), "evil.example") {
		t.Fatal("hop page followed an unlisted destination")
	}
}

// A service running without a cluster definition (seen only via heartbeat
// placements) can still be protected — by an admin — and then can't be deleted.
func TestDeleteProtectionWithoutDefinition(t *testing.T) {
	h, _ := mcpEnv(t)
	clusterState.ProcessHeartbeat("node-b", "127.0.0.1:1", models.NodeResources{}, []models.ContainerPlacement{{
		ContainerID: "0123456789abcdef0123", ContainerName: "orch-ai-presentation-0",
		ServiceName: "ai-presentation", Image: "hisosic/ai-presentation", NodeName: "node-b", Status: "running",
	}})
	if clusterState.GetService("ai-presentation") != nil {
		t.Fatal("test setup: service should have no definition")
	}
	bob, _ := auth.EnsureUserSession("bob@parametacorp.com", auth.RoleUser)
	alice, _ := auth.EnsureUserSession("alice@parametacorp.com", auth.RoleAdmin)

	if w := postAs(h, "/v1/services/ai-presentation/delete-protection", `{"enabled":true}`, bob.Token); w.Code != http.StatusForbidden {
		t.Fatalf("non-admin on undefined service: %d %s", w.Code, w.Body.String())
	}
	if w := postAs(h, "/v1/services/no-such-svc/delete-protection", `{"enabled":true}`, alice.Token); w.Code != http.StatusNotFound {
		t.Fatalf("unknown service: %d", w.Code)
	}
	if w := postAs(h, "/v1/services/ai-presentation/delete-protection", `{"enabled":true}`, alice.Token); w.Code != http.StatusOK {
		t.Fatalf("admin protect: %d %s", w.Code, w.Body.String())
	}
	if w := postAs(h, "/v1/cluster/delete", `{"service_name":"ai-presentation"}`, alice.Token); w.Code != http.StatusConflict {
		t.Errorf("delete: %d %s", w.Code, w.Body.String())
	}
	if len(clusterState.GetPlacements("ai-presentation", "")) == 0 {
		t.Fatal("placements were removed")
	}
	// The listing reports the flag even without a definition.
	if !serviceDeleteProtected("ai-presentation") {
		t.Fatal("flag not visible")
	}
}
