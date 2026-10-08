package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/models"
)

func forwardAs(h http.Handler, svc, uri, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", "/v1/auth/forward?svc="+svc, nil)
	req.Header.Set("X-Forwarded-Uri", uri)
	if token != "" {
		req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: token})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestShareWithSpecificUsers(t *testing.T) {
	h, _ := mcpEnv(t) // bob owns bob-api
	setGoogleEnv(t)   // also restricts accounts to @parametacorp.com
	bob, _ := auth.EnsureUserSession("bob@parametacorp.com", auth.RoleUser)
	carol, _ := auth.EnsureUserSession("carol@parametacorp.com", auth.RoleUser)
	dave, _ := auth.EnsureUserSession("dave@parametacorp.com", auth.RoleUser)
	alice, _ := auth.EnsureUserSession("alice@parametacorp.com", auth.RoleAdmin)

	// Validation.
	for _, body := range []string{
		`{"name":"bob-api","shared":true,"mode":"users","users":[]}`,
		`{"name":"bob-api","shared":true,"mode":"users","users":["not-an-email"]}`,
		`{"name":"bob-api","shared":true,"mode":"users","users":["x@gmail.com"]}`,
		`{"name":"bob-api","shared":true,"mode":"team"}`,
	} {
		if w := postAs(h, "/v1/user/service-share", body, bob.Token); w.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", body, w.Code, w.Body.String())
		}
	}
	// Only the owner may set it.
	if w := postAs(h, "/v1/user/service-share", `{"name":"bob-api","shared":true,"mode":"users","users":["carol@parametacorp.com"]}`, carol.Token); w.Code != http.StatusForbidden {
		t.Fatalf("non-owner: %d", w.Code)
	}
	// Listing only yourself is accepted (a private, protected link).
	if w := postAs(h, "/v1/user/service-share", `{"name":"bob-api","shared":true,"mode":"users","users":["bob@parametacorp.com"]}`, bob.Token); w.Code != http.StatusOK {
		t.Fatalf("owner-only share: %d %s", w.Code, w.Body.String())
	}
	if w := postAs(h, "/v1/user/service-share", `{"name":"bob-api","shared":true,"mode":"users","users":["Carol@parametacorp.com "," carol@parametacorp.com"]}`, bob.Token); w.Code != http.StatusOK {
		t.Fatalf("owner share: %d %s", w.Code, w.Body.String())
	}
	if mode, users := clusterState.ShareSettings("bob-api"); mode != "users" || len(users) != 1 || users[0] != "carol@parametacorp.com" {
		t.Fatalf("stored: %s %v", mode, users)
	}

	// Link access (what Traefik asks for every request).
	for name, c := range map[string]struct {
		token string
		want  int
	}{
		"anonymous":   {"", http.StatusUnauthorized},
		"owner":       {bob.Token, http.StatusNoContent},
		"shared user": {carol.Token, http.StatusNoContent},
		"other user":  {dave.Token, http.StatusForbidden},
		"admin":       {alice.Token, http.StatusForbidden},
	} {
		if w := forwardAs(h, "bob-api", "/bob-api/report?id=7", c.token); w.Code != c.want {
			t.Errorf("%s: %d, want %d", name, w.Code, c.want)
		}
	}
	// The sign-in page sends the visitor back to the page they asked for.
	w := forwardAs(h, "bob-api", "/bob-api/report?id=7", "")
	if !strings.Contains(w.Body.String(), "redirect=%2Fbob-api%2Freport%3Fid%3D7") {
		t.Errorf("login link missing destination: %s", w.Body.String())
	}
	if googleLoginDest("/bob-api/report?id=7") != "/bob-api/report?id=7" || googleLoginDest("/v1/auth/logout") != "/portal" ||
		googleLoginDest("/a/../b") != "/portal" || googleLoginDest("//evil.com/x") != "/portal" {
		t.Error("googleLoginDest allowlist")
	}

	// Listing: only allowed viewers see it; nobody but the owner sees the list.
	public := func(token string) string {
		req := httptest.NewRequest("GET", "/v1/public/services", nil)
		if token != "" {
			req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: token})
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Body.String()
	}
	if strings.Contains(public(""), "bob-api") || strings.Contains(public(dave.Token), "bob-api") {
		t.Error("restricted service visible to others")
	}
	if got := public(carol.Token); !strings.Contains(got, "bob-api") || !strings.Contains(got, `"shared_with_me":true`) || strings.Contains(got, "share_users") {
		t.Errorf("carol listing: %s", got)
	}

	// Traefik: the restricted service gets forwardAuth-guarded routes.
	dir := t.TempDir()
	old := traefikDynamicDir
	traefikDynamicDir = dir
	defer func() { traefikDynamicDir = old }()
	clusterState.ProcessHeartbeat("node-b", "10.0.0.2:8000", models.NodeResources{}, []models.ContainerPlacement{{
		ContainerID: "c0ffee0123456789abcd", ContainerName: "orch-bob-api-0", ServiceName: "bob-api",
		Image: "reg/bob-api:v1", NodeName: "node-b", Status: "running",
	}})
	syncTraefikRoutes()
	yml, _ := os.ReadFile(filepath.Join(dir, "cluster-routes.yml"))
	for _, want := range []string{"access-bob-api-path:", "priority: 290", "- access-bob-api", "forwardAuth:", "/v1/auth/forward?svc=bob-api"} {
		if !strings.Contains(string(yml), want) {
			t.Errorf("routes missing %q:\n%s", want, yml)
		}
	}

	// Back to share-with-everyone: link protection disappears.
	if w := postAs(h, "/v1/user/service-share", `{"name":"bob-api","shared":true,"mode":"all"}`, bob.Token); w.Code != http.StatusOK {
		t.Fatalf("share all: %d", w.Code)
	}
	syncTraefikRoutes()
	yml, _ = os.ReadFile(filepath.Join(dir, "cluster-routes.yml"))
	if strings.Contains(string(yml), "access-bob-api") {
		t.Error("protection kept after switching to everyone")
	}
	if w := forwardAs(h, "bob-api", "/bob-api/", ""); w.Code != http.StatusNoContent {
		t.Errorf("forward after unrestrict: %d", w.Code)
	}
	if !strings.Contains(public(""), "bob-api") {
		t.Error("not public after share all")
	}
}
