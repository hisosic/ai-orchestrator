package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/clusterstate"
	"ai-container-go/internal/models"
)

func TestNormalizePathVariants(t *testing.T) {
	for _, uri := range []string{
		"/bob-api/admin", "/bob-api/Admin/", "/BOB-API/ADMIN", "/bob-api//admin", "/bob-api/./admin",
		"/bob-api/x/../admin", "/bob-api/%61dmin", "/bob-api/%2561dmin", "/bob-api%2Fadmin",
		"/bob-api/admin;jsessionid=1", "/bob-api/admin?x=1",
	} {
		if got := appPath(uri, "bob-api"); got != "/admin" {
			t.Errorf("appPath(%q) = %q, want /admin", uri, got)
		}
	}
	if got := appPath("/static/app.js", "bob-api"); got != "/static/app.js" {
		t.Errorf("referer-routed asset path = %q", got)
	}
	rules := []clusterstate.PathRule{{Path: "/admin", Allow: "login"}, {Path: "/admin/keys", Allow: "owner"}}
	if r := matchPathRule(rules, "/admin/keys/1"); r == nil || r.Path != "/admin/keys" {
		t.Errorf("longest match: %+v", r)
	}
	if r := matchPathRule(rules, "/administrator"); r != nil {
		t.Errorf("/administrator must not match /admin: %+v", r)
	}
}

func TestPathRules(t *testing.T) {
	h, _ := mcpEnv(t) // bob owns bob-api
	setGoogleEnv(t)
	bob, _ := auth.EnsureUserSession("bob@parametacorp.com", auth.RoleUser)
	carol, _ := auth.EnsureUserSession("carol@parametacorp.com", auth.RoleUser)
	dave, _ := auth.EnsureUserSession("dave@parametacorp.com", auth.RoleUser)

	for _, body := range []string{
		`{"rules":[{"path":"admin","allow":"owner"}]}`,
		`{"rules":[{"path":"/admin","allow":"everyone"}]}`,
		`{"rules":[{"path":"/admin","allow":"users","users":[]}]}`,
		`{"rules":[{"path":"/Admin","allow":"owner"},{"path":"/admin/","allow":"login"}]}`,
	} {
		if w := postAs(h, "/v1/services/bob-api/path-rules", body, bob.Token); w.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", body, w.Code, w.Body.String())
		}
	}
	if w := postAs(h, "/v1/services/bob-api/path-rules", `{"rules":[{"path":"/admin","allow":"owner"}]}`, carol.Token); w.Code != http.StatusForbidden {
		t.Fatalf("non-owner: %d", w.Code)
	}
	w := postAs(h, "/v1/services/bob-api/path-rules", `{"rules":[
		{"path":"/Admin","allow":"owner"},
		{"path":"/reports","allow":"users","users":["carol@parametacorp.com"]},
		{"path":"/members","allow":"login"}]}`, bob.Token)
	if w.Code != http.StatusOK {
		t.Fatalf("save: %d %s", w.Code, w.Body.String())
	}

	check := func(uri, token string) (int, string) {
		r := forwardAs(h, "bob-api", uri, token)
		return r.Code, r.Header().Get(itdaUserHeader)
	}
	for _, c := range []struct {
		uri, who, token string
		want            int
	}{
		{"/bob-api/", "anonymous", "", http.StatusNoContent}, // unmatched paths stay open
		{"/bob-api/admin", "anonymous", "", http.StatusUnauthorized},
		{"/bob-api/ADMIN/../admin/x", "carol", carol.Token, http.StatusForbidden},
		{"/bob-api/%61dmin", "dave", dave.Token, http.StatusForbidden},
		{"/bob-api//admin;x", "dave", dave.Token, http.StatusForbidden},
		{"/bob-api/admin", "owner", bob.Token, http.StatusNoContent},
		{"/bob-api/reports/q3", "carol", carol.Token, http.StatusNoContent},
		{"/bob-api/reports/q3", "dave", dave.Token, http.StatusForbidden},
		{"/bob-api/reports", "owner", bob.Token, http.StatusNoContent},
		{"/bob-api/members", "anonymous", "", http.StatusUnauthorized},
		{"/bob-api/members", "dave", dave.Token, http.StatusNoContent},
	} {
		if code, _ := check(c.uri, c.token); code != c.want {
			t.Errorf("%s as %s: %d, want %d", c.uri, c.who, code, c.want)
		}
	}
	// The app learns who the visitor is on passed requests.
	if _, user := check("/bob-api/members", carol.Token); user != "carol@parametacorp.com" {
		t.Errorf("X-Itda-User = %q", user)
	}
	if _, user := check("/bob-api/", ""); user != "" {
		t.Errorf("anonymous got X-Itda-User = %q", user)
	}

	// Traefik: whole service goes through the check, client header dropped,
	// identity forwarded.
	dir := t.TempDir()
	old := traefikDynamicDir
	traefikDynamicDir = dir
	defer func() { traefikDynamicDir = old }()
	clusterState.ProcessHeartbeat("node-b", "10.0.0.2:8000", models.NodeResources{}, []models.ContainerPlacement{{
		ContainerID: "feedbeef0123456789ab", ContainerName: "orch-bob-api-0", ServiceName: "bob-api",
		Image: "reg/bob-api:v1", NodeName: "node-b", Status: "running",
	}})
	syncTraefikRoutes()
	yml, _ := os.ReadFile(filepath.Join(dir, "cluster-routes.yml"))
	for _, want := range []string{"access-bob-api-path:", "- itda-nouser", "authResponseHeaders:", `- "X-Itda-User"`, `X-Itda-User: ""`} {
		if !strings.Contains(string(yml), want) {
			t.Errorf("routes missing %q:\n%s", want, yml)
		}
	}

	// Clearing the rules removes the protection.
	if w := postAs(h, "/v1/services/bob-api/path-rules", `{"rules":[]}`, bob.Token); w.Code != http.StatusOK {
		t.Fatalf("clear: %d", w.Code)
	}
	syncTraefikRoutes()
	yml, _ = os.ReadFile(filepath.Join(dir, "cluster-routes.yml"))
	if strings.Contains(string(yml), "access-bob-api") {
		t.Error("protection kept after clearing rules")
	}
}
