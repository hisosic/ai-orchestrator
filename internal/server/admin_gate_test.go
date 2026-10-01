package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-container-go/internal/auth"
)

func getAs(h http.Handler, path, sessionToken string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", path, nil)
	if sessionToken != "" {
		req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: sessionToken})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestAdminPageRequiresAdminLogin(t *testing.T) {
	h := setupTestServer(t)
	DashboardHTML = "<html>DASHBOARD</html>"

	// Anonymous: no dashboard HTML, even without Google configured.
	for _, p := range []string{"/admin", "/dashboard"} {
		if w := getAs(h, p, ""); w.Code != http.StatusUnauthorized || bytesContain(w, "DASHBOARD") {
			t.Errorf("anonymous %s: %d", p, w.Code)
		}
	}
	// Anonymous with Google configured: redirected to Google login.
	setGoogleEnv(t)
	if w := getAs(h, "/admin", ""); w.Code != http.StatusFound || w.Header().Get("Location") != "/v1/auth/google/login?redirect=/admin" {
		t.Errorf("anonymous+google: %d %s", w.Code, w.Header().Get("Location"))
	}

	user, err := auth.EnsureUserSession("dev@parametacorp.com", auth.RoleUser)
	if err != nil {
		t.Fatal(err)
	}
	if w := getAs(h, "/admin", user.Token); w.Code != http.StatusForbidden || bytesContain(w, "DASHBOARD") {
		t.Errorf("non-admin: %d", w.Code)
	}
	admin, err := auth.EnsureUserSession("boss@parametacorp.com", auth.RoleAdmin)
	if err != nil {
		t.Fatal(err)
	}
	if w := getAs(h, "/admin", admin.Token); w.Code != http.StatusOK || !bytesContain(w, "DASHBOARD") {
		t.Errorf("admin: %d", w.Code)
	}
}

func TestAnonymousAPIReadsBlocked(t *testing.T) {
	t.Setenv("ORCHESTRATOR_ADVERTISE_ADDR", "127.0.0.1:1") // fail node fan-out fast
	h := setupTestServer(t)
	for _, p := range []string{"/v1/services", "/v1/cluster/nodes", "/v1/cluster/stats", "/v1/system", "/v1/registry/status", "/v1/stream"} {
		if w := getAs(h, p, ""); w.Code != http.StatusUnauthorized {
			t.Errorf("anonymous GET %s: %d, want 401", p, w.Code)
		}
	}
	for _, p := range []string{"/v1/public/services", "/v1/public/stats", "/v1/auth/google/status", "/health", "/"} {
		if w := getAs(h, p, ""); w.Code != http.StatusOK {
			t.Errorf("anonymous GET %s: %d, want 200", p, w.Code)
		}
	}
}

func bytesContain(w *httptest.ResponseRecorder, s string) bool {
	return strings.Contains(w.Body.String(), s)
}
