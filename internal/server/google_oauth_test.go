package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestGoogleLoginDest(t *testing.T) {
	for in, want := range map[string]string{
		"/":                  "/",
		"/admin":             "/admin",
		"/portal":            "/portal",
		"":                   "/portal",
		"https://evil.com":   "/portal",
		"//evil.com":         "/portal",
		"/admin/../evil.com": "/portal",
	} {
		if got := googleLoginDest(in); got != want {
			t.Errorf("googleLoginDest(%q) = %q, want %q", in, got, want)
		}
	}
}

func setGoogleEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GOOGLE_OAUTH_CLIENT_ID", "cid.apps.googleusercontent.com")
	t.Setenv("GOOGLE_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("GOOGLE_OAUTH_REDIRECT_URL", "https://itda.example.com/v1/auth/google/callback")
	t.Setenv("GOOGLE_OAUTH_ALLOWED_DOMAIN", "parametacorp.com")
}

func TestGoogleLoginBouncesToCallbackHost(t *testing.T) {
	setGoogleEnv(t)
	req := httptest.NewRequest("GET", "http://20.20.6.248:8000/v1/auth/google/login?redirect=/admin", nil)
	w := httptest.NewRecorder()
	handleGoogleLogin(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "https://itda.example.com/v1/auth/google/login?redirect=/admin" {
		t.Fatalf("Location = %q", loc)
	}
	if len(w.Result().Cookies()) != 0 {
		t.Fatal("state cookie must not be set on the wrong host")
	}
}

func TestGoogleLoginRedirectsToConsent(t *testing.T) {
	setGoogleEnv(t)
	req := httptest.NewRequest("GET", "https://itda.example.com/v1/auth/google/login?redirect=/admin", nil)
	w := httptest.NewRecorder()
	handleGoogleLogin(w, req)

	loc, err := url.Parse(w.Header().Get("Location"))
	if err != nil || !strings.HasPrefix(loc.String(), "https://accounts.google.com/") {
		t.Fatalf("Location = %q", w.Header().Get("Location"))
	}
	q := loc.Query()
	if q.Get("hd") != "parametacorp.com" || q.Get("redirect_uri") != "https://itda.example.com/v1/auth/google/callback" {
		t.Fatalf("unexpected consent params: %v", q)
	}
	var state *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == googleStateCookie {
			state = c
		}
	}
	if state == nil {
		t.Fatal("no state cookie")
	}
	if _, dest := parseStateCookie(state.Value); dest != "/admin" {
		t.Fatalf("state cookie = %+v", state)
	}
}

func TestGoogleRedirectPerDomain(t *testing.T) {
	setGoogleEnv(t)
	t.Setenv("GOOGLE_OAUTH_REDIRECT_URL", "https://itda.24x365.online/v1/auth/google/callback, https://itda.rgrg.im/v1/auth/google/callback")

	login := func(target string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handleGoogleLogin(w, httptest.NewRequest("GET", target, nil))
		return w
	}
	// New domain: consent with its own callback, no bounce.
	w := login("https://itda.rgrg.im/v1/auth/google/login?redirect=/admin")
	loc, _ := url.Parse(w.Header().Get("Location"))
	if loc.Host != "accounts.google.com" || loc.Query().Get("redirect_uri") != "https://itda.rgrg.im/v1/auth/google/callback" {
		t.Fatalf("rgrg login → %s", w.Header().Get("Location"))
	}
	// Old domain keeps working.
	w = login("https://itda.24x365.online/v1/auth/google/login")
	loc, _ = url.Parse(w.Header().Get("Location"))
	if loc.Query().Get("redirect_uri") != "https://itda.24x365.online/v1/auth/google/callback" {
		t.Fatalf("24x365 login → %s", w.Header().Get("Location"))
	}
	// Unknown host (direct IP) bounces to the first configured domain.
	w = login("http://20.20.6.248:8000/v1/auth/google/login?redirect=/admin")
	if w.Header().Get("Location") != "https://itda.24x365.online/v1/auth/google/login?redirect=/admin" {
		t.Fatalf("IP login → %s", w.Header().Get("Location"))
	}
}
