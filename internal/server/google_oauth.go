// Package server — Google OAuth 2.0 login.
//
// Flow:
//   GET /v1/auth/google/login    → 302 to Google consent (sets state cookie)
//   GET /v1/auth/google/callback → exchange code, fetch userinfo, find-or-
//                                   create local user, issue session, redirect
//   GET /v1/auth/google/status   → {enabled} so the UI shows/hides the button
//
// Configuration (env):
//   GOOGLE_OAUTH_CLIENT_ID       (required to enable)
//   GOOGLE_OAUTH_CLIENT_SECRET   (required to enable)
//   GOOGLE_OAUTH_REDIRECT_URL    (optional; else derived from the request)
//   GOOGLE_OAUTH_ALLOWED_DOMAIN  (optional; restrict to a workspace domain,
//                                 e.g. "parametacorp.com")
//   GOOGLE_OAUTH_ADMIN_EMAILS    (optional; comma-separated emails granted the
//                                 admin role on first login; others get user)
package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/runtime"
)

const googleStateCookie = "orch_goog_state"

func googleOAuthConfigured() bool {
	return strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_CLIENT_ID")) != "" &&
		strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")) != ""
}

// googleRedirectURL returns the configured callback URL, or derives one from
// the incoming request (scheme+host) so it works behind the reverse proxy.
func googleRedirectURL(r *http.Request) string {
	// GOOGLE_OAUTH_REDIRECT_URL may list several callbacks (one per public
	// domain, comma separated); use the one on the host the user is on so the
	// login round trip stays on that domain. Unknown hosts get the first.
	var urls []string
	for _, u := range strings.Split(os.Getenv("GOOGLE_OAUTH_REDIRECT_URL"), ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) > 0 {
		host := strings.ToLower(requestHost(r))
		for _, u := range urls {
			if pu, err := url.Parse(u); err == nil && strings.EqualFold(pu.Host, host) {
				return u
			}
		}
		return urls[0]
	}
	scheme := "https"
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	} else if r.TLS == nil && !isPublicPathHost(r.Host) {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s/v1/auth/google/callback", scheme, requestHost(r))
}

// isPublicPathHost reports whether host is one of the public domains
// (served over HTTPS by the edge proxy).
func isPublicPathHost(host string) bool {
	for _, h := range runtime.PathHosts() {
		if strings.EqualFold(h, host) {
			return true
		}
	}
	return false
}

// googleLoginDest validates the post-login landing page against an allowlist
// so the redirect parameter can't be used as an open redirect.
func googleLoginDest(d string) string {
	switch d {
	case "/", "/admin", "/dashboard", "/portal":
		return d
	}
	return "/portal"
}

// handleGoogleStatus reports whether Google login is available.
func handleGoogleStatus(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{"enabled": googleOAuthConfigured(), "password_login": passwordLoginEnabled()}
	if ph := runtime.PathHost(); ph != "" {
		resp["public_url"] = "https://" + ph // canonical portal URL (MCP setup, links)
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleGoogleLogin redirects the browser to Google's consent screen.
func handleGoogleLogin(w http.ResponseWriter, r *http.Request) {
	if !googleOAuthConfigured() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false, "message": "Google 로그인이 설정되지 않았습니다 (GOOGLE_OAUTH_CLIENT_ID/SECRET 필요)",
		})
		return
	}
	// The state cookie must be set on the same host Google redirects back to,
	// so bounce logins arriving via another host (e.g. the internal IP) to
	// the configured callback host first.
	if cb, err := url.Parse(googleRedirectURL(r)); err == nil && cb.Host != "" && !strings.EqualFold(cb.Host, r.Host) {
		http.Redirect(w, r, cb.Scheme+"://"+cb.Host+r.URL.RequestURI(), http.StatusFound)
		return
	}
	state, err := randomURLToken()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": err.Error()})
		return
	}
	// Persist where to land after login (default /portal).
	dest := googleLoginDest(r.URL.Query().Get("redirect"))
	http.SetCookie(w, &http.Cookie{
		Name:     googleStateCookie,
		Value:    state + "|" + dest,
		Path:     "/",
		HttpOnly: true,
		Secure:   cookieSecure(r),
		SameSite: http.SameSiteLaxMode, // Lax so it survives the Google redirect back
		MaxAge:   600,
	})

	params := url.Values{}
	params.Set("client_id", strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_CLIENT_ID")))
	params.Set("redirect_uri", googleRedirectURL(r))
	params.Set("response_type", "code")
	params.Set("scope", "openid email profile")
	params.Set("state", state)
	params.Set("access_type", "online")
	params.Set("prompt", "select_account")
	if dom := strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_ALLOWED_DOMAIN")); dom != "" {
		params.Set("hd", dom) // hint Google to prefer that workspace domain
	}
	http.Redirect(w, r, "https://accounts.google.com/o/oauth2/v2/auth?"+params.Encode(), http.StatusFound)
}

// handleGoogleCallback completes the OAuth flow.
func handleGoogleCallback(w http.ResponseWriter, r *http.Request) {
	if !googleOAuthConfigured() {
		http.Error(w, "Google 로그인이 설정되지 않았습니다", http.StatusServiceUnavailable)
		return
	}
	// Validate state (CSRF) against the cookie.
	c, err := r.Cookie(googleStateCookie)
	if err != nil {
		googleFail(w, r, "state 쿠키 없음")
		return
	}
	parts := strings.SplitN(c.Value, "|", 2)
	wantState := parts[0]
	dest := "/portal"
	if len(parts) == 2 {
		dest = googleLoginDest(parts[1])
	}
	// Clear the state cookie.
	http.SetCookie(w, &http.Cookie{Name: googleStateCookie, Value: "", Path: "/", MaxAge: -1})

	if r.URL.Query().Get("state") == "" || r.URL.Query().Get("state") != wantState {
		googleFail(w, r, "state 불일치 (CSRF 방지)")
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		googleFail(w, r, "code 없음 (사용자가 취소했을 수 있음)")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	// 1) Exchange the auth code for tokens.
	tok := url.Values{}
	tok.Set("code", code)
	tok.Set("client_id", strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_CLIENT_ID")))
	tok.Set("client_secret", strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")))
	tok.Set("redirect_uri", googleRedirectURL(r))
	tok.Set("grant_type", "authorization_code")
	treq, _ := http.NewRequestWithContext(ctx, "POST", "https://oauth2.googleapis.com/token", strings.NewReader(tok.Encode()))
	treq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tresp, err := http.DefaultClient.Do(treq)
	if err != nil {
		googleFail(w, r, "토큰 교환 실패: "+err.Error())
		return
	}
	defer tresp.Body.Close()
	var td struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	body, _ := io.ReadAll(io.LimitReader(tresp.Body, 1<<20))
	json.Unmarshal(body, &td)
	if td.AccessToken == "" {
		googleFail(w, r, "access_token 없음")
		return
	}

	// 2) Fetch the user profile.
	ureq, _ := http.NewRequestWithContext(ctx, "GET", "https://openidconnect.googleapis.com/v1/userinfo", nil)
	ureq.Header.Set("Authorization", "Bearer "+td.AccessToken)
	uresp, err := http.DefaultClient.Do(ureq)
	if err != nil {
		googleFail(w, r, "사용자 정보 조회 실패: "+err.Error())
		return
	}
	defer uresp.Body.Close()
	var ui struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
		HD            string `json:"hd"`
	}
	ubody, _ := io.ReadAll(io.LimitReader(uresp.Body, 1<<20))
	json.Unmarshal(ubody, &ui)
	email := strings.ToLower(strings.TrimSpace(ui.Email))
	if email == "" {
		googleFail(w, r, "이메일을 가져오지 못했습니다")
		return
	}

	if !ui.EmailVerified {
		googleFail(w, r, "이메일 인증이 완료되지 않은 계정입니다")
		return
	}

	// Optional workspace-domain restriction. The hd claim is only present for
	// accounts managed by that Workspace; checking the email suffix alone would
	// admit a personal Google account registered with a company address.
	if dom := strings.ToLower(strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_ALLOWED_DOMAIN"))); dom != "" {
		if !strings.HasSuffix(email, "@"+dom) || !strings.EqualFold(ui.HD, dom) {
			googleFail(w, r, fmt.Sprintf("%s 도메인 계정만 로그인할 수 있습니다", dom))
			return
		}
	}

	// Decide role: admin if listed in GOOGLE_OAUTH_ADMIN_EMAILS, else user.
	role := auth.RoleUser
	for _, a := range strings.Split(strings.ToLower(os.Getenv("GOOGLE_OAUTH_ADMIN_EMAILS")), ",") {
		if strings.TrimSpace(a) == email {
			role = auth.RoleAdmin
			break
		}
	}

	// 3) Find-or-create the local user (keyed by email) + issue a session.
	sess, err := auth.EnsureUserSession(email, role)
	if err != nil {
		googleFail(w, r, "세션 생성 실패: "+err.Error())
		return
	}
	auth.SetSessionCookieStrict(w, sess.Token, cookieSecure(r))
	setCSRFCookie(w, r)
	auditLog(email, string(sess.Role), clientIPString(r), "login", email, "google_oauth")

	writePostLoginHop(w, dest)
}

// writePostLoginHop finishes the OAuth round trip with a tiny same-origin
// page that navigates to dest, instead of a 302. The session cookie is
// SameSite=Strict, and browsers don't send Strict cookies on a redirect
// chain that started at accounts.google.com, so a direct redirect to a
// server-gated page like /admin would arrive without the session. A
// navigation started by this page is same-site, so the cookie is sent.
func writePostLoginHop(w http.ResponseWriter, dest string) {
	dest = googleLoginDest(dest) // allowlisted, safe to embed
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta http-equiv="refresh" content="0;url=%[1]s"><title>로그인 중…</title></head>
<body style="margin:0;background:#0a0f1a;color:#9aa8c4;font-family:system-ui,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh">
<p>로그인 중… <a href="%[1]s" style="color:#5aa7ff">계속</a></p><script>location.replace(%[1]q)</script></body></html>`, dest)
}

// googleFail redirects back to the page the login started from (portal or
// admin sign-in) with an error query param so it can surface the reason.
func googleFail(w http.ResponseWriter, r *http.Request, msg string) {
	back := "/portal"
	if c, err := r.Cookie(googleStateCookie); err == nil {
		if parts := strings.SplitN(c.Value, "|", 2); len(parts) == 2 {
			if d := googleLoginDest(parts[1]); d == "/admin" || d == "/dashboard" {
				back = "/admin"
			}
		}
	}
	http.Redirect(w, r, back+"?login_error="+url.QueryEscape("Google 로그인 실패: "+msg), http.StatusFound)
}

// googleAllowedDomain is the Workspace domain logins are restricted to, if any.
func googleAllowedDomain() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_ALLOWED_DOMAIN")))
}

func clientIPString(r *http.Request) string {
	if ip := clientIP(r); ip != nil {
		return ip.String()
	}
	return "unknown"
}

func randomURLToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
