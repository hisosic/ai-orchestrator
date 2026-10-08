// Package server — sharing a service with specific users.
//
// A service can be shared with everyone (listed on the public portal, link
// open as before) or with specific users only. In the latter mode the link
// itself is protected: the master's Traefik puts a forwardAuth middleware in
// front of the service's routes (see syncTraefikRoutes), which asks
// GET /v1/auth/forward?svc=NAME on every request. Only the owner and the
// listed users, signed in on that domain, get through; others receive the
// sign-in or "no access" page.
//
// Only services without the protection are affected by an orchestrator
// outage: the extra hop exists just for user-restricted services.
package server

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/clusterstate"

	"github.com/go-chi/chi/v5"
)

const maxShareUsers = 50

var emailRe = regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)

// normalizeShareUsers validates and de-duplicates the allowed emails. The
// owner may be listed too (harmless: owners always have access), so sharing
// with just yourself is a way to make the link private.
func normalizeShareUsers(in []string, owner string) ([]string, error) {
	dom := googleAllowedDomain()
	seen := map[string]bool{}
	var out []string
	for _, e := range in {
		e = strings.ToLower(strings.TrimSpace(e))
		if e == "" || seen[e] {
			continue
		}
		if !emailRe.MatchString(e) {
			return nil, fmt.Errorf("'%s'는 올바른 이메일이 아닙니다", e)
		}
		if dom != "" && !strings.HasSuffix(e, "@"+dom) {
			return nil, fmt.Errorf("'%s': @%s 계정만 지정할 수 있습니다", e, dom)
		}
		seen[e] = true
		out = append(out, e)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("공유할 사용자 이메일을 1명 이상 입력하세요")
	}
	if len(out) > maxShareUsers {
		return nil, fmt.Errorf("공유 사용자는 최대 %d명까지 지정할 수 있습니다", maxShareUsers)
	}
	sort.Strings(out)
	return out, nil
}

// canViewSharedService reports whether username may open a service that is
// shared with specific users: its owner or one of the listed users.
func canViewSharedService(service, username string) bool {
	if username == "" || clusterState == nil {
		return false
	}
	info := clusterState.GetService(service)
	if info == nil {
		return false
	}
	if owner, _ := info["owner"].(string); owner != "" && owner == username {
		return true
	}
	_, users := clusterState.ShareSettings(service)
	for _, u := range users {
		if u == username {
			return true
		}
	}
	return false
}

// handleAuthForward: GET /v1/auth/forward?svc=NAME — Traefik forwardAuth
// target for protected services (shared with specific users, or with
// per-path rules). 2xx lets the request through; any other response is
// returned to the browser as-is. On success the signed-in user's email is
// returned in X-Itda-User, which Traefik forwards to the app.
func handleAuthForward(w http.ResponseWriter, r *http.Request) {
	svc := strings.TrimSpace(r.URL.Query().Get("svc"))
	if svc == "" || clusterState == nil {
		http.Error(w, "service required", http.StatusBadRequest)
		return
	}
	sess := auth.SessionFromRequest(r)
	user := ""
	if sess != nil && !sess.Internal {
		user = sess.Username
	}
	uri := r.Header.Get("X-Forwarded-Uri")

	allowed := true
	if mode, _ := clusterState.ShareSettings(svc); mode == "users" && !canViewSharedService(svc, user) {
		allowed = false
	}
	if allowed {
		if rule := matchPathRule(clusterState.PathRules(svc), appPath(uri, svc)); rule != nil && !pathRuleAllows(rule, svc, user) {
			allowed = false
		}
	}
	if allowed {
		if user != "" {
			w.Header().Set(itdaUserHeader, user)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Land back on the page the visitor asked for after signing in.
	next := uri
	if next == "" || !safeRelativePath(next) {
		next = "/" + svc + "/"
	}
	d := adminGateData{
		Service:       svc,
		Next:          next,
		Notice:        fmt.Sprintf("'%s'의 이 페이지는 권한이 있는 사용자만 볼 수 있습니다.", svc),
		ForbiddenText: fmt.Sprintf("이 계정은 '%s'의 이 페이지에 접근할 권한이 없습니다.", svc),
	}
	if user == "" {
		writeAdminGate(w, r, http.StatusUnauthorized, d)
		return
	}
	d.Forbidden = true
	d.Username = user
	auditLog(user, string(sess.Role), clientIPString(r), "service_access", svc+" "+truncate(uri, 200), "denied")
	writeAdminGate(w, r, http.StatusForbidden, d)
}

// itdaUserHeader carries the signed-in user's email to apps behind a
// protected route. Client-sent copies are stripped on every route.
const itdaUserHeader = "X-Itda-User"

// normalizePath canonicalizes a request path for rule matching so encoding
// and spelling variants (%2F, //, /./, \, ;params, letter case) can't slip
// past a rule: percent-decoded until stable, backslashes as slashes,
// ;params dropped, cleaned, lower-cased.
func normalizePath(p string) string {
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		p = p[:i]
	}
	for i := 0; i < 3; i++ {
		d, err := url.PathUnescape(p)
		if err != nil || d == p {
			break
		}
		p = d
	}
	p = strings.ReplaceAll(p, "\\", "/")
	segs := strings.Split(p, "/")
	for i, sg := range segs {
		if k := strings.IndexByte(sg, ';'); k >= 0 {
			segs[i] = sg[:k]
		}
	}
	p = path.Clean("/" + strings.Join(segs, "/"))
	return strings.ToLower(p)
}

// appPath turns the forwarded request URI into the path the app sees:
// /svc/admin/x → /admin/x. Requests routed by Referer (assets the app
// loads with absolute paths) already carry the app path.
func appPath(uri, svc string) string {
	p := normalizePath(uri)
	prefix := "/" + strings.ToLower(svc)
	if p == prefix {
		return "/"
	}
	if strings.HasPrefix(p, prefix+"/") {
		return p[len(prefix):]
	}
	return p
}

// matchPathRule returns the most specific rule covering p, if any.
func matchPathRule(rules []clusterstate.PathRule, p string) *clusterstate.PathRule {
	var best *clusterstate.PathRule
	for i := range rules {
		rp := rules[i].Path
		if rp == "/" || p == rp || strings.HasPrefix(p, rp+"/") {
			if best == nil || len(rp) > len(best.Path) {
				best = &rules[i]
			}
		}
	}
	return best
}

// pathRuleAllows reports whether user (empty = anonymous) passes rule.
func pathRuleAllows(rule *clusterstate.PathRule, svc, user string) bool {
	if user == "" {
		return false
	}
	owner := serviceOwnerOf(svc)
	switch rule.Allow {
	case "login":
		return true
	case "owner":
		return user == owner
	case "users":
		if user == owner {
			return true
		}
		for _, u := range rule.Users {
			if u == user {
				return true
			}
		}
	}
	return false
}

const maxPathRules = 20

// validatePathRules normalizes and checks rules submitted by the owner.
func validatePathRules(in []clusterstate.PathRule, owner string) ([]clusterstate.PathRule, error) {
	seen := map[string]bool{}
	out := []clusterstate.PathRule{}
	for _, r := range in {
		raw := strings.TrimSpace(r.Path)
		if raw == "" {
			continue
		}
		if !strings.HasPrefix(raw, "/") || len(raw) > 200 {
			return nil, fmt.Errorf("경로 '%s'는 /로 시작해야 합니다 (예: /admin)", raw)
		}
		p := normalizePath(raw)
		if seen[p] {
			return nil, fmt.Errorf("경로 '%s'가 중복되었습니다", p)
		}
		seen[p] = true
		rule := clusterstate.PathRule{Path: p, Allow: r.Allow}
		switch r.Allow {
		case "owner", "login":
		case "users":
			users, err := normalizeShareUsers(r.Users, owner)
			if err != nil {
				return nil, fmt.Errorf("경로 '%s': %v", p, err)
			}
			rule.Users = users
		default:
			return nil, fmt.Errorf("경로 '%s'의 허용 대상은 owner, login, users 중 하나입니다", p)
		}
		out = append(out, rule)
	}
	if len(out) > maxPathRules {
		return nil, fmt.Errorf("접근 제한 경로는 최대 %d개까지 설정할 수 있습니다", maxPathRules)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// handleServicePathRules: POST /v1/services/{name}/path-rules {rules:[...]}
// — owner or admin, browser session.
func handleServicePathRules(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil || sess.Internal {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "로그인이 필요합니다", "code": "unauthenticated"})
		return
	}
	name := strings.TrimSpace(chi.URLParam(r, "name"))
	var body struct {
		Rules []clusterstate.PathRule `json:"rules"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다"})
		return
	}
	info := clusterState.GetService(name)
	if info == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "클러스터 정의가 있는 서비스만 경로 제한을 설정할 수 있습니다: " + name})
		return
	}
	if ok, owner := canManageService(r, info); !ok {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false,
			"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 접근 제한을 바꿀 수 있습니다", name, owner),
			"code":    "forbidden_owner",
		})
		return
	}
	if len(body.Rules) > 0 && serviceUsesSubdomain(name) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "서브도메인으로 연결된 서비스는 경로 제한을 지원하지 않습니다 (경로 방식만 지원)"})
		return
	}
	owner, _ := info["owner"].(string)
	rules, err := validatePathRules(body.Rules, owner)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	if !clusterState.SetPathRules(name, rules) {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "저장 실패"})
		return
	}
	auditRequest(r, "POST /v1/services/"+name+"/path-rules", name, fmt.Sprintf("rules:%d", len(rules)))
	go syncTraefikRoutes()
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "name": name, "path_rules": rules})
}

// safePathRe limits post-login destinations to plain same-host paths.
var safePathRe = regexp.MustCompile(`^/[A-Za-z0-9._~%/?=&+\-:@,;]*$`)

// safeRelativePath reports whether p is a same-host path that is safe to
// redirect to and to embed in a page (no scheme, no "//host", no quotes).
func safeRelativePath(p string) bool {
	if len(p) > 512 || strings.HasPrefix(p, "//") || strings.Contains(p, "..") || !safePathRe.MatchString(p) {
		return false
	}
	u, err := url.Parse(p)
	return err == nil && u.Scheme == "" && u.Host == ""
}

func serviceOwnerOf(service string) string {
	if clusterState == nil {
		return ""
	}
	if info := clusterState.GetService(service); info != nil {
		o, _ := info["owner"].(string)
		return o
	}
	return ""
}

// shareFieldsForOwner adds the share scope to a listing entry; the user
// list is only shown to the service's owner (and admins).
func shareFieldsForOwner(entry map[string]any, service string, owned bool) {
	if clusterState == nil {
		return
	}
	mode, users := clusterState.ShareSettings(service)
	entry["shared"] = mode != ""
	entry["share_mode"] = mode
	if owned && mode == "users" {
		entry["share_users"] = users
	}
	if owned {
		if rules := clusterState.PathRules(service); len(rules) > 0 {
			entry["path_rules"] = rules
		}
	}
}
