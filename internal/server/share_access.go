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
	"regexp"
	"sort"
	"strings"

	"ai-container-go/internal/auth"
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
// target for user-restricted services. 2xx lets the request through; any
// other response is returned to the browser as-is.
func handleAuthForward(w http.ResponseWriter, r *http.Request) {
	svc := strings.TrimSpace(r.URL.Query().Get("svc"))
	if svc == "" || clusterState == nil {
		http.Error(w, "service required", http.StatusBadRequest)
		return
	}
	mode, _ := clusterState.ShareSettings(svc)
	if mode != "users" {
		// Not (or no longer) restricted; the route is dropped on the next sync.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	sess := auth.SessionFromRequest(r)
	if sess != nil && !sess.Internal && canViewSharedService(svc, sess.Username) {
		w.Header().Set("X-Itda-User", sess.Username)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Land back on the page the visitor asked for after signing in.
	next := r.Header.Get("X-Forwarded-Uri")
	if next == "" || !safeRelativePath(next) {
		next = "/" + svc + "/"
	}
	d := adminGateData{
		Service:       svc,
		Next:          next,
		Notice:        fmt.Sprintf("'%s'는 공유받은 사용자만 볼 수 있습니다.", svc),
		ForbiddenText: fmt.Sprintf("이 계정은 '%s' 서비스를 공유받지 않았습니다.", svc),
	}
	if sess == nil {
		writeAdminGate(w, r, http.StatusUnauthorized, d)
		return
	}
	d.Forbidden = true
	d.Username = sess.Username
	auditLog(sess.Username, string(sess.Role), clientIPString(r), "service_access", svc, "denied")
	writeAdminGate(w, r, http.StatusForbidden, d)
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
}
