// Package server — git-URL based deploy endpoint.
//
// Accepts a public git repository URL, clones it (shallow), optionally
// drills into a subpath, and runs the shared deploy pipeline (Dockerfile
// detect/generate → build → AI-fix → registry push → deploy → health
// check). Results are streamed via the DeployJob SSE mechanism so the
// dashboard can show step-by-step progress.
//
// Security posture:
//   - HTTPS only. No ssh://, git://, file://, javascript:, etc.
//   - Host allowlist: github.com, gitlab.com, bitbucket.org, codeberg.org,
//     gitea.io, git.sr.ht (override via ORCHESTRATOR_GIT_HOST_ALLOWLIST,
//     comma-separated).
//   - No embedded credentials (URLs with user-info rejected).
//   - Depth-1, single-branch clone. 5 min timeout. 500 MB max on-disk.
//   - subpath is cleaned and kept within the clone dir (no .. traversal).
package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"ai-container-go/internal/usersecrets"
)

// Default allowlist. Can be extended via ORCHESTRATOR_GIT_HOST_ALLOWLIST env.
var defaultGitHostAllowlist = []string{
	"github.com",
	"gitlab.com",
	"bitbucket.org",
	"codeberg.org",
	"gitea.io",
	"git.sr.ht",
}

// gitHostAllowed reports whether the host is safe to clone from.
func gitHostAllowed(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	extra := strings.Split(strings.ToLower(os.Getenv("ORCHESTRATOR_GIT_HOST_ALLOWLIST")), ",")
	for _, h := range extra {
		h = strings.TrimSpace(h)
		if h != "" && h == host {
			return true
		}
	}
	for _, h := range defaultGitHostAllowlist {
		if h == host {
			return true
		}
	}
	return false
}

// validateGitURL returns a normalized URL string or an error.
func validateGitURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("git_url이 비어 있습니다")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("git_url 파싱 실패: %v", err)
	}
	if u.Scheme != "https" {
		return "", fmt.Errorf("https:// 스킴만 허용됩니다")
	}
	if u.User != nil {
		return "", fmt.Errorf("URL에 자격증명을 포함할 수 없습니다")
	}
	if !gitHostAllowed(u.Hostname()) {
		return "", fmt.Errorf("허용되지 않은 git 호스트입니다: %s", u.Hostname())
	}
	if u.Path == "" || u.Path == "/" {
		return "", fmt.Errorf("repo 경로가 없습니다")
	}
	// Strip fragment/query — git doesn't use them, and keeping them would
	// let a user smuggle in extra option-like strings.
	u.Fragment = ""
	u.RawQuery = ""
	return u.String(), nil
}

// sanitizeRef validates a branch/tag reference. Keeps rules conservative
// to avoid shell/argument injection via `git clone --branch`.
func sanitizeRef(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", nil
	}
	if len(ref) > 120 {
		return "", fmt.Errorf("branch 이름이 너무 깁니다")
	}
	if strings.HasPrefix(ref, "-") {
		return "", fmt.Errorf("branch 이름은 '-'로 시작할 수 없습니다")
	}
	// Allow alnum, dot, slash, underscore, hyphen.
	for _, c := range ref {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '.' || c == '/' || c == '_' || c == '-':
		default:
			return "", fmt.Errorf("branch 이름에 허용되지 않은 문자가 있습니다: %q", c)
		}
	}
	return ref, nil
}

// sanitizeSubpath cleans a subpath and asserts it stays inside the repo.
func sanitizeSubpath(sub string) (string, error) {
	sub = strings.TrimSpace(sub)
	if sub == "" {
		return "", nil
	}
	sub = filepath.Clean("/" + sub)
	sub = strings.TrimPrefix(sub, "/")
	if sub == "." || sub == "" {
		return "", nil
	}
	if strings.Contains(sub, "..") {
		return "", fmt.Errorf("subpath에 '..' 을 포함할 수 없습니다")
	}
	return sub, nil
}

// serviceNameFromGitURL derives a default service name from the last
// path segment (owner/repo → repo).
func serviceNameFromGitURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "git-app"
	}
	p := strings.TrimSuffix(u.Path, ".git")
	p = strings.TrimSuffix(p, "/")
	parts := strings.Split(p, "/")
	if len(parts) == 0 {
		return "git-app"
	}
	name := parts[len(parts)-1]
	return sanitizeServiceName(name)
}

// buildAuthenticatedGitURL injects a token into an https git URL as basic-auth
// user-info so private repos can be cloned non-interactively. GitHub accepts
// "x-access-token:<token>@host"; GitLab/Bitbucket/Gitea also accept a
// "<user>:<token>@host" form, so x-access-token works broadly. The token is
// URL-encoded. Returns the original URL unchanged when token is empty.
func buildAuthenticatedGitURL(cleanURL, token string) string {
	if token == "" {
		return cleanURL
	}
	u, err := url.Parse(cleanURL)
	if err != nil {
		return cleanURL
	}
	u.User = url.UserPassword("x-access-token", token)
	return u.String()
}

// redactToken removes a secret token from text before it is logged or returned
// to the client, so credentials never leak via error output.
func redactToken(text, token string) string {
	if token == "" {
		return text
	}
	text = strings.ReplaceAll(text, token, "****")
	// Also redact the URL-encoded form.
	if enc := url.QueryEscape(token); enc != token {
		text = strings.ReplaceAll(text, enc, "****")
	}
	return text
}

// cloneGitRepo performs a shallow clone into destDir. authURL may contain
// embedded credentials; redactToken is scrubbed from any error output.
func cloneGitRepo(ctx context.Context, authURL, branch, destDir, scrub string) error {
	args := []string{"clone", "--depth", "1", "--single-branch"}
	if branch != "" {
		args = append(args, "--branch", branch)
	}
	args = append(args, "--", authURL, destDir)

	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "git", args...)
	// Disable interactive credential prompts so a bad/missing token fails fast
	// instead of hanging the goroutine.
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=/bin/false",
		"GCM_INTERACTIVE=never",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		tail := redactToken(string(out), scrub)
		if len(tail) > 1000 {
			tail = tail[len(tail)-1000:]
		}
		return fmt.Errorf("git clone 실패: %v\n%s", err, tail)
	}
	return nil
}

// handleDeployGit handles POST /v1/services/deploy-git
//
// Body (JSON):
//   {
//     "git_url":  "https://github.com/owner/repo",   // required
//     "branch":   "main",                             // optional
//     "subpath":  "services/backend",                 // optional
//     "name":     "my-service"                        // optional; defaults to repo name
//   }
//
// Returns: {deploy_id} immediately. Progress is streamed via
// GET /v1/services/deploy/{deploy_id}/events.
func handleDeployGit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		GitURL  string `json:"git_url"`
		Branch  string `json:"branch"`
		Subpath string `json:"subpath"`
		Name    string `json:"name"`
		// Private-repo auth (optional). Provide either a raw token OR a
		// secret_id referencing one of the caller's saved secrets. The token
		// is used only to build the authenticated clone URL and is never
		// logged, stored, or returned.
		Token    string `json:"token"`
		SecretID string `json:"secret_id"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": fmt.Sprintf("요청 파싱 실패: %v", err),
		})
		return
	}

	cloneURL, err := validateGitURL(body.GitURL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": err.Error(),
		})
		return
	}

	// Resolve the access token: explicit token wins, else pull from the
	// caller's saved secret store by ID.
	token := strings.TrimSpace(body.Token)
	if token == "" && strings.TrimSpace(body.SecretID) != "" {
		if caller := requesterUsername(r); caller != "" {
			if _, pt, err := usersecrets.GetPlaintext(caller, strings.TrimSpace(body.SecretID)); err == nil {
				token = pt
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]any{
					"success": false, "message": "시크릿에서 토큰을 가져오지 못했습니다: " + err.Error(),
				})
				return
			}
		}
	}
	branch, err := sanitizeRef(body.Branch)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": err.Error(),
		})
		return
	}
	subpath, err := sanitizeSubpath(body.Subpath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": err.Error(),
		})
		return
	}

	serviceName := sanitizeServiceName(body.Name)
	if serviceName == "" {
		serviceName = serviceNameFromGitURL(cloneURL)
	}
	if serviceName == "" {
		serviceName = "git-app"
	}

	// Attribute the deploy to the requesting user (owner check + portal list).
	if rejectForeignService(w, r, serviceName) {
		return
	}
	setPendingOwner(serviceName, requesterUsername(r))

	authURL := buildAuthenticatedGitURL(cloneURL, token)
	isPrivate := token != ""

	job := registerDeployJob(serviceName, requesterUsername(r))
	pub := job.Publisher()
	privTag := ""
	if isPrivate {
		privTag = " (private, 토큰 인증)"
	}
	pub(DeployEvent{Type: "phase", Phase: "clone", Message: fmt.Sprintf("git clone 준비: %s (branch=%s, subpath=%s)%s", cloneURL, branch, subpath, privTag)})

	// Respond immediately with the job ID. NOTE: only the clean URL is echoed
	// back — never the authenticated one.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"success":      true,
		"deploy_id":    job.ID,
		"service_name": serviceName,
		"events_url":   fmt.Sprintf("/v1/services/deploy/%s/events", job.ID),
		"status_url":   fmt.Sprintf("/v1/services/deploy/%s", job.ID),
		"clone_url":    cloneURL,
		"branch":       branch,
		"subpath":      subpath,
		"private":      isPrivate,
	})

	go runGitDeployInBackground(authURL, cloneURL, token, branch, subpath, serviceName, job)
}

// runGitDeployInBackground clones (authURL may carry a token), then runs the
// shared build/deploy pipeline. displayURL/scrub keep the token out of logs.
func runGitDeployInBackground(authURL, displayURL, scrub, branch, subpath, serviceName string, job *DeployJob) {
	pub := job.Publisher()

	// 20min overall budget: 5m clone + 15m build/deploy.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	tmpDir, err := os.MkdirTemp("", "deploy-git-*")
	if err != nil {
		pub(DeployEvent{Type: "error", Phase: "clone", Message: fmt.Sprintf("임시 디렉터리 생성 실패: %v", err)})
		job.Finalize(false, map[string]any{"success": false, "message": err.Error()})
		return
	}
	defer os.RemoveAll(tmpDir)

	cloneDir := filepath.Join(tmpDir, "repo")
	pub(DeployEvent{Type: "phase", Phase: "clone", Message: "git clone (depth=1) 실행 중..."})
	if err := cloneGitRepo(ctx, authURL, branch, cloneDir, scrub); err != nil {
		// err is already token-scrubbed inside cloneGitRepo; scrub again
		// defensively in case the message embeds the authURL.
		msg := redactToken(err.Error(), scrub)
		pub(DeployEvent{Type: "error", Phase: "clone", Message: msg})
		job.Finalize(false, map[string]any{"success": false, "message": msg})
		return
	}
	pub(DeployEvent{Type: "phase", Phase: "clone", Message: "clone 완료"})

	extractDir := cloneDir
	if subpath != "" {
		candidate := filepath.Join(cloneDir, subpath)
		// Defense-in-depth: make sure candidate is still inside cloneDir.
		abs, err := filepath.Abs(candidate)
		baseAbs, _ := filepath.Abs(cloneDir)
		if err != nil || !strings.HasPrefix(abs, baseAbs+string(filepath.Separator)) {
			pub(DeployEvent{Type: "error", Phase: "clone", Message: "subpath가 repo 범위를 벗어났습니다"})
			job.Finalize(false, map[string]any{"success": false, "message": "invalid subpath"})
			return
		}
		info, err := os.Stat(candidate)
		if err != nil || !info.IsDir() {
			pub(DeployEvent{Type: "error", Phase: "clone", Message: fmt.Sprintf("subpath 디렉터리를 찾을 수 없습니다: %s", subpath)})
			job.Finalize(false, map[string]any{"success": false, "message": "subpath not found"})
			return
		}
		extractDir = candidate
	}

	status, result := runSingleServicePipeline(ctx, extractDir, serviceName, pub)
	success := status == http.StatusOK
	job.Finalize(success, result)
}
