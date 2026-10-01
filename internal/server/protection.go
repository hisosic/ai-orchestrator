// Package server — delete protection for services.
//
// When a service has delete protection on, every console/API path that
// deletes the service or removes one of its containers refuses, for every
// role. The flag lives in cluster state and survives redeploys; the cluster
// state layer also refuses to drop a protected definition as a backstop.
// Only the owner (or an admin) can turn it off again from the portal or
// dashboard; the MCP toolset deliberately has no way to change it.
package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/runtime"

	"github.com/go-chi/chi/v5"
)

func serviceDeleteProtected(service string) bool {
	return service != "" && clusterState != nil && clusterState.IsDeleteProtected(service)
}

// rejectProtectedDelete writes a 409 and returns true when the service is
// delete-protected.
func rejectProtectedDelete(w http.ResponseWriter, service string) bool {
	if !serviceDeleteProtected(service) {
		return false
	}
	writeJSON(w, http.StatusConflict, map[string]any{
		"success": false,
		"message": fmt.Sprintf("'%s' 서비스는 삭제 방지가 켜져 있어 삭제할 수 없습니다. 먼저 삭제 방지를 해제하세요", service),
		"code":    "delete_protected",
	})
	return true
}

// serviceOfContainer resolves which service a container (ID, ID prefix or
// name) belongs to, using cluster placements, the local Docker label, and
// finally the orch-<service>-<n> naming convention.
func serviceOfContainer(ctx context.Context, ref string) string {
	ref = strings.TrimPrefix(strings.TrimSpace(ref), "/")
	if ref == "" {
		return ""
	}
	if clusterState != nil {
		for _, p := range clusterState.GetPlacements("", "") {
			if p.ContainerName == ref || (len(ref) >= 12 && strings.HasPrefix(p.ContainerID, ref)) || (len(p.ContainerID) >= 12 && strings.HasPrefix(ref, p.ContainerID)) {
				if p.ServiceName != "" {
					return p.ServiceName
				}
			}
		}
	}
	if cli := runtime.DockerClient(); cli != nil {
		if ins, err := cli.ContainerInspect(ctx, ref); err == nil {
			if ins.Config != nil {
				if svc := ins.Config.Labels[runtime.LabelService]; svc != "" {
					return svc
				}
			}
			ref = strings.TrimPrefix(ins.Name, "/")
		}
	}
	if strings.HasPrefix(ref, "orch-") {
		s := strings.TrimPrefix(ref, "orch-")
		if i := strings.LastIndex(s, "-"); i > 0 {
			return s[:i]
		}
	}
	return ""
}

// rejectProtectedContainer is rejectProtectedDelete for a container reference.
func rejectProtectedContainer(w http.ResponseWriter, ref string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rejectProtectedDelete(w, serviceOfContainer(ctx, ref))
}

// handleServiceDeleteProtection: POST /v1/services/{name}/delete-protection
// {enabled: bool}. Owner or admin, browser session only (not API tokens).
func handleServiceDeleteProtection(w http.ResponseWriter, r *http.Request) {
	sess := auth.SessionFromRequest(r)
	if sess == nil || sess.Internal {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "삭제 방지 설정은 로그인한 브라우저에서만 바꿀 수 있습니다", "code": "unauthenticated"})
		return
	}
	name := strings.TrimSpace(chi.URLParam(r, "name"))
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if err := readJSON(r, &body); err != nil || body.Enabled == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "enabled(true/false)가 필요합니다"})
		return
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다"})
		return
	}
	info := clusterState.GetService(name)
	if info == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "서비스를 찾을 수 없습니다: " + name})
		return
	}
	if ok, owner := canManageService(r, info); !ok {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"success": false,
			"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 삭제 방지를 바꿀 수 있습니다", name, owner),
			"code":    "forbidden_owner",
		})
		return
	}
	if !clusterState.SetDeleteProtection(name, *body.Enabled) {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "삭제 방지 설정 저장 실패"})
		return
	}
	state := "off"
	if *body.Enabled {
		state = "on"
	}
	auditRequest(r, "POST /v1/services/"+name+"/delete-protection", name, "delete_protection:"+state)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "name": name, "delete_protected": *body.Enabled})
}
