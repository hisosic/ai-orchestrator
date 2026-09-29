// Package server: service-group endpoints.
//
// A "group" ties together containers deployed from the same docker-compose
// project. Containers are tagged with label ai.orchestrator.group=<project>
// (and optionally ai.orchestrator.frontend=true) at deploy time. Group
// endpoints aggregate across the cluster by querying every node.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/go-chi/chi/v5"

	"ai-container-go/internal/runtime"
)

// swapImageTag replaces the tag portion of a Docker image reference with a
// new tag. Handles registry hosts that contain colons (host:port/repo:tag)
// by anchoring on the LAST segment after the last "/".
//
//	nginx:alpine               + "latest" → nginx:latest
//	docker.io/library/nginx    + "1.25"   → docker.io/library/nginx:1.25
//	host:5000/repo:v1          + "v2"     → host:5000/repo:v2
//	repo@sha256:abc            + "v2"     → repo:v2
func swapImageTag(image, newTag string) string {
	if newTag == "" {
		return image
	}
	// Strip digest suffix (@sha256:...).
	if at := strings.Index(image, "@"); at >= 0 {
		image = image[:at]
	}
	// Find last slash — tag colon is only valid after the last slash.
	lastSlash := strings.LastIndex(image, "/")
	repo := image
	if colon := strings.LastIndex(image, ":"); colon > lastSlash {
		repo = image[:colon]
	}
	return repo + ":" + newTag
}

func osGetenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func dockerClientLocal() *client.Client { return runtime.DockerClient() }
func stdCtx() context.Context           { return context.Background() }
func dockerListAllOpts() container.ListOptions {
	return container.ListOptions{All: true}
}

func safeStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// GroupMember describes one container within a service group.
type GroupMember struct {
	ContainerID   string `json:"container_id"`
	ContainerName string `json:"container_name"`
	ServiceName   string `json:"service_name"`
	NodeName      string `json:"node_name"`
	Image         string `json:"image"`
	Status        string `json:"status"`
	Frontend      bool   `json:"frontend"`
}

// GroupInfo is the aggregate of a compose project on the cluster.
type GroupInfo struct {
	Name         string        `json:"name"`
	FrontendName string        `json:"frontend"`
	Members      []GroupMember `json:"members"`
	RunningCount int           `json:"running_count"`
	TotalCount   int           `json:"total_count"`
}

// collectGroups walks every node in the cluster, reads its containers, and
// groups them by the ai.orchestrator.group label.
func collectGroups() []GroupInfo {
	type raw struct {
		id, name, svc, node, image, status, group string
		frontend                                  bool
	}
	var all []raw

	// Local node via direct Docker: avoids a self-HTTP loop.
	localNodeName := osGetenv("ORCHESTRATOR_NODE_NAME", "master")
	if locals := readLocalContainersForGroups(); len(locals) > 0 {
		for _, c := range locals {
			all = append(all, raw{
				id: c["id"].(string), name: c["name"].(string),
				svc: safeStr(c["service"]), node: localNodeName,
				image: safeStr(c["image"]), status: safeStr(c["state"]),
				group: safeStr(c["group"]), frontend: c["frontend"] == true,
			})
		}
	}

	// Cluster workers via HTTP.
	if clusterState != nil {
		for _, node := range clusterState.ListNodes() {
			if node.Name == localNodeName || node.Role == "master" || node.Status == "offline" {
				continue
			}
			base := nodeBaseURL(&node)
			req, _ := http.NewRequest(http.MethodGet, base+"/v1/containers", nil)
			setNodeHeaders(req, &node)
			resp, err := httpClient.Do(req)
			if err != nil {
				continue
			}
			var wrap struct {
				Containers []map[string]any `json:"containers"`
			}
			json.NewDecoder(resp.Body).Decode(&wrap)
			resp.Body.Close()
			for _, c := range wrap.Containers {
				grp := safeStr(c["group"])
				if grp == "" {
					// Worker API currently doesn't expose labels. Fall back to
					// inspecting labels via an "inspect" proxy call (expensive)
					// OR rely on container name prefix. For now, require grp to
					// be present in the /v1/containers response (we'll enrich it).
					continue
				}
				all = append(all, raw{
					id: safeStr(c["id"]), name: safeStr(c["name"]),
					svc: safeStr(c["service"]), node: node.Name,
					image: safeStr(c["image"]), status: safeStr(c["state"]),
					group: grp, frontend: c["frontend"] == true,
				})
			}
		}
	}

	byGroup := map[string]*GroupInfo{}
	for _, r := range all {
		if r.group == "" {
			continue
		}
		g, ok := byGroup[r.group]
		if !ok {
			g = &GroupInfo{Name: r.group}
			byGroup[r.group] = g
		}
		g.Members = append(g.Members, GroupMember{
			ContainerID: r.id, ContainerName: r.name, ServiceName: r.svc,
			NodeName: r.node, Image: r.image, Status: r.status, Frontend: r.frontend,
		})
		g.TotalCount++
		if r.status == "running" {
			g.RunningCount++
		}
		if r.frontend && g.FrontendName == "" {
			g.FrontendName = r.svc
		}
	}

	out := make([]GroupInfo, 0, len(byGroup))
	for _, g := range byGroup {
		sort.Slice(g.Members, func(i, j int) bool { return g.Members[i].ContainerName < g.Members[j].ContainerName })
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// readLocalContainersForGroups inspects local containers and returns rows
// enriched with the group/frontend labels.
func readLocalContainersForGroups() []map[string]any {
	cli := dockerClientLocal()
	if cli == nil {
		return nil
	}
	ctx := stdCtx()
	containers, err := cli.ContainerList(ctx, dockerListAllOpts())
	if err != nil {
		return nil
	}
	var out []map[string]any
	for _, c := range containers {
		labels := c.Labels
		if labels == nil {
			continue
		}
		group := labels["ai.orchestrator.group"]
		if group == "" {
			continue
		}
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		cid := c.ID
		if len(cid) > 12 {
			cid = cid[:12]
		}
		out = append(out, map[string]any{
			"id":       cid,
			"name":     name,
			"image":    c.Image,
			"state":    c.State,
			"service":  labels["ai.orchestrator.service"],
			"group":    group,
			"frontend": labels["ai.orchestrator.frontend"] == "true",
		})
	}
	return out
}

func handleGroupsList(w http.ResponseWriter, r *http.Request) {
	groups := collectGroups()
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups, "total": len(groups)})
}

// handleServiceUpdate triggers a fresh pull of a service's image on every node
// that currently runs a replica, then kills the old containers so the
// reconcile loop respawns them using the now-fresh local image.
//
// Reconcile respawns containers to match the service's replicas count using
// the same image tag — once that tag resolves to a newer digest locally, the
// new containers run the updated image. This is a simple "rolling" behavior:
// replicas are recreated on their original nodes.
func handleServiceUpdate(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(chi.URLParam(r, "name"))
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service name required"})
		return
	}
	if clusterState == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "클러스터 모드가 아닙니다"})
		return
	}

	// Optional body: {"image": "repo:tag"} or {"tag": "newtag"} to switch
	// the service to a different image reference. If absent, we just re-pull
	// the current tag (rolling update to the latest digest).
	var body struct {
		Image  string `json:"image"`
		Tag    string `json:"tag"`
		Memory string `json:"memory"`
		CPU    string `json:"cpu"`
	}
	_ = readJSON(r, &body)
	body.Image = strings.TrimSpace(body.Image)
	body.Tag = strings.TrimSpace(body.Tag)
	body.Memory = strings.TrimSpace(body.Memory)
	body.CPU = strings.TrimSpace(body.CPU)

	svcInfo := clusterState.GetService(name)
	// Owner check: admin updates all; a user may only update services it owns.
	if svcInfo != nil {
		if ok, owner := canManageService(r, svcInfo); !ok {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"success": false,
				"message": fmt.Sprintf("'%s' 서비스는 소유자(%s)만 업데이트할 수 있습니다", name, owner),
				"code":    "forbidden_owner",
			})
			return
		}
	}
	image := ""
	if svcInfo != nil {
		if img, ok := svcInfo["image"].(string); ok {
			image = img
		}
	}
	if image == "" {
		for _, p := range clusterState.GetPlacements(name, "") {
			if p.Image != "" {
				image = p.Image
				break
			}
		}
	}
	if image == "" && body.Image == "" {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "서비스 이미지를 확인할 수 없습니다"})
		return
	}

	// Resolve the target image:
	// - Explicit image: use it as-is.
	// - Explicit tag: replace the :tag part of the current image.
	// - Neither: reuse the current image.
	target := image
	if body.Image != "" {
		target = body.Image
	} else if body.Tag != "" {
		target = swapImageTag(image, body.Tag)
	}

	// Persist + propagate when the image changes OR resource limits change so
	// reconcile recreates containers with the new image/constraints.
	needPropagate := target != image || body.Memory != "" || body.CPU != ""
	if needPropagate {
		replicas := 0
		if svcInfo != nil {
			switch v := svcInfo["replicas"].(type) {
			case float64:
				replicas = int(v)
			case int:
				replicas = v
			}
		}
		// Update the cluster record's limits in place before persisting.
		if svcInfo != nil {
			if body.Memory != "" {
				svcInfo["memory_limit"] = body.Memory
			}
			if body.CPU != "" {
				svcInfo["cpu_limit"] = body.CPU
			}
		}
		clusterState.SaveService(name, target, replicas, svcInfo)
		// Propagate to worker state so reconcile/ExecuteScale uses the new
		// image and limits on recreate.
		for _, n := range clusterState.ListNodes() {
			if n.Role == "master" {
				continue
			}
			payload, _ := json.Marshal(map[string]any{
				"service_name": name,
				"image":        target,
				"replicas":     replicas,
				"memory":       body.Memory,
				"cpu":          body.CPU,
			})
			req, _ := http.NewRequest(http.MethodPost, nodeBaseURL(&n)+"/v1/agent/update-image", bytes.NewReader(payload))
			setNodeHeaders(req, &n)
			resp, err := httpClient.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}
		image = target
	}

	placements := clusterState.GetPlacements(name, "")
	if len(placements) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "실행 중인 컨테이너가 없습니다"})
		return
	}

	// Unique set of nodes hosting this service.
	nodeSet := map[string]bool{}
	for _, p := range placements {
		if p.NodeName != "" {
			nodeSet[p.NodeName] = true
		}
	}

	var pullResults []map[string]any
	for nodeName := range nodeSet {
		node := clusterState.GetNode(nodeName)
		if node == nil {
			pullResults = append(pullResults, map[string]any{"node": nodeName, "success": false, "error": "node not found"})
			continue
		}
		payload, _ := json.Marshal(map[string]any{"image": image, "skip_push": true})
		req, _ := http.NewRequest(http.MethodPost, nodeBaseURL(node)+"/v1/images/pull", bytes.NewReader(payload))
		setNodeHeaders(req, node)
		resp, err := longHTTPClient.Do(req)
		if err != nil {
			pullResults = append(pullResults, map[string]any{"node": nodeName, "success": false, "error": err.Error()})
			continue
		}
		var rd map[string]any
		json.NewDecoder(resp.Body).Decode(&rd)
		resp.Body.Close()
		pullResults = append(pullResults, map[string]any{
			"node":    nodeName,
			"success": rd["success"],
			"message": rd["message"],
		})
	}

	// Kill old containers so reconcile respawns them with the fresh image.
	var killResults []map[string]any
	for _, p := range placements {
		node := clusterState.GetNode(p.NodeName)
		if node == nil {
			continue
		}
		baseURL := nodeBaseURL(node)
		// Try by ID first, then by container name.
		killed := false
		for _, id := range []string{p.ContainerID, p.ContainerName} {
			if id == "" {
				continue
			}
			// Stop then delete
			stopReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/containers/%s/stop", baseURL, id), nil)
			setNodeHeaders(stopReq, node)
			if resp, err := httpClient.Do(stopReq); err == nil {
				resp.Body.Close()
			}
			delReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", baseURL, id), nil)
			setNodeHeaders(delReq, node)
			resp, err := httpClient.Do(delReq)
			if err == nil && resp.StatusCode == http.StatusOK {
				killed = true
				resp.Body.Close()
				break
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
		killResults = append(killResults, map[string]any{
			"container": p.ContainerName,
			"node":      p.NodeName,
			"removed":   killed,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"service":  name,
		"image":    image,
		"message":  fmt.Sprintf("'%s' 업데이트 트리거: %d개 노드에서 pull, %d개 컨테이너 재시작 예약 (reconcile이 새 이미지로 재생성)", name, len(pullResults), len(killResults)),
		"pulls":    pullResults,
		"replaced": killResults,
	})
}

// handleGroupUpdate runs handleServiceUpdate for every service in a group.
func handleGroupUpdate(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(chi.URLParam(r, "name"))
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "group name required"})
		return
	}
	// Use label-based membership only. Running members are identified via
	// the ai.orchestrator.group label and are consistent with what the UI
	// shows. Invocations run in parallel so no service's kill can race
	// the next service's membership snapshot.
	svcSet := map[string]bool{}
	groups := collectGroups()
	for _, g := range groups {
		if g.Name == name {
			for _, m := range g.Members {
				if m.ServiceName != "" {
					svcSet[m.ServiceName] = true
				}
			}
			break
		}
	}
	if len(svcSet) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "group not found or has no running members"})
		return
	}

	type updateRes struct{ r map[string]any }
	ch := make(chan updateRes, len(svcSet))
	for svc := range svcSet {
		go func(svc string) {
			req, _ := http.NewRequest(http.MethodPost, "http://localhost:"+getServerPort()+"/v1/services/"+svc+"/update", nil)
			if tok := strings.TrimSpace(osGetenv("ORCHESTRATOR_API_TOKEN", "")); tok != "" {
				req.Header.Set("Authorization", "Bearer "+tok)
			}
			resp, err := longHTTPClient.Do(req)
			r := map[string]any{"service": svc}
			if err != nil {
				r["success"] = false
				r["error"] = err.Error()
			} else {
				var rd map[string]any
				json.NewDecoder(resp.Body).Decode(&rd)
				resp.Body.Close()
				r["success"] = rd["success"]
				r["message"] = rd["message"]
			}
			ch <- updateRes{r}
		}(svc)
	}
	var results []map[string]any
	for i := 0; i < len(svcSet); i++ {
		results = append(results, (<-ch).r)
	}

	updated := 0
	for _, r := range results {
		if s, _ := r["success"].(bool); s {
			updated++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": updated > 0,
		"message": fmt.Sprintf("'%s' 그룹: %d/%d 서비스 업데이트 트리거", name, updated, len(results)),
		"results": results,
	})
}

func handleGroupStop(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(chi.URLParam(r, "name"))
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "group name required"})
		return
	}
	groups := collectGroups()
	var target *GroupInfo
	for i := range groups {
		if groups[i].Name == name {
			target = &groups[i]
			break
		}
	}
	if target == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "group not found"})
		return
	}

	// Group by distinct service name — one /v1/cluster/stop call per service
	// (which sets replicas=0 so reconcile doesn't respawn them).
	svcSet := map[string]bool{}
	for _, m := range target.Members {
		if m.ServiceName != "" {
			svcSet[m.ServiceName] = true
		}
	}

	// Owner check: reject the entire stop request if any member service is
	// owned by a different user. Mixed-owner groups must be torn down by
	// their respective owners (or admin who deployed each piece).
	if caller := requesterUsername(r); caller != "" && clusterState != nil {
		for svc := range svcSet {
			info := clusterState.GetService(svc)
			if info == nil {
				continue
			}
			owner, _ := info["owner"].(string)
			if owner != "" && owner != caller {
				writeJSON(w, http.StatusForbidden, map[string]any{
					"success": false,
					"message": fmt.Sprintf("그룹 내 '%s' 서비스의 소유자(%s)와 일치해야 합니다", svc, owner),
					"code":    "forbidden_owner",
				})
				return
			}
		}
	}

	var results []map[string]any
	for svc := range svcSet {
		body, _ := json.Marshal(map[string]any{"service_name": svc})
		req, _ := http.NewRequest(http.MethodPost, "http://localhost:"+getServerPort()+"/v1/cluster/stop", bytes.NewReader(body))
		if tok := strings.TrimSpace(osGetenv("ORCHESTRATOR_API_TOKEN", "")); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		result := map[string]any{"service": svc}
		if err != nil {
			result["success"] = false
			result["error"] = err.Error()
		} else {
			var rd map[string]any
			json.NewDecoder(resp.Body).Decode(&rd)
			resp.Body.Close()
			result["success"] = rd["success"]
			result["message"] = rd["message"]
		}
		results = append(results, result)
	}

	stopped := 0
	for _, r := range results {
		if s, _ := r["success"].(bool); s {
			stopped++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": stopped > 0,
		"message": fmt.Sprintf("'%s' 그룹: %d/%d 서비스 중지", name, stopped, len(results)),
		"results": results,
	})
}
