// Package server — managed persistent storage for services.
//
// Users ask for container paths to keep (persistent_paths: ["/data"]); the
// orchestrator turns each into a Docker named volume
//
//	orch-data-<owner>-<service>-<id>-<owner-hash>
//
// where <id> is a slug of the container path, or — when the user names the
// volume explicitly (named_volumes: [{name, path}]) — a slug of that name.
// The name reads as account_service_volume, is unique per owner, and is
// mounted at the requested path. Named volumes live on one node's disk, so a service
// with managed volumes is pinned to the node it first ran on (stored as a
// single-node affinity) and limited to one replica; restarts, image updates,
// scale 0→1 and auto-heal all land on that node and remount the same data.
// Moving it with migrate/drain is refused. The owner hash means a different
// user who later reuses the service name gets a fresh volume, never the
// previous owner's data.
//
// Raw "host:container" volume specs mount arbitrary host paths, so only
// admins (browser session) and inter-node calls may pass them.
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/models"
	"ai-container-go/internal/runtime"
)

const (
	managedVolumePrefix = "orch-data-"
	maxPersistentPaths  = 5
)

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(s, fallback string) string {
	slug := strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(s), "-"), "-")
	if slug == "" {
		slug = fallback
	}
	if len(slug) > 40 {
		slug = strings.Trim(slug[:40], "-")
	}
	return slug
}

// managedVolumeName builds the name of a service's managed volume for a given
// identifier (a slug of the container path, or of a user-chosen volume name).
// The name reads as orch-data-<owner>-<service>-<id>-<owner-hash>: grouped by
// account and service, and unique per owner so a later reuse of the service
// name never reattaches the previous owner's data.
func managedVolumeName(service, owner, id string) string {
	acct := slugify(safeRepoPrefix(owner), "user")
	sum := sha256.Sum256([]byte(owner))
	return fmt.Sprintf("%s%s-%s-%s-%s", managedVolumePrefix, acct, service, slugify(id, "root"), hex.EncodeToString(sum[:])[:8])
}

// persistentVolumeName is the named volume backing containerPath of a
// service owned by owner.
func persistentVolumeName(service, owner, containerPath string) string {
	return managedVolumeName(service, owner, containerPath)
}

// namedVolume is a user-named managed volume: a volume name plus the container
// path it mounts at.
type namedVolume struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// normalizeNamedVolumes validates user-named volumes: each needs a name and a
// valid container path, and paths must not repeat.
func normalizeNamedVolumes(in []namedVolume) ([]namedVolume, error) {
	seenPath := map[string]bool{}
	seenName := map[string]bool{}
	var out []namedVolume
	for _, v := range in {
		name := strings.TrimSpace(v.Name)
		if name == "" {
			return nil, fmt.Errorf("볼륨 이름을 지정하세요")
		}
		if slugify(name, "") == "" {
			return nil, fmt.Errorf("볼륨 이름 '%s'에는 영문/숫자가 포함되어야 합니다", name)
		}
		paths, err := normalizePersistentPaths([]string{v.Path})
		if err != nil {
			return nil, err
		}
		if len(paths) == 0 {
			return nil, fmt.Errorf("볼륨 '%s'의 마운트 경로(예: /data)를 지정하세요", name)
		}
		p := paths[0]
		if seenPath[p] {
			return nil, fmt.Errorf("마운트 경로 '%s'가 중복됩니다", p)
		}
		if seenName[slugify(name, "")] {
			return nil, fmt.Errorf("볼륨 이름 '%s'가 중복됩니다", name)
		}
		seenPath[p], seenName[slugify(name, "")] = true, true
		out = append(out, namedVolume{Name: name, Path: p})
	}
	return out, nil
}

// normalizePersistentPaths validates requested container paths.
func normalizePersistentPaths(paths []string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !strings.HasPrefix(p, "/") || strings.ContainsAny(p, ":,") || strings.Contains(p, "..") {
			return nil, fmt.Errorf("영구 저장 경로 '%s'가 올바르지 않습니다 (예: /data)", p)
		}
		p = path.Clean(p)
		if p == "/" {
			return nil, fmt.Errorf("컨테이너 루트(/) 전체는 영구 저장소로 지정할 수 없습니다")
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	if len(out) > maxPersistentPaths {
		return nil, fmt.Errorf("영구 저장 경로는 최대 %d개까지 지정할 수 있습니다", maxPersistentPaths)
	}
	return out, nil
}

// serviceVolumeSpecs returns the stored volume specs of a service definition.
func serviceVolumeSpecs(info map[string]any) []string {
	var specs []string
	switch v := info["volumes"].(type) {
	case string:
		json.Unmarshal([]byte(v), &specs)
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok {
				specs = append(specs, s)
			}
		}
	}
	return specs
}

// managedVolumeSpecs filters specs down to orchestrator-managed volumes.
func managedVolumeSpecs(specs []string) []string {
	var out []string
	for _, s := range specs {
		if strings.HasPrefix(strings.TrimSpace(s), managedVolumePrefix) {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

// persistentPathsOf lists the container paths of a service's managed volumes.
func persistentPathsOf(info map[string]any) []string {
	var out []string
	for _, s := range managedVolumeSpecs(serviceVolumeSpecs(info)) {
		if parts := strings.SplitN(s, ":", 3); len(parts) >= 2 {
			out = append(out, parts[1])
		}
	}
	return out
}

func volumeNameOfSpec(spec string) string {
	return strings.SplitN(spec, ":", 2)[0]
}

// pinnedNodeOf returns the single node a service is pinned to, if any.
func pinnedNodeOf(info map[string]any) string {
	raw, _ := info["schedule_constraints"].(string)
	if raw == "" {
		return ""
	}
	var c models.ScheduleConstraints
	if json.Unmarshal([]byte(raw), &c) == nil && len(c.NodeAffinity) == 1 {
		return c.NodeAffinity[0]
	}
	return ""
}

// servicePersistent reports whether a service keeps data in managed volumes.
func servicePersistent(name string) bool {
	if clusterState == nil {
		return false
	}
	info := clusterState.GetService(name)
	return info != nil && len(managedVolumeSpecs(serviceVolumeSpecs(info))) > 0
}

// persistentSpecsFor returns the managed volume specs of a service, if any.
func persistentSpecsFor(service string) []string {
	if clusterState == nil {
		return nil
	}
	info := clusterState.GetService(service)
	if info == nil {
		return nil
	}
	return managedVolumeSpecs(serviceVolumeSpecs(info))
}

// mayUseRawVolumes: arbitrary host-path mounts are for admins (browser
// session) and trusted inter-node calls only.
func mayUseRawVolumes(r *http.Request) bool {
	sess := auth.SessionFromRequest(r)
	if sess == nil {
		return true // only the cluster-token path reaches mutating handlers without a session
	}
	return sess.Role == auth.RoleAdmin && !sess.Internal
}

// persistencePlan is the storage/placement outcome for a deploy.
type persistencePlan struct {
	specs []string // managed volume specs to mount
	node  string   // pinned node ("" = not persistent or not yet placed)
}

// planPersistence merges newly requested paths and user-named volumes with the
// service's existing managed volumes (existing names are kept so data carries
// over) and works out the pinned node.
func planPersistence(service, owner string, existing map[string]any, requested []string, named []namedVolume) (persistencePlan, error) {
	paths, err := normalizePersistentPaths(requested)
	if err != nil {
		return persistencePlan{}, err
	}
	nvs, err := normalizeNamedVolumes(named)
	if err != nil {
		return persistencePlan{}, err
	}
	var plan persistencePlan
	have := map[string]bool{}
	if existing != nil {
		if o, _ := existing["owner"].(string); o != "" {
			owner = o // keep volume names stable when an admin redeploys
		}
		for _, s := range managedVolumeSpecs(serviceVolumeSpecs(existing)) {
			plan.specs = append(plan.specs, s)
			if parts := strings.SplitN(s, ":", 3); len(parts) >= 2 {
				have[parts[1]] = true
			}
		}
		plan.node = pinnedNodeOf(existing)
		if plan.node == "" && len(plan.specs) > 0 {
			if ps := clusterState.GetPlacements(service, ""); len(ps) > 0 {
				plan.node = ps[0].NodeName
			}
		}
	}
	for _, p := range paths {
		if !have[p] {
			plan.specs = append(plan.specs, persistentVolumeName(service, owner, p)+":"+p)
			have[p] = true
		}
	}
	for _, v := range nvs {
		if !have[v.Path] {
			plan.specs = append(plan.specs, managedVolumeName(service, owner, v.Name)+":"+v.Path)
			have[v.Path] = true
		}
	}
	if n := len(managedVolumeSpecs(plan.specs)); n > maxPersistentPaths {
		return persistencePlan{}, fmt.Errorf("영구 저장소는 최대 %d개까지 지정할 수 있습니다", maxPersistentPaths)
	}
	return plan, nil
}

// pinnedDecision validates that a pinned node can take the service.
func pinnedDecision(service, nodeName string, count int) ([]models.ScheduleDecision, error) {
	node := clusterState.GetNode(nodeName)
	if node == nil {
		return nil, fmt.Errorf("'%s'의 데이터가 있는 노드(%s)를 찾을 수 없습니다", service, nodeName)
	}
	if node.Status == models.NodeOffline || node.Status == models.NodeCordoned || node.Status == models.NodeDraining {
		return nil, fmt.Errorf("'%s'의 데이터가 있는 노드(%s)가 %s 상태라 지금은 기동할 수 없습니다", service, nodeName, node.Status)
	}
	return []models.ScheduleDecision{{NodeName: nodeName, Count: count}}, nil
}

func pinConstraints(nodeName string) map[string]any {
	return map[string]any{"node_affinity": []string{nodeName}}
}

// purgeServiceVolumes removes a deleted service's managed volumes on every
// node that might hold them. Returns the names removed per node.
func purgeServiceVolumes(service string, specs []string) map[string]any {
	var names []string
	for _, s := range managedVolumeSpecs(specs) {
		names = append(names, volumeNameOfSpec(s))
	}
	out := map[string]any{}
	if len(names) == 0 || clusterState == nil {
		return out
	}
	payload, _ := json.Marshal(map[string]any{"service_name": service, "names": names})
	for _, n := range clusterState.ListNodes() {
		node := n
		req, _ := http.NewRequest(http.MethodPost, nodeBaseURL(&node)+"/v1/agent/volume-remove", bytes.NewReader(payload))
		setNodeHeaders(req, &node)
		resp, err := httpClient.Do(req)
		if err != nil {
			continue
		}
		var d struct {
			Removed []string `json:"removed"`
		}
		json.NewDecoder(resp.Body).Decode(&d)
		resp.Body.Close()
		if len(d.Removed) > 0 {
			out[node.Name] = d.Removed
		}
	}
	return out
}

// handleAgentVolumeRemove: POST /v1/agent/volume-remove {service_name, names}
// (cluster token). Removes only orch-data-<service>-* volumes.
func handleAgentVolumeRemove(w http.ResponseWriter, r *http.Request) {
	if !agentTokenOK(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "message": "Unauthorized"})
		return
	}
	var body struct {
		ServiceName string   `json:"service_name"`
		Names       []string `json:"names"`
	}
	if err := readJSON(r, &body); err != nil || strings.TrimSpace(body.ServiceName) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "service_name required"})
		return
	}
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "message": "Docker connection failed"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	// Names come from the orchestrator (the deleted service's own stored, owner-
	// checked specs). Guard anyway: only orchestrator-managed volumes whose name
	// carries this service as a segment may be removed.
	removed := []string{}
	for _, name := range body.Names {
		if !strings.HasPrefix(name, managedVolumePrefix) || !strings.Contains(name, "-"+body.ServiceName+"-") {
			continue
		}
		if err := cli.VolumeRemove(ctx, name, false); err == nil {
			removed = append(removed, name)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "removed": removed})
}

// serviceVolumeReport describes a service's persistent storage for UIs/MCP.
func serviceVolumeReport(name string) map[string]any {
	info := clusterState.GetService(name)
	if info == nil {
		return nil
	}
	specs := managedVolumeSpecs(serviceVolumeSpecs(info))
	if len(specs) == 0 {
		return nil
	}
	node := pinnedNodeOf(info)
	vols := []map[string]any{}
	for _, s := range specs {
		parts := strings.SplitN(s, ":", 3)
		v := map[string]any{"volume": parts[0], "node": node}
		if len(parts) >= 2 {
			v["path"] = parts[1]
		}
		vols = append(vols, v)
	}
	return map[string]any{"node": node, "volumes": vols}
}
