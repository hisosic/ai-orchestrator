package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"

	"ai-container-go/internal/runtime"
)

const (
	registryContainerName = "orch-registry"
	registryImage         = "registry:2"
	registryPort          = 5000
	registryVolume        = "orch-registry-data"
	registryLabel         = "ai.orchestrator.registry"
)

// ---------------------------------------------------------------------------
// GET /v1/registry/status
// ---------------------------------------------------------------------------

func handleRegistryStatus(w http.ResponseWriter, r *http.Request) {
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	info := getRegistryInfo(ctx)
	writeJSON(w, http.StatusOK, info)
}

// ---------------------------------------------------------------------------
// POST /v1/registry/enable
// ---------------------------------------------------------------------------

func handleRegistryEnable(w http.ResponseWriter, r *http.Request) {
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
	defer cancel()

	// Check if already running
	info := getRegistryInfo(ctx)
	if info["running"] == true {
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"message": "Registry가 이미 실행 중입니다",
			"registry": info,
		})
		return
	}

	// Pull registry image if not present
	log.Printf("[registry] Pulling image %s", registryImage)
	pullOut, err := cli.ImagePull(ctx, registryImage, image.PullOptions{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("Registry 이미지 Pull 실패: %v", err),
		})
		return
	}
	io.Copy(io.Discard, pullOut)
	pullOut.Close()

	// Determine host port
	hostPort := registryPort
	advertiseAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	hostIP := ""
	if advertiseAddr != "" {
		parts := strings.Split(advertiseAddr, ":")
		hostIP = parts[0]
	}

	// Ensure orch-internal network
	networkID := runtime.EnsureNetwork(ctx, cli)

	// Create and start registry container
	portBinding := nat.PortMap{
		nat.Port(fmt.Sprintf("%d/tcp", registryPort)): []nat.PortBinding{
			{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", hostPort)},
		},
	}

	containerConfig := &container.Config{
		Image: registryImage,
		ExposedPorts: nat.PortSet{
			nat.Port(fmt.Sprintf("%d/tcp", registryPort)): struct{}{},
		},
		Labels: map[string]string{
			runtime.LabelOrchestrator: "true",
			runtime.LabelService:      "registry",
			registryLabel:             "true",
		},
		Env: []string{
			"REGISTRY_STORAGE_DELETE_ENABLED=true",
		},
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBinding,
		RestartPolicy: container.RestartPolicy{
			Name: container.RestartPolicyUnlessStopped,
		},
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Source: registryVolume,
				Target: "/var/lib/registry",
			},
		},
	}

	networkConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			runtime.OrchNetwork: {
				NetworkID: networkID,
				Aliases:   []string{"registry", registryContainerName},
			},
		},
	}

	log.Printf("[registry] Creating container %s", registryContainerName)
	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, networkConfig, nil, registryContainerName)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("Registry 컨테이너 생성 실패: %v", err),
		})
		return
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("Registry 컨테이너 시작 실패: %v", err),
		})
		return
	}

	log.Printf("[registry] Registry started: container=%s port=%d", resp.ID[:12], hostPort)

	registryURL := fmt.Sprintf("http://%s:%d", hostIP, hostPort)
	internalURL := fmt.Sprintf("http://%s:%d", registryContainerName, registryPort)

	if hub != nil {
		if data, err := json.Marshal(map[string]any{
			"event": "registry-enabled", "url": registryURL,
		}); err == nil {
			hub.broadcast(data)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":       true,
		"message":       fmt.Sprintf("Container Registry 활성화 완료 (포트: %d)", hostPort),
		"container_id":  resp.ID[:12],
		"registry_url":  registryURL,
		"internal_url":  internalURL,
		"host_port":     hostPort,
		"push_example":  fmt.Sprintf("docker tag myimage %s:%d/myimage:latest && docker push %s:%d/myimage:latest", hostIP, hostPort, hostIP, hostPort),
		"pull_example":  fmt.Sprintf("docker pull %s:%d/myimage:latest", hostIP, hostPort),
	})
}

// ---------------------------------------------------------------------------
// POST /v1/registry/disable
// ---------------------------------------------------------------------------

func handleRegistryDisable(w http.ResponseWriter, r *http.Request) {
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	cID := findRegistryContainer(ctx)
	if cID == "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"success": true, "message": "Registry가 실행 중이 아닙니다",
		})
		return
	}

	timeout := 10
	if err := cli.ContainerStop(ctx, cID, container.StopOptions{Timeout: &timeout}); err != nil {
		log.Printf("[registry] Stop error: %v", err)
	}
	if err := cli.ContainerRemove(ctx, cID, container.RemoveOptions{Force: true}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("Registry 컨테이너 제거 실패: %v", err),
		})
		return
	}

	log.Printf("[registry] Registry disabled")

	if hub != nil {
		if data, err := json.Marshal(map[string]any{"event": "registry-disabled"}); err == nil {
			hub.broadcast(data)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true, "message": "Container Registry 비활성화 완료",
	})
}

// ---------------------------------------------------------------------------
// GET /v1/registry/catalog — list repositories
// ---------------------------------------------------------------------------

func handleRegistryCatalog(w http.ResponseWriter, r *http.Request) {
	registryURL := getRegistryInternalURL(r.Context())
	if registryURL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false, "message": "Registry가 실행 중이 아닙니다. 먼저 활성화하세요.",
		})
		return
	}

	resp, err := http.Get(registryURL + "/v2/_catalog")
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"success": false, "message": fmt.Sprintf("Registry 연결 실패: %v", err),
		})
		return
	}
	defer resp.Body.Close()

	var catalog struct {
		Repositories []string `json:"repositories"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&catalog); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"success": false, "message": fmt.Sprintf("응답 파싱 실패: %v", err),
		})
		return
	}

	// Fetch tags for each repository
	type repoInfo struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	var repos []repoInfo
	for _, name := range catalog.Repositories {
		tags := fetchTags(registryURL, name)
		repos = append(repos, repoInfo{Name: name, Tags: tags})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"repositories": repos,
		"total":        len(repos),
	})
}

// ---------------------------------------------------------------------------
// GET /v1/registry/tags/{name} — list tags for a repository
// ---------------------------------------------------------------------------

func handleRegistryTags(w http.ResponseWriter, r *http.Request) {
	registryURL := getRegistryInternalURL(r.Context())
	if registryURL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false, "message": "Registry가 실행 중이 아닙니다",
		})
		return
	}

	// Extract repo name from URL path (everything after /v1/registry/tags/)
	name := strings.TrimPrefix(r.URL.Path, "/v1/registry/tags/")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "리포지토리 이름이 필요합니다",
		})
		return
	}

	tags := fetchTags(registryURL, name)
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"name":    name,
		"tags":    tags,
	})
}

// ---------------------------------------------------------------------------
// /v1/registry/v2/* — reverse proxy to registry HTTP API
// Allows direct Docker push/pull through the orchestrator endpoint
// ---------------------------------------------------------------------------

func registryProxy() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		registryURL := getRegistryInternalURL(r.Context())
		if registryURL == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"success": false, "message": "Registry가 실행 중이 아닙니다",
			})
			return
		}

		target, _ := url.Parse(registryURL)
		proxy := httputil.NewSingleHostReverseProxy(target)

		// Rewrite path: /v1/registry/v2/* → /v2/*
		originalDirector := proxy.Director
		proxy.Director = func(req *http.Request) {
			originalDirector(req)
			req.URL.Path = strings.TrimPrefix(req.URL.Path, "/v1/registry")
			req.URL.RawPath = strings.TrimPrefix(req.URL.RawPath, "/v1/registry")
			req.Host = target.Host
		}

		proxy.ServeHTTP(w, r)
	}
}

// ---------------------------------------------------------------------------
// POST /v1/registry/push — push a local image to the registry
// Body: {"image": "nginx:latest", "tag": "myapp:v1"}
// ---------------------------------------------------------------------------

func handleRegistryPush(w http.ResponseWriter, r *http.Request) {
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	info := getRegistryInfo(ctx)
	if info["running"] != true {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false, "message": "Registry가 실행 중이 아닙니다",
		})
		return
	}

	var req struct {
		Image string `json:"image"` // source image (local)
		Tag   string `json:"tag"`   // target name in registry (optional, defaults to image name)
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "잘못된 요청입니다",
		})
		return
	}
	if req.Image == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "image 필드가 필요합니다",
		})
		return
	}

	registryAddr := fmt.Sprintf("localhost:%d", registryPort)
	targetTag := req.Tag
	if targetTag == "" {
		// Strip registry prefix if present, use base name
		parts := strings.Split(req.Image, "/")
		targetTag = parts[len(parts)-1]
	}
	registryTag := fmt.Sprintf("%s/%s", registryAddr, targetTag)

	// Tag the image for the local registry
	if err := cli.ImageTag(ctx, req.Image, registryTag); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("이미지 태그 실패: %v", err),
		})
		return
	}

	// Push to registry
	pushOut, err := cli.ImagePush(ctx, registryTag, image.PushOptions{RegistryAuth: "e30="}) // empty auth base64("{}")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("이미지 Push 실패: %v", err),
		})
		return
	}
	defer pushOut.Close()

	// Read push output to completion
	var lastErr string
	decoder := json.NewDecoder(pushOut)
	for {
		var msg struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if err := decoder.Decode(&msg); err != nil {
			break
		}
		if msg.Error != "" {
			lastErr = msg.Error
		}
	}

	if lastErr != "" {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("Push 실패: %s", lastErr),
		})
		return
	}

	// Build external registry tag for response
	externalAddr := info["endpoint"]
	externalTag := fmt.Sprintf("%s/%s", externalAddr, targetTag)

	log.Printf("[registry] Pushed %s → %s", req.Image, registryTag)

	writeJSON(w, http.StatusOK, map[string]any{
		"success":       true,
		"message":       fmt.Sprintf("이미지 '%s'를 Registry에 Push 완료", req.Image),
		"registry_tag":  registryTag,
		"external_tag":  externalTag,
		"pull_command":  fmt.Sprintf("docker pull %s", externalTag),
	})
}

// ---------------------------------------------------------------------------
// POST /v1/registry/pull — pull an image from the registry to local
// Body: {"image": "myapp:v1"}
// ---------------------------------------------------------------------------

func handleRegistryPull(w http.ResponseWriter, r *http.Request) {
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	info := getRegistryInfo(ctx)
	if info["running"] != true {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false, "message": "Registry가 실행 중이 아닙니다",
		})
		return
	}

	var req struct {
		Image string `json:"image"` // image name in registry (e.g. "myapp:v1")
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Image == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "image 필드가 필요합니다",
		})
		return
	}

	registryAddr := fmt.Sprintf("localhost:%d", registryPort)
	fullImage := req.Image
	// Add registry prefix if not already present
	if !strings.Contains(fullImage, registryAddr) {
		fullImage = fmt.Sprintf("%s/%s", registryAddr, req.Image)
	}

	pullOut, err := cli.ImagePull(ctx, fullImage, image.PullOptions{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("이미지 Pull 실패: %v", err),
		})
		return
	}
	defer pullOut.Close()
	io.Copy(io.Discard, pullOut)

	log.Printf("[registry] Pulled %s", fullImage)

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("이미지 '%s' Pull 완료", fullImage),
		"image":   fullImage,
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func findRegistryContainer(ctx context.Context) string {
	cli := runtime.DockerClient()
	if cli == nil {
		return ""
	}

	containers, err := cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("name", registryContainerName)),
	})
	if err != nil || len(containers) == 0 {
		return ""
	}
	return containers[0].ID
}

func getRegistryInfo(ctx context.Context) map[string]any {
	cli := runtime.DockerClient()
	if cli == nil {
		return map[string]any{"running": false, "enabled": false}
	}

	containers, err := cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("name", registryContainerName)),
	})
	if err != nil || len(containers) == 0 {
		return map[string]any{"running": false, "enabled": false}
	}

	c := containers[0]
	running := strings.HasPrefix(strings.ToLower(c.State), "running")

	// Get host port
	hostPort := registryPort
	for _, p := range c.Ports {
		if int(p.PrivatePort) == registryPort && p.PublicPort > 0 {
			hostPort = int(p.PublicPort)
			break
		}
	}
	_ = hostPort

	advertiseAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	hostIP := "localhost"
	if advertiseAddr != "" {
		parts := strings.Split(advertiseAddr, ":")
		hostIP = parts[0]
	}

	endpoint := fmt.Sprintf("%s:%d", hostIP, registryPort)

	return map[string]any{
		"running":      running,
		"enabled":      true,
		"container_id": c.ID[:12],
		"state":        c.State,
		"endpoint":     endpoint,
		"registry_url": fmt.Sprintf("http://%s", endpoint),
		"internal_url": fmt.Sprintf("http://%s:%d", registryContainerName, registryPort),
		"host_port":    registryPort,
	}
}

func getRegistryInternalURL(ctx context.Context) string {
	info := getRegistryInfo(ctx)
	if info["running"] != true {
		return ""
	}
	// Use Docker network DNS name (orch-internal) or host IP for access from within containers
	if url, ok := info["internal_url"].(string); ok && url != "" {
		return url
	}
	advertiseAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	if advertiseAddr != "" {
		hostIP := strings.Split(advertiseAddr, ":")[0]
		return fmt.Sprintf("http://%s:%d", hostIP, registryPort)
	}
	return fmt.Sprintf("http://localhost:%d", registryPort)
}

func fetchTags(registryURL, repoName string) []string {
	resp, err := http.Get(fmt.Sprintf("%s/v2/%s/tags/list", registryURL, repoName))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var tagList struct {
		Tags []string `json:"tags"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tagList); err != nil {
		return nil
	}
	return tagList.Tags
}

// isRegistryRunning returns true if the local registry container is running.
func isRegistryRunning(ctx context.Context) bool {
	info := getRegistryInfo(ctx)
	return info["running"] == true
}

// registryAddr returns the Docker-daemon-accessible registry address (e.g. "localhost:5000").
// Returns "" if registry is not running.
func registryAddr(ctx context.Context) string {
	if !isRegistryRunning(ctx) {
		return ""
	}
	return fmt.Sprintf("localhost:%d", registryPort)
}

// pushImageToRegistry tags a local image and pushes it to the internal registry.
// Returns the registry-qualified image name (e.g. "localhost:5000/myapp:latest") or error.
func pushImageToRegistry(ctx context.Context, localImage string, repoTag string) (string, error) {
	cli := runtime.DockerClient()
	if cli == nil {
		return "", fmt.Errorf("Docker 연결 실패")
	}

	addr := registryAddr(ctx)
	if addr == "" {
		return "", fmt.Errorf("Registry가 실행 중이 아닙니다")
	}

	registryImage := fmt.Sprintf("%s/%s", addr, repoTag)

	// Tag
	if err := cli.ImageTag(ctx, localImage, registryImage); err != nil {
		return "", fmt.Errorf("이미지 태그 실패: %w", err)
	}

	// Push
	pushOut, err := cli.ImagePush(ctx, registryImage, image.PushOptions{RegistryAuth: "e30="})
	if err != nil {
		return "", fmt.Errorf("이미지 Push 실패: %w", err)
	}
	defer pushOut.Close()

	var lastErr string
	decoder := json.NewDecoder(pushOut)
	for {
		var msg struct {
			Error string `json:"error"`
		}
		if err := decoder.Decode(&msg); err != nil {
			break
		}
		if msg.Error != "" {
			lastErr = msg.Error
		}
	}

	if lastErr != "" {
		return "", fmt.Errorf("Push 실패: %s", lastErr)
	}

	log.Printf("[registry] Pushed %s → %s", localImage, registryImage)
	return registryImage, nil
}

// registryExternalTag returns the externally accessible registry tag for an image.
// e.g. "20.20.6.248:5000/myapp:latest"
func registryExternalTag(repoTag string) string {
	advertiseAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	hostIP := "localhost"
	if advertiseAddr != "" {
		parts := strings.Split(advertiseAddr, ":")
		hostIP = parts[0]
	}
	return fmt.Sprintf("%s:%d/%s", hostIP, registryPort, repoTag)
}
