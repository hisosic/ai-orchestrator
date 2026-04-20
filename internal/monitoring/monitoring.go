// Package monitoring provides Docker container monitoring, stats collection,
// and system/environment information.
package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"

	rt "ai-container-go/internal/runtime"
)

// ---------------------------------------------------------------------------
// TTL cache for expensive Docker API calls
// ---------------------------------------------------------------------------

type cachedResult struct {
	data      any
	expiresAt time.Time
}

var (
	cacheMu       sync.RWMutex
	cacheStore    = map[string]cachedResult{}
)

func cacheGet(key string) (any, bool) {
	cacheMu.RLock()
	entry, ok := cacheStore[key]
	cacheMu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return entry.data, true
}

func cacheSet(key string, data any, ttl time.Duration) {
	cacheMu.Lock()
	cacheStore[key] = cachedResult{data: data, expiresAt: time.Now().Add(ttl)}
	cacheMu.Unlock()
}

// InvalidateCache removes a specific cache entry (e.g. after image pull/build).
func InvalidateCache(key string) {
	cacheMu.Lock()
	delete(cacheStore, key)
	cacheMu.Unlock()
}

// GetSystemInfo returns host and environment information including Docker server details.
// Results are cached for 30 seconds since Docker daemon info rarely changes.
func GetSystemInfo() map[string]any {
	if cached, ok := cacheGet("system_info"); ok {
		return cached.(map[string]any)
	}

	info := map[string]any{
		"hostname":  hostname(),
		"go":        runtime.Version(),
		"state_dir": envOrDefault("ORCHESTRATOR_STATE_DIR", "/data"),
	}

	cli := rt.DockerClient()
	if cli == nil {
		info["docker"] = map[string]any{"error": "Docker not available"}
		return info
	}

	ctx := context.Background()
	dinfo, err := cli.Info(ctx)
	if err != nil {
		info["docker"] = map[string]any{"error": err.Error()}
		return info
	}

	info["docker"] = map[string]any{
		"containers":         dinfo.Containers,
		"containers_running": dinfo.ContainersRunning,
		"containers_paused":  dinfo.ContainersPaused,
		"containers_stopped": dinfo.ContainersStopped,
		"images":             dinfo.Images,
		"server_version":     dinfo.ServerVersion,
		"operating_system":   dinfo.OperatingSystem,
		"architecture":       dinfo.Architecture,
	}

	cacheSet("system_info", info, 30*time.Second)
	return info
}

// ListContainers returns basic information for all containers.
// If includeStopped is true, stopped containers are included.
// Results are cached for 5 seconds.
func ListContainers(includeStopped bool) []map[string]any {
	cacheKey := "containers_running"
	if includeStopped {
		cacheKey = "containers_all"
	}
	if cached, ok := cacheGet(cacheKey); ok {
		return cached.([]map[string]any)
	}

	cli := rt.DockerClient()
	if cli == nil {
		return []map[string]any{}
	}

	ctx := context.Background()
	opts := container.ListOptions{All: includeStopped}
	containers, err := cli.ContainerList(ctx, opts)
	if err != nil {
		return []map[string]any{}
	}

	out := make([]map[string]any, 0, len(containers))
	for _, c := range containers {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}

		cid := c.ID
		if len(cid) > 12 {
			cid = cid[:12]
		}

		labels := c.Labels
		if labels == nil {
			labels = map[string]string{}
		}

		managed := labels[rt.LabelOrchestrator] == "true"
		service := labels[rt.LabelService]

		out = append(out, map[string]any{
			"id":      cid,
			"name":    name,
			"image":   c.Image,
			"status":  c.Status,
			"state":   c.State,
			"created": c.Created,
			"managed": managed,
			"service": service,
			"ports":   formatPorts(c.Ports),
		})
	}

	cacheSet(cacheKey, out, 5*time.Second)
	return out
}

// GetContainerStats fetches live CPU, memory, and network stats for a single container.
func GetContainerStats(ctx context.Context, cli *client.Client, containerID string) map[string]any {
	resp, err := cli.ContainerStatsOneShot(ctx, containerID)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var stats types.StatsJSON
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&stats); err != nil {
		return nil
	}
	return parseStats(stats)
}

// GetAllContainersWithStats lists all containers and attaches live stats
// for running containers. Stats are fetched in parallel using goroutines
// with a concurrency limit of 10. Results are cached for 5 seconds.
func GetAllContainersWithStats() []map[string]any {
	if cached, ok := cacheGet("containers_with_stats"); ok {
		return cached.([]map[string]any)
	}

	containers := ListContainers(true)

	// Identify running containers.
	type indexedContainer struct {
		idx int
		id  string
	}
	var running []indexedContainer
	for i, c := range containers {
		c["stats"] = nil
		if state, _ := c["state"].(string); strings.EqualFold(state, "running") {
			if id, _ := c["id"].(string); id != "" {
				running = append(running, indexedContainer{idx: i, id: id})
			}
		}
	}
	if len(running) == 0 {
		return containers
	}

	cli := rt.DockerClient()
	if cli == nil {
		return containers
	}

	// Semaphore to limit concurrency.
	sem := make(chan struct{}, 10)
	var mu sync.Mutex
	var wg sync.WaitGroup

	ctx := context.Background()
	for _, rc := range running {
		wg.Add(1)
		go func(idx int, cid string) {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			st := GetContainerStats(ctx, cli, cid)
			mu.Lock()
			containers[idx]["stats"] = st
			mu.Unlock()
		}(rc.idx, rc.id)
	}
	wg.Wait()

	cacheSet("containers_with_stats", containers, 5*time.Second)
	return containers
}

// ListImages returns information about all local Docker images.
// Results are cached for 10 seconds.
func ListImages() []map[string]any {
	if cached, ok := cacheGet("images"); ok {
		return cached.([]map[string]any)
	}

	cli := rt.DockerClient()
	if cli == nil {
		return []map[string]any{}
	}

	ctx := context.Background()
	images, err := cli.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return []map[string]any{}
	}

	out := make([]map[string]any, 0, len(images))
	for _, img := range images {
		tags := img.RepoTags
		if len(tags) == 0 {
			tags = []string{"<none>:<none>"}
		}

		id := img.ID
		id = strings.TrimPrefix(id, "sha256:")
		if len(id) > 12 {
			id = id[:12]
		}

		repo := tags[0]
		tag := "latest"
		if parts := strings.SplitN(tags[0], ":", 2); len(parts) == 2 {
			repo = parts[0]
			tag = parts[1]
		}

		sizeMB := roundTo(float64(img.Size)/(1024*1024), 2)

		created := time.Unix(img.Created, 0).UTC().Format(time.RFC3339)

		out = append(out, map[string]any{
			"id":         id,
			"tags":       tags,
			"repository": repo,
			"tag":        tag,
			"size_mb":    sizeMB,
			"created":    created,
		})
	}

	cacheSet("images", out, 10*time.Second)
	return out
}

// parseStats extracts CPU%, memory, and network I/O from a Docker StatsJSON response.
func parseStats(raw types.StatsJSON) map[string]any {
	out := map[string]any{
		"cpu_percent":      0.0,
		"memory_usage_mb":  0.0,
		"memory_limit_mb":  0.0,
		"memory_percent":   0.0,
		"net_rx_mb":        0.0,
		"net_tx_mb":        0.0,
	}

	// CPU percent calculation.
	cpuDelta := float64(raw.CPUStats.CPUUsage.TotalUsage) - float64(raw.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(raw.CPUStats.SystemUsage) - float64(raw.PreCPUStats.SystemUsage)
	if systemDelta > 0 && cpuDelta > 0 {
		nCPU := len(raw.CPUStats.CPUUsage.PercpuUsage)
		if nCPU == 0 {
			nCPU = 1
		}
		out["cpu_percent"] = roundTo((cpuDelta/systemDelta)*float64(nCPU)*100.0, 2)
	}

	// Memory usage.
	usage := float64(raw.MemoryStats.Usage)
	limit := float64(raw.MemoryStats.Limit)
	out["memory_usage_mb"] = roundTo(usage/(1024*1024), 2)
	if limit > 0 {
		out["memory_limit_mb"] = roundTo(limit/(1024*1024), 2)
		out["memory_percent"] = roundTo((usage/limit)*100.0, 2)
	}

	// Network I/O across all interfaces.
	var rxBytes, txBytes uint64
	for _, net := range raw.Networks {
		rxBytes += net.RxBytes
		txBytes += net.TxBytes
	}
	if rxBytes > 0 {
		out["net_rx_mb"] = roundTo(float64(rxBytes)/(1024*1024), 2)
	}
	if txBytes > 0 {
		out["net_tx_mb"] = roundTo(float64(txBytes)/(1024*1024), 2)
	}

	return out
}

// formatPorts converts a list of Docker API port entries to human-readable strings.
func formatPorts(ports []types.Port) []string {
	out := make([]string, 0, len(ports))
	for _, p := range ports {
		if len(out) >= 10 {
			break
		}
		priv := fmt.Sprintf("%d/%s", p.PrivatePort, p.Type)
		if p.PublicPort > 0 {
			out = append(out, fmt.Sprintf("%s->%d", priv, p.PublicPort))
		} else {
			out = append(out, priv)
		}
	}
	return out
}

// --- helpers ---

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func roundTo(val float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(val*p) / p
}
