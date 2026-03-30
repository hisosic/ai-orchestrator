// Package agent implements the worker agent that runs on each node,
// periodically sends heartbeats to the master, and handles container
// migration export/import operations.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"ai-container-go/internal/models"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

// WorkerAgent runs on each worker node to communicate with the master.
type WorkerAgent struct {
	NodeName          string
	MasterURL         string
	Token             string
	HeartbeatInterval time.Duration
	AgentVersion      string
	running           bool
	stopCh            chan struct{}
	dockerClient      *client.Client
}

// NewWorkerAgent creates a new WorkerAgent, reading configuration from
// environment variables with sensible defaults.
func NewWorkerAgent() *WorkerAgent {
	nodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if nodeName == "" {
		h, err := os.Hostname()
		if err == nil {
			nodeName = h
		} else {
			nodeName = "unknown"
		}
	}

	masterURL := os.Getenv("ORCHESTRATOR_MASTER_URL")
	token := os.Getenv("ORCHESTRATOR_API_TOKEN")

	intervalSec := 10
	if v := os.Getenv("ORCHESTRATOR_HEARTBEAT_INTERVAL"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			intervalSec = parsed
		}
	}

	var dockerCli *client.Client
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Printf("[agent] Failed to connect to Docker: %v", err)
	} else {
		dockerCli = cli
	}

	return &WorkerAgent{
		NodeName:          nodeName,
		MasterURL:         masterURL,
		Token:             token,
		HeartbeatInterval: time.Duration(intervalSec) * time.Second,
		AgentVersion:      "0.2.0",
		running:           false,
		stopCh:            make(chan struct{}),
		dockerClient:      dockerCli,
	}
}

// Start begins the heartbeat loop in a background goroutine.
// If no master URL is configured, heartbeating is skipped.
func (a *WorkerAgent) Start() {
	if a.MasterURL == "" {
		log.Println("[agent] No master URL configured, agent heartbeat disabled")
		return
	}

	a.running = true
	go a.heartbeatLoop()
	log.Printf("[agent] Worker agent started: node=%s, master=%s, interval=%s",
		a.NodeName, a.MasterURL, a.HeartbeatInterval)
}

// Stop signals the heartbeat loop to terminate.
func (a *WorkerAgent) Stop() {
	if !a.running {
		return
	}
	a.running = false
	close(a.stopCh)
	log.Println("[agent] Worker agent stopped")
}

// heartbeatLoop periodically sends heartbeat payloads to the master.
func (a *WorkerAgent) heartbeatLoop() {
	ticker := time.NewTicker(a.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
			a.sendHeartbeat()
		}
	}
}

// sendHeartbeat builds and POSTs a single heartbeat to the master.
func (a *WorkerAgent) sendHeartbeat() {
	payload := a.buildHeartbeat()

	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[agent] Failed to marshal heartbeat: %v", err)
		return
	}

	url := strings.TrimRight(a.MasterURL, "/") + "/v1/cluster/heartbeat"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("[agent] Failed to create heartbeat request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if a.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.Token)
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("[agent] Heartbeat error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var hbResp models.HeartbeatResponse
		if err := json.NewDecoder(resp.Body).Decode(&hbResp); err == nil {
			for _, cmd := range hbResp.Commands {
				a.handleMasterCommand(cmd)
			}
		}
	} else {
		log.Printf("[agent] Heartbeat failed: %d", resp.StatusCode)
	}
}

// buildHeartbeat constructs the heartbeat payload.
func (a *WorkerAgent) buildHeartbeat() map[string]any {
	resources := a.GetNodeResources()
	containers := a.GetManagedContainers()

	addr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	return map[string]any{
		"node_name":     a.NodeName,
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
		"resources":     resources,
		"containers":    containers,
		"agent_version": a.AgentVersion,
		"address":       addr,
	}
}

// GetNodeResources returns current node resource usage including CPU, memory,
// disk, network, and container counts.
func (a *WorkerAgent) GetNodeResources() map[string]any {
	cpuCores, _ := cpu.Counts(true)
	cpuPercents, _ := cpu.Percent(100*time.Millisecond, false)
	cpuPct := 0.0
	if len(cpuPercents) > 0 {
		cpuPct = math.Round(cpuPercents[0]*10) / 10
	}

	vmStat, _ := mem.VirtualMemory()
	memTotalMB := 0
	memUsedMB := 0
	if vmStat != nil {
		memTotalMB = int(vmStat.Total / 1024 / 1024)
		memUsedMB = int(vmStat.Used / 1024 / 1024)
	}

	diskStat, _ := disk.Usage("/")
	diskTotalGB := 0.0
	diskUsedGB := 0.0
	if diskStat != nil {
		diskTotalGB = math.Round(float64(diskStat.Total)/1024/1024/1024*10) / 10
		diskUsedGB = math.Round(float64(diskStat.Used)/1024/1024/1024*10) / 10
	}

	containersRunning := 0
	containersTotal := 0
	if a.dockerClient != nil {
		ctx := context.Background()
		allContainers, err := a.dockerClient.ContainerList(ctx, container.ListOptions{All: true})
		if err == nil {
			containersTotal = len(allContainers)
			for _, c := range allContainers {
				if c.State == "running" {
					containersRunning++
				}
			}
		}
	}

	netRxMB, netTxMB := a.getHostNetwork()

	return map[string]any{
		"cpu_cores":           cpuCores,
		"cpu_used_percent":    cpuPct,
		"memory_total_mb":     memTotalMB,
		"memory_used_mb":      memUsedMB,
		"disk_total_gb":       diskTotalGB,
		"disk_used_gb":        diskUsedGB,
		"net_rx_mb":           netRxMB,
		"net_tx_mb":           netTxMB,
		"containers_running":  containersRunning,
		"containers_total":    containersTotal,
	}
}

// getHostNetwork reads host network stats from /host/net/dev (populated by
// host cron) or falls back to nsenter into PID 1 network namespace.
func (a *WorkerAgent) getHostNetwork() (float64, float64) {
	// Method 1: Host-written file
	if rxMB, txMB, ok := parseNetDev("/host/net/dev"); ok {
		return rxMB, txMB
	}

	// Method 2: nsenter into host PID 1 network namespace
	if a.dockerClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, "nsenter", "--target", "1", "--net", "cat", "/proc/net/dev")
		out, err := cmd.Output()
		if err == nil {
			if rxMB, txMB, ok := parseNetDevContent(string(out)); ok {
				return rxMB, txMB
			}
		}
	}

	return 0.0, 0.0
}

// parseNetDev reads and parses a /proc/net/dev formatted file.
func parseNetDev(path string) (float64, float64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		return 0, 0, false
	}
	return parseNetDevContent(string(content))
}

// parseNetDevContent parses /proc/net/dev formatted content, summing physical
// interfaces (ens*, eth*, em*, bond*, enp*).
func parseNetDevContent(content string) (float64, float64, bool) {
	lines := strings.Split(content, "\n")
	if len(lines) < 3 {
		return 0, 0, false
	}

	var rxTotal, txTotal int64
	for _, line := range lines[2:] {
		line = strings.TrimSpace(line)
		parts := strings.Fields(line)
		if len(parts) < 10 {
			continue
		}
		iface := strings.TrimRight(parts[0], ":")
		if strings.HasPrefix(iface, "ens") ||
			strings.HasPrefix(iface, "eth") ||
			strings.HasPrefix(iface, "em") ||
			strings.HasPrefix(iface, "bond") ||
			strings.HasPrefix(iface, "enp") {
			rx, _ := strconv.ParseInt(parts[1], 10, 64)
			tx, _ := strconv.ParseInt(parts[9], 10, 64)
			rxTotal += rx
			txTotal += tx
		}
	}

	if rxTotal > 0 || txTotal > 0 {
		rxMB := math.Round(float64(rxTotal)/1024/1024*100) / 100
		txMB := math.Round(float64(txTotal)/1024/1024*100) / 100
		return rxMB, txMB, true
	}
	return 0, 0, false
}

// getContainerStats retrieves CPU, memory and network stats for a single
// running container via the Docker stats API.
func (a *WorkerAgent) getContainerStats(containerID string) map[string]any {
	result := map[string]any{
		"cpu_percent":    0.0,
		"memory_mb":      0.0,
		"memory_limit_mb": 0.0,
		"net_rx_mb":      0.0,
		"net_tx_mb":      0.0,
	}

	if a.dockerClient == nil {
		return result
	}

	ctx := context.Background()
	stats, err := a.dockerClient.ContainerStats(ctx, containerID, false)
	if err != nil {
		return result
	}
	defer stats.Body.Close()

	var raw struct {
		CPUStats struct {
			CPUUsage struct {
				TotalUsage  uint64   `json:"total_usage"`
				PercpuUsage []uint64 `json:"percpu_usage"`
			} `json:"cpu_usage"`
			SystemCPUUsage uint64 `json:"system_cpu_usage"`
		} `json:"cpu_stats"`
		PreCPUStats struct {
			CPUUsage struct {
				TotalUsage uint64 `json:"total_usage"`
			} `json:"cpu_usage"`
			SystemCPUUsage uint64 `json:"system_cpu_usage"`
		} `json:"precpu_stats"`
		MemoryStats struct {
			Usage uint64 `json:"usage"`
			Limit uint64 `json:"limit"`
		} `json:"memory_stats"`
		Networks map[string]struct {
			RxBytes uint64 `json:"rx_bytes"`
			TxBytes uint64 `json:"tx_bytes"`
		} `json:"networks"`
	}

	if err := json.NewDecoder(stats.Body).Decode(&raw); err != nil {
		return result
	}

	// CPU percentage
	cpuDelta := float64(raw.CPUStats.CPUUsage.TotalUsage - raw.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(raw.CPUStats.SystemCPUUsage - raw.PreCPUStats.SystemCPUUsage)
	nCPU := len(raw.CPUStats.CPUUsage.PercpuUsage)
	if nCPU == 0 {
		nCPU = 1
	}
	cpuPct := 0.0
	if sysDelta > 0 && cpuDelta > 0 {
		cpuPct = math.Round((cpuDelta/sysDelta)*float64(nCPU)*100*100) / 100
	}

	// Memory
	memUsage := math.Round(float64(raw.MemoryStats.Usage)/1024/1024*10) / 10
	memLimit := math.Round(float64(raw.MemoryStats.Limit)/1024/1024*10) / 10

	// Network
	var netRx, netTx uint64
	for _, v := range raw.Networks {
		netRx += v.RxBytes
		netTx += v.TxBytes
	}

	result["cpu_percent"] = cpuPct
	result["memory_mb"] = memUsage
	result["memory_limit_mb"] = memLimit
	result["net_rx_mb"] = math.Round(float64(netRx)/1024/1024*100) / 100
	result["net_tx_mb"] = math.Round(float64(netTx)/1024/1024*100) / 100

	return result
}

// GetManagedContainers returns all containers on this node with resource stats.
// Stats for running containers are collected in parallel via goroutines.
func (a *WorkerAgent) GetManagedContainers() []map[string]any {
	if a.dockerClient == nil {
		return nil
	}

	ctx := context.Background()
	allContainers, err := a.dockerClient.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		log.Printf("[agent] Error listing containers: %v", err)
		return nil
	}

	// Collect stats in parallel for running containers
	type statsResult struct {
		id    string
		stats map[string]any
	}
	var wg sync.WaitGroup
	statsCh := make(chan statsResult, len(allContainers))

	running := 0
	for _, c := range allContainers {
		if c.State == "running" {
			running++
			wg.Add(1)
			go func(cID string) {
				defer wg.Done()
				st := a.getContainerStats(cID)
				statsCh <- statsResult{id: cID[:12], stats: st}
			}(c.ID)
		}
	}

	wg.Wait()
	close(statsCh)

	statsMap := make(map[string]map[string]any)
	for sr := range statsCh {
		statsMap[sr.id] = sr.stats
	}

	var result []map[string]any
	for _, c := range allContainers {
		cID := c.ID[:12]

		// Determine service name from labels or container name
		labels := c.Labels
		if labels == nil {
			labels = make(map[string]string)
		}
		serviceName := labels["ai.orchestrator.service"]
		if serviceName == "" {
			name := strings.TrimPrefix(firstOrEmpty(c.Names), "/")
			if strings.HasPrefix(name, "orch-") && strings.Contains(name[5:], "-") {
				parts := strings.Split(name[5:], "-")
				last := parts[len(parts)-1]
				if _, err := strconv.Atoi(last); err == nil {
					serviceName = strings.Join(parts[:len(parts)-1], "-")
				} else {
					serviceName = name
				}
			} else {
				serviceName = name
			}
		}

		// Image tag
		image := c.Image
		if image == "" && len(c.ImageID) > 12 {
			image = c.ImageID[:12]
		}

		st := statsMap[cID]
		if st == nil {
			st = map[string]any{
				"cpu_percent":    0.0,
				"memory_mb":      0.0,
				"memory_limit_mb": 0.0,
				"net_rx_mb":      0.0,
				"net_tx_mb":      0.0,
			}
		}

		result = append(result, map[string]any{
			"container_id":    cID,
			"container_name":  strings.TrimPrefix(firstOrEmpty(c.Names), "/"),
			"service_name":    serviceName,
			"image":           image,
			"node_name":       a.NodeName,
			"status":          c.State,
			"cpu_percent":     st["cpu_percent"],
			"memory_mb":       st["memory_mb"],
			"memory_limit_mb": st["memory_limit_mb"],
			"net_rx_mb":       st["net_rx_mb"],
			"net_tx_mb":       st["net_tx_mb"],
		})
	}

	return result
}

// ExportContainer commits a container to an image and saves it as a tar
// archive. Returns the tar bytes, container config for recreation, and any error.
func (a *WorkerAgent) ExportContainer(containerID string) ([]byte, map[string]any, error) {
	if a.dockerClient == nil {
		return nil, nil, fmt.Errorf("docker client not available")
	}

	ctx := context.Background()

	// Inspect the container for config details
	inspect, err := a.dockerClient.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect container: %w", err)
	}

	shortID := containerID
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}

	// Commit the container to an image
	commitTag := fmt.Sprintf("migration-%s:latest", shortID)
	log.Printf("[agent] Committing container %s as %s", containerID, commitTag)

	commitResp, err := a.dockerClient.ContainerCommit(ctx, containerID, container.CommitOptions{
		Reference: commitTag,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("commit container: %w", err)
	}

	// Save the image as tar
	log.Printf("[agent] Saving image %s (ID: %s)", commitTag, commitResp.ID)
	reader, err := a.dockerClient.ImageSave(ctx, []string{commitTag})
	if err != nil {
		return nil, nil, fmt.Errorf("save image: %w", err)
	}
	defer reader.Close()

	tarData, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("read image tar: %w", err)
	}

	// Build container config for recreation
	config := map[string]any{
		"name":        strings.TrimPrefix(inspect.Name, "/"),
		"image":       commitTag,
		"labels":      inspect.Config.Labels,
		"environment": inspect.Config.Env,
		"ports":       inspect.NetworkSettings.Ports,
		"status":      inspect.State.Status,
	}

	log.Printf("[agent] Exported container %s: %d bytes", containerID, len(tarData))
	return tarData, config, nil
}

// ImportContainer loads a migration image from tar data and starts a new
// container with the provided config. Returns the new container short ID.
func (a *WorkerAgent) ImportContainer(tarData []byte, config map[string]any) (string, error) {
	if a.dockerClient == nil {
		return "", fmt.Errorf("docker client not available")
	}

	ctx := context.Background()

	// Load the image from tar
	log.Printf("[agent] Loading migration image (%d bytes)", len(tarData))
	loadResp, err := a.dockerClient.ImageLoad(ctx, bytes.NewReader(tarData), true)
	if err != nil {
		return "", fmt.Errorf("load image: %w", err)
	}
	defer loadResp.Body.Close()

	// Read load response to get image info
	loadBody, _ := io.ReadAll(loadResp.Body)
	log.Printf("[agent] Image load response: %s", string(loadBody))

	// Determine image name from config
	imageName, _ := config["image"].(string)
	if imageName == "" {
		return "", fmt.Errorf("no image name in config")
	}

	// Container name
	name, _ := config["name"].(string)
	if name == "" {
		name = "migrated"
	}

	// Labels
	labels := make(map[string]string)
	if rawLabels, ok := config["labels"]; ok {
		switch v := rawLabels.(type) {
		case map[string]string:
			labels = v
		case map[string]any:
			for k, val := range v {
				labels[k] = fmt.Sprintf("%v", val)
			}
		}
	}
	labels["ai.orchestrator.managed"] = "true"
	labels["ai.orchestrator.migrated"] = "true"

	// Environment
	var envList []string
	if rawEnv, ok := config["environment"]; ok {
		switch v := rawEnv.(type) {
		case []string:
			envList = v
		case []any:
			for _, e := range v {
				envList = append(envList, fmt.Sprintf("%v", e))
			}
		}
	}

	// Remove existing container with same name if it exists
	if existing, err := a.dockerClient.ContainerInspect(ctx, name); err == nil {
		log.Printf("[agent] Removing existing container with name: %s", name)
		timeout := 5
		stopOpts := container.StopOptions{Timeout: &timeout}
		_ = a.dockerClient.ContainerStop(ctx, existing.ID, stopOpts)
		_ = a.dockerClient.ContainerRemove(ctx, existing.ID, container.RemoveOptions{Force: true})
	}

	// Create and start the container
	log.Printf("[agent] Starting migrated container: %s from %s", name, imageName)
	createResp, err := a.dockerClient.ContainerCreate(ctx,
		&container.Config{
			Image:  imageName,
			Labels: labels,
			Env:    envList,
		},
		&container.HostConfig{},
		nil, nil, name,
	)
	if err != nil {
		return "", fmt.Errorf("create container: %w", err)
	}

	if err := a.dockerClient.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start container: %w", err)
	}

	shortID := createResp.ID
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}
	log.Printf("[agent] Migrated container started: %s", shortID)
	return shortID, nil
}

// StopContainer stops and removes a container by ID.
func (a *WorkerAgent) StopContainer(containerID string) error {
	if a.dockerClient == nil {
		return fmt.Errorf("docker client not available")
	}

	ctx := context.Background()
	timeout := 10
	stopOpts := container.StopOptions{Timeout: &timeout}

	if err := a.dockerClient.ContainerStop(ctx, containerID, stopOpts); err != nil {
		return fmt.Errorf("stop container %s: %w", containerID, err)
	}

	if err := a.dockerClient.ContainerRemove(ctx, containerID, container.RemoveOptions{}); err != nil {
		return fmt.Errorf("remove container %s: %w", containerID, err)
	}

	log.Printf("[agent] Container %s stopped and removed", containerID)
	return nil
}

// handleMasterCommand processes a command received from the master in a
// heartbeat response.
func (a *WorkerAgent) handleMasterCommand(cmd map[string]any) {
	action, _ := cmd["action"].(string)
	log.Printf("[agent] Received master command: %s", action)

	switch action {
	case "stop_container":
		if cid, ok := cmd["container_id"].(string); ok && cid != "" {
			if err := a.StopContainer(cid); err != nil {
				log.Printf("[agent] Failed to stop container %s: %v", cid, err)
			}
		}
	case "run_container":
		// Delegate to local runtime (not implemented here)
	default:
		log.Printf("[agent] Unknown master command: %s", action)
	}
}

// firstOrEmpty returns the first element of a string slice, or empty string.
func firstOrEmpty(s []string) string {
	if len(s) > 0 {
		return s[0]
	}
	return ""
}

