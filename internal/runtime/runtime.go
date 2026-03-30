// Package runtime provides the Docker runtime adapter: scale, deploy, resource limits, and container lifecycle.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	dtypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"

	"ai-container-go/internal/models"
	"ai-container-go/internal/state"
)

// Labels to mark containers managed by this orchestrator.
const (
	LabelOrchestrator = "ai.orchestrator.managed"
	LabelService      = "ai.orchestrator.service"
	OrchNetwork       = "orch-internal"
	TraefikHTTPPort   = 80
)

// Cached Docker client.
var (
	cachedClient   *client.Client
	cachedClientMu sync.Mutex
)

// Reconcile skip management.
var (
	reconcileSkip   = make(map[string]bool)
	reconcileSkipMu sync.Mutex
)

// ReconcileSkipAdd marks a service to skip reconcile (during migration/move).
func ReconcileSkipAdd(serviceName string) {
	reconcileSkipMu.Lock()
	defer reconcileSkipMu.Unlock()
	reconcileSkip[serviceName] = true
}

// ReconcileSkipRemove unmarks a service from reconcile skip.
func ReconcileSkipRemove(serviceName string) {
	reconcileSkipMu.Lock()
	defer reconcileSkipMu.Unlock()
	delete(reconcileSkip, serviceName)
}

func isReconcileSkipped(serviceName string) bool {
	reconcileSkipMu.Lock()
	defer reconcileSkipMu.Unlock()
	return reconcileSkip[serviceName]
}

// RunContainerOpts holds optional parameters for RunContainer.
type RunContainerOpts struct {
	Name               string
	Memory             string
	CPU                string
	Replicas           int
	UseInternalNetwork bool
	Environment        []string
	Volumes            []string
	Ports              []string
	User               string
	VolumeMode         string
	AutoPull           bool
}

// DockerClient returns a cached Docker client using environment configuration.
func DockerClient() *client.Client {
	cachedClientMu.Lock()
	defer cachedClientMu.Unlock()
	if cachedClient != nil {
		return cachedClient
	}
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Printf("Failed to create Docker client: %v", err)
		return nil
	}
	cachedClient = cli
	return cachedClient
}

// EnsureNetwork creates the internal overlay network for service discovery if it does not exist.
// Returns the network ID.
func EnsureNetwork(ctx context.Context, cli *client.Client) string {
	nets, err := cli.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", OrchNetwork)),
	})
	if err != nil {
		log.Printf("Failed to list networks: %v", err)
		return ""
	}
	for _, n := range nets {
		if n.Name == OrchNetwork {
			return n.ID
		}
	}
	resp, err := cli.NetworkCreate(ctx, OrchNetwork, network.CreateOptions{
		Driver: "bridge",
	})
	if err != nil {
		log.Printf("Failed to create network %s: %v", OrchNetwork, err)
		return ""
	}
	return resp.ID
}

// TraefikLabels returns Traefik ingress labels for a service. Same router/service name groups replicas for LB.
func TraefikLabels(serviceName string, port int) map[string]string {
	re := regexp.MustCompile(`[^a-z0-9-]`)
	safe := strings.Trim(re.ReplaceAllString(strings.ToLower(serviceName), "-"), "-")
	if safe == "" {
		safe = "svc"
	}
	pathPrefix := strings.Trim(serviceName, "/")
	if pathPrefix == "" {
		pathPrefix = "svc"
	}
	return map[string]string{
		"traefik.enable": "true",
		fmt.Sprintf("traefik.http.routers.%s.rule", safe):                              fmt.Sprintf("Host(`%s.local`)", serviceName),
		fmt.Sprintf("traefik.http.routers.%s-path.rule", safe):                         fmt.Sprintf("PathPrefix(`/%s/`)", pathPrefix),
		fmt.Sprintf("traefik.http.routers.%s-path.service", safe):                      safe,
		fmt.Sprintf("traefik.http.routers.%s-path.middlewares", safe):                   fmt.Sprintf("%s-strip", safe),
		fmt.Sprintf("traefik.http.middlewares.%s-strip.stripprefix.prefixes", safe):     fmt.Sprintf("/%s", pathPrefix),
		fmt.Sprintf("traefik.http.services.%s.loadbalancer.server.port", safe):          strconv.Itoa(port),
	}
}

// ParseMemory converts memory strings like "512m" or "1g" to bytes.
func ParseMemory(s string) int64 {
	if s == "" {
		return 0
	}
	s = strings.TrimSpace(strings.ToLower(s))
	re := regexp.MustCompile(`^(\d+)\s*(m|mb|g|gb)?$`)
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return 0
	}
	num, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0
	}
	unit := matches[2]
	unit = strings.Replace(unit, "mb", "m", 1)
	unit = strings.Replace(unit, "gb", "g", 1)
	if unit == "" {
		unit = "m"
	}
	if unit == "g" {
		return num * 1024 * 1024 * 1024
	}
	return num * 1024 * 1024
}

// ContainerName returns the standard container name for a service replica: "orch-{service}-{index}".
func ContainerName(service string, index int) string {
	return fmt.Sprintf("orch-%s-%d", service, index)
}

// IndexFromContainerName parses the index from "orch-{service}-{index}". Returns -1 if not matching.
func IndexFromContainerName(service, name string) int {
	name = strings.TrimLeft(strings.TrimSpace(name), "/")
	prefix := fmt.Sprintf("orch-%s-", service)
	if !strings.HasPrefix(name, prefix) {
		return -1
	}
	suffix := name[len(prefix):]
	idx, err := strconv.Atoi(suffix)
	if err != nil {
		return -1
	}
	return idx
}

// getContainersForService lists all containers (running or stopped) with the service label.
func getContainersForService(ctx context.Context, cli *client.Client, serviceName string) []dtypes.Container {
	list, err := cli.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", fmt.Sprintf("%s=%s", LabelService, serviceName)),
		),
	})
	if err != nil {
		log.Printf("Failed to list containers for service %s: %v", serviceName, err)
		return nil
	}
	return list
}

// getContainersByName finds containers whose name equals or starts with the given name.
func getContainersByName(ctx context.Context, cli *client.Client, name string) []dtypes.Container {
	all, err := cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil
	}
	nameClean := strings.ToLower(strings.TrimSpace(name))
	var out []dtypes.Container
	for _, c := range all {
		for _, cn := range c.Names {
			cname := strings.ToLower(strings.TrimLeft(cn, "/"))
			if cname == nameClean ||
				strings.HasPrefix(cname, nameClean+"-") ||
				strings.HasPrefix(cname, "orch-"+nameClean+"-") {
				out = append(out, c)
				break
			}
		}
	}
	return out
}

// buildLabels creates the standard label set for a managed container.
func buildLabels(serviceName string) map[string]string {
	labels := map[string]string{
		LabelOrchestrator: "true",
		LabelService:      serviceName,
	}
	for k, v := range TraefikLabels(serviceName, TraefikHTTPPort) {
		labels[k] = v
	}
	return labels
}

// buildMounts converts volume specs like "host:container" or "host:container:ro" to Docker mounts.
func buildMounts(volumes []string) []mount.Mount {
	var mounts []mount.Mount
	for _, v := range volumes {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		parts := strings.Split(v, ":")
		if len(parts) < 2 {
			continue
		}
		hostPath := strings.TrimSpace(parts[0])
		containerPath := strings.TrimSpace(parts[1])
		readOnly := len(parts) > 2 && strings.ToLower(parts[2]) == "ro"
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   hostPath,
			Target:   containerPath,
			ReadOnly: readOnly,
		})
	}
	return mounts
}

// buildPortBindings converts port specs like "8080:80" to Docker port bindings.
func buildPortBindings(ports []string) (nat.PortSet, nat.PortMap) {
	exposedPorts := nat.PortSet{}
	portBindings := nat.PortMap{}
	for _, p := range ports {
		p = strings.TrimSpace(p)
		if !strings.Contains(p, ":") {
			continue
		}
		parts := strings.SplitN(p, ":", 2)
		hostPort := strings.TrimSpace(parts[0])
		containerPort := strings.TrimSpace(parts[1])
		natPort := nat.Port(containerPort + "/tcp")
		exposedPorts[natPort] = struct{}{}
		portBindings[natPort] = []nat.PortBinding{
			{HostIP: "", HostPort: hostPort},
		}
	}
	return exposedPorts, portBindings
}

// volumesForReplica adjusts volume specs for per_replica mode by appending the container name to host paths.
func volumesForReplica(base []string, containerName string, mode string) []string {
	if len(base) == 0 {
		return base
	}
	if mode != "per_replica" {
		return base
	}
	var result []string
	for _, spec := range base {
		s := strings.TrimSpace(spec)
		if s == "" {
			continue
		}
		parts := strings.Split(s, ":")
		if len(parts) >= 2 {
			host := strings.TrimRight(strings.TrimSpace(parts[0]), "/")
			rest := strings.Join(parts[1:], ":")
			hostWithName := filepath.Join(host, containerName)
			// Create directory and context file
			if err := os.MkdirAll(hostWithName, 0o755); err == nil {
				ctxPath := filepath.Join(hostWithName, "context.json")
				ctxData, _ := json.Marshal(map[string]string{
					"container":   containerName,
					"base_volume": s,
					"host_path":   hostWithName,
					"mode":        mode,
				})
				os.WriteFile(ctxPath, ctxData, 0o644)
			}
			result = append(result, hostWithName+":"+rest)
		} else {
			result = append(result, s)
		}
	}
	return result
}

// getStringSlice extracts a []string from a map[string]any field.
func getStringSlice(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	if arr, ok := v.([]any); ok {
		var result []string
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		if len(result) == 0 {
			return nil
		}
		return result
	}
	if arr, ok := v.([]string); ok {
		return arr
	}
	return nil
}

// getString extracts a string from a map[string]any field.
func getString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

// getInt extracts an int from a map[string]any field.
func getInt(m map[string]any, key string, defaultVal int) int {
	v, ok := m[key]
	if !ok || v == nil {
		return defaultVal
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return defaultVal
}

// createAndStartContainer creates a single container with the given configuration.
// Returns (containerID, error).
func createAndStartContainer(
	ctx context.Context,
	cli *client.Client,
	imageName string,
	name string,
	labels map[string]string,
	memoryBytes int64,
	nanoCPUs int64,
	env []string,
	mounts []mount.Mount,
	exposedPorts nat.PortSet,
	portBindings nat.PortMap,
	user string,
	networkID string,
	serviceName string,
) (string, error) {
	config := &container.Config{
		Image:        imageName,
		Labels:       labels,
		ExposedPorts: exposedPorts,
	}
	if len(env) > 0 {
		config.Env = env
	}
	if user != "" {
		config.User = user
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		Mounts:       mounts,
	}
	if memoryBytes > 0 {
		hostConfig.Resources.Memory = memoryBytes
	}
	if nanoCPUs > 0 {
		hostConfig.Resources.NanoCPUs = nanoCPUs
	}

	var networkConfig *network.NetworkingConfig
	if networkID != "" && serviceName != "" {
		networkConfig = &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				OrchNetwork: {
					NetworkID: networkID,
					Aliases:   []string{serviceName},
				},
			},
		}
	}

	// Auto-pull image if not present locally
	_, _, imgErr := cli.ImageInspectWithRaw(ctx, imageName)
	if imgErr != nil {
		log.Printf("Image %s not found locally, pulling...", imageName)
		pullReader, pullErr := cli.ImagePull(ctx, imageName, image.PullOptions{})
		if pullErr != nil {
			return "", fmt.Errorf("image pull failed: %w", pullErr)
		}
		io.Copy(io.Discard, pullReader)
		pullReader.Close()
		log.Printf("Image %s pulled successfully", imageName)
	}

	// Detect exposed port from image and update Traefik label if needed
	if imgInspect, _, err := cli.ImageInspectWithRaw(ctx, imageName); err == nil {
		for p := range imgInspect.Config.ExposedPorts {
			port := p.Int()
			if port > 0 && port != TraefikHTTPPort {
				traefikKey := fmt.Sprintf("traefik.http.services.%s.loadbalancer.server.port",
					strings.Trim(regexp.MustCompile(`[^a-z0-9-]`).ReplaceAllString(strings.ToLower(serviceName), "-"), "-"))
				labels[traefikKey] = strconv.Itoa(port)
				break
			}
		}
	}

	resp, err := cli.ContainerCreate(ctx, config, hostConfig, networkConfig, nil, name)
	if err != nil {
		return "", fmt.Errorf("container create failed: %w", err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("container start failed: %w", err)
	}

	return resp.ID, nil
}

// parseCPU converts a CPU string like "0.5" or "2" to NanoCPUs.
func parseCPU(cpu string) int64 {
	if cpu == "" {
		return 0
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(cpu), 64)
	if err != nil {
		return 0
	}
	return int64(f * 1e9)
}

// containerShortID returns the first 12 characters of a container ID.
func containerShortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// containerNameFromSummary returns the first name from a container summary, stripped of the leading slash.
func containerNameFromSummary(c dtypes.Container) string {
	if len(c.Names) == 0 {
		return ""
	}
	return strings.TrimLeft(c.Names[0], "/")
}

// ExecuteScale scales a service to N replicas. Stopped containers are removed first, then add/remove to match target.
func ExecuteScale(ctx context.Context, cli *client.Client, serviceName string, replicas int) (bool, string, map[string]any) {
	info := state.GetService(serviceName)
	if info == nil {
		return false, fmt.Sprintf("Service '%s' not found. Deploy it first.", serviceName), nil
	}

	allContainers := getContainersForService(ctx, cli, serviceName)
	var running, stopped []dtypes.Container
	for _, c := range allContainers {
		if c.State == "running" {
			running = append(running, c)
		} else {
			stopped = append(stopped, c)
		}
	}

	// Remove stopped containers
	var removed []string
	for _, c := range stopped {
		timeout := 5
		cli.ContainerStop(ctx, c.ID, container.StopOptions{Timeout: &timeout})
		cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true})
		removed = append(removed, containerShortID(c.ID))
	}

	current := running
	target := replicas
	imgName := getString(info, "image")
	memory := getString(info, "memory_limit")
	cpu := getString(info, "cpu_limit")
	env := getStringSlice(info, "environment")
	baseVols := getStringSlice(info, "volumes")
	prts := getStringSlice(info, "ports")
	usr := getString(info, "user")
	volumeMode := getString(info, "volume_mode")
	if volumeMode == "" {
		volumeMode = "shared"
	}

	// Remove excess running containers
	toRemove := len(current) - target
	for i := 0; i < toRemove && len(current) > 0; i++ {
		c := current[len(current)-1]
		current = current[:len(current)-1]
		timeout := 5
		cli.ContainerStop(ctx, c.ID, container.StopOptions{Timeout: &timeout})
		cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true})
		removed = append(removed, containerShortID(c.ID))
	}

	networkID := EnsureNetwork(ctx, cli)

	// Connect existing containers to the network
	for _, c := range current {
		cli.NetworkConnect(ctx, networkID, c.ID, &network.EndpointSettings{
			Aliases: []string{serviceName},
		})
	}

	// Determine used indices
	usedIndices := make(map[int]bool)
	for _, c := range current {
		n := containerNameFromSummary(c)
		idx := IndexFromContainerName(serviceName, n)
		if idx >= 0 {
			usedIndices[idx] = true
		}
	}

	// Create new containers to reach target
	memBytes := ParseMemory(memory)
	nanoCPUs := parseCPU(cpu)
	labels := buildLabels(serviceName)

	var cleanEnv []string
	for _, e := range env {
		e = strings.TrimSpace(e)
		if e != "" {
			cleanEnv = append(cleanEnv, e)
		}
	}

	exposedPorts, portBindings := buildPortBindings(prts)

	var created []string
	toCreate := target - len(current)
	for i := 0; i < toCreate; i++ {
		idx := 0
		for usedIndices[idx] {
			idx++
		}
		usedIndices[idx] = true
		cname := ContainerName(serviceName, idx)
		vols := volumesForReplica(baseVols, cname, volumeMode)
		mounts := buildMounts(vols)

		cid, err := createAndStartContainer(
			ctx, cli, imgName, cname, labels,
			memBytes, nanoCPUs, cleanEnv, mounts,
			exposedPorts, portBindings, usr,
			networkID, serviceName,
		)
		if err != nil {
			return false, fmt.Sprintf("Container creation failed: %v", err), map[string]any{"removed": removed}
		}
		created = append(created, containerShortID(cid))
	}

	// Build final container ID list
	allIDs := make([]string, 0, len(current)+len(created))
	for _, c := range current {
		allIDs = append(allIDs, containerShortID(c.ID))
	}
	allIDs = append(allIDs, created...)

	// Update state
	var upsertOpts []state.UpsertOption
	if memory != "" {
		upsertOpts = append(upsertOpts, state.WithMemoryLimit(memory))
	}
	if cpu != "" {
		upsertOpts = append(upsertOpts, state.WithCPULimit(cpu))
	}
	upsertOpts = append(upsertOpts, state.WithContainerIDs(allIDs))
	if env != nil {
		upsertOpts = append(upsertOpts, state.WithEnvironment(env))
	}
	if baseVols != nil {
		upsertOpts = append(upsertOpts, state.WithVolumes(baseVols))
	}
	if prts != nil {
		upsertOpts = append(upsertOpts, state.WithPorts(prts))
	}
	if usr != "" {
		upsertOpts = append(upsertOpts, state.WithUser(usr))
	}
	upsertOpts = append(upsertOpts, state.WithVolumeMode(volumeMode))

	state.UpsertService(serviceName, imgName, target, upsertOpts...)
	state.UpdateServiceContainers(serviceName, allIDs)

	return true, fmt.Sprintf("Scaled '%s' to %d replicas.", serviceName, target), map[string]any{
		"replicas": target,
		"removed":  removed,
		"created":  created,
	}
}

// ExecuteDeploy deploys a new service (or updates an existing one). If image is empty, uses name as image.
func ExecuteDeploy(ctx context.Context, cli *client.Client, name string, imageName string, requestedReplicas int) (bool, string, map[string]any) {
	img := imageName
	if img == "" {
		img = name
	}
	replicas := requestedReplicas
	if replicas < 1 {
		replicas = 1
	}
	var memory, cpu string

	info := state.GetService(name)
	if info != nil {
		if requestedReplicas < 1 {
			replicas = getInt(info, "replicas", 1)
		}
		memory = getString(info, "memory_limit")
		cpu = getString(info, "cpu_limit")
	}

	var upsertOpts []state.UpsertOption
	if memory != "" {
		upsertOpts = append(upsertOpts, state.WithMemoryLimit(memory))
	}
	if cpu != "" {
		upsertOpts = append(upsertOpts, state.WithCPULimit(cpu))
	}
	state.UpsertService(name, img, replicas, upsertOpts...)

	success, msg, details := ExecuteScale(ctx, cli, name, replicas)
	if success {
		msg = fmt.Sprintf("Service '%s' (image: %s) deployed.", name, img)
	}
	return success, msg, details
}

// ExecuteResource updates resource limits for a service. Requires recreating containers.
func ExecuteResource(ctx context.Context, cli *client.Client, serviceName string, memory, cpu string) (bool, string, map[string]any) {
	info := state.GetService(serviceName)
	if info == nil {
		return false, fmt.Sprintf("Service '%s' not found.", serviceName), nil
	}

	newMemory := memory
	if newMemory == "" {
		newMemory = getString(info, "memory_limit")
	}
	newCPU := cpu
	if newCPU == "" {
		newCPU = getString(info, "cpu_limit")
	}

	imgName := getString(info, "image")
	replicas := getInt(info, "replicas", 1)

	var upsertOpts []state.UpsertOption
	if newMemory != "" {
		upsertOpts = append(upsertOpts, state.WithMemoryLimit(newMemory))
	}
	if newCPU != "" {
		upsertOpts = append(upsertOpts, state.WithCPULimit(newCPU))
	}
	containerIDs := getStringSlice(info, "container_ids")
	if containerIDs != nil {
		upsertOpts = append(upsertOpts, state.WithContainerIDs(containerIDs))
	}
	state.UpsertService(serviceName, imgName, replicas, upsertOpts...)

	// Recreate with new limits
	return ExecuteScale(ctx, cli, serviceName, replicas)
}

// ExecuteStop stops and removes all containers for a service.
func ExecuteStop(ctx context.Context, cli *client.Client, serviceName string) (bool, string, map[string]any) {
	info := state.GetService(serviceName)
	if info != nil {
		current := getContainersForService(ctx, cli, serviceName)
		var removed []string
		for _, c := range current {
			timeout := 5
			cli.ContainerStop(ctx, c.ID, container.StopOptions{Timeout: &timeout})
			cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true})
			removed = append(removed, containerShortID(c.ID))
		}
		state.UpdateServiceContainers(serviceName, []string{})

		imgName := getString(info, "image")
		var upsertOpts []state.UpsertOption
		mem := getString(info, "memory_limit")
		if mem != "" {
			upsertOpts = append(upsertOpts, state.WithMemoryLimit(mem))
		}
		cpuLim := getString(info, "cpu_limit")
		if cpuLim != "" {
			upsertOpts = append(upsertOpts, state.WithCPULimit(cpuLim))
		}
		upsertOpts = append(upsertOpts, state.WithContainerIDs([]string{}))
		state.UpsertService(serviceName, imgName, 0, upsertOpts...)

		return true, fmt.Sprintf("Service '%s' stopped.", serviceName), map[string]any{"removed": removed}
	}

	// Not a managed service - find by container name
	current := getContainersByName(ctx, cli, serviceName)
	if len(current) == 0 {
		return false, fmt.Sprintf("Service or container '%s' not found.", serviceName), nil
	}

	var removed []string
	for _, c := range current {
		timeout := 5
		cli.ContainerStop(ctx, c.ID, container.StopOptions{Timeout: &timeout})
		cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true})
		removed = append(removed, containerShortID(c.ID))
	}
	return true, fmt.Sprintf("Container '%s' stopped.", serviceName), map[string]any{"removed": removed}
}

// PullImage pulls a Docker image. Returns (success, message).
func PullImage(ctx context.Context, cli *client.Client, imageName string) (bool, string) {
	log.Printf("Pulling image: %s", imageName)
	reader, err := cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return false, fmt.Sprintf("Image pull failed: %v", err)
	}
	defer reader.Close()
	// Consume the pull output
	io.Copy(io.Discard, reader)
	return true, fmt.Sprintf("Image pulled: %s", imageName)
}

// RunContainer runs one or more containers from an image.
// If AutoPull is true (default), automatically pulls the image if not found locally.
func RunContainer(ctx context.Context, cli *client.Client, imageName string, opts RunContainerOpts) (bool, string, map[string]any) {
	// Auto-pull logic
	if opts.AutoPull {
		_, _, err := cli.ImageInspectWithRaw(ctx, imageName)
		if err != nil {
			ok, pullMsg := PullImage(ctx, cli, imageName)
			if !ok {
				return false, pullMsg, nil
			}
		}
	}

	count := opts.Replicas
	if count <= 0 {
		count = 1
	}

	groupName := opts.Name
	if groupName == "" {
		// Extract short name from image: "library/nginx:latest" -> "nginx"
		parts := strings.Split(strings.Split(imageName, ":")[0], "/")
		groupName = parts[len(parts)-1]
	}

	mode := opts.VolumeMode
	if mode == "" {
		mode = "shared"
	}

	var networkID string
	if opts.UseInternalNetwork {
		networkID = EnsureNetwork(ctx, cli)
	}

	labels := buildLabels(groupName)
	memBytes := ParseMemory(opts.Memory)
	nanoCPUs := parseCPU(opts.CPU)
	exposedPorts, portBindings := buildPortBindings(opts.Ports)

	var cleanEnv []string
	for _, e := range opts.Environment {
		e = strings.TrimSpace(e)
		if e != "" {
			cleanEnv = append(cleanEnv, e)
		}
	}

	var user string
	if strings.TrimSpace(opts.User) != "" {
		user = strings.TrimSpace(opts.User)
	}

	var ids []string
	for i := 0; i < count; i++ {
		cname := ContainerName(groupName, i)
		vols := volumesForReplica(opts.Volumes, cname, mode)
		mnts := buildMounts(vols)

		cid, err := createAndStartContainer(
			ctx, cli, imageName, cname, labels,
			memBytes, nanoCPUs, cleanEnv, mnts,
			exposedPorts, portBindings, user,
			networkID, groupName,
		)
		if err != nil {
			errMsg := err.Error()
			if strings.Contains(errMsg, "bind source path does not exist") {
				errMsg = "Volume mount host path does not exist or is not shared with Docker: " + errMsg
			}
			return false, errMsg, nil
		}
		ids = append(ids, containerShortID(cid))
	}

	// Update state
	var upsertOpts []state.UpsertOption
	if opts.Memory != "" {
		upsertOpts = append(upsertOpts, state.WithMemoryLimit(opts.Memory))
	}
	if opts.CPU != "" {
		upsertOpts = append(upsertOpts, state.WithCPULimit(opts.CPU))
	}
	upsertOpts = append(upsertOpts, state.WithContainerIDs(ids))
	if opts.Environment != nil {
		upsertOpts = append(upsertOpts, state.WithEnvironment(opts.Environment))
	}
	if opts.Volumes != nil {
		upsertOpts = append(upsertOpts, state.WithVolumes(opts.Volumes))
	}
	if opts.Ports != nil {
		upsertOpts = append(upsertOpts, state.WithPorts(opts.Ports))
	}
	if user != "" {
		upsertOpts = append(upsertOpts, state.WithUser(user))
	}
	upsertOpts = append(upsertOpts, state.WithVolumeMode(mode))
	state.UpsertService(groupName, imageName, len(ids), upsertOpts...)

	var msg string
	if len(ids) > 1 {
		msg = fmt.Sprintf("Started %d containers.", len(ids))
	} else if len(ids) == 1 {
		msg = fmt.Sprintf("Container started: %s", ids[0])
	} else {
		msg = "Failed to start containers."
	}

	return true, msg, map[string]any{
		"container_ids": ids,
		"service_name":  groupName,
	}
}

// StopContainerByID stops a container by ID or name (does not remove).
func StopContainerByID(ctx context.Context, cli *client.Client, containerID string) (bool, string, map[string]any) {
	timeout := 10
	err := cli.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout})
	if err != nil {
		return false, err.Error(), nil
	}
	return true, fmt.Sprintf("Container '%s' stopped.", containerID), nil
}

// RemoveContainerByID stops and removes a container by ID or name. Returns service_name from labels if set.
func RemoveContainerByID(ctx context.Context, cli *client.Client, containerID string) (bool, string, map[string]any) {
	inspect, err := cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return false, err.Error(), nil
	}

	serviceName := ""
	if inspect.Config != nil && inspect.Config.Labels != nil {
		serviceName = inspect.Config.Labels[LabelService]
	}

	// Stop (ignore error if already stopped)
	timeout := 10
	cli.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout})

	// Remove
	cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})

	details := map[string]any{}
	if serviceName != "" {
		details["service_name"] = serviceName
	}
	return true, fmt.Sprintf("Container '%s' removed.", containerID), details
}

// InspectContainerByID returns docker inspect information for a container.
func InspectContainerByID(ctx context.Context, cli *client.Client, containerID string) (bool, string, map[string]any) {
	inspect, err := cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return false, err.Error(), nil
	}
	return true, "inspect", map[string]any{
		"id":    inspect.ID,
		"name":  strings.TrimLeft(inspect.Name, "/"),
		"attrs": inspect,
	}
}

// RemoveImageByID removes a Docker image by ID or tag.
func RemoveImageByID(ctx context.Context, cli *client.Client, imageID string) (bool, string, map[string]any) {
	// Try as-is first (works for tags like nginx:alpine)
	_, err := cli.ImageRemove(ctx, imageID, image.RemoveOptions{Force: true})
	if err == nil {
		return true, fmt.Sprintf("Image '%s' removed.", imageID), nil
	}

	// Try with sha256: prefix
	_, err = cli.ImageRemove(ctx, "sha256:"+imageID, image.RemoveOptions{Force: true})
	if err == nil {
		return true, fmt.Sprintf("Image '%s' removed.", imageID), nil
	}

	// Try getting full ID first then removing
	inspectResp, _, inspectErr := cli.ImageInspectWithRaw(ctx, imageID)
	if inspectErr == nil {
		_, err = cli.ImageRemove(ctx, inspectResp.ID, image.RemoveOptions{Force: true})
		if err == nil {
			return true, fmt.Sprintf("Image '%s' removed.", imageID), nil
		}
	}

	return false, fmt.Sprintf("Failed to remove image '%s': %v", imageID, err), nil
}

// ExecuteIntent executes a parsed intent. Returns (success, message, details).
func ExecuteIntent(intent models.ParsedIntent, dryRun bool) (bool, string, map[string]any) {
	if dryRun {
		return true, fmt.Sprintf("[Dry run] Would execute: %s %+v", intent.Action, intent), nil
	}

	cli := DockerClient()
	if cli == nil {
		return false, "Docker connection failed.", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	switch intent.Action {
	case models.IntentScale:
		if intent.ServiceName == "" || intent.Replicas == nil {
			return false, "Specify service name and replica count for scaling.", nil
		}
		return ExecuteScale(ctx, cli, intent.ServiceName, *intent.Replicas)

	case models.IntentDeploy:
		name := intent.ServiceName
		if name == "" {
			name = intent.Image
		}
		if name == "" {
			return false, "Specify service name or image for deployment.", nil
		}
		if intent.ServiceName == "" && intent.Image != "" {
			parts := strings.Split(strings.Split(intent.Image, ":")[0], "/")
			name = parts[len(parts)-1]
		}
		img := intent.Image
		replicas := 0
		if intent.Replicas != nil {
			replicas = *intent.Replicas
		}
		return ExecuteDeploy(ctx, cli, name, img, replicas)

	case models.IntentResource:
		if intent.ServiceName == "" {
			return false, "Specify service name for resource update.", nil
		}
		return ExecuteResource(ctx, cli, intent.ServiceName, intent.Memory, intent.CPU)

	case models.IntentStop:
		if intent.ServiceName == "" {
			return false, "Specify service name to stop.", nil
		}
		return ExecuteStop(ctx, cli, intent.ServiceName)

	case models.IntentList:
		services := state.ListServices()
		svcList := make([]map[string]any, 0, len(services))
		for _, s := range services {
			svcList = append(svcList, map[string]any{
				"name":          s.Name,
				"image":         s.Image,
				"replicas":      s.Replicas,
				"memory_limit":  s.MemoryLimit,
				"cpu_limit":     s.CPULimit,
				"status":        s.Status,
				"container_ids": s.ContainerIDs,
			})
		}
		return true, "Service list", map[string]any{"services": svcList}

	case models.IntentMigrate:
		service := intent.ServiceName
		targetNode := intent.TargetNode
		if service == "" || targetNode == "" {
			return false, "Specify service and target node for migration.", nil
		}
		return true, "Use cluster API for migration: POST /v1/cluster/migrate", map[string]any{
			"service_name": service,
			"target_node":  targetNode,
			"hint":         "Drag containers in the dashboard to migrate.",
		}

	case models.IntentDrain:
		targetNode := intent.TargetNode
		if targetNode == "" {
			return false, "Specify node to drain.", nil
		}
		return true, fmt.Sprintf("Use cluster API for drain: POST /v1/cluster/nodes/%s/drain", targetNode), map[string]any{
			"target_node": targetNode,
		}

	case models.IntentClusterStatus:
		return true, "Check cluster status via dashboard or GET /v1/cluster/status.", nil

	case models.IntentNodeList:
		return true, "Check node list via dashboard or GET /v1/cluster/nodes.", nil

	default:
		return false, fmt.Sprintf("Unsupported command: %s", intent.Raw), nil
	}
}

// ReconcileReplicas ensures running container counts match desired replicas for all services.
func ReconcileReplicas(ctx context.Context, cli *client.Client) {
	services := state.ListServices()
	for _, svc := range services {
		if isReconcileSkipped(svc.Name) {
			continue
		}
		desired := svc.Replicas
		current := getContainersForService(ctx, cli, svc.Name)
		var runningCount int
		for _, c := range current {
			if c.State == "running" {
				runningCount++
			}
		}
		if runningCount != desired {
			ExecuteScale(ctx, cli, svc.Name, desired)
		}
	}
}
