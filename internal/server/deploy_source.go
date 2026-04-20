package server

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"ai-container-go/internal/aifix"
	"ai-container-go/internal/models"
	"ai-container-go/internal/multiservice"
	"ai-container-go/internal/runtime"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

const maxUploadSize = 200 << 20 // 200MB

// handleDeploySource handles POST /v1/services/deploy-source
// Accepts a multipart form with:
//   - file: zip or tar.gz archive containing source code
//   - name: (optional) service name
func handleDeploySource(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "파일이 너무 크거나 잘못된 요청입니다 (최대 200MB)",
		})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "파일이 필요합니다. 'file' 필드로 zip 또는 tar.gz 파일을 업로드하세요.",
		})
		return
	}
	defer file.Close()

	filename := header.Filename
	if !isAllowedArchive(filename) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "zip 또는 tar.gz 파일만 지원합니다.",
		})
		return
	}

	// Derive service name
	serviceName := sanitizeServiceName(r.FormValue("name"))
	if serviceName == "" {
		serviceName = sanitizeServiceName(stripArchiveExt(filename))
	}
	if serviceName == "" {
		serviceName = "source-app"
	}

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "deploy-source-*")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "임시 디렉터리 생성 실패",
		})
		return
	}
	defer os.RemoveAll(tmpDir)

	// Save uploaded file
	archivePath := filepath.Join(tmpDir, filename)
	dst, err := os.Create(archivePath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "파일 저장 실패",
		})
		return
	}
	if _, err := io.Copy(dst, file); err != nil {
		dst.Close()
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "파일 저장 실패",
		})
		return
	}
	dst.Close()

	// Extract archive
	extractDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "디렉터리 생성 실패",
		})
		return
	}

	if strings.HasSuffix(filename, ".zip") {
		err = extractZip(archivePath, extractDir)
	} else {
		err = extractTarGz(archivePath, extractDir)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("압축 해제 실패: %v", err),
		})
		return
	}

	// If archive had a single root directory, use that as context
	extractDir = resolveRootDir(extractDir)

	// Check for multi-service project (docker-compose, multiple Dockerfiles, etc.)
	multiResult, multiErr := multiservice.Detect(extractDir)
	if multiErr == nil && multiResult != nil && multiResult.IsMultiService {
		handleMultiServiceDeploy(w, r, extractDir, serviceName, multiResult)
		return
	}

	// Detect or generate Dockerfile
	dockerfilePath, found := runtime.DetectDockerfile(extractDir)
	var containerPort int
	if found {
		containerPort = runtime.DetectContainerPort(extractDir, dockerfilePath)
		if containerPort == 0 {
			containerPort = 8080 // default fallback
		}
	} else {
		_, port, genErr := runtime.GenerateDockerfile(extractDir)
		if genErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false,
				"message": genErr.Error(),
			})
			return
		}
		dockerfilePath = "Dockerfile"
		containerPort = port
	}

	// Build Docker image
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		})
		return
	}

	imageName := fmt.Sprintf("orch-source-%s:latest", serviceName)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	log.Printf("[deploy-source] Building image %s from %s (Dockerfile: %s)", imageName, extractDir, dockerfilePath)
	buildResult, buildErr := runtime.BuildImage(ctx, cli, extractDir, imageName, dockerfilePath)

	// AI-powered auto-fix on build failure
	var aifixInfo map[string]any
	if buildErr != nil && aifix.GetAPIKey() != "" {
		log.Printf("[deploy-source] Build failed, attempting AI-powered fix: %v", buildErr)

		buildLog := ""
		if buildResult != nil {
			buildLog = buildResult.BuildLog
		}

		for attempt := 1; attempt <= aifix.MaxRetries; attempt++ {
			log.Printf("[deploy-source] AI fix attempt %d/%d", attempt, aifix.MaxRetries)

			fixResult, fixErr := aifix.AnalyzeAndFix(ctx, extractDir, dockerfilePath, buildLog)
			if fixErr != nil {
				log.Printf("[deploy-source] AI analysis failed: %v", fixErr)
				break
			}

			if len(fixResult.Fixes) == 0 {
				log.Printf("[deploy-source] AI found no actionable fixes")
				break
			}

			// Apply suggested fixes
			if err := aifix.ApplyFixes(extractDir, fixResult.Fixes); err != nil {
				log.Printf("[deploy-source] Failed to apply AI fixes: %v", err)
				break
			}

			// Retry build
			log.Printf("[deploy-source] Retrying build after AI fix (attempt %d)", attempt)
			buildResult, buildErr = runtime.BuildImage(ctx, cli, extractDir, imageName, dockerfilePath)
			if buildErr == nil {
				aifixInfo = map[string]any{
					"applied":    true,
					"attempt":    attempt,
					"analysis":   fixResult.Analysis,
					"suggestion": fixResult.Suggestion,
					"files_fixed": len(fixResult.Fixes),
				}
				log.Printf("[deploy-source] Build succeeded after AI fix (attempt %d): %s", attempt, fixResult.Suggestion)
				break
			}

			// Update build log for next attempt
			if buildResult != nil {
				buildLog = buildResult.BuildLog
			}
			log.Printf("[deploy-source] Build still failing after AI fix attempt %d: %v", attempt, buildErr)
		}
	}

	if buildErr != nil {
		resp := map[string]any{
			"success": false,
			"message": fmt.Sprintf("이미지 빌드 실패: %v", buildErr),
		}
		if buildResult != nil && buildResult.BuildLog != "" {
			logTail := buildResult.BuildLog
			if len(logTail) > 2000 {
				logTail = logTail[len(logTail)-2000:]
			}
			resp["build_log"] = logTail
		}
		if aifix.GetAPIKey() == "" {
			resp["hint"] = "ANTHROPIC_API_KEY 환경 변수를 설정하면 AI가 빌드 에러를 자동 분석하고 수정을 시도합니다"
		}
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}

	// Push to registry if running, then use registry image for deployment
	runImage := imageName
	var registryInfo map[string]any
	if isRegistryRunning(ctx) {
		repoTag := fmt.Sprintf("%s:latest", serviceName)
		regImage, pushErr := pushImageToRegistry(ctx, imageName, repoTag)
		if pushErr != nil {
			log.Printf("[deploy-source] Registry push failed (will use local image): %v", pushErr)
		} else {
			runImage = regImage
			registryInfo = map[string]any{
				"pushed":       true,
				"registry_tag": regImage,
				"external_tag": registryExternalTag(repoTag),
			}
			log.Printf("[deploy-source] Image pushed to registry: %s", regImage)
		}
	}

	// Deploy container — use scheduler to pick least-loaded node if cluster is available
	deployResult := deployToOptimalNode(ctx, cli, runImage, serviceName, containerPort, nil)

	if !deployResult.OK {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("컨테이너 실행 실패: %s", deployResult.Message),
		})
		return
	}
	hostPort := deployResult.HostPort
	details := deployResult.Details

	// Post-deploy health check (only for local deployments where we have source)
	var healthInfo map[string]any
	localNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if deployResult.NodeName == "" || deployResult.NodeName == localNodeName || deployResult.NodeName == "local" {
		containerIDs, _ := details["container_ids"].([]string)
		if len(containerIDs) > 0 {
			hResult := postDeployHealthCheck(ctx, cli, containerIDs[0], extractDir, dockerfilePath, imageName, serviceName, containerPort)
			if hResult != nil {
				healthInfo = hResult
				if newCID, ok := hResult["new_container_id"].(string); ok && newCID != "" {
					details["container_ids"] = []string{newCID}
					if newPort, ok := hResult["new_host_port"].(int); ok && newPort > 0 {
						hostPort = newPort
					}
					if newImg, ok := hResult["new_image"].(string); ok && newImg != "" {
						runImage = newImg
					}
				}
			}
		}
	}

	if hub != nil {
		if data, err := json.Marshal(map[string]any{
			"event": "deploy-source", "service": serviceName, "image": runImage, "port": hostPort,
		}); err == nil {
			hub.broadcast(data)
		}
	}

	result := map[string]any{
		"success":        true,
		"message":        fmt.Sprintf("서비스 '%s' 배포 완료 (포트: %d, 노드: %s)", serviceName, hostPort, deployResult.NodeName),
		"service_name":   serviceName,
		"image":          runImage,
		"build_image":    imageName,
		"host_port":      hostPort,
		"container_port": containerPort,
		"details":        details,
		"url":            fmt.Sprintf("http://%s:%d", deployResult.NodeIP, hostPort),
		"deploy_message": deployResult.Message,
		"deployed_node":  deployResult.NodeName,
	}
	if aifixInfo != nil {
		result["ai_fix"] = aifixInfo
	}
	if registryInfo != nil {
		result["registry"] = registryInfo
	}
	if healthInfo != nil {
		result["health_check"] = healthInfo
	}
	writeJSON(w, http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Multi-service deployment
// ---------------------------------------------------------------------------

// handleMultiServiceDeploy deploys multiple services detected from the source archive.
// Services are built/pulled, started in dependency order, and connected to the same network.
func handleMultiServiceDeploy(w http.ResponseWriter, r *http.Request, extractDir string, projectName string, result *multiservice.DetectResult) {
	cli := runtime.DockerClient()
	if cli == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	services := result.Services
	deployOrder := multiservice.SortByDependency(services)

	log.Printf("[deploy-source] Multi-service project detected (%s): %d services", result.Source, len(services))

	type deployedService struct {
		Name          string `json:"name"`
		Image         string `json:"image"`
		HostPort      int    `json:"host_port"`
		ContainerPort int    `json:"container_port"`
		URL           string `json:"url"`
		Message       string `json:"message"`
	}

	var deployed []deployedService
	var errors []map[string]any
	// Track deployed service names → internal DNS aliases for env injection
	serviceNetworkMap := map[string]int{} // serviceName → containerPort

	for _, idx := range deployOrder {
		svc := services[idx]
		svcFullName := projectName + "-" + svc.Name
		log.Printf("[deploy-source] Deploying service %d/%d: %s", len(deployed)+len(errors)+1, len(services), svcFullName)

		var imageName string

		if svc.Image != "" && svc.BuildContext == "" {
			// Pre-built image: pull
			imageName = svc.Image
			log.Printf("[deploy-source] Pulling image %s for service %s", imageName, svc.Name)
		} else {
			// Build from source
			buildContext := extractDir
			if svc.BuildContext != "" && svc.BuildContext != "." {
				buildContext = filepath.Join(extractDir, svc.BuildContext)
			}

			// Check if Dockerfile exists, generate if needed
			dockerfilePath := svc.Dockerfile
			if dockerfilePath == "" {
				dockerfilePath = "Dockerfile"
			}
			dfFullPath := filepath.Join(buildContext, dockerfilePath)
			if _, err := os.Stat(dfFullPath); err != nil {
				// Try to auto-generate
				_, port, genErr := runtime.GenerateDockerfile(buildContext)
				if genErr != nil {
					errors = append(errors, map[string]any{
						"service": svc.Name,
						"error":   fmt.Sprintf("Dockerfile 없음 및 자동 생성 실패: %v", genErr),
					})
					continue
				}
				dockerfilePath = "Dockerfile"
				if svc.ContainerPort == 0 {
					svc.ContainerPort = port
				}
			}

			imageName = fmt.Sprintf("orch-source-%s:latest", svcFullName)
			log.Printf("[deploy-source] Building %s from %s (Dockerfile: %s)", imageName, buildContext, dockerfilePath)

			buildResult, buildErr := runtime.BuildImage(ctx, cli, buildContext, imageName, dockerfilePath)

			// AI auto-fix on build failure
			if buildErr != nil && aifix.GetAPIKey() != "" {
				buildLog := ""
				if buildResult != nil {
					buildLog = buildResult.BuildLog
				}
				for attempt := 1; attempt <= aifix.MaxRetries; attempt++ {
					fixResult, fixErr := aifix.AnalyzeAndFix(ctx, buildContext, dockerfilePath, buildLog)
					if fixErr != nil || len(fixResult.Fixes) == 0 {
						break
					}
					if err := aifix.ApplyFixes(buildContext, fixResult.Fixes); err != nil {
						break
					}
					buildResult, buildErr = runtime.BuildImage(ctx, cli, buildContext, imageName, dockerfilePath)
					if buildErr == nil {
						log.Printf("[deploy-source] %s build succeeded after AI fix (attempt %d)", svc.Name, attempt)
						break
					}
					if buildResult != nil {
						buildLog = buildResult.BuildLog
					}
				}
			}

			if buildErr != nil {
				errors = append(errors, map[string]any{
					"service": svc.Name,
					"error":   fmt.Sprintf("빌드 실패: %v", buildErr),
				})
				continue
			}
		}

		// Find available host port
		hostPort, err := runtime.FindAvailablePort(10000, 60000)
		if err != nil {
			errors = append(errors, map[string]any{
				"service": svc.Name,
				"error":   err.Error(),
			})
			continue
		}

		containerPort := svc.ContainerPort
		if containerPort == 0 {
			containerPort = 8080
		}

		// Build environment with service discovery info
		// Inject connection info for dependent services (e.g., DB_HOST=projectName-postgres)
		envVars := make([]string, len(svc.Environment))
		copy(envVars, svc.Environment)
		for depName, depPort := range serviceNetworkMap {
			alias := projectName + "-" + depName
			upperName := strings.ToUpper(strings.ReplaceAll(depName, "-", "_"))
			envVars = append(envVars,
				fmt.Sprintf("%s_HOST=%s", upperName, alias),
				fmt.Sprintf("%s_PORT=%d", upperName, depPort),
				fmt.Sprintf("%s_URL=%s:%d", upperName, alias, depPort),
			)
		}

		// Push to registry if running, then use registry image
		runImage := imageName
		if svc.BuildContext != "" && isRegistryRunning(ctx) {
			repoTag := fmt.Sprintf("%s:latest", svcFullName)
			regImage, pushErr := pushImageToRegistry(ctx, imageName, repoTag)
			if pushErr != nil {
				log.Printf("[deploy-source] Registry push failed for %s (using local): %v", svc.Name, pushErr)
			} else {
				runImage = regImage
				log.Printf("[deploy-source] %s pushed to registry: %s", svc.Name, regImage)
			}
		}

		dr := deployToOptimalNode(ctx, cli, runImage, svcFullName, containerPort, envVars)
		if !dr.OK {
			errors = append(errors, map[string]any{
				"service": svc.Name,
				"error":   fmt.Sprintf("컨테이너 실행 실패: %s", dr.Message),
			})
			continue
		}

		// Register in network map for subsequent services
		serviceNetworkMap[svc.Name] = containerPort

		deployed = append(deployed, deployedService{
			Name:          svcFullName,
			Image:         runImage,
			HostPort:      dr.HostPort,
			ContainerPort: containerPort,
			URL:           fmt.Sprintf("http://%s:%d", dr.NodeIP, dr.HostPort),
			Message:       fmt.Sprintf("%s (노드: %s)", dr.Message, dr.NodeName),
		})

		log.Printf("[deploy-source] Service %s deployed on port %d", svcFullName, hostPort)
	}

	if hub != nil {
		if data, err := json.Marshal(map[string]any{
			"event": "deploy-multi-service", "project": projectName, "services": deployed,
		}); err == nil {
			hub.broadcast(data)
		}
	}

	success := len(deployed) > 0
	message := fmt.Sprintf("멀티서비스 프로젝트 '%s' 배포: %d개 성공", projectName, len(deployed))
	if len(errors) > 0 {
		message += fmt.Sprintf(", %d개 실패", len(errors))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":          success,
		"message":          message,
		"multi_service":    true,
		"source":           result.Source,
		"project_name":     projectName,
		"services":         deployed,
		"errors":           errors,
		"total_services":   len(services),
		"deployed_count":   len(deployed),
		"failed_count":     len(errors),
	})
}

// ---------------------------------------------------------------------------
// Optimal node deployment
// ---------------------------------------------------------------------------

// deployNodeResult holds the result of deploying to a node.
type deployNodeResult struct {
	OK       bool
	Message  string
	HostPort int
	NodeName string
	NodeIP   string
	Details  map[string]any
}

// deployToOptimalNode uses the scheduler to pick the least-loaded node and deploy there.
// If cluster/scheduler is not available, falls back to local deployment.
func deployToOptimalNode(ctx context.Context, cli *client.Client, image string, serviceName string, containerPort int, envVars []string) deployNodeResult {
	// Try cluster-aware deployment
	if sched != nil && clusterState != nil {
		decisions, err := sched.Schedule(serviceName, image, 1, nil, "least-loaded")
		if err == nil && len(decisions) > 0 {
			targetNodeName := decisions[0].NodeName
			node := clusterState.GetNode(targetNodeName)

			localNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
			if node != nil && targetNodeName != localNodeName {
				// Deploy to remote node
				log.Printf("[deploy-source] Scheduler selected node '%s' (least-loaded) for %s", targetNodeName, serviceName)
				return deployToRemoteNode(ctx, node, image, serviceName, containerPort, envVars)
			}
			// Scheduler picked the master node — fall through to local deploy
			log.Printf("[deploy-source] Scheduler selected master node for %s", serviceName)
		} else if err != nil {
			log.Printf("[deploy-source] Scheduler error (falling back to local): %v", err)
		}
	}

	// Local deployment
	return deployLocally(ctx, cli, image, serviceName, containerPort, envVars)
}

func deployLocally(ctx context.Context, cli *client.Client, image string, serviceName string, containerPort int, envVars []string) deployNodeResult {
	hostPort, err := runtime.FindAvailablePort(10000, 60000)
	if err != nil {
		return deployNodeResult{OK: false, Message: err.Error()}
	}

	portMapping := fmt.Sprintf("%d:%d", hostPort, containerPort)
	ok, msg, details := runtime.RunContainer(ctx, cli, image, runtime.RunContainerOpts{
		Name:               serviceName,
		Ports:              []string{portMapping},
		Replicas:           1,
		UseInternalNetwork: true,
		Environment:        envVars,
	})

	nodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if nodeName == "" {
		nodeName = "local"
	}
	nodeIP := "localhost"
	advertiseAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	if advertiseAddr != "" {
		nodeIP = strings.Split(advertiseAddr, ":")[0]
	}

	return deployNodeResult{
		OK:       ok,
		Message:  msg,
		HostPort: hostPort,
		NodeName: nodeName,
		NodeIP:   nodeIP,
		Details:  details,
	}
}

func deployToRemoteNode(ctx context.Context, node *models.NodeInfo, image string, serviceName string, containerPort int, envVars []string) deployNodeResult {
	baseURL := nodeBaseURL(node)
	nodeIP := strings.Split(strings.TrimPrefix(strings.TrimPrefix(node.Address, "http://"), "https://"), ":")[0]

	// Step 1: Pull image on remote node
	pullPayload, _ := json.Marshal(map[string]any{"image": image})
	pullReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/images/pull", bytes.NewReader(pullPayload))
	setNodeHeaders(pullReq, node)
	pullResp, err := longHTTPClient.Do(pullReq)
	if err != nil {
		log.Printf("[deploy-source] Remote pull failed on %s: %v, falling back to local", node.Name, err)
		return deployLocally(ctx, runtime.DockerClient(), image, serviceName, containerPort, envVars)
	}
	var pullData map[string]any
	json.NewDecoder(pullResp.Body).Decode(&pullData)
	pullResp.Body.Close()
	if s, _ := pullData["success"].(bool); !s {
		errMsg, _ := pullData["message"].(string)
		log.Printf("[deploy-source] Remote pull failed on %s: %s, falling back to local", node.Name, errMsg)
		return deployLocally(ctx, runtime.DockerClient(), image, serviceName, containerPort, envVars)
	}

	// Step 2: Run container on remote node
	portStr := fmt.Sprintf("%d", containerPort)
	runPayload, _ := json.Marshal(map[string]any{
		"image":                image,
		"name":                 serviceName,
		"replicas":             1,
		"use_internal_network": true,
		"environment":          envVars,
		"ports":                []string{fmt.Sprintf("%s:%s", portStr, portStr)},
	})
	runReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/containers/run", bytes.NewReader(runPayload))
	setNodeHeaders(runReq, node)
	runResp, err := longHTTPClient.Do(runReq)
	if err != nil {
		log.Printf("[deploy-source] Remote run failed on %s: %v, falling back to local", node.Name, err)
		return deployLocally(ctx, runtime.DockerClient(), image, serviceName, containerPort, envVars)
	}
	var runData map[string]any
	json.NewDecoder(runResp.Body).Decode(&runData)
	runResp.Body.Close()

	ok, _ := runData["success"].(bool)
	msg, _ := runData["message"].(string)

	// Extract host port from remote response details
	hostPort := containerPort
	if det, _ := runData["details"].(map[string]any); det != nil {
		// Remote node may return port info
		if p, ok := det["host_port"].(float64); ok && p > 0 {
			hostPort = int(p)
		}
	}

	return deployNodeResult{
		OK:       ok,
		Message:  msg,
		HostPort: hostPort,
		NodeName: node.Name,
		NodeIP:   nodeIP,
		Details:  runData,
	}
}

// ---------------------------------------------------------------------------
// Post-deploy health check
// ---------------------------------------------------------------------------

const (
	healthCheckWait    = 8 * time.Second  // wait before first check
	healthCheckRetries = 3                // number of log checks
	healthCheckInterval = 5 * time.Second // between retries
	maxRuntimeFixAttempts = 2
)

// postDeployHealthCheck verifies a container is running healthy after deployment.
// If it detects a crash or error logs, it uses Claude API to diagnose and auto-fix.
// Returns health check info map or nil if healthy.
func postDeployHealthCheck(ctx context.Context, cli *client.Client, containerID string, sourceDir string, dockerfilePath string, imageName string, serviceName string, containerPort int) map[string]any {
	log.Printf("[health-check] Waiting %s for container %s to stabilize...", healthCheckWait, containerID[:12])
	time.Sleep(healthCheckWait)

	// Check container state
	healthy, state, logs := checkContainerHealth(ctx, cli, containerID)
	if healthy {
		log.Printf("[health-check] Container %s is running normally", containerID[:12])
		return map[string]any{
			"status":  "healthy",
			"message": "컨테이너 정상 기동 확인",
		}
	}

	log.Printf("[health-check] Container %s unhealthy (state: %s)", containerID[:12], state)

	// If no AI key, just report the problem
	if aifix.GetAPIKey() == "" {
		return map[string]any{
			"status":  "unhealthy",
			"state":   state,
			"logs":    truncateLogs(logs, 2000),
			"message": "컨테이너 기동 실패 감지. ANTHROPIC_API_KEY를 설정하면 자동 수정을 시도합니다.",
		}
	}

	// AI-powered runtime fix loop
	for attempt := 1; attempt <= maxRuntimeFixAttempts; attempt++ {
		log.Printf("[health-check] AI runtime fix attempt %d/%d", attempt, maxRuntimeFixAttempts)

		fixResult, fixErr := aifix.AnalyzeRuntimeError(ctx, sourceDir, dockerfilePath, logs, state)
		if fixErr != nil {
			log.Printf("[health-check] AI analysis failed: %v", fixErr)
			return map[string]any{
				"status":   "unhealthy",
				"state":    state,
				"logs":     truncateLogs(logs, 2000),
				"message":  fmt.Sprintf("AI 분석 실패: %v", fixErr),
			}
		}

		log.Printf("[health-check] AI diagnosis: root_cause=%s, needs_rebuild=%v, suggestion=%s",
			fixResult.RootCause, fixResult.NeedsRebuild, fixResult.Suggestion)

		// Stop and remove the failed container
		timeout := 5
		cli.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout})
		cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})

		if fixResult.NeedsRebuild && sourceDir != "" {
			// Apply file fixes and rebuild
			if len(fixResult.Fixes) > 0 {
				if err := aifix.ApplyFixes(sourceDir, fixResult.Fixes); err != nil {
					log.Printf("[health-check] Failed to apply fixes: %v", err)
					continue
				}
			}

			// Rebuild image
			log.Printf("[health-check] Rebuilding image %s after fix", imageName)
			_, buildErr := runtime.BuildImage(ctx, cli, sourceDir, imageName, dockerfilePath)
			if buildErr != nil {
				log.Printf("[health-check] Rebuild failed: %v", buildErr)
				continue
			}

			// Re-push to registry if running
			runImage := imageName
			if isRegistryRunning(ctx) {
				repoTag := fmt.Sprintf("%s:latest", serviceName)
				if regImg, err := pushImageToRegistry(ctx, imageName, repoTag); err == nil {
					runImage = regImg
				}
			}

			// Find new port and redeploy
			newPort, portErr := runtime.FindAvailablePort(10000, 60000)
			if portErr != nil {
				log.Printf("[health-check] No available port: %v", portErr)
				continue
			}

			portMapping := fmt.Sprintf("%d:%d", newPort, containerPort)
			ok, msg, details := runtime.RunContainer(ctx, cli, runImage, runtime.RunContainerOpts{
				Name:               serviceName,
				Ports:              []string{portMapping},
				Replicas:           1,
				UseInternalNetwork: true,
			})
			if !ok {
				log.Printf("[health-check] Redeploy failed: %s", msg)
				continue
			}

			// Wait and re-check
			time.Sleep(healthCheckWait)
			newIDs, _ := details["container_ids"].([]string)
			newCID := ""
			if len(newIDs) > 0 {
				newCID = newIDs[0]
			}

			healthy2, state2, logs2 := checkContainerHealth(ctx, cli, newCID)
			if healthy2 {
				log.Printf("[health-check] Container recovered after AI fix (attempt %d)", attempt)
				return map[string]any{
					"status":           "recovered",
					"attempt":          attempt,
					"analysis":         fixResult.Analysis,
					"root_cause":       fixResult.RootCause,
					"suggestion":       fixResult.Suggestion,
					"message":          fmt.Sprintf("AI 자동 수정으로 복구 완료 (시도 %d회)", attempt),
					"new_container_id": newCID,
					"new_host_port":    newPort,
					"new_image":        runImage,
				}
			}
			// Update for next attempt
			containerID = newCID
			state = state2
			logs = logs2

		} else {
			// Environment/config fix — redeploy with new env vars
			envVars := fixResult.EnvFixes

			newPort, portErr := runtime.FindAvailablePort(10000, 60000)
			if portErr != nil {
				continue
			}

			runImage := imageName
			if isRegistryRunning(ctx) {
				repoTag := fmt.Sprintf("%s:latest", serviceName)
				if regImg, err := pushImageToRegistry(ctx, imageName, repoTag); err == nil {
					runImage = regImg
				}
			}

			portMapping := fmt.Sprintf("%d:%d", newPort, containerPort)
			ok, msg, details := runtime.RunContainer(ctx, cli, runImage, runtime.RunContainerOpts{
				Name:               serviceName,
				Ports:              []string{portMapping},
				Replicas:           1,
				UseInternalNetwork: true,
				Environment:        envVars,
			})
			if !ok {
				log.Printf("[health-check] Redeploy with env fix failed: %s", msg)
				continue
			}

			time.Sleep(healthCheckWait)
			newIDs, _ := details["container_ids"].([]string)
			newCID := ""
			if len(newIDs) > 0 {
				newCID = newIDs[0]
			}

			healthy2, state2, logs2 := checkContainerHealth(ctx, cli, newCID)
			if healthy2 {
				log.Printf("[health-check] Container recovered with env fix (attempt %d)", attempt)
				return map[string]any{
					"status":           "recovered",
					"attempt":          attempt,
					"analysis":         fixResult.Analysis,
					"root_cause":       fixResult.RootCause,
					"suggestion":       fixResult.Suggestion,
					"env_fixes":        envVars,
					"message":          fmt.Sprintf("환경변수 수정으로 복구 완료 (시도 %d회)", attempt),
					"new_container_id": newCID,
					"new_host_port":    newPort,
					"new_image":        runImage,
				}
			}
			containerID = newCID
			state = state2
			logs = logs2
		}
	}

	return map[string]any{
		"status":  "unhealthy",
		"state":   state,
		"logs":    truncateLogs(logs, 2000),
		"message": "AI 자동 수정 시도 후에도 컨테이너 기동 실패",
	}
}

// checkContainerHealth inspects container state and fetches recent logs.
// Returns (isHealthy, stateDescription, logs).
func checkContainerHealth(ctx context.Context, cli *client.Client, containerID string) (bool, string, string) {
	if containerID == "" {
		return false, "unknown", ""
	}

	inspect, err := cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return false, fmt.Sprintf("inspect failed: %v", err), ""
	}

	state := inspect.State
	stateDesc := fmt.Sprintf("status=%s, running=%v, exit_code=%d", state.Status, state.Running, state.ExitCode)
	if state.OOMKilled {
		stateDesc += ", OOMKilled=true"
	}
	if state.Error != "" {
		stateDesc += fmt.Sprintf(", error=%s", state.Error)
	}

	// Fetch logs (last 100 lines)
	logsReader, err := cli.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       "100",
		Timestamps: true,
	})
	containerLogs := ""
	if err == nil {
		defer logsReader.Close()
		logBytes, _ := io.ReadAll(logsReader)
		containerLogs = stripDockerLogHeaders(string(logBytes))
	}

	// Determine if healthy
	if state.Running && state.ExitCode == 0 {
		// Running — check logs for fatal errors
		if containsFatalError(containerLogs) {
			return false, stateDesc + " (fatal error in logs)", containerLogs
		}
		return true, stateDesc, containerLogs
	}

	// Restarting or exited with error
	if state.Restarting || state.ExitCode != 0 || !state.Running {
		return false, stateDesc, containerLogs
	}

	return true, stateDesc, containerLogs
}

// containsFatalError checks if logs contain obvious fatal/crash patterns.
func containsFatalError(logs string) bool {
	lower := strings.ToLower(logs)
	fatalPatterns := []string{
		"fatal error", "panic:", "segmentation fault",
		"killed", "oomkilled",
		"error: cannot find module",
		"modulenotfounderror", "importerror",
		"enoent", "eacces", "eaddrinuse",
		"connection refused",
		"exec format error",
	}
	for _, p := range fatalPatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

// stripDockerLogHeaders removes the 8-byte Docker log stream header from each line.
func stripDockerLogHeaders(raw string) string {
	var clean strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		// Docker multiplexed stream: first 8 bytes are header
		if len(line) > 8 {
			clean.WriteString(line[8:])
		} else {
			clean.WriteString(line)
		}
		clean.WriteByte('\n')
	}
	return clean.String()
}

func truncateLogs(logs string, maxLen int) string {
	if len(logs) <= maxLen {
		return logs
	}
	return logs[len(logs)-maxLen:]
}

// ---------------------------------------------------------------------------
// Archive helpers
// ---------------------------------------------------------------------------

func isAllowedArchive(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".zip") ||
		strings.HasSuffix(lower, ".tar.gz") ||
		strings.HasSuffix(lower, ".tgz")
}

func stripArchiveExt(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".tar.gz"):
		return name[:len(name)-7]
	case strings.HasSuffix(lower, ".tgz"):
		return name[:len(name)-4]
	case strings.HasSuffix(lower, ".zip"):
		return name[:len(name)-4]
	}
	return name
}

var safeNameRe = regexp.MustCompile(`[^a-z0-9-]`)

func sanitizeServiceName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = safeNameRe.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if len(name) > 40 {
		name = name[:40]
	}
	return name
}

// resolveRootDir checks if extractDir contains a single subdirectory and returns it.
// This handles the common case where archives have a single root folder.
func resolveRootDir(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return dir
	}
	// Filter out hidden files
	var visible []os.DirEntry
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			visible = append(visible, e)
		}
	}
	if len(visible) == 1 && visible[0].IsDir() {
		return filepath.Join(dir, visible[0].Name())
	}
	return dir
}

func extractZip(archivePath, destDir string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		target := filepath.Join(destDir, f.Name)
		// Prevent zip slip
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destDir)+string(os.PathSeparator)) {
			continue
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(target, 0755)
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}

		out, err := os.Create(target)
		if err != nil {
			rc.Close()
			return err
		}

		_, err = io.Copy(out, rc)
		out.Close()
		rc.Close()
		if err != nil {
			return err
		}

		// Preserve executable bit
		if f.Mode()&0111 != 0 {
			os.Chmod(target, 0755)
		}
	}
	return nil
}

func extractTarGz(archivePath, destDir string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, hdr.Name)
		// Prevent tar slip
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destDir)+string(os.PathSeparator)) {
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			os.MkdirAll(target, 0755)
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			out, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return err
			}
			out.Close()
			if hdr.Mode&0111 != 0 {
				os.Chmod(target, 0755)
			}
		}
	}
	return nil
}
