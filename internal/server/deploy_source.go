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
	"sync"
	"time"

	"ai-container-go/internal/aifix"
	"ai-container-go/internal/models"
	"ai-container-go/internal/multiservice"
	"ai-container-go/internal/runtime"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

const maxUploadSize = 800 << 20 // 800MB

// handleDeploySource handles POST /v1/services/deploy-source
// Accepts a multipart form with:
//   - file: zip or tar.gz archive containing source code
//   - name: (optional) service name
func handleDeploySource(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "파일이 너무 크거나 잘못된 요청입니다 (최대 800MB)",
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

	// Sanitize filename — strip any path component, reject empty.
	filename := filepath.Base(header.Filename)
	if filename == "." || filename == "/" || filename == "" || strings.Contains(filename, "..") {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "잘못된 파일 이름입니다.",
		})
		return
	}
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
	// Attribute the deploy to the requesting user (portal accounts) so owner
	// checks + "my services" listing work. Consumed in deployToOptimalNode.
	if rejectForeignService(w, r, serviceName) {
		return
	}
	setPendingOwner(serviceName, requesterUsername(r))

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

	// Edited-Dockerfile override: if the client supplies "dockerfile" content
	// (from the preview/edit flow), write it into the build context so the
	// pipeline uses it verbatim instead of detecting/generating.
	if df := strings.TrimSpace(r.FormValue("dockerfile")); df != "" {
		_ = os.WriteFile(filepath.Join(extractDir, "Dockerfile"), []byte(df+"\n"), 0644)
	}

	// Check for multi-service project (docker-compose, multiple Dockerfiles, etc.)
	multiResult, multiErr := multiservice.Detect(extractDir)
	if multiErr == nil && multiResult != nil && multiResult.IsMultiService {
		handleMultiServiceDeploy(w, r, extractDir, serviceName, multiResult)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// "deploy" form field (default true): when "false", build + push to the
	// internal registry but skip deployment (자동 빌드 & 배포 체크박스 해제).
	deployFlag := r.FormValue("deploy") != "false"
	status, result := runSingleServicePipelineOpts(ctx, extractDir, serviceName, noopPublisher(), deployFlag)
	writeJSON(w, status, result)
}

// runSingleServicePipeline runs the shared build → AI-fix → registry push
// → deploy → health-check pipeline on an already-extracted source tree.
// Progress events are emitted through pub so async callers can stream
// them to the dashboard; synchronous callers pass noopPublisher().
//
// Returned status is the HTTP status the caller should write, and result
// is the JSON body. Success/failure is both reported via pub (as phase
// and error events) AND encoded in the returned result map.
func runSingleServicePipeline(ctx context.Context, extractDir, serviceName string, pub Publisher) (int, map[string]any) {
	return runSingleServicePipelineOpts(ctx, extractDir, serviceName, pub, true)
}

// runSingleServicePipelineOpts is the pipeline with an explicit deploy flag.
// When deploy=false the source is built and pushed to the internal registry
// but NOT deployed — the caller can later start a service from the returned
// registry image (portal "자동 빌드 & 배포" checkbox unchecked).
func runSingleServicePipelineOpts(ctx context.Context, extractDir, serviceName string, pub Publisher, deploy bool) (int, map[string]any) {
	phase := func(p, msg string) {
		pub(DeployEvent{Type: "phase", Phase: p, Message: msg})
		log.Printf("[deploy-pipeline] %s: %s", p, msg)
	}

	// Detect or generate Dockerfile
	phase("detect", "Dockerfile 감지 중...")
	dockerfilePath, found := runtime.DetectDockerfile(extractDir)
	var containerPort int
	dockerfileWasGenerated := false
	if found {
		containerPort = runtime.DetectContainerPort(extractDir, dockerfilePath)
		if containerPort == 0 {
			containerPort = 8080
		}
		phase("detect", fmt.Sprintf("기존 Dockerfile 감지: %s (port %d)", dockerfilePath, containerPort))
	} else {
		phase("detect", "Dockerfile 없음 → 언어 자동 감지 후 생성")
		_, port, genErr := runtime.GenerateDockerfile(extractDir)
		if genErr != nil {
			// Rule-based detection gave up. Fall back to Claude: feed it
			// the file tree and ask for a Dockerfile. Only attempted when
			// an API key is configured.
			if aifix.GetAPIKey() != "" {
				phase("aifix", "룰 기반 감지 실패 → Claude에 Dockerfile 생성 요청")
				prefPort, _ := runtime.FindAvailablePort(10000, 20000)
				_, aiPort, aiErr := aifix.GenerateDockerfileWithAI(ctx, extractDir, prefPort)
				if aiErr != nil {
					combined := fmt.Sprintf("%v / AI 생성 실패: %v", genErr, aiErr)
					pub(DeployEvent{Type: "error", Phase: "detect", Message: combined})
					return http.StatusBadRequest, map[string]any{
						"success": false, "message": combined,
					}
				}
				dockerfilePath = "Dockerfile"
				containerPort = aiPort
				dockerfileWasGenerated = true
				phase("detect", fmt.Sprintf("AI가 Dockerfile 생성 완료 (port %d)", containerPort))
			} else {
				msg := fmt.Sprintf("%v — ANTHROPIC_API_KEY가 설정되면 AI가 Dockerfile을 자동 생성합니다", genErr)
				pub(DeployEvent{Type: "error", Phase: "detect", Message: msg})
				return http.StatusBadRequest, map[string]any{
					"success": false, "message": msg,
				}
			}
		} else {
			dockerfilePath = "Dockerfile"
			containerPort = port
			dockerfileWasGenerated = true
			phase("detect", fmt.Sprintf("Dockerfile 자동 생성 완료 (port %d)", containerPort))
		}
	}

	// Capture the (possibly generated) Dockerfile content so the UI can show,
	// edit, and download it.
	generatedDockerfileContent := ""
	if dfBytes, derr := os.ReadFile(filepath.Join(extractDir, dockerfilePath)); derr == nil {
		generatedDockerfileContent = string(dfBytes)
	}

	cli := runtime.DockerClient()
	if cli == nil {
		pub(DeployEvent{Type: "error", Phase: "build", Message: "Docker 연결 실패"})
		return http.StatusInternalServerError, map[string]any{
			"success": false, "message": "Docker 연결 실패",
		}
	}

	imageName := fmt.Sprintf("orch-source-%s:latest", serviceName)
	phase("build", fmt.Sprintf("이미지 빌드 시작: %s", imageName))

	buildResult, buildErr := runtime.BuildImage(ctx, cli, extractDir, imageName, dockerfilePath)

	// AI-powered auto-fix on build failure
	var aifixInfo map[string]any
	aiAttempts := 0           // how many AI fix+rebuild attempts actually ran
	lastAISuggestion := ""    // last suggestion the AI proposed (for failure feedback)
	lastAIAnalysis := ""      // last root-cause analysis the AI produced
	aiStopReason := ""        // why the AI loop stopped early (no-fix / analyze-error)
	if buildErr != nil && aifix.GetAPIKey() != "" {
		phase("aifix", fmt.Sprintf("빌드 실패 → Claude가 로그 분석 중: %v", buildErr))

		buildLog := ""
		if buildResult != nil {
			buildLog = buildResult.BuildLog
		}

		for attempt := 1; attempt <= aifix.MaxRetries; attempt++ {
			phase("aifix", fmt.Sprintf("AI 수정 시도 %d/%d", attempt, aifix.MaxRetries))
			aiAttempts = attempt

			fixResult, fixErr := aifix.AnalyzeAndFix(ctx, extractDir, dockerfilePath, buildLog)
			if fixErr != nil {
				aiStopReason = fmt.Sprintf("AI 분석 호출 실패: %v", fixErr)
				pub(DeployEvent{Type: "error", Phase: "aifix", Message: aiStopReason})
				break
			}
			if fixResult.Analysis != "" {
				lastAIAnalysis = fixResult.Analysis
			}
			if fixResult.Suggestion != "" {
				lastAISuggestion = fixResult.Suggestion
			}
			if len(fixResult.Fixes) == 0 {
				aiStopReason = "AI가 더 이상 적용할 수정 사항을 찾지 못했습니다 (소스/설정 수동 점검 필요)"
				pub(DeployEvent{Type: "error", Phase: "aifix", Message: aiStopReason})
				break
			}
			phase("aifix", fmt.Sprintf("AI 제안 적용: %s (%d개 파일)", fixResult.Suggestion, len(fixResult.Fixes)))
			if err := aifix.ApplyFixes(extractDir, fixResult.Fixes); err != nil {
				aiStopReason = fmt.Sprintf("AI 수정 적용 실패: %v", err)
				pub(DeployEvent{Type: "error", Phase: "aifix", Message: aiStopReason})
				break
			}

			phase("build", fmt.Sprintf("AI 수정 후 재빌드 시도 %d/%d", attempt, aifix.MaxRetries))
			buildResult, buildErr = runtime.BuildImage(ctx, cli, extractDir, imageName, dockerfilePath)
			if buildErr == nil {
				aifixInfo = map[string]any{
					"applied":     true,
					"attempt":     attempt,
					"analysis":    fixResult.Analysis,
					"suggestion":  fixResult.Suggestion,
					"files_fixed": len(fixResult.Fixes),
				}
				phase("aifix", fmt.Sprintf("✓ AI 수정으로 빌드 성공 (%d/%d회차)", attempt, aifix.MaxRetries))
				break
			}
			if buildResult != nil {
				buildLog = buildResult.BuildLog
			}
			phase("aifix", fmt.Sprintf("시도 %d/%d 재빌드 실패: %v", attempt, aifix.MaxRetries, buildErr))
		}
	}

	if buildErr != nil {
		resp := buildFailureFeedback(buildErr, buildResult, aiAttempts, lastAIAnalysis, lastAISuggestion, aiStopReason)
		if logTail, ok := resp["build_log"].(string); ok && logTail != "" {
			pub(DeployEvent{Type: "log", Line: logTail})
		}
		pub(DeployEvent{Type: "error", Phase: "build", Message: resp["message"].(string)})
		// Stream the actionable next-steps so the user sees them live too.
		if steps, ok := resp["next_steps"].([]string); ok {
			for _, s := range steps {
				pub(DeployEvent{Type: "log", Line: "→ " + s})
			}
		}
		return http.StatusInternalServerError, resp
	}

	// Push to registry if running, then use registry image for deployment
	runImage := imageName
	var registryInfo map[string]any
	if isRegistryRunning(ctx) {
		phase("push", "내장 registry에 이미지 업로드 중...")
		repoTag := fmt.Sprintf("%s:latest", serviceName)
		regImage, pushErr := pushImageToRegistry(ctx, imageName, repoTag)
		if pushErr != nil {
			phase("push", fmt.Sprintf("registry push 실패, 로컬 이미지로 진행: %v", pushErr))
		} else {
			runImage = regImage
			registryInfo = map[string]any{
				"pushed":       true,
				"registry_tag": regImage,
				"external_tag": registryExternalTag(repoTag),
			}
			phase("push", fmt.Sprintf("registry 업로드 완료: %s", regImage))
		}
	}

	// Build-only mode: skip deployment, return the built/registry image so the
	// user can start a service from it later via /v1/cluster/deploy.
	if !deploy {
		phase("done", "빌드 완료 (자동 배포 안 함) — 이미지로 직접 기동할 수 있습니다")
		res := map[string]any{
			"success":      true,
			"build_only":   true,
			"message":      fmt.Sprintf("'%s' 이미지 빌드 완료 (배포는 건너뜀)", serviceName),
			"service_name": serviceName,
			"image":        runImage,
			"build_image":  imageName,
		}
		if aifixInfo != nil {
			res["ai_fix"] = aifixInfo
		}
		if registryInfo != nil {
			res["registry"] = registryInfo
		}
		if generatedDockerfileContent != "" {
			res["dockerfile"] = generatedDockerfileContent
			res["dockerfile_generated"] = dockerfileWasGenerated
			res["container_port"] = containerPort
		}
		return http.StatusOK, res
	}

	// Deploy container — use scheduler to pick least-loaded node if cluster is available
	phase("deploy", "노드 선정 및 컨테이너 기동...")
	deployResult := deployToOptimalNode(ctx, cli, runImage, serviceName, containerPort, nil)

	if !deployResult.OK {
		msg := fmt.Sprintf("컨테이너 실행 실패: %s", deployResult.Message)
		pub(DeployEvent{Type: "error", Phase: "deploy", Message: msg})
		return http.StatusInternalServerError, map[string]any{
			"success": false, "message": msg,
		}
	}
	hostPort := deployResult.HostPort
	details := deployResult.Details
	phase("deploy", fmt.Sprintf("노드 %s에 기동 완료 (port %d)", deployResult.NodeName, hostPort))

	// Post-deploy health check (only for local deployments where we have source)
	var healthInfo map[string]any
	localNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if deployResult.NodeName == "" || deployResult.NodeName == localNodeName || deployResult.NodeName == "local" {
		containerIDs, _ := details["container_ids"].([]string)
		if len(containerIDs) > 0 {
			phase("health", "컨테이너 헬스체크 및 런타임 AI 진단...")
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
					phase("health", "런타임 에러를 AI가 자동 복구했습니다")
				} else if ok, _ := hResult["healthy"].(bool); ok {
					phase("health", "헬스체크 통과")
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
	if generatedDockerfileContent != "" {
		result["dockerfile"] = generatedDockerfileContent
		result["dockerfile_generated"] = dockerfileWasGenerated
	}
	return http.StatusOK, result
}

// handleGenerateDockerfile previews the Dockerfile that would be used for an
// uploaded source — detecting an existing one or generating it (rule-based,
// then AI). It does NOT build or deploy. Returns the Dockerfile content + the
// chosen container port so the user can edit/download it and then deploy.
//
// POST multipart: file=<zip|tar.gz>
func handleGenerateDockerfile(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "파일이 너무 크거나 잘못된 요청입니다"})
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "파일이 필요합니다"})
		return
	}
	defer file.Close()
	filename := filepath.Base(header.Filename)
	if !isAllowedArchive(filename) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "zip 또는 tar.gz 파일만 지원합니다"})
		return
	}

	tmpDir, err := os.MkdirTemp("", "gen-dockerfile-*")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "임시 디렉터리 생성 실패"})
		return
	}
	defer os.RemoveAll(tmpDir)

	archivePath := filepath.Join(tmpDir, filename)
	dst, err := os.Create(archivePath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "파일 저장 실패"})
		return
	}
	if _, err := io.Copy(dst, file); err != nil {
		dst.Close()
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": "파일 저장 실패"})
		return
	}
	dst.Close()

	extractDir := filepath.Join(tmpDir, "source")
	os.MkdirAll(extractDir, 0755)
	var extractErr error
	if strings.HasSuffix(filename, ".zip") {
		extractErr = extractZip(archivePath, extractDir)
	} else {
		extractErr = extractTarGz(archivePath, extractDir)
	}
	if extractErr != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": fmt.Sprintf("압축 해제 실패: %v", extractErr)})
		return
	}
	extractDir = resolveRootDir(extractDir)

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	dfPath, found := runtime.DetectDockerfile(extractDir)
	var port int
	generated := false
	if found {
		port = runtime.DetectContainerPort(extractDir, dfPath)
		if port == 0 {
			port = 8080
		}
	} else {
		_, p, genErr := runtime.GenerateDockerfile(extractDir)
		if genErr != nil {
			if aifix.GetAPIKey() != "" {
				prefPort, _ := runtime.FindAvailablePort(10000, 20000)
				_, aiPort, aiErr := aifix.GenerateDockerfileWithAI(ctx, extractDir, prefPort)
				if aiErr != nil {
					writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": fmt.Sprintf("Dockerfile 생성 실패: %v / %v", genErr, aiErr)})
					return
				}
				dfPath = "Dockerfile"
				port = aiPort
				generated = true
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": genErr.Error()})
				return
			}
		} else {
			dfPath = "Dockerfile"
			port = p
			generated = true
		}
	}

	content := ""
	if b, e := os.ReadFile(filepath.Join(extractDir, dfPath)); e == nil {
		content = string(b)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":        true,
		"dockerfile":     content,
		"container_port": port,
		"generated":      generated, // false = existing Dockerfile found in the archive
		"filename":       filename,
	})
}

// handleDeploySourceAsync is the streaming variant of handleDeploySource.
// Same multipart contract, but responds immediately with {deploy_id} and
// runs the pipeline in the background, publishing progress events to
// /v1/services/deploy/{id}/events.
//
// Path: POST /v1/services/deploy-source/async
func handleDeploySourceAsync(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "파일이 너무 크거나 잘못된 요청입니다 (최대 800MB)",
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

	filename := filepath.Base(header.Filename)
	if filename == "." || filename == "/" || filename == "" || strings.Contains(filename, "..") {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "잘못된 파일 이름입니다.",
		})
		return
	}
	if !isAllowedArchive(filename) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "zip 또는 tar.gz 파일만 지원합니다.",
		})
		return
	}

	serviceName := sanitizeServiceName(r.FormValue("name"))
	if serviceName == "" {
		serviceName = sanitizeServiceName(stripArchiveExt(filename))
	}
	if serviceName == "" {
		serviceName = "source-app"
	}
	// Attribute the async deploy to the requesting user (consumed later in
	// the background deploy via deployToOptimalNode).
	if rejectForeignService(w, r, serviceName) {
		return
	}
	setPendingOwner(serviceName, requesterUsername(r))

	// Persistent temp dir — the goroutine outlives this handler so we
	// cannot use `defer os.RemoveAll`. The goroutine itself cleans up.
	tmpDir, err := os.MkdirTemp("", "deploy-source-async-*")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "임시 디렉터리 생성 실패",
		})
		return
	}

	archivePath := filepath.Join(tmpDir, filename)
	dst, err := os.Create(archivePath)
	if err != nil {
		os.RemoveAll(tmpDir)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "파일 저장 실패",
		})
		return
	}
	if _, err := io.Copy(dst, file); err != nil {
		dst.Close()
		os.RemoveAll(tmpDir)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "파일 저장 실패",
		})
		return
	}
	dst.Close()

	job := registerDeployJob(serviceName, requesterUsername(r))
	pub := job.Publisher()
	pub(DeployEvent{Type: "phase", Phase: "upload", Message: fmt.Sprintf("업로드 완료: %s", filename)})

	writeJSON(w, http.StatusAccepted, map[string]any{
		"success":      true,
		"deploy_id":    job.ID,
		"service_name": serviceName,
		"events_url":   fmt.Sprintf("/v1/services/deploy/%s/events", job.ID),
		"status_url":   fmt.Sprintf("/v1/services/deploy/%s", job.ID),
	})

	deployFlag := r.FormValue("deploy") != "false"
	editedDockerfile := strings.TrimSpace(r.FormValue("dockerfile"))
	go runSourceDeployInBackground(tmpDir, archivePath, filename, serviceName, job, deployFlag, editedDockerfile)
}

func runSourceDeployInBackground(tmpDir, archivePath, filename, serviceName string, job *DeployJob, deploy bool, editedDockerfile string) {
	defer os.RemoveAll(tmpDir)
	pub := job.Publisher()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	extractDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		pub(DeployEvent{Type: "error", Phase: "extract", Message: err.Error()})
		job.Finalize(false, map[string]any{"success": false, "message": err.Error()})
		return
	}

	pub(DeployEvent{Type: "phase", Phase: "extract", Message: "압축 해제 중..."})
	var extractErr error
	if strings.HasSuffix(filename, ".zip") {
		extractErr = extractZip(archivePath, extractDir)
	} else {
		extractErr = extractTarGz(archivePath, extractDir)
	}
	if extractErr != nil {
		msg := fmt.Sprintf("압축 해제 실패: %v", extractErr)
		pub(DeployEvent{Type: "error", Phase: "extract", Message: msg})
		job.Finalize(false, map[string]any{"success": false, "message": msg})
		return
	}
	extractDir = resolveRootDir(extractDir)
	pub(DeployEvent{Type: "phase", Phase: "extract", Message: "압축 해제 완료"})

	// Edited-Dockerfile override (from the preview/edit flow).
	if editedDockerfile != "" {
		_ = os.WriteFile(filepath.Join(extractDir, "Dockerfile"), []byte(editedDockerfile+"\n"), 0644)
		pub(DeployEvent{Type: "phase", Phase: "detect", Message: "사용자가 편집한 Dockerfile 적용"})
	}

	// Multi-service projects: the async pipeline doesn't cover them yet.
	// Point the caller at the synchronous multi-service endpoint.
	multiResult, multiErr := multiservice.Detect(extractDir)
	if multiErr == nil && multiResult != nil && multiResult.IsMultiService {
		msg := "멀티 서비스 프로젝트는 동기 엔드포인트(/v1/services/deploy-source)로 배포해주세요"
		pub(DeployEvent{Type: "error", Phase: "detect", Message: msg})
		job.Finalize(false, map[string]any{"success": false, "message": msg, "multi_service": true})
		return
	}

	status, result := runSingleServicePipelineOpts(ctx, extractDir, serviceName, pub, deploy)
	job.Finalize(status == http.StatusOK, result)
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
	serviceNetworkMap := map[string]int{}

	frontendName := pickFrontendService(services)
	log.Printf("[deploy-source] group=%s frontend=%s", projectName, frontendName)

	for _, idx := range deployOrder {
		svc := services[idx]
		svcFullName := projectName + "-" + svc.Name

		// Tag every container with the group name so we can stop/list them
		// as a unit. Exactly one service is also tagged as the public-facing
		// frontend for endpoint filtering.
		labels := map[string]string{
			"ai.orchestrator.group":   projectName,
			"ai.orchestrator.service": svcFullName,
		}
		if svc.Name == frontendName {
			labels["ai.orchestrator.frontend"] = "true"
		}
		setPendingLabels(svcFullName, labels)
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
				svcAIAttempts := 0
				svcLastSuggestion := ""
				svcLastAnalysis := ""
				for attempt := 1; attempt <= aifix.MaxRetries; attempt++ {
					svcAIAttempts = attempt
					fixResult, fixErr := aifix.AnalyzeAndFix(ctx, buildContext, dockerfilePath, buildLog)
					if fixErr != nil || len(fixResult.Fixes) == 0 {
						break
					}
					if fixResult.Suggestion != "" {
						svcLastSuggestion = fixResult.Suggestion
					}
					if fixResult.Analysis != "" {
						svcLastAnalysis = fixResult.Analysis
					}
					if err := aifix.ApplyFixes(buildContext, fixResult.Fixes); err != nil {
						break
					}
					buildResult, buildErr = runtime.BuildImage(ctx, cli, buildContext, imageName, dockerfilePath)
					if buildErr == nil {
						log.Printf("[deploy-source] %s build succeeded after AI fix (attempt %d/%d)", svc.Name, attempt, aifix.MaxRetries)
						break
					}
					if buildResult != nil {
						buildLog = buildResult.BuildLog
					}
				}

				if buildErr != nil {
					fb := buildFailureFeedback(buildErr, buildResult, svcAIAttempts, svcLastAnalysis, svcLastSuggestion, "")
					errEntry := map[string]any{
						"service":    svc.Name,
						"error":      fmt.Sprintf("빌드 실패: %v", buildErr),
						"detail":     fb["detail"],
						"next_steps": fb["next_steps"],
					}
					if s, ok := fb["ai_summary"].(string); ok {
						errEntry["ai_summary"] = s
					}
					errors = append(errors, errEntry)
					continue
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

		// Pass the compose service's short name as additional DNS alias so
		// inter-service references (e.g. hostname "db" in DATABASE_URL)
		// resolve correctly alongside the project-prefixed full name.
		dr := deployToOptimalNode(ctx, cli, runImage, svcFullName, containerPort, envVars, svc.Name)
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

		log.Printf("[deploy-source] Service %s deployed on port %d", svcFullName, dr.HostPort)
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

// pickFrontendService chooses the "public face" of a compose project.
// Heuristic (first match wins):
//  1. Name matches common frontend names: frontend, web, app, ui, api, nginx, gateway.
//  2. Service declares ports: (externally exposed) AND image isn't a well-known backend
//     store (postgres/mysql/mongo/redis/kafka/elasticsearch/...).
//  3. No one depends_on it AND it depends_on something (top of dep tree).
//  4. Fallback: first service.
func pickFrontendService(services []multiservice.ServiceDef) string {
	if len(services) == 0 {
		return ""
	}
	// 1. preferred names
	preferred := map[string]int{
		"frontend": 1, "web": 2, "ui": 2, "app": 3, "nginx": 3, "gateway": 4, "api": 5,
	}
	var bestName string
	bestRank := 999
	for _, s := range services {
		if r, ok := preferred[strings.ToLower(s.Name)]; ok && r < bestRank {
			bestRank = r
			bestName = s.Name
		}
	}
	if bestName != "" {
		return bestName
	}

	// 2. has ports + not a backend store
	isStore := func(img string) bool {
		img = strings.ToLower(img)
		for _, kw := range []string{"postgres", "postgresql", "mysql", "mariadb", "mongo", "redis", "memcached", "kafka", "rabbitmq", "elasticsearch", "etcd", "zookeeper", "cassandra", "couchdb", "clickhouse"} {
			if strings.Contains(img, kw) {
				return true
			}
		}
		return false
	}
	for _, s := range services {
		if len(s.Ports) > 0 && !isStore(s.Image) {
			return s.Name
		}
	}

	// 3. top of dependency tree (no one depends on it, it depends on others)
	dependedOn := map[string]bool{}
	for _, s := range services {
		for _, d := range s.DependsOn {
			dependedOn[d] = true
		}
	}
	for _, s := range services {
		if !dependedOn[s.Name] && len(s.DependsOn) > 0 {
			return s.Name
		}
	}

	// 4. fallback
	return services[0].Name
}

// pendingDeployLabels stashes compose-specific labels (e.g. group, frontend)
// for the next deployToOptimalNode call. Keyed by service name to avoid
// bloating function signatures across the call chain. Consumed and cleared
// by deployLocally/deployToRemoteNode when they build the run request.
var (
	pendingDeployLabelsMu sync.Mutex
	pendingDeployLabels   = map[string]map[string]string{}
)

func setPendingLabels(serviceName string, labels map[string]string) {
	pendingDeployLabelsMu.Lock()
	pendingDeployLabels[serviceName] = labels
	pendingDeployLabelsMu.Unlock()
}

func consumePendingLabels(serviceName string) map[string]string {
	pendingDeployLabelsMu.Lock()
	defer pendingDeployLabelsMu.Unlock()
	labels := pendingDeployLabels[serviceName]
	delete(pendingDeployLabels, serviceName)
	return labels
}

// pendingDeployOwners records the deploying user for a service name so the
// owner can be persisted into cluster state once the container is up (the
// deploy call chain doesn't thread the request through).
var (
	pendingOwnerMu sync.Mutex
	pendingOwners  = map[string]string{}
)

func setPendingOwner(serviceName, owner string) {
	if owner == "" {
		return
	}
	pendingOwnerMu.Lock()
	pendingOwners[serviceName] = owner
	pendingOwnerMu.Unlock()
}

func consumePendingOwner(serviceName string) string {
	pendingOwnerMu.Lock()
	defer pendingOwnerMu.Unlock()
	o := pendingOwners[serviceName]
	delete(pendingOwners, serviceName)
	return o
}

// buildFailureFeedback assembles a detailed, user-facing response when an
// image build ultimately fails — after exhausting the AI auto-fix retries (or
// when AI is disabled). It explains what was tried and gives concrete,
// ordered next steps so the user knows exactly how to recover.
func buildFailureFeedback(buildErr error, buildResult *runtime.BuildResult, aiAttempts int, lastAnalysis, lastSuggestion, aiStopReason string) map[string]any {
	resp := map[string]any{
		"success": false,
		"message": fmt.Sprintf("이미지 빌드 실패: %v", buildErr),
	}

	// Build-log tail — the single most useful artifact for diagnosis.
	logTail := ""
	if buildResult != nil && buildResult.BuildLog != "" {
		logTail = buildResult.BuildLog
		if len(logTail) > 2500 {
			logTail = logTail[len(logTail)-2500:]
		}
		resp["build_log"] = logTail
	}

	aiEnabled := aifix.GetAPIKey() != ""

	// Summary line describing what the auto-fixer did.
	var summary string
	switch {
	case !aiEnabled:
		summary = "AI 자동 수정이 비활성화되어 있어 빌드 에러를 자동으로 고치지 못했습니다."
	case aiAttempts == 0:
		summary = "빌드가 실패했지만 AI 자동 수정 단계까지 진행되지 못했습니다."
	default:
		summary = fmt.Sprintf("AI가 최대 %d회까지 자동 수정·재빌드를 시도했지만 (%d회 실행) 빌드를 성공시키지 못했습니다.",
			aifix.MaxRetries, aiAttempts)
	}
	resp["ai_summary"] = summary
	if aiAttempts > 0 {
		resp["ai_attempts"] = aiAttempts
		resp["ai_max_retries"] = aifix.MaxRetries
	}
	if lastAnalysis != "" {
		resp["ai_last_analysis"] = lastAnalysis
	}
	if lastSuggestion != "" {
		resp["ai_last_suggestion"] = lastSuggestion
	}
	if aiStopReason != "" {
		resp["ai_stop_reason"] = aiStopReason
	}

	// Ordered, actionable next steps.
	steps := []string{}
	if !aiEnabled {
		steps = append(steps, "관리자에게 ANTHROPIC_API_KEY 설정을 요청하면 빌드 에러를 AI가 자동 분석·수정합니다.")
	}
	if logTail != "" {
		steps = append(steps, "위 빌드 로그의 마지막 에러 메시지를 확인하세요 — 대부분 누락된 의존성, 잘못된 베이스 이미지, 또는 컴파일/문법 오류입니다.")
	}
	if lastSuggestion != "" {
		steps = append(steps, "AI 제안(\""+lastSuggestion+"\")을 참고해 소스 또는 Dockerfile을 직접 수정한 뒤 다시 업로드하세요.")
	}
	steps = append(steps,
		"프로젝트 루트에 직접 작성한 Dockerfile을 포함해 업로드하면 자동 생성보다 안정적으로 빌드됩니다.",
		"의존성 설치 실패라면 requirements.txt / package.json / go.mod 등 의존성 파일이 아카이브에 포함됐는지 확인하세요.",
		"베이스 이미지 pull 실패라면 공개 이미지 태그(예: python:3.12-slim, node:20-alpine)가 정확한지 확인하세요.",
		"로컬에서 먼저 'docker build .' 로 빌드가 되는지 검증한 뒤 그 소스를 업로드하면 가장 확실합니다.",
	)
	resp["next_steps"] = steps

	// A single human-readable message combining everything (for clients that
	// only render `message`).
	var b strings.Builder
	b.WriteString(summary)
	if lastSuggestion != "" {
		b.WriteString(" 마지막 AI 제안: " + lastSuggestion + ".")
	}
	b.WriteString(" 다음 조치를 권장합니다: ")
	for i, s := range steps {
		b.WriteString(fmt.Sprintf("%d) %s ", i+1, s))
	}
	resp["detail"] = b.String()

	return resp
}

// deployToOptimalNode uses the scheduler to pick the least-loaded node and deploy there.
// If cluster/scheduler is not available, falls back to local deployment.
// extraAliases adds short Docker DNS aliases (e.g. "db" for "myapp-db") so
// references inside compose files continue to work after deployment.
func deployToOptimalNode(ctx context.Context, cli *client.Client, image string, serviceName string, containerPort int, envVars []string, extraAliases ...string) deployNodeResult {
	// Try cluster-aware deployment
	if sched != nil && clusterState != nil {
		decisions, err := sched.Schedule(serviceName, image, 1, nil, "least-loaded")
		if err == nil && len(decisions) > 0 {
			targetNodeName := decisions[0].NodeName
			node := clusterState.GetNode(targetNodeName)

			localNodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
			if node != nil && targetNodeName != localNodeName {
				log.Printf("[deploy-source] Scheduler selected node '%s' (least-loaded) for %s", targetNodeName, serviceName)
				rres := deployToRemoteNode(ctx, node, image, serviceName, containerPort, envVars, extraAliases...)
				recordDeployOwner(serviceName, image, rres.OK)
				return rres
			}
			log.Printf("[deploy-source] Scheduler selected master node for %s", serviceName)
		} else if err != nil {
			log.Printf("[deploy-source] Scheduler error (falling back to local): %v", err)
		}
	}

	res := deployLocally(ctx, cli, image, serviceName, containerPort, envVars, extraAliases...)
	recordDeployOwner(serviceName, image, res.OK)
	return res
}

// recordDeployOwner persists the deploying user as the service owner in cluster
// state once a source-deploy succeeds, so owner checks and the user portal's
// "my services" listing attribute it correctly. No-op without a pending owner.
func recordDeployOwner(serviceName, image string, ok bool) {
	if !ok || clusterState == nil {
		return
	}
	owner := consumePendingOwner(serviceName)
	if owner == "" {
		return
	}
	clusterState.SaveService(serviceName, image, 1, map[string]any{"owner": owner})
}

// runContainerWithPortRetry runs a single-container service with automatic
// retry on "port is already allocated" errors. FindAvailablePort already
// excludes known Docker-bound ports, but a few states still slip through
// (concurrent deploys, exited containers whose port table entry lingers,
// host-OS races). If RunContainer fails with a port conflict, we mark
// that port tried and try another — up to maxAttempts.
//
// Returns (ok, message, details, chosenHostPort).
func runContainerWithPortRetry(ctx context.Context, cli *client.Client, image string, containerPort int, opts runtime.RunContainerOpts) (bool, string, map[string]any, int) {
	const maxAttempts = 8
	tried := make(map[int]bool, maxAttempts)
	var lastMsg string
	var lastPort int
	for attempt := 0; attempt < maxAttempts; attempt++ {
		port, err := runtime.FindAvailablePortExcluding(10000, 60000, tried)
		if err != nil {
			return false, err.Error(), nil, 0
		}
		tried[port] = true
		lastPort = port

		attemptOpts := opts
		attemptOpts.Ports = []string{fmt.Sprintf("%d:%d", port, containerPort)}

		ok, msg, details := runtime.RunContainer(ctx, cli, image, attemptOpts)
		if ok {
			return true, msg, details, port
		}
		lastMsg = msg
		if !runtime.IsPortAllocationError(fmt.Errorf("%s", msg)) {
			break
		}
		log.Printf("[deploy-retry] port %d conflict on attempt %d/%d (%s); retrying with a different port",
			port, attempt+1, maxAttempts, strings.TrimSpace(msg))
	}
	return false, lastMsg, nil, lastPort
}

func deployLocally(ctx context.Context, cli *client.Client, image string, serviceName string, containerPort int, envVars []string, extraAliases ...string) deployNodeResult {
	nodeName := os.Getenv("ORCHESTRATOR_NODE_NAME")
	if nodeName == "" {
		nodeName = "local"
	}
	nodeIP := "localhost"
	advertiseAddr := os.Getenv("ORCHESTRATOR_ADVERTISE_ADDR")
	if advertiseAddr != "" {
		nodeIP = strings.Split(advertiseAddr, ":")[0]
	}

	ok, msg, details, hostPort := runContainerWithPortRetry(ctx, cli, image, containerPort, runtime.RunContainerOpts{
		Name:               serviceName,
		Replicas:           1,
		UseInternalNetwork: true,
		Environment:        envVars,
		ExtraAliases:       extraAliases,
		ExtraLabels:        consumePendingLabels(serviceName),
	})

	return deployNodeResult{
		OK:       ok,
		Message:  msg,
		HostPort: hostPort,
		NodeName: nodeName,
		NodeIP:   nodeIP,
		Details:  details,
	}
}

func deployToRemoteNode(ctx context.Context, node *models.NodeInfo, image string, serviceName string, containerPort int, envVars []string, extraAliases ...string) deployNodeResult {
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

	// Step 2: Run container on remote node.
	// Don't bind the container port onto the worker host — multiple services
	// (especially from docker-compose) often share the same container port,
	// which would cause "port already allocated" collisions. Instead rely on
	// the orch-internal network + Traefik path routing via master.
	// If a specific host port is needed, that still works at master via the
	// single-service deploy path (deployLocally).
	payload := map[string]any{
		"image":                image,
		"name":                 serviceName,
		"replicas":             1,
		"use_internal_network": true,
		"environment":          envVars,
		"extra_aliases":        extraAliases,
	}
	if labels := consumePendingLabels(serviceName); len(labels) > 0 {
		payload["extra_labels"] = labels
	}
	runPayload, _ := json.Marshal(payload)
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

			// Find new port and redeploy (with automatic port-conflict retry)
			ok, msg, details, newPort := runContainerWithPortRetry(ctx, cli, runImage, containerPort, runtime.RunContainerOpts{
				Name:               serviceName,
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

			runImage := imageName
			if isRegistryRunning(ctx) {
				repoTag := fmt.Sprintf("%s:latest", serviceName)
				if regImg, err := pushImageToRegistry(ctx, imageName, repoTag); err == nil {
					runImage = regImg
				}
			}

			ok, msg, details, newPort := runContainerWithPortRetry(ctx, cli, runImage, containerPort, runtime.RunContainerOpts{
				Name:               serviceName,
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

// Archive extraction limits — size/count ceilings removed per operator request.
// The remaining defences (zip-slip path validation, symlink rejection, non-
// regular-file filter) stay in effect. Extraction will only fail on disk-full
// or parser errors. Uploaders are still bounded by the HTTP body cap (800MB).

// safeExtractPath validates target is strictly inside destDir (anti zip-slip).
func safeExtractPath(destDir, name string) (string, bool) {
	target := filepath.Join(destDir, name)
	clean := filepath.Clean(target)
	root := filepath.Clean(destDir) + string(os.PathSeparator)
	if !strings.HasPrefix(clean, root) && clean != filepath.Clean(destDir) {
		return "", false
	}
	return clean, true
}

func extractZip(archivePath, destDir string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		target, ok := safeExtractPath(destDir, f.Name)
		if !ok {
			continue // zip-slip attempt
		}
		// Skip all non-regular-file / non-directory entries (symlinks, devices).
		mode := f.Mode()
		if mode&os.ModeSymlink != 0 || mode&os.ModeDevice != 0 || mode&os.ModeSocket != 0 || mode&os.ModeNamedPipe != 0 {
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

		target, ok := safeExtractPath(destDir, hdr.Name)
		if !ok {
			continue // tar-slip attempt
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
			_, err = io.Copy(out, tr)
			out.Close()
			if err != nil {
				return err
			}
			if hdr.Mode&0111 != 0 {
				os.Chmod(target, 0755)
			}
		case tar.TypeSymlink, tar.TypeLink:
			// Reject — symlink targets can escape the extraction root.
			continue
		default:
			// Ignore FIFOs, char/block devices, etc.
			continue
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// docker-compose.yml content deploy
// ---------------------------------------------------------------------------

const maxComposeBytes = 512 * 1024 // 512KB

// handleDeployCompose accepts a docker-compose.yml content (no build context)
// and deploys all its services via the existing multi-service pipeline.
//
// Supported inputs:
//   - multipart/form-data: field "file" = uploaded compose file, optional "name" field
//   - application/json:    {"compose": "...yaml...", "project_name": "myproj"}
//
// Services with a build: section are rejected — those need the source archive
// endpoint (POST /v1/services/deploy-source) since compose content alone
// doesn't include the Dockerfile build context.
func handleDeployCompose(w http.ResponseWriter, r *http.Request) {
	var composeContent []byte
	var projectName string

	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "multipart/form-data") {
		r.Body = http.MaxBytesReader(w, r.Body, maxComposeBytes)
		if err := r.ParseMultipartForm(maxComposeBytes); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false, "message": "파일이 너무 크거나 잘못된 요청입니다 (최대 512KB)",
			})
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false, "message": "file 필드가 필요합니다",
			})
			return
		}
		defer file.Close()
		buf, err := io.ReadAll(io.LimitReader(file, maxComposeBytes))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"success": false, "message": "파일 읽기 실패",
			})
			return
		}
		composeContent = buf
		projectName = r.FormValue("name")
	} else {
		var body struct {
			Compose     string `json:"compose"`
			ProjectName string `json:"project_name"`
		}
		if err := readJSON(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false, "message": "JSON body 또는 multipart 업로드가 필요합니다",
			})
			return
		}
		if len(body.Compose) > maxComposeBytes {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false, "message": "compose 내용이 너무 큽니다 (최대 512KB)",
			})
			return
		}
		composeContent = []byte(body.Compose)
		projectName = body.ProjectName
	}

	if len(composeContent) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "compose 내용이 비어 있습니다",
		})
		return
	}

	// Write to a temp directory so handleMultiServiceDeploy's build paths (if any)
	// resolve. Even for image-only composes, some code paths in the pipeline
	// pass through an extractDir, so keeping a real directory is the safer choice.
	tmpDir, err := os.MkdirTemp("", "deploy-compose-*")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "임시 디렉터리 생성 실패",
		})
		return
	}
	defer os.RemoveAll(tmpDir)

	composePath := filepath.Join(tmpDir, "docker-compose.yml")
	if err := os.WriteFile(composePath, composeContent, 0o600); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": "compose 파일 저장 실패",
		})
		return
	}

	services, err := multiservice.ParseComposeBytes(composeContent, tmpDir)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "docker-compose 파싱 실패: " + err.Error(),
		})
		return
	}
	if len(services) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "message": "services 섹션이 비어 있습니다",
		})
		return
	}

	// Reject services with a build: directive (we have no context to build from).
	for _, s := range services {
		if s.BuildContext != "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false,
				"message": fmt.Sprintf("'%s' 서비스에 build: 섹션이 있습니다. build 소스가 필요하면 '/v1/services/deploy-source'로 전체 프로젝트를 zip/tar.gz로 업로드하세요.", s.Name),
			})
			return
		}
		if s.Image == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false,
				"message": fmt.Sprintf("'%s' 서비스에 image: 또는 build:가 모두 없습니다", s.Name),
			})
			return
		}
	}

	projectName = sanitizeServiceName(projectName)
	if projectName == "" {
		projectName = "compose"
	}

	// Auto-save as a reusable template keyed by project name.
	// Stored BEFORE the deploy so users can retrieve the YAML even if deploy
	// partially fails (they can fix + redeploy).
	if err := UpsertComposeTemplate(projectName, projectName, string(composeContent), true); err != nil {
		log.Printf("[deploy-compose] template save failed (non-fatal): %v", err)
	}

	result := &multiservice.DetectResult{
		IsMultiService: len(services) > 1,
		Source:         "docker-compose",
		Services:       services,
	}

	log.Printf("[deploy-compose] project=%s services=%d (template saved)", projectName, len(services))
	handleMultiServiceDeploy(w, r, tmpDir, projectName, result)
}
