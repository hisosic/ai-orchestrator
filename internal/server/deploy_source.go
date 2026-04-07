package server

import (
	"archive/tar"
	"archive/zip"
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

	"ai-container-go/internal/runtime"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	log.Printf("[deploy-source] Building image %s from %s (Dockerfile: %s)", imageName, extractDir, dockerfilePath)
	_, buildErr := runtime.BuildImage(ctx, cli, extractDir, imageName, dockerfilePath)
	if buildErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("이미지 빌드 실패: %v", buildErr),
		})
		return
	}

	// Find available port
	hostPort, err := runtime.FindAvailablePort(10000, 60000)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": err.Error(),
		})
		return
	}

	// Deploy container
	portMapping := fmt.Sprintf("%d:%d", hostPort, containerPort)
	ok, msg, details := runtime.RunContainer(ctx, cli, imageName, runtime.RunContainerOpts{
		Name:               serviceName,
		Ports:              []string{portMapping},
		Replicas:           1,
		UseInternalNetwork: true,
	})

	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false, "message": fmt.Sprintf("컨테이너 실행 실패: %s", msg),
		})
		return
	}

	if hub != nil {
		if data, err := json.Marshal(map[string]any{
			"event": "deploy-source", "service": serviceName, "image": imageName, "port": hostPort,
		}); err == nil {
			hub.broadcast(data)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":        true,
		"message":        fmt.Sprintf("서비스 '%s' 배포 완료 (포트: %d)", serviceName, hostPort),
		"service_name":   serviceName,
		"image":          imageName,
		"host_port":      hostPort,
		"container_port": containerPort,
		"details":        details,
		"url":            fmt.Sprintf("http://localhost:%d", hostPort),
		"deploy_message": msg,
	})
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
