package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	dtypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// BuildResult contains the output of a Docker image build.
type BuildResult struct {
	ImageID  string
	BuildLog string
}

// BuildImage builds a Docker image from a directory context.
// Returns BuildResult with the image ID and full build log.
func BuildImage(ctx context.Context, cli *client.Client, contextDir string, imageName string, dockerfile string) (*BuildResult, error) {
	buildCtx, err := createBuildContext(contextDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create build context: %w", err)
	}

	if dockerfile == "" {
		dockerfile = "Dockerfile"
	}

	opts := dtypes.ImageBuildOptions{
		Tags:       []string{imageName},
		Dockerfile: dockerfile,
		Remove:     true,
		ForceRemove: true,
	}

	resp, err := cli.ImageBuild(ctx, buildCtx, opts)
	if err != nil {
		return nil, fmt.Errorf("docker build failed: %w", err)
	}
	defer resp.Body.Close()

	// Parse build output stream for errors
	var imageID string
	var buildLog strings.Builder
	decoder := json.NewDecoder(resp.Body)
	for {
		var msg struct {
			Stream string `json:"stream"`
			Error  string `json:"error"`
			Aux    struct {
				ID string `json:"ID"`
			} `json:"aux"`
		}
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			break
		}
		if msg.Stream != "" {
			buildLog.WriteString(msg.Stream)
			log.Printf("[build] %s", strings.TrimRight(msg.Stream, "\n"))
		}
		if msg.Error != "" {
			buildLog.WriteString("ERROR: " + msg.Error + "\n")
			return &BuildResult{BuildLog: buildLog.String()}, fmt.Errorf("build error: %s", msg.Error)
		}
		if msg.Aux.ID != "" {
			imageID = msg.Aux.ID
		}
	}

	return &BuildResult{ImageID: imageID, BuildLog: buildLog.String()}, nil
}

// DetectDockerfile searches for an existing Dockerfile in the directory.
// Checks root first, then one level deep. Returns the relative path and whether it was found.
func DetectDockerfile(dir string) (string, bool) {
	// Check root
	if _, err := os.Stat(filepath.Join(dir, "Dockerfile")); err == nil {
		return "Dockerfile", true
	}
	if _, err := os.Stat(filepath.Join(dir, "dockerfile")); err == nil {
		return "dockerfile", true
	}

	// Check one level deep
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		candidate := filepath.Join(e.Name(), "Dockerfile")
		if _, err := os.Stat(filepath.Join(dir, candidate)); err == nil {
			return candidate, true
		}
	}
	return "", false
}

// generatedContainerPort returns an unused TCP port >= 10000 to use as the
// generated Dockerfile's EXPOSE/listen port. Falls back to a value in-range if
// the availability probe fails so generation never blocks.
func generatedContainerPort() int {
	if p, err := FindAvailablePort(10000, 20000); err == nil && p >= 10000 {
		return p
	}
	return 10080
}

// GenerateDockerfile auto-generates a Dockerfile based on detected language.
// Returns the Dockerfile content, the chosen container port, and any error.
// The container/listen port is allocated from the unused 10000+ range and the
// app is told to bind to it via the PORT/SERVER_PORT env (honored by most
// frameworks) or, for nginx static sites, by rewriting the listen directive.
func GenerateDockerfile(dir string) (string, int, error) {
	port := generatedContainerPort()

	// Node.js
	if _, err := os.Stat(filepath.Join(dir, "package.json")); err == nil {
		content := fmt.Sprintf(`FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
ENV PORT=%d
EXPOSE %d
CMD ["npm", "start"]
`, port, port)
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Python
	if _, err := os.Stat(filepath.Join(dir, "requirements.txt")); err == nil {
		cmd := detectPythonCmd(dir, port)
		content := fmt.Sprintf(`FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=%d
EXPOSE %d
CMD %s
`, port, port, cmd)
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Go
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
		content := fmt.Sprintf(`FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o server .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/server .
ENV PORT=%d
EXPOSE %d
CMD ["./server"]
`, port, port)
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Java Maven (Spring Boot honors SERVER_PORT)
	if _, err := os.Stat(filepath.Join(dir, "pom.xml")); err == nil {
		content := fmt.Sprintf(`FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app
COPY . .
RUN mvn package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
ENV SERVER_PORT=%d
ENV PORT=%d
EXPOSE %d
CMD ["java", "-jar", "app.jar"]
`, port, port, port)
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Java Gradle
	if _, err := os.Stat(filepath.Join(dir, "build.gradle")); err == nil {
		content := fmt.Sprintf(`FROM gradle:8-jdk17 AS builder
WORKDIR /app
COPY . .
RUN gradle build -x test

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar
ENV SERVER_PORT=%d
ENV PORT=%d
EXPOSE %d
CMD ["java", "-jar", "app.jar"]
`, port, port, port)
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Static HTML (nginx) — rewrite nginx's listen port to the 10000+ port.
	if _, err := os.Stat(filepath.Join(dir, "index.html")); err == nil {
		content := fmt.Sprintf(`FROM nginx:alpine
COPY . /usr/share/nginx/html
RUN sed -i 's/listen[[:space:]]*80;/listen %d;/' /etc/nginx/conf.d/default.conf
EXPOSE %d
CMD ["nginx", "-g", "daemon off;"]
`, port, port)
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	return "", 0, fmt.Errorf("소스 코드의 언어를 감지할 수 없습니다. Dockerfile을 포함해 주세요")
}

// FindAvailablePort finds an available TCP port in the given range.
// Checks OS-level (net.Listen) and Docker-level (container port bindings
// for running + stopped containers) allocations. Docker keeps a port
// reserved for exited containers until they're removed, and net.Listen
// won't notice that — so we need both checks or we'll hit
// "port is already allocated" on ContainerStart.
func FindAvailablePort(rangeStart, rangeEnd int) (int, error) {
	return FindAvailablePortExcluding(rangeStart, rangeEnd, nil)
}

// FindAvailablePortExcluding is like FindAvailablePort but additionally skips
// any ports present in the exclude set. Useful when you already know some
// ports are in-flight (just handed out but not yet bound).
func FindAvailablePortExcluding(rangeStart, rangeEnd int, exclude map[int]bool) (int, error) {
	dockerUsed := dockerUsedHostPorts(context.Background())
	for port := rangeStart; port <= rangeEnd; port++ {
		if exclude[port] || dockerUsed[port] {
			continue
		}
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			continue
		}
		ln.Close()
		return port, nil
	}
	return 0, fmt.Errorf("사용 가능한 포트가 없습니다 (%d-%d)", rangeStart, rangeEnd)
}

// IsPortAllocationError reports whether err is a Docker port-allocation
// conflict — typically the wording "port is already allocated" or
// "Bind for 0.0.0.0:PORT failed". Used to trigger automatic port reassignment.
func IsPortAllocationError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "port is already allocated") ||
		strings.Contains(s, "address already in use") ||
		(strings.Contains(s, "Bind for ") && strings.Contains(s, "failed"))
}

// dockerUsedHostPorts returns a set of host ports currently bound by any
// Docker container (running or exited). Returns nil if Docker isn't
// reachable — callers then fall back to net.Listen only.
func dockerUsedHostPorts(ctx context.Context) map[int]bool {
	cli := DockerClient()
	if cli == nil {
		return nil
	}
	list, err := cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil
	}
	used := make(map[int]bool, 64)
	for _, c := range list {
		for _, p := range c.Ports {
			if p.PublicPort > 0 {
				used[int(p.PublicPort)] = true
			}
		}
	}
	// Also consult inspect on every container in case ContainerList omitted
	// port bindings (happens for some stopped-container states). This is
	// more expensive but we only do it when the ContainerList port list is
	// empty for a given container.
	for _, c := range list {
		if len(c.Ports) > 0 {
			continue
		}
		ins, err := cli.ContainerInspect(ctx, c.ID)
		if err != nil || ins.HostConfig == nil {
			continue
		}
		for _, bindings := range ins.HostConfig.PortBindings {
			for _, b := range bindings {
				if b.HostPort == "" {
					continue
				}
				if n, err := strconv.Atoi(b.HostPort); err == nil && n > 0 {
					used[n] = true
				}
			}
		}
	}
	return used
}

// DetectContainerPort tries to parse EXPOSE from a Dockerfile.
// Returns 0 if not found.
func DetectContainerPort(dir string, dockerfilePath string) int {
	data, err := os.ReadFile(filepath.Join(dir, dockerfilePath))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(line), "EXPOSE") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				portStr := strings.Split(parts[1], "/")[0] // strip /tcp, /udp
				if p, err := strconv.Atoi(portStr); err == nil {
					return p
				}
			}
		}
	}
	return 0
}

// detectPythonCmd determines the CMD for a Python project.
func detectPythonCmd(dir string, port int) string {
	// Check for common entry points. main.py/app.py read the port themselves
	// (we set ENV PORT in the Dockerfile); django/uvicorn get it as an arg.
	if _, err := os.Stat(filepath.Join(dir, "main.py")); err == nil {
		return `["python", "main.py"]`
	}
	if _, err := os.Stat(filepath.Join(dir, "app.py")); err == nil {
		return `["python", "app.py"]`
	}
	if _, err := os.Stat(filepath.Join(dir, "manage.py")); err == nil {
		return fmt.Sprintf(`["python", "manage.py", "runserver", "0.0.0.0:%d"]`, port)
	}
	return fmt.Sprintf(`["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "%d"]`, port)
}

// createBuildContext creates a tar archive from a directory for Docker image builds.
func createBuildContext(dir string) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		// Skip .git directory
		if info.IsDir() && info.Name() == ".git" {
			return filepath.SkipDir
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(relPath)

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})

	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}

	return &buf, nil
}
