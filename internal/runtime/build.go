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
	"github.com/docker/docker/client"
)

// BuildImage builds a Docker image from a directory context.
// Returns the built image ID.
func BuildImage(ctx context.Context, cli *client.Client, contextDir string, imageName string, dockerfile string) (string, error) {
	buildCtx, err := createBuildContext(contextDir)
	if err != nil {
		return "", fmt.Errorf("failed to create build context: %w", err)
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
		return "", fmt.Errorf("docker build failed: %w", err)
	}
	defer resp.Body.Close()

	// Parse build output stream for errors
	var imageID string
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
		if msg.Error != "" {
			return "", fmt.Errorf("build error: %s", msg.Error)
		}
		if msg.Aux.ID != "" {
			imageID = msg.Aux.ID
		}
		if msg.Stream != "" {
			log.Printf("[build] %s", strings.TrimRight(msg.Stream, "\n"))
		}
	}

	return imageID, nil
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

// GenerateDockerfile auto-generates a Dockerfile based on detected language.
// Returns the Dockerfile content, the default container port, and any error.
func GenerateDockerfile(dir string) (string, int, error) {
	// Node.js
	if _, err := os.Stat(filepath.Join(dir, "package.json")); err == nil {
		port := 3000
		content := `FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
`
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Python
	if _, err := os.Stat(filepath.Join(dir, "requirements.txt")); err == nil {
		port := 8000
		cmd := detectPythonCmd(dir)
		content := fmt.Sprintf(`FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE %d
CMD %s
`, port, cmd)
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Go
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
		port := 8080
		content := `FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o server .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 8080
CMD ["./server"]
`
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Java Maven
	if _, err := os.Stat(filepath.Join(dir, "pom.xml")); err == nil {
		port := 8080
		content := `FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app
COPY . .
RUN mvn package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
EXPOSE 8080
CMD ["java", "-jar", "app.jar"]
`
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Java Gradle
	if _, err := os.Stat(filepath.Join(dir, "build.gradle")); err == nil {
		port := 8080
		content := `FROM gradle:8-jdk17 AS builder
WORKDIR /app
COPY . .
RUN gradle build -x test

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar
EXPOSE 8080
CMD ["java", "-jar", "app.jar"]
`
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	// Static HTML (nginx)
	if _, err := os.Stat(filepath.Join(dir, "index.html")); err == nil {
		port := 80
		content := `FROM nginx:alpine
COPY . /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
`
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(content), 0644); err != nil {
			return "", 0, err
		}
		return content, port, nil
	}

	return "", 0, fmt.Errorf("소스 코드의 언어를 감지할 수 없습니다. Dockerfile을 포함해 주세요")
}

// FindAvailablePort finds an available TCP port in the given range.
func FindAvailablePort(rangeStart, rangeEnd int) (int, error) {
	for port := rangeStart; port <= rangeEnd; port++ {
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err == nil {
			ln.Close()
			return port, nil
		}
	}
	return 0, fmt.Errorf("사용 가능한 포트가 없습니다 (%d-%d)", rangeStart, rangeEnd)
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
func detectPythonCmd(dir string) string {
	// Check for common entry points
	if _, err := os.Stat(filepath.Join(dir, "main.py")); err == nil {
		return `["python", "main.py"]`
	}
	if _, err := os.Stat(filepath.Join(dir, "app.py")); err == nil {
		return `["python", "app.py"]`
	}
	if _, err := os.Stat(filepath.Join(dir, "manage.py")); err == nil {
		return `["python", "manage.py", "runserver", "0.0.0.0:8000"]`
	}
	return `["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]`
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
