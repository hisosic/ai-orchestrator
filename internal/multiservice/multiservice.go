// Package multiservice detects and manages multi-container deployments from uploaded source archives.
// It supports docker-compose.yml parsing, multiple Dockerfile detection, and subdirectory-based services.
package multiservice

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// ServiceDef represents a single service to be deployed.
type ServiceDef struct {
	Name          string            `json:"name"`
	BuildContext  string            `json:"build_context"`   // relative path to build context
	Dockerfile    string            `json:"dockerfile"`      // Dockerfile path relative to build context
	Image         string            `json:"image"`           // pre-built image (pull instead of build)
	Ports         []string          `json:"ports"`           // "host:container" or "container"
	ContainerPort int               `json:"container_port"`  // primary exposed port
	Environment   []string          `json:"environment"`     // KEY=VALUE
	Volumes       []string          `json:"volumes"`         // volume mounts
	DependsOn     []string          `json:"depends_on"`      // service names this depends on
	Command       []string          `json:"command"`         // override CMD
	Links         []string          `json:"links"`           // legacy docker links / dns aliases
}

// DetectResult holds the analysis of a source directory.
type DetectResult struct {
	IsMultiService bool         `json:"is_multi_service"`
	Source         string       `json:"source"` // "docker-compose", "multi-dockerfile", "subdirectory"
	Services       []ServiceDef `json:"services"`
}

// Detect analyzes a source directory and returns multi-service definitions if found.
// Detection priority:
//  1. docker-compose.yml / docker-compose.yaml
//  2. Multiple Dockerfile.* files in root (e.g., Dockerfile.frontend, Dockerfile.backend)
//  3. Subdirectories each containing their own Dockerfile
func Detect(dir string) (*DetectResult, error) {
	// 1. docker-compose.yml
	for _, name := range []string{"docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"} {
		composePath := filepath.Join(dir, name)
		if _, err := os.Stat(composePath); err == nil {
			services, err := parseComposeFile(composePath, dir)
			if err != nil {
				return nil, fmt.Errorf("docker-compose 파싱 실패: %w", err)
			}
			if len(services) > 1 {
				return &DetectResult{
					IsMultiService: true,
					Source:         "docker-compose",
					Services:       services,
				}, nil
			}
			if len(services) == 1 {
				return &DetectResult{
					IsMultiService: false,
					Source:         "docker-compose",
					Services:       services,
				}, nil
			}
		}
	}

	// 2. Multiple Dockerfile.* in root
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var multiDockerfiles []ServiceDef
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		lower := strings.ToLower(name)
		// Match Dockerfile.xxx but not plain Dockerfile
		if strings.HasPrefix(lower, "dockerfile.") && len(name) > len("dockerfile.") {
			svcName := strings.ToLower(name[len("Dockerfile."):])
			svcName = sanitizeName(svcName)
			if svcName == "" {
				continue
			}
			port := guessPortByName(svcName)
			multiDockerfiles = append(multiDockerfiles, ServiceDef{
				Name:          svcName,
				BuildContext:  ".",
				Dockerfile:    name,
				ContainerPort: port,
			})
		}
	}
	if len(multiDockerfiles) > 1 {
		return &DetectResult{
			IsMultiService: true,
			Source:         "multi-dockerfile",
			Services:       multiDockerfiles,
		}, nil
	}

	// 3. Subdirectories with Dockerfiles
	var subServices []ServiceDef
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		subDir := filepath.Join(dir, e.Name())
		dockerfilePath := ""
		for _, df := range []string{"Dockerfile", "dockerfile"} {
			if _, err := os.Stat(filepath.Join(subDir, df)); err == nil {
				dockerfilePath = df
				break
			}
		}
		if dockerfilePath == "" {
			continue
		}
		svcName := sanitizeName(e.Name())
		if svcName == "" {
			continue
		}
		port := detectPortFromDockerfile(filepath.Join(subDir, dockerfilePath))
		if port == 0 {
			port = guessPortByName(svcName)
		}
		subServices = append(subServices, ServiceDef{
			Name:          svcName,
			BuildContext:  e.Name(),
			Dockerfile:    dockerfilePath,
			ContainerPort: port,
		})
	}
	if len(subServices) > 1 {
		return &DetectResult{
			IsMultiService: true,
			Source:         "subdirectory",
			Services:       subServices,
		}, nil
	}

	return &DetectResult{IsMultiService: false}, nil
}

// ---------------------------------------------------------------------------
// docker-compose.yml parser
// ---------------------------------------------------------------------------

// composeFile is a minimal docker-compose file structure.
type composeFile struct {
	Version  string                    `yaml:"version"`
	Services map[string]composeService `yaml:"services"`
}

type composeService struct {
	Image       string            `yaml:"image"`
	Build       interface{}       `yaml:"build"` // string or map
	Ports       []string          `yaml:"ports"`
	Environment interface{}       `yaml:"environment"` // list or map
	Volumes     []string          `yaml:"volumes"`
	DependsOn   interface{}       `yaml:"depends_on"` // list or map
	Command     interface{}       `yaml:"command"`     // string or list
	Links       []string          `yaml:"links"`
	Restart     string            `yaml:"restart"`
	EnvFile     interface{}       `yaml:"env_file"` // string or list
	Labels      map[string]string `yaml:"labels"`
}

func parseComposeFile(path string, baseDir string) ([]ServiceDef, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cf composeFile
	if err := yaml.Unmarshal(data, &cf); err != nil {
		return nil, fmt.Errorf("YAML 파싱 오류: %w", err)
	}

	if len(cf.Services) == 0 {
		return nil, fmt.Errorf("services 섹션이 비어 있습니다")
	}

	var services []ServiceDef
	for name, svc := range cf.Services {
		sd := ServiceDef{
			Name:    sanitizeName(name),
			Ports:   svc.Ports,
			Volumes: svc.Volumes,
			Links:   svc.Links,
		}

		// Parse build context
		switch b := svc.Build.(type) {
		case string:
			sd.BuildContext = b
			sd.Dockerfile = "Dockerfile"
		case map[string]interface{}:
			if ctx, ok := b["context"].(string); ok {
				sd.BuildContext = ctx
			} else {
				sd.BuildContext = "."
			}
			if df, ok := b["dockerfile"].(string); ok {
				sd.Dockerfile = df
			} else {
				sd.Dockerfile = "Dockerfile"
			}
		default:
			// No build section - use image
			sd.Image = svc.Image
		}

		// If no build and no image, skip
		if sd.BuildContext == "" && sd.Image == "" {
			sd.Image = svc.Image
		}

		// Parse container port from ports
		sd.ContainerPort = extractContainerPort(svc.Ports)
		if sd.ContainerPort == 0 && sd.BuildContext != "" {
			// Try detecting from Dockerfile
			dfPath := filepath.Join(baseDir, sd.BuildContext, sd.Dockerfile)
			sd.ContainerPort = detectPortFromDockerfile(dfPath)
		}
		if sd.ContainerPort == 0 {
			sd.ContainerPort = guessPortByName(sd.Name)
		}

		// Parse environment
		sd.Environment = parseEnvironment(svc.Environment)

		// Load env_file if present
		envFileVars := loadEnvFiles(svc.EnvFile, baseDir)
		if len(envFileVars) > 0 {
			sd.Environment = append(envFileVars, sd.Environment...)
		}

		// Parse depends_on
		sd.DependsOn = parseDependsOn(svc.DependsOn)

		// Parse command
		sd.Command = parseCommand(svc.Command)

		services = append(services, sd)
	}

	return services, nil
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func extractContainerPort(ports []string) int {
	if len(ports) == 0 {
		return 0
	}
	// Parse first port mapping: "8080:80" → container port 80, "3000" → 3000
	p := ports[0]
	// Remove protocol suffix
	p = strings.Split(p, "/")[0]
	parts := strings.Split(p, ":")
	switch len(parts) {
	case 1:
		if v, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
			return v
		}
	case 2:
		if v, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			return v
		}
	case 3: // "ip:host:container"
		if v, err := strconv.Atoi(strings.TrimSpace(parts[2])); err == nil {
			return v
		}
	}
	return 0
}

func parseEnvironment(env interface{}) []string {
	switch e := env.(type) {
	case []interface{}:
		var result []string
		for _, v := range e {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case map[string]interface{}:
		var result []string
		for k, v := range e {
			result = append(result, fmt.Sprintf("%s=%v", k, v))
		}
		return result
	}
	return nil
}

func parseDependsOn(dep interface{}) []string {
	switch d := dep.(type) {
	case []interface{}:
		var result []string
		for _, v := range d {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case map[string]interface{}:
		var result []string
		for k := range d {
			result = append(result, k)
		}
		return result
	}
	return nil
}

func parseCommand(cmd interface{}) []string {
	switch c := cmd.(type) {
	case string:
		return strings.Fields(c)
	case []interface{}:
		var result []string
		for _, v := range c {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}

func loadEnvFiles(envFile interface{}, baseDir string) []string {
	var files []string
	switch ef := envFile.(type) {
	case string:
		files = []string{ef}
	case []interface{}:
		for _, v := range ef {
			if s, ok := v.(string); ok {
				files = append(files, s)
			}
		}
	}

	var vars []string
	for _, f := range files {
		path := filepath.Join(baseDir, f)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if strings.Contains(line, "=") {
				vars = append(vars, line)
			}
		}
	}
	return vars
}

func detectPortFromDockerfile(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(line), "EXPOSE") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				portStr := strings.Split(parts[1], "/")[0]
				if p, err := strconv.Atoi(portStr); err == nil {
					return p
				}
			}
		}
	}
	return 0
}

func guessPortByName(name string) int {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "postgres") || strings.Contains(lower, "pgsql"):
		return 5432
	case strings.Contains(lower, "mysql") || strings.Contains(lower, "mariadb"):
		return 3306
	case strings.Contains(lower, "mongo"):
		return 27017
	case strings.Contains(lower, "redis"):
		return 6379
	case strings.Contains(lower, "rabbitmq"):
		return 5672
	case strings.Contains(lower, "kafka"):
		return 9092
	case strings.Contains(lower, "elasticsearch") || strings.Contains(lower, "elastic"):
		return 9200
	case strings.Contains(lower, "nginx") || strings.Contains(lower, "web") || strings.Contains(lower, "frontend"):
		return 80
	case strings.Contains(lower, "api") || strings.Contains(lower, "backend"):
		return 8080
	default:
		return 8080
	}
}

var nameReplacer = strings.NewReplacer(" ", "-", "_", "-", ".", "-")

func sanitizeName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = nameReplacer.Replace(name)
	// Remove non-alphanumeric except hyphens
	var clean []byte
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			clean = append(clean, c)
		}
	}
	name = strings.Trim(string(clean), "-")
	if len(name) > 40 {
		name = name[:40]
	}
	return name
}

// SortByDependency returns service indices in dependency order (dependencies first).
func SortByDependency(services []ServiceDef) []int {
	nameIdx := make(map[string]int)
	for i, s := range services {
		nameIdx[s.Name] = i
	}

	visited := make(map[int]bool)
	var order []int

	var visit func(i int)
	visit = func(i int) {
		if visited[i] {
			return
		}
		visited[i] = true
		for _, dep := range services[i].DependsOn {
			if j, ok := nameIdx[dep]; ok {
				visit(j)
			}
		}
		order = append(order, i)
	}

	for i := range services {
		visit(i)
	}
	return order
}
