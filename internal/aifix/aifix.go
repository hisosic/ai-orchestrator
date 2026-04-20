// Package aifix provides AI-powered build error analysis and auto-fix using the Claude API.
package aifix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	claudeAPIURL = "https://api.anthropic.com/v1/messages"
	claudeModel  = "claude-sonnet-4-20250514"
	// MaxRetries is the maximum number of AI fix + rebuild attempts.
	MaxRetries = 2
)

// FixResult contains the AI-suggested fixes.
type FixResult struct {
	Analysis    string     `json:"analysis"`
	Fixes       []FileFix  `json:"fixes"`
	Suggestion  string     `json:"suggestion"`
	FixApplied  bool       `json:"fix_applied"`
}

// FileFix represents a fix to a specific file.
type FileFix struct {
	FilePath string `json:"file_path"`
	Content  string `json:"content"`
	Action   string `json:"action"` // "replace" or "create"
}

// claudeRequest is the Claude API request structure.
type claudeRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	System    string          `json:"system"`
	Messages  []claudeMessage `json:"messages"`
}

// claudeMessage represents a message in the Claude API.
type claudeMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// claudeResponse is the Claude API response structure.
type claudeResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason string `json:"stop_reason"`
}

// GetAPIKey returns the Claude API key from environment.
func GetAPIKey() string {
	return os.Getenv("ANTHROPIC_API_KEY")
}

// AnalyzeAndFix analyzes a build error and suggests fixes.
// It reads the Dockerfile and relevant source files, sends them to Claude API,
// and returns suggested fixes.
func AnalyzeAndFix(ctx context.Context, sourceDir string, dockerfilePath string, buildLog string) (*FixResult, error) {
	apiKey := GetAPIKey()
	if apiKey == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY 환경 변수가 설정되지 않았습니다")
	}

	// Read Dockerfile
	dockerfileContent, err := os.ReadFile(filepath.Join(sourceDir, dockerfilePath))
	if err != nil {
		return nil, fmt.Errorf("Dockerfile 읽기 실패: %w", err)
	}

	// Collect relevant source files for context
	sourceContext := collectSourceContext(sourceDir, 5)

	prompt := buildPrompt(string(dockerfileContent), dockerfilePath, buildLog, sourceContext)

	result, err := callClaudeAPI(ctx, apiKey, prompt)
	if err != nil {
		return nil, fmt.Errorf("Claude API 호출 실패: %w", err)
	}

	return result, nil
}

// ApplyFixes applies the suggested fixes to the source directory.
func ApplyFixes(sourceDir string, fixes []FileFix) error {
	for _, fix := range fixes {
		targetPath := filepath.Join(sourceDir, fix.FilePath)

		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("디렉토리 생성 실패 (%s): %w", fix.FilePath, err)
		}

		if err := os.WriteFile(targetPath, []byte(fix.Content), 0644); err != nil {
			return fmt.Errorf("파일 쓰기 실패 (%s): %w", fix.FilePath, err)
		}
		log.Printf("[aifix] Applied fix to %s (%s)", fix.FilePath, fix.Action)
	}
	return nil
}

// collectSourceContext reads key source files for context (package.json, requirements.txt, go.mod, etc.)
func collectSourceContext(dir string, maxFiles int) string {
	// Priority files to include for context
	priorityFiles := []string{
		"package.json",
		"requirements.txt",
		"go.mod",
		"go.sum",
		"pom.xml",
		"build.gradle",
		"Makefile",
		"docker-compose.yml",
		".dockerignore",
		"main.go",
		"main.py",
		"app.py",
		"app.js",
		"index.js",
		"server.js",
		"index.html",
	}

	var sb strings.Builder
	count := 0

	for _, name := range priorityFiles {
		if count >= maxFiles {
			break
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// Limit each file to 2000 bytes
		content := string(data)
		if len(content) > 2000 {
			content = content[:2000] + "\n... (truncated)"
		}
		sb.WriteString(fmt.Sprintf("\n--- %s ---\n%s\n", name, content))
		count++
	}

	return sb.String()
}

func buildPrompt(dockerfile, dockerfilePath, buildLog, sourceContext string) string {
	return fmt.Sprintf(`Docker 이미지 빌드가 실패했습니다. 에러를 분석하고 수정해주세요.

## Dockerfile (%s)
%s

## 빌드 에러 로그
%s

## 프로젝트 소스 파일
%s

## 응답 형식
반드시 아래 JSON 형식으로만 응답하세요. 다른 텍스트는 포함하지 마세요.

{
  "analysis": "에러 원인 분석 (한국어)",
  "suggestion": "수정 내용 요약 (한국어)",
  "fixes": [
    {
      "file_path": "수정할 파일의 상대 경로 (예: Dockerfile)",
      "content": "수정된 전체 파일 내용",
      "action": "replace 또는 create"
    }
  ]
}

주의사항:
- Dockerfile 경로는 "%s"입니다
- fixes 배열에는 수정이 필요한 파일만 포함하세요
- content에는 수정된 파일의 전체 내용을 포함하세요
- 빌드가 성공할 수 있도록 실질적인 수정을 제안하세요
- .dockerignore가 필요하면 추가하세요`, dockerfilePath, dockerfile, buildLog, sourceContext, dockerfilePath)
}

func callClaudeAPI(ctx context.Context, apiKey string, prompt string) (*FixResult, error) {
	reqBody := claudeRequest{
		Model:     claudeModel,
		MaxTokens: 4096,
		System:    "당신은 Docker 빌드 에러를 분석하고 수정하는 전문가입니다. 반드시 유효한 JSON만 응답하세요.",
		Messages: []claudeMessage{
			{Role: "user", Content: prompt},
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("요청 직렬화 실패: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", claudeAPIURL, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 실패: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API 요청 실패: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("응답 읽기 실패: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API 응답 에러 (status %d): %s", resp.StatusCode, string(body))
	}

	var claudeResp claudeResponse
	if err := json.Unmarshal(body, &claudeResp); err != nil {
		return nil, fmt.Errorf("응답 파싱 실패: %w", err)
	}

	if len(claudeResp.Content) == 0 {
		return nil, fmt.Errorf("API 응답이 비어 있습니다")
	}

	// Extract text content
	text := claudeResp.Content[0].Text

	// Parse JSON from response (handle potential markdown code blocks)
	text = extractJSON(text)

	var result FixResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("수정 결과 파싱 실패: %w\n응답: %s", err, text)
	}

	return &result, nil
}

// RuntimeFixResult contains the AI analysis of container runtime errors.
type RuntimeFixResult struct {
	Analysis   string     `json:"analysis"`
	RootCause  string     `json:"root_cause"`  // "dockerfile", "source", "env", "dependency", "config"
	Suggestion string     `json:"suggestion"`
	Fixes      []FileFix  `json:"fixes"`
	EnvFixes   []string   `json:"env_fixes"`   // environment variables to add/change
	NeedsRebuild bool    `json:"needs_rebuild"`
}

// AnalyzeRuntimeError analyzes container startup failure using logs, Dockerfile, and source context.
func AnalyzeRuntimeError(ctx context.Context, sourceDir string, dockerfilePath string, containerLogs string, containerState string) (*RuntimeFixResult, error) {
	apiKey := GetAPIKey()
	if apiKey == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY 환경 변수가 설정되지 않았습니다")
	}

	dockerfileContent := ""
	if sourceDir != "" && dockerfilePath != "" {
		if data, err := os.ReadFile(filepath.Join(sourceDir, dockerfilePath)); err == nil {
			dockerfileContent = string(data)
		}
	}

	sourceContext := ""
	if sourceDir != "" {
		sourceContext = collectSourceContext(sourceDir, 5)
	}

	prompt := buildRuntimePrompt(dockerfileContent, dockerfilePath, containerLogs, containerState, sourceContext)

	reqBody := claudeRequest{
		Model:     claudeModel,
		MaxTokens: 4096,
		System:    "당신은 Docker 컨테이너 런타임 에러를 진단하고 수정하는 전문가입니다. 반드시 유효한 JSON만 응답하세요.",
		Messages: []claudeMessage{
			{Role: "user", Content: prompt},
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("요청 직렬화 실패: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", claudeAPIURL, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("HTTP 요청 생성 실패: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API 요청 실패: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("응답 읽기 실패: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API 응답 에러 (status %d): %s", resp.StatusCode, string(body))
	}

	var claudeResp claudeResponse
	if err := json.Unmarshal(body, &claudeResp); err != nil {
		return nil, fmt.Errorf("응답 파싱 실패: %w", err)
	}
	if len(claudeResp.Content) == 0 {
		return nil, fmt.Errorf("API 응답이 비어 있습니다")
	}

	text := extractJSON(claudeResp.Content[0].Text)
	var result RuntimeFixResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("런타임 분석 결과 파싱 실패: %w\n응답: %s", err, text)
	}
	return &result, nil
}

func buildRuntimePrompt(dockerfile, dockerfilePath, containerLogs, containerState, sourceContext string) string {
	return fmt.Sprintf(`Docker 컨테이너가 기동 후 비정상 종료되었습니다. 로그를 분석하고 수정해주세요.

## 컨테이너 상태
%s

## 컨테이너 로그
%s

## Dockerfile (%s)
%s

## 프로젝트 소스 파일
%s

## 응답 형식
반드시 아래 JSON 형식으로만 응답하세요.

{
  "analysis": "에러 원인 상세 분석 (한국어)",
  "root_cause": "dockerfile | source | env | dependency | config 중 하나",
  "suggestion": "수정 내용 요약 (한국어)",
  "needs_rebuild": true 또는 false,
  "fixes": [
    {
      "file_path": "수정할 파일 상대 경로",
      "content": "수정된 전체 파일 내용",
      "action": "replace 또는 create"
    }
  ],
  "env_fixes": ["KEY=VALUE 형식의 환경변수 (필요시)"]
}

주의사항:
- root_cause가 "dockerfile" 또는 "source"이면 needs_rebuild를 true로 설정
- root_cause가 "env" 또는 "config"이면 needs_rebuild를 false로 설정하고 env_fixes에 환경변수 추가
- fixes에 파일 수정이 필요 없으면 빈 배열로
- 실질적으로 문제를 해결하는 수정안을 제시하세요`, containerState, containerLogs, dockerfilePath, dockerfile, sourceContext)
}

// extractJSON extracts JSON content from a string that may contain markdown code blocks.
func extractJSON(text string) string {
	text = strings.TrimSpace(text)

	// Remove ```json ... ``` wrapper if present
	if strings.HasPrefix(text, "```") {
		lines := strings.Split(text, "\n")
		start := 0
		end := len(lines)
		for i, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				if i == 0 {
					start = 1
				} else {
					end = i
					break
				}
			}
		}
		text = strings.Join(lines[start:end], "\n")
	}

	// If it starts with {, assume it's already JSON
	text = strings.TrimSpace(text)
	return text
}
