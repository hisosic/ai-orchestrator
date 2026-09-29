package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

// ComposeTemplate is a saved docker-compose.yml snippet.
type ComposeTemplate struct {
	Name        string    `json:"name"`
	ProjectName string    `json:"project_name,omitempty"`
	Content     string    `json:"content"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	DeployCount int       `json:"deploy_count"`
}

var (
	ctMu       sync.RWMutex
	ctPath     string
	ctData     = map[string]*ComposeTemplate{}
	ctLoaded   bool
)

func composeTemplatesPath() string {
	if ctPath != "" {
		return ctPath
	}
	dir := os.Getenv("ORCHESTRATOR_STATE_DIR")
	if dir == "" {
		dir = "/data"
	}
	ctPath = filepath.Join(dir, "compose-templates.json")
	return ctPath
}

// loadComposeTemplates reads templates from disk. Safe to call multiple times.
func loadComposeTemplates() {
	ctMu.Lock()
	defer ctMu.Unlock()
	if ctLoaded {
		return
	}
	ctLoaded = true
	data, err := os.ReadFile(composeTemplatesPath())
	if err != nil {
		return
	}
	var list []*ComposeTemplate
	if err := json.Unmarshal(data, &list); err != nil {
		return
	}
	for _, t := range list {
		if t != nil && t.Name != "" {
			ctData[t.Name] = t
		}
	}
}

func saveComposeTemplates() error {
	list := make([]*ComposeTemplate, 0, len(ctData))
	for _, t := range ctData {
		list = append(list, t)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(composeTemplatesPath(), data, 0o600)
}

// UpsertComposeTemplate saves or updates a template by name.
// If incrementDeploy is true, bumps the DeployCount (used on successful deploys).
func UpsertComposeTemplate(name, projectName, content string, incrementDeploy bool) error {
	loadComposeTemplates()
	name = sanitizeTemplateName(name)
	if name == "" {
		return fmt.Errorf("template name required")
	}
	if len(content) == 0 {
		return fmt.Errorf("template content empty")
	}
	ctMu.Lock()
	defer ctMu.Unlock()
	now := time.Now().UTC()
	existing := ctData[name]
	if existing == nil {
		ctData[name] = &ComposeTemplate{
			Name:        name,
			ProjectName: projectName,
			Content:     content,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if incrementDeploy {
			ctData[name].DeployCount = 1
		}
	} else {
		existing.Content = content
		if projectName != "" {
			existing.ProjectName = projectName
		}
		existing.UpdatedAt = now
		if incrementDeploy {
			existing.DeployCount++
		}
	}
	return saveComposeTemplates()
}

func sanitizeTemplateName(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' {
			b.WriteRune(c)
		} else if c == ' ' {
			b.WriteRune('-')
		}
	}
	out := b.String()
	if len(out) > 64 {
		out = out[:64]
	}
	return out
}

// HTTP handlers

func handleComposeTemplateList(w http.ResponseWriter, r *http.Request) {
	loadComposeTemplates()
	ctMu.RLock()
	list := make([]*ComposeTemplate, 0, len(ctData))
	for _, t := range ctData {
		list = append(list, t)
	}
	ctMu.RUnlock()
	sort.Slice(list, func(i, j int) bool { return list[i].UpdatedAt.After(list[j].UpdatedAt) })
	// Don't dump full content in listing — return summary only.
	type listItem struct {
		Name        string    `json:"name"`
		ProjectName string    `json:"project_name"`
		Size        int       `json:"size"`
		CreatedAt   time.Time `json:"created_at"`
		UpdatedAt   time.Time `json:"updated_at"`
		DeployCount int       `json:"deploy_count"`
	}
	out := make([]listItem, 0, len(list))
	for _, t := range list {
		out = append(out, listItem{
			Name: t.Name, ProjectName: t.ProjectName, Size: len(t.Content),
			CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt, DeployCount: t.DeployCount,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"templates": out, "total": len(out)})
}

func handleComposeTemplateGet(w http.ResponseWriter, r *http.Request) {
	loadComposeTemplates()
	name := sanitizeTemplateName(chi.URLParam(r, "name"))
	ctMu.RLock()
	t := ctData[name]
	ctMu.RUnlock()
	if t == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "not found"})
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func handleComposeTemplateSave(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name        string `json:"name"`
		ProjectName string `json:"project_name"`
		Content     string `json:"content"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "Invalid request"})
		return
	}
	if err := UpsertComposeTemplate(body.Name, body.ProjectName, body.Content, false); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "name": sanitizeTemplateName(body.Name)})
}

func handleComposeTemplateDelete(w http.ResponseWriter, r *http.Request) {
	loadComposeTemplates()
	name := sanitizeTemplateName(chi.URLParam(r, "name"))
	ctMu.Lock()
	_, ok := ctData[name]
	delete(ctData, name)
	err := saveComposeTemplates()
	ctMu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "message": "not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}
