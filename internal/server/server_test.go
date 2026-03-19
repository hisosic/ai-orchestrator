package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func setupTestServer(t *testing.T) http.Handler {
	t.Helper()
	tmpDir := t.TempDir()
	t.Setenv("ORCHESTRATOR_STATE_DIR", tmpDir)
	t.Setenv("ORCHESTRATOR_ROLE", "master")
	// Disable bearer token auth for tests
	t.Setenv("ORCHESTRATOR_TOKEN", "")

	InitCluster()
	return NewRouter()
}

func TestHealthEndpoint(t *testing.T) {
	handler := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", body["status"])
	}
}

func TestListServicesEndpoint(t *testing.T) {
	handler := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/services", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body []any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("expected JSON array response, got error: %v", err)
	}
}

func TestCommandEndpoint(t *testing.T) {
	handler := setupTestServer(t)

	payload := map[string]any{
		"command": "scale nginx to 3",
		"dry_run": true,
	}
	payloadBytes, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/v1/command", bytes.NewReader(payloadBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	intent, ok := body["intent"].(map[string]any)
	if !ok {
		t.Fatal("expected intent in response")
	}
	if intent["action"] != "scale" {
		t.Fatalf("expected intent action=scale, got %v", intent["action"])
	}
}

func TestSystemEndpoint(t *testing.T) {
	handler := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/system", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := body["hostname"]; !ok {
		t.Fatal("expected hostname field in system response")
	}
}
