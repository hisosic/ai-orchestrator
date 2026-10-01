package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

const testBearerToken = "test-orch-token-abcdef"

func setupTestServer(t *testing.T) http.Handler {
	t.Helper()
	tmpDir := t.TempDir()
	t.Setenv("ORCHESTRATOR_STATE_DIR", tmpDir)
	t.Setenv("ORCHESTRATOR_ROLE", "master")
	// Use a well-known shared API token so authenticated tests can bypass
	// the session auth middleware via Bearer header.
	t.Setenv("ORCHESTRATOR_API_TOKEN", testBearerToken)

	InitCluster()
	return NewRouter()
}

// authReq wraps httptest.NewRequest to attach the bearer token so the
// request passes sessionAuthMiddleware as an admin equivalent.
func authReq(method, path string, body *bytes.Buffer) *http.Request {
	var r *http.Request
	if body == nil {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, body)
	}
	r.Header.Set("Authorization", "Bearer "+testBearerToken)
	return r
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

	req := authReq(http.MethodGet, "/v1/services", nil) // anonymous reads now require login
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

	req := authReq(http.MethodPost, "/v1/command", bytes.NewBuffer(payloadBytes))
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

	req := authReq(http.MethodGet, "/v1/system", nil) // anonymous reads now require login
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
