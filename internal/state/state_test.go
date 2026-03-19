package state

import (
	"testing"
)

func TestUpsertAndGetService(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("ORCHESTRATOR_STATE_DIR", tmpDir)

	UpsertService("web", "nginx:latest", 3, WithMemoryLimit("256m"))

	svc := GetService("web")
	if svc == nil {
		t.Fatal("expected service to exist after upsert")
	}
	if svc["name"] != "web" {
		t.Fatalf("expected name=web, got %v", svc["name"])
	}
	if svc["image"] != "nginx:latest" {
		t.Fatalf("expected image=nginx:latest, got %v", svc["image"])
	}
	// replicas is stored as int but round-trips through JSON as float64
	switch r := svc["replicas"].(type) {
	case int:
		if r != 3 {
			t.Fatalf("expected replicas=3, got %d", r)
		}
	case float64:
		if int(r) != 3 {
			t.Fatalf("expected replicas=3, got %v", r)
		}
	default:
		t.Fatalf("unexpected replicas type %T, value %v", svc["replicas"], svc["replicas"])
	}
	if svc["memory_limit"] != "256m" {
		t.Fatalf("expected memory_limit=256m, got %v", svc["memory_limit"])
	}
}

func TestListServices(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("ORCHESTRATOR_STATE_DIR", tmpDir)

	UpsertService("svc-a", "nginx:latest", 1)
	UpsertService("svc-b", "redis:latest", 2)

	services := ListServices()
	if len(services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(services))
	}
}

func TestDeleteService(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("ORCHESTRATOR_STATE_DIR", tmpDir)

	UpsertService("to-delete", "alpine:latest", 1)
	svc := GetService("to-delete")
	if svc == nil {
		t.Fatal("expected service to exist before delete")
	}

	DeleteService("to-delete")

	svc = GetService("to-delete")
	if svc != nil {
		t.Fatal("expected service to be gone after delete")
	}
}

func TestUpsertAndListNodes(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("ORCHESTRATOR_STATE_DIR", tmpDir)

	UpsertNode("node-a", "http://10.0.0.1:7333", "token-a")

	nodes := ListNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].Name != "node-a" {
		t.Fatalf("expected node name=node-a, got %s", nodes[0].Name)
	}
	if nodes[0].BaseURL != "http://10.0.0.1:7333" {
		t.Fatalf("expected base_url=http://10.0.0.1:7333, got %s", nodes[0].BaseURL)
	}
}

func TestDeleteNode(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("ORCHESTRATOR_STATE_DIR", tmpDir)

	UpsertNode("node-b", "http://10.0.0.2:7333", "token-b")
	nodes := ListNodes()
	if len(nodes) != 1 {
		t.Fatal("expected 1 node before delete")
	}

	DeleteNode("node-b")

	nodes = ListNodes()
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes after delete, got %d", len(nodes))
	}
}
