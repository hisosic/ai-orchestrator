package clusterstate

import (
	"path/filepath"
	"testing"
)

// A partial SaveService (e.g. from a scale call) must not wipe settings it
// didn't mention.
func TestSaveServiceKeepsUnspecifiedFields(t *testing.T) {
	m := NewClusterStateManager(filepath.Join(t.TempDir(), "c.db"))
	m.SaveService("web", "img:1", 1, map[string]any{
		"environment":          []string{"A=1"},
		"volumes":              []string{"orch-data-web-data:/data"},
		"ports":                []string{"8080:80"},
		"env_vars":             []map[string]any{{"name": "A", "value": "1"}},
		"subdomain":            true,
		"owner":                "alice",
		"schedule_constraints": map[string]any{"node_affinity": []string{"node-b"}},
	})
	m.SaveService("web", "img:1", 3, nil)

	got := m.GetService("web")
	if got["desired_replicas"] != 3 {
		t.Errorf("replicas = %v", got["desired_replicas"])
	}
	for k, want := range map[string]string{
		"environment": `["A=1"]`, "volumes": `["orch-data-web-data:/data"]`, "ports": `["8080:80"]`,
		"schedule_constraints": `{"node_affinity":["node-b"]}`, "owner": "alice",
	} {
		if got[k] != want {
			t.Errorf("%s = %v, want %s", k, got[k], want)
		}
	}
	if got["subdomain"] != true || got["env_vars"] == nil {
		t.Errorf("subdomain=%v env_vars=%v", got["subdomain"], got["env_vars"])
	}

	// Round-tripping a GetService map (as the image-update path does) keeps
	// list columns and constraints intact.
	m.SaveService("web", "img:2", 1, m.GetService("web"))
	if got := m.GetService("web"); got["volumes"] != `["orch-data-web-data:/data"]` || got["schedule_constraints"] != `{"node_affinity":["node-b"]}` || got["environment"] != `["A=1"]` {
		t.Errorf("round trip: volumes=%v constraints=%v env=%v", got["volumes"], got["schedule_constraints"], got["environment"])
	}

	// Explicit empty values still clear.
	m.SaveService("web", "img:1", 1, map[string]any{"volumes": []string{}, "subdomain": false})
	if got := m.GetService("web"); got["volumes"] != "[]" || got["subdomain"] != false {
		t.Errorf("explicit clear: volumes=%v subdomain=%v", got["volumes"], got["subdomain"])
	}
	// A brand-new service saved without options gets usable defaults.
	m.SaveService("fresh", "img:1", 1, nil)
	if got := m.GetService("fresh"); got == nil || got["environment"] != "[]" || got["volumes"] != "[]" {
		t.Errorf("fresh defaults: %v", got)
	}
}
