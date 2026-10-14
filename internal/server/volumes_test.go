package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"ai-container-go/internal/auth"
	"ai-container-go/internal/migrate"
)

func TestPersistentVolumeNaming(t *testing.T) {
	a := persistentVolumeName("web", "alice@x.com", "/var/lib/mysql")
	b := persistentVolumeName("web", "bob@x.com", "/var/lib/mysql")
	// Name reads as orch-data-<account>-<service>-<path-slug>-<owner-hash>.
	if !strings.HasPrefix(a, "orch-data-alice-web-var-lib-mysql-") || a == b {
		t.Fatalf("names: %s / %s (must be per-owner)", a, b)
	}
	for _, bad := range [][]string{{"data"}, {"/"}, {"/a/../etc"}, {"/a:b"}, {"/1", "/2", "/3", "/4", "/5", "/6"}} {
		if _, err := normalizePersistentPaths(bad); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
	if got, err := normalizePersistentPaths([]string{"/data/", " /data", "/app/uploads"}); err != nil || len(got) != 2 || got[0] != "/data" {
		t.Fatalf("normalize: %v %v", got, err)
	}
}

func TestNamedVolumeNaming(t *testing.T) {
	// A user-named volume reads as orch-data-<account>-<service>-<name>-<hash>
	// and mounts at the requested path.
	n := managedVolumeName("api", "jslee@parametacorp.com", "uploads")
	if !strings.HasPrefix(n, "orch-data-jslee-api-uploads-") {
		t.Fatalf("named volume = %s", n)
	}

	got, err := normalizeNamedVolumes([]namedVolume{{Name: "Up Loads", Path: "/app/uploads/"}})
	if err != nil || len(got) != 1 || got[0].Path != "/app/uploads" {
		t.Fatalf("normalize named: %v %v", got, err)
	}
	for _, bad := range []namedVolume{
		{Name: "", Path: "/data"},    // no name
		{Name: "x", Path: "data"},    // relative path
		{Name: "x", Path: "/"},       // root
		{Name: "###", Path: "/data"}, // name with no alnum
		{Name: "x", Path: "/a:b"},    // path with colon
	} {
		if _, err := normalizeNamedVolumes([]namedVolume{bad}); err == nil {
			t.Errorf("accepted bad named volume %+v", bad)
		}
	}
	// Duplicate mount paths are rejected.
	if _, err := normalizeNamedVolumes([]namedVolume{{Name: "a", Path: "/data"}, {Name: "b", Path: "/data"}}); err == nil {
		t.Error("accepted duplicate mount path")
	}
}

func TestUsersCannotMountHostPaths(t *testing.T) {
	h, _ := mcpEnv(t)
	bob, _ := auth.EnsureUserSession("bob@parametacorp.com", auth.RoleUser)
	for _, vols := range []string{`["/:/host"]`, `["/var/run/docker.sock:/var/run/docker.sock"]`, `["orch-data-alice-web-data-1234:/steal"]`} {
		w := postAs(h, "/v1/cluster/deploy", `{"name":"bob-x","image":"nginx","volumes":`+vols+`}`, bob.Token)
		if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "forbidden_volumes") {
			t.Errorf("volumes %s: %d %s", vols, w.Code, w.Body.String())
		}
	}
	if clusterState.GetService("bob-x") != nil {
		t.Fatal("service was created")
	}
}

func TestPersistentServiceLifecycle(t *testing.T) {
	h, tok := mcpEnv(t)
	bob, _ := auth.EnsureUserSession("bob@parametacorp.com", auth.RoleUser)

	if w := postAs(h, "/v1/cluster/deploy", `{"name":"bob-db","image":"postgres:16","replicas":2,"persistent_paths":["/var/lib/postgresql/data"]}`, bob.Token); w.Code != http.StatusBadRequest {
		t.Fatalf("2 replicas with storage: %d %s", w.Code, w.Body.String())
	}
	// Nodes are unreachable in tests, so the run fails, but the definition
	// (volumes + pin) is what we check.
	postAs(h, "/v1/cluster/deploy", `{"name":"bob-db","image":"postgres:16","persistent_paths":["/var/lib/postgresql/data"]}`, bob.Token)
	info := clusterState.GetService("bob-db")
	if info == nil {
		t.Fatal("not saved")
	}
	specs := managedVolumeSpecs(serviceVolumeSpecs(info))
	want := persistentVolumeName("bob-db", "bob@parametacorp.com", "/var/lib/postgresql/data") + ":/var/lib/postgresql/data"
	if len(specs) != 1 || specs[0] != want || pinnedNodeOf(info) == "" {
		t.Fatalf("specs=%v pinned=%q", specs, pinnedNodeOf(info))
	}
	pinned := pinnedNodeOf(info)

	// Redeploy without paths keeps the same volume and node.
	postAs(h, "/v1/cluster/deploy", `{"name":"bob-db","image":"postgres:16.4"}`, bob.Token)
	info = clusterState.GetService("bob-db")
	if s := managedVolumeSpecs(serviceVolumeSpecs(info)); len(s) != 1 || s[0] != want || pinnedNodeOf(info) != pinned {
		t.Fatalf("redeploy lost storage: %v %q", s, pinnedNodeOf(info))
	}

	// Scaling past one replica is refused; scale/update round trips keep storage.
	if w := postAs(h, "/v1/cluster/scale", `{"service_name":"bob-db","replicas":3}`, bob.Token); !strings.Contains(w.Body.String(), "복제본 1개") {
		t.Errorf("scale 3: %s", w.Body.String())
	}
	clusterState.SaveService("bob-db", "postgres:16.5", 1, clusterState.GetService("bob-db"))
	clusterState.SaveService("bob-db", "postgres:16.5", 0, nil)
	if !servicePersistent("bob-db") || pinnedNodeOf(clusterState.GetService("bob-db")) != pinned {
		t.Fatal("storage lost after update/scale saves")
	}

	// Moving it to another node is refused.
	mc := migrate.NewMigrationController(clusterState)
	mc.Pinned = servicePersistent
	if res := mc.Migrate("abc", pinned, "node-c", "orch-bob-db-0", "bob-db"); res["success"] != false {
		t.Fatalf("migrate: %v", res)
	}

	// MCP reports the storage to its owner only.
	_, out := mcpCall(t, h, tok, "tools/call", map[string]any{"name": "list_volumes"})
	if text, _ := toolText(t, out); strings.Contains(text, "bob-db") {
		t.Fatalf("alice sees bob's volumes: %s", text)
	}

	// Delete keeps data by default and says where.
	w := postAs(h, "/v1/cluster/delete", `{"service_name":"bob-db"}`, bob.Token)
	var d map[string]any
	json.Unmarshal(w.Body.Bytes(), &d)
	if kept, _ := d["kept_volumes"].([]any); len(kept) != 1 || d["kept_on_node"] != pinned {
		t.Fatalf("delete: %s", w.Body.String())
	}
}
