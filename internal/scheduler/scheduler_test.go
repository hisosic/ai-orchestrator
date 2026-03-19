package scheduler

import (
	"testing"

	"ai-container-go/internal/models"
)

// mockClusterState implements ClusterStateProvider with configurable data.
type mockClusterState struct {
	nodes      []models.NodeInfo
	placements []models.ContainerPlacement
}

func (m *mockClusterState) ListNodes(status ...models.NodeStatus) []models.NodeInfo {
	if len(status) > 0 && status[0] != "" {
		var filtered []models.NodeInfo
		for _, n := range m.nodes {
			if n.Status == status[0] {
				filtered = append(filtered, n)
			}
		}
		return filtered
	}
	return m.nodes
}

func (m *mockClusterState) GetPlacements(serviceName, nodeName string) []models.ContainerPlacement {
	var result []models.ContainerPlacement
	for _, p := range m.placements {
		if serviceName != "" && p.ServiceName != serviceName {
			continue
		}
		if nodeName != "" && p.NodeName != nodeName {
			continue
		}
		result = append(result, p)
	}
	return result
}

func makeNode(name string, memTotal, memUsed int) models.NodeInfo {
	return models.NodeInfo{
		Name:   name,
		Status: models.NodeHealthy,
		Role:   "worker",
		Resources: &models.NodeResources{
			CPUCores:       4,
			CPUUsedPercent: 20.0,
			MemoryTotalMB:  memTotal,
			MemoryUsedMB:   memUsed,
		},
	}
}

func totalCount(decisions []models.ScheduleDecision) int {
	total := 0
	for _, d := range decisions {
		total += d.Count
	}
	return total
}

func decisionMap(decisions []models.ScheduleDecision) map[string]int {
	m := make(map[string]int)
	for _, d := range decisions {
		m[d.NodeName] = d.Count
	}
	return m
}

func TestScheduleSpread(t *testing.T) {
	mock := &mockClusterState{
		nodes: []models.NodeInfo{
			makeNode("node-a", 4096, 1024),
			makeNode("node-b", 4096, 1024),
		},
	}

	sched := NewScheduler(mock)
	decisions, err := sched.Schedule("web", "nginx:latest", 3, nil, "spread")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if totalCount(decisions) != 3 {
		t.Fatalf("expected 3 total replicas, got %d", totalCount(decisions))
	}

	dm := decisionMap(decisions)
	// With spread strategy and 3 replicas across 2 nodes, both nodes should get containers.
	if dm["node-a"] == 0 || dm["node-b"] == 0 {
		t.Fatalf("expected both nodes to receive containers, got %v", dm)
	}
}

func TestScheduleLeastLoaded(t *testing.T) {
	mock := &mockClusterState{
		nodes: []models.NodeInfo{
			makeNode("node-a", 4096, 3000), // heavily loaded
			makeNode("node-b", 4096, 500),  // lightly loaded
		},
	}

	sched := NewScheduler(mock)
	decisions, err := sched.Schedule("web", "nginx:latest", 3, nil, "least-loaded")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if totalCount(decisions) != 3 {
		t.Fatalf("expected 3 total replicas, got %d", totalCount(decisions))
	}

	dm := decisionMap(decisions)
	// Least-loaded (node-b) should get more or equal containers than node-a.
	if dm["node-b"] < dm["node-a"] {
		t.Fatalf("expected node-b (least loaded) to get >= containers than node-a, got node-a=%d, node-b=%d", dm["node-a"], dm["node-b"])
	}
}

func TestScheduleBinpack(t *testing.T) {
	mock := &mockClusterState{
		nodes: []models.NodeInfo{
			makeNode("node-a", 4096, 3500), // almost full, ~596 MB avail -> capacity=2
			makeNode("node-b", 4096, 1024), // lots of room, ~3072 MB avail -> capacity=12
		},
	}

	sched := NewScheduler(mock)
	decisions, err := sched.Schedule("web", "nginx:latest", 3, nil, "binpack")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if totalCount(decisions) != 3 {
		t.Fatalf("expected 3 total replicas, got %d", totalCount(decisions))
	}

	dm := decisionMap(decisions)
	// Binpack sorts by available memory ascending (least available first),
	// so node-a (most packed) gets filled first with up to its capacity.
	if dm["node-a"] == 0 {
		t.Fatalf("expected node-a (most packed) to receive containers in binpack, got %v", dm)
	}
}

func TestScheduleNoNodes(t *testing.T) {
	mock := &mockClusterState{
		nodes: []models.NodeInfo{},
	}

	sched := NewScheduler(mock)
	_, err := sched.Schedule("web", "nginx:latest", 3, nil, "spread")
	if err == nil {
		t.Fatal("expected error when no nodes available")
	}

	schedErr, ok := err.(*SchedulerError)
	if !ok {
		t.Fatalf("expected *SchedulerError, got %T", err)
	}
	if schedErr.Message == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestFindBestNodeForMigration(t *testing.T) {
	mock := &mockClusterState{
		nodes: []models.NodeInfo{
			makeNode("node-a", 4096, 3000), // source node to exclude
			makeNode("node-b", 4096, 2000), // moderately loaded
			makeNode("node-c", 4096, 500),  // least loaded
		},
	}

	sched := NewScheduler(mock)
	best := sched.FindBestNodeForMigration("container-123", "node-a")

	if best != "node-c" {
		t.Fatalf("expected best migration target=node-c (least loaded), got %s", best)
	}
}

func TestFindBestNodeForMigrationNoCandidate(t *testing.T) {
	mock := &mockClusterState{
		nodes: []models.NodeInfo{
			makeNode("node-a", 4096, 1024),
		},
	}

	sched := NewScheduler(mock)
	best := sched.FindBestNodeForMigration("container-123", "node-a")

	if best != "" {
		t.Fatalf("expected empty string when no candidate nodes, got %s", best)
	}
}
