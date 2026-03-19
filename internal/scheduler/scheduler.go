// Package scheduler decides container placement across cluster nodes.
//
// Strategies:
//   - spread:       Distribute replicas across maximum number of distinct nodes
//   - least-loaded: Place on nodes with most available memory
//   - binpack:      Pack containers onto fewest nodes possible
package scheduler

import (
	"sort"

	"ai-container-go/internal/models"
)

// ClusterStateProvider abstracts access to cluster state so the scheduler
// can be tested and used without a direct dependency on the state manager.
type ClusterStateProvider interface {
	ListNodes(status ...models.NodeStatus) []models.NodeInfo
	GetPlacements(serviceName, nodeName string) []models.ContainerPlacement
}

// SchedulerError is a custom error type for scheduling failures.
type SchedulerError struct {
	Message string
}

func (e *SchedulerError) Error() string {
	return e.Message
}

// Scheduler decides container placement across cluster nodes.
type Scheduler struct {
	state ClusterStateProvider
}

// NewScheduler creates a new Scheduler backed by the given state provider.
func NewScheduler(state ClusterStateProvider) *Scheduler {
	return &Scheduler{state: state}
}

// Schedule decides placement for the requested number of replicas of a service.
//
// Algorithm:
//  1. Get all schedulable nodes (status == "healthy")
//  2. Filter by affinity / anti-affinity constraints
//  3. Check resource requirements (memory, CPU)
//  4. Apply the chosen strategy to distribute replicas
func (s *Scheduler) Schedule(
	serviceName, image string,
	replicas int,
	constraints *models.ScheduleConstraints,
	strategy string,
) ([]models.ScheduleDecision, error) {

	nodes := s.state.ListNodes()

	// 1. Filter schedulable nodes — only healthy nodes are eligible.
	schedulable := filterHealthy(nodes)
	if len(schedulable) == 0 {
		return nil, &SchedulerError{Message: "No schedulable nodes available"}
	}

	// 2. Apply constraints.
	if constraints != nil {
		schedulable = applyConstraints(schedulable, constraints)
	}
	if len(schedulable) == 0 {
		return nil, &SchedulerError{Message: "No nodes satisfy scheduling constraints"}
	}

	// 3. Gather existing placements for the service (used by spread strategy).
	existingPerNode := make(map[string]int)
	placements := s.state.GetPlacements(serviceName, "")
	for _, p := range placements {
		existingPerNode[p.NodeName]++
	}

	// 4. Apply strategy.
	switch strategy {
	case "spread":
		return strategySpread(schedulable, replicas, existingPerNode), nil
	case "binpack":
		return strategyBinpack(schedulable, replicas), nil
	default: // "least-loaded" or any unrecognised value
		return strategyLeastLoaded(schedulable, replicas), nil
	}
}

// FindBestNodeForMigration returns the name of the best target node for
// migrating a container away from excludeNode. It picks the least-loaded
// healthy node. Returns an empty string if no suitable node exists.
func (s *Scheduler) FindBestNodeForMigration(containerID, excludeNode string) string {
	nodes := s.state.ListNodes()

	var candidates []models.NodeInfo
	for _, n := range nodes {
		if n.Status == models.NodeHealthy && n.Name != excludeNode {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		return ""
	}

	// Sort by memory used ratio ascending (least loaded first).
	sort.Slice(candidates, func(i, j int) bool {
		return memUsedRatio(candidates[i]) < memUsedRatio(candidates[j])
	})

	return candidates[0].Name
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// filterHealthy returns only nodes whose status is "healthy".
func filterHealthy(nodes []models.NodeInfo) []models.NodeInfo {
	out := make([]models.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		if n.Status == models.NodeHealthy {
			out = append(out, n)
		}
	}
	return out
}

// applyConstraints filters nodes by the provided scheduling constraints.
func applyConstraints(nodes []models.NodeInfo, c *models.ScheduleConstraints) []models.NodeInfo {
	out := nodes

	// Node affinity — only allow listed nodes.
	if len(c.NodeAffinity) > 0 {
		allowed := toSet(c.NodeAffinity)
		out = filterNodes(out, func(n models.NodeInfo) bool {
			_, ok := allowed[n.Name]
			return ok
		})
	}

	// Node anti-affinity — exclude listed nodes.
	if len(c.NodeAntiAffinity) > 0 {
		excluded := toSet(c.NodeAntiAffinity)
		out = filterNodes(out, func(n models.NodeInfo) bool {
			_, ok := excluded[n.Name]
			return !ok
		})
	}

	// Memory requirement.
	if c.MemoryRequiredMB != nil {
		reqMB := *c.MemoryRequiredMB
		out = filterNodes(out, func(n models.NodeInfo) bool {
			if n.Resources == nil {
				return false
			}
			avail := n.Resources.MemoryTotalMB - n.Resources.MemoryUsedMB
			return avail >= reqMB
		})
	}

	// CPU requirement.
	if c.CPURequired != nil {
		reqCPU := *c.CPURequired
		out = filterNodes(out, func(n models.NodeInfo) bool {
			if n.Resources == nil {
				return false
			}
			availCPU := float64(n.Resources.CPUCores) * (100 - n.Resources.CPUUsedPercent) / 100
			return availCPU >= reqCPU
		})
	}

	return out
}

// --- Strategies ---

// scoredNode is used internally for sorting during strategy evaluation.
type scoredNode struct {
	node  models.NodeInfo
	score float64 // meaning depends on the strategy
	extra int     // secondary sort key (e.g. existing count)
}

// strategySpread distributes replicas across as many distinct nodes as
// possible, preferring nodes that already have fewer replicas of the service.
func strategySpread(nodes []models.NodeInfo, replicas int, existingPerNode map[string]int) []models.ScheduleDecision {
	scored := make([]scoredNode, 0, len(nodes))
	for _, n := range nodes {
		existing := existingPerNode[n.Name]
		availMem := availableMemory(n)
		scored = append(scored, scoredNode{
			node:  n,
			score: float64(availMem),
			extra: existing,
		})
	}

	// Sort by existing replica count ascending, then available memory descending.
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].extra != scored[j].extra {
			return scored[i].extra < scored[j].extra
		}
		return scored[i].score > scored[j].score
	})

	return roundRobin(scored, replicas)
}

// strategyLeastLoaded places replicas on nodes sorted by memory usage ratio
// (ascending), distributing via round-robin.
func strategyLeastLoaded(nodes []models.NodeInfo, replicas int) []models.ScheduleDecision {
	scored := make([]scoredNode, 0, len(nodes))
	for _, n := range nodes {
		scored = append(scored, scoredNode{
			node:  n,
			score: memUsedRatio(n) * 100,
		})
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score < scored[j].score
	})

	return roundRobin(scored, replicas)
}

// strategyBinpack packs containers onto the fewest nodes possible, filling
// the most-loaded nodes first. It estimates capacity at 256 MB per container.
func strategyBinpack(nodes []models.NodeInfo, replicas int) []models.ScheduleDecision {
	type nodeAvail struct {
		node  models.NodeInfo
		avail int // available memory in MB
	}

	items := make([]nodeAvail, 0, len(nodes))
	for _, n := range nodes {
		items = append(items, nodeAvail{node: n, avail: availableMemory(n)})
	}

	// Sort by available memory ascending (least available first = most packed).
	sort.Slice(items, func(i, j int) bool {
		return items[i].avail < items[j].avail
	})

	placement := make(map[string]int)
	remaining := replicas

	for _, item := range items {
		if remaining <= 0 {
			break
		}
		capacity := item.avail / 256
		if capacity < 1 {
			capacity = 1
		}
		assign := remaining
		if assign > capacity {
			assign = capacity
		}
		if assign > 0 {
			placement[item.node.Name] = assign
			remaining -= assign
		}
	}

	// If replicas still remain, overflow onto the first (most packed) node.
	if remaining > 0 {
		first := items[0].node.Name
		placement[first] += remaining
	}

	return mapToDecisions(placement)
}

// --- Utility helpers ---

// roundRobin distributes replicas across scored nodes in order.
func roundRobin(scored []scoredNode, replicas int) []models.ScheduleDecision {
	placement := make(map[string]int)
	n := len(scored)
	for i := 0; i < replicas; i++ {
		name := scored[i%n].node.Name
		placement[name]++
	}
	return mapToDecisions(placement)
}

// mapToDecisions converts a node-name -> count map to a slice of ScheduleDecision.
func mapToDecisions(m map[string]int) []models.ScheduleDecision {
	decisions := make([]models.ScheduleDecision, 0, len(m))
	for name, count := range m {
		decisions = append(decisions, models.ScheduleDecision{
			NodeName: name,
			Count:    count,
		})
	}
	// Deterministic output order.
	sort.Slice(decisions, func(i, j int) bool {
		return decisions[i].NodeName < decisions[j].NodeName
	})
	return decisions
}

// availableMemory returns (total - used) MB, or 0 if resources are nil.
func availableMemory(n models.NodeInfo) int {
	if n.Resources == nil {
		return 0
	}
	avail := n.Resources.MemoryTotalMB - n.Resources.MemoryUsedMB
	if avail < 0 {
		return 0
	}
	return avail
}

// memUsedRatio returns the fraction of memory in use (0.0–1.0).
func memUsedRatio(n models.NodeInfo) float64 {
	if n.Resources == nil {
		return 1.0
	}
	total := n.Resources.MemoryTotalMB
	if total <= 0 {
		return 1.0
	}
	return float64(n.Resources.MemoryUsedMB) / float64(total)
}

// filterNodes returns nodes for which the predicate returns true.
func filterNodes(nodes []models.NodeInfo, pred func(models.NodeInfo) bool) []models.NodeInfo {
	out := make([]models.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		if pred(n) {
			out = append(out, n)
		}
	}
	return out
}

// toSet converts a string slice to a map for O(1) lookups.
func toSet(items []string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, item := range items {
		m[item] = struct{}{}
	}
	return m
}

// Ensure SchedulerError always satisfies the error interface (compile-time check).
var _ error = (*SchedulerError)(nil)

