// Package alerts implements a rules engine that monitors cluster node health
// metrics from heartbeat data and generates alerts.
//
// Alert conditions:
//   - Node offline (heartbeat timeout) -> critical
//   - Node degraded -> warning
//   - CPU > 90% -> critical, > 80% -> warning
//   - Memory > 90% -> critical, > 80% -> warning
//   - Disk > 90% -> critical, > 80% -> warning
package alerts

import (
	"fmt"
	"log"
	"sync"
	"time"

	"ai-container-go/internal/models"
)

// AlertStateProvider is the interface the engine uses to inspect cluster state
// and create alerts. Typically backed by the cluster state manager.
type AlertStateProvider interface {
	CheckNodeHealth(timeoutDegraded, timeoutOffline int)
	ListNodes(status ...models.NodeStatus) []models.NodeInfo
	CreateAlert(nodeName string, severity models.AlertSeverity, condition, message string) models.AlertInfo
}

// AlertEngine monitors cluster state on a periodic tick and fires alerts when
// resource thresholds are breached or nodes become unhealthy.
type AlertEngine struct {
	state         AlertStateProvider
	running       bool
	checkInterval time.Duration
	recentAlerts  map[string]bool
	mu            sync.Mutex
	stopCh        chan struct{}
}

// NewAlertEngine creates a new AlertEngine that monitors the given state provider.
func NewAlertEngine(state AlertStateProvider) *AlertEngine {
	return &AlertEngine{
		state:         state,
		checkInterval: 15 * time.Second,
		recentAlerts:  make(map[string]bool),
		stopCh:        make(chan struct{}),
	}
}

// Start begins the background monitoring goroutine.
func (e *AlertEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	go e.monitorLoop()
	log.Println("Alert engine started")
}

// Stop signals the monitoring goroutine to exit.
func (e *AlertEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// monitorLoop runs in its own goroutine, periodically checking all rules.
func (e *AlertEngine) monitorLoop() {
	ticker := time.NewTicker(e.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Alert check panic: %v", r)
					}
				}()
				e.checkAllRules()
			}()
		case <-e.stopCh:
			return
		}
	}
}

// checkAllRules evaluates every alert rule against the current cluster state.
func (e *AlertEngine) checkAllRules() {
	// Refresh node health based on heartbeat timeouts.
	e.state.CheckNodeHealth(30, 90)

	nodes := e.state.ListNodes()

	for _, node := range nodes {
		// Node status alerts.
		switch node.Status {
		case models.NodeOffline:
			e.fireAlert(node.Name, string(models.SeverityCritical),
				"status == offline",
				fmt.Sprintf("Node %s is offline", node.Name))
		case models.NodeDegraded:
			e.fireAlert(node.Name, string(models.SeverityWarning),
				"status == degraded",
				fmt.Sprintf("Node %s is degraded", node.Name))
		}

		// Resource alerts require a non-nil Resources field.
		if node.Resources == nil {
			continue
		}
		r := node.Resources

		// CPU
		if r.CPUUsedPercent > 90 {
			e.fireAlert(node.Name, string(models.SeverityCritical),
				fmt.Sprintf("cpu=%.0f%%", r.CPUUsedPercent),
				fmt.Sprintf("Node %s: CPU at %.0f%%", node.Name, r.CPUUsedPercent))
		} else if r.CPUUsedPercent > 80 {
			e.fireAlert(node.Name, string(models.SeverityWarning),
				fmt.Sprintf("cpu=%.0f%%", r.CPUUsedPercent),
				fmt.Sprintf("Node %s: CPU at %.0f%%", node.Name, r.CPUUsedPercent))
		}

		// Memory
		memTotal := r.MemoryTotalMB
		if memTotal < 1 {
			memTotal = 1
		}
		memPercent := float64(r.MemoryUsedMB) / float64(memTotal) * 100
		if memPercent > 90 {
			e.fireAlert(node.Name, string(models.SeverityCritical),
				fmt.Sprintf("memory=%.0f%%", memPercent),
				fmt.Sprintf("Node %s: Memory at %.0f%%", node.Name, memPercent))
		} else if memPercent > 80 {
			e.fireAlert(node.Name, string(models.SeverityWarning),
				fmt.Sprintf("memory=%.0f%%", memPercent),
				fmt.Sprintf("Node %s: Memory at %.0f%%", node.Name, memPercent))
		}

		// Disk
		diskTotal := r.DiskTotalGB
		if diskTotal < 0.1 {
			diskTotal = 0.1
		}
		diskPercent := r.DiskUsedGB / diskTotal * 100
		if diskPercent > 90 {
			e.fireAlert(node.Name, string(models.SeverityCritical),
				fmt.Sprintf("disk=%.0f%%", diskPercent),
				fmt.Sprintf("Node %s: Disk at %.0f%%", node.Name, diskPercent))
		} else if diskPercent > 80 {
			e.fireAlert(node.Name, string(models.SeverityWarning),
				fmt.Sprintf("disk=%.0f%%", diskPercent),
				fmt.Sprintf("Node %s: Disk at %.0f%%", node.Name, diskPercent))
		}
	}
}

// fireAlert creates an alert unless the same node+condition was recently fired.
func (e *AlertEngine) fireAlert(nodeName, severity, condition, message string) {
	alertKey := nodeName + ":" + condition

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.recentAlerts[alertKey] {
		return // duplicate suppressed
	}

	e.recentAlerts[alertKey] = true

	// Prune if the map grows too large.
	if len(e.recentAlerts) > 1000 {
		e.recentAlerts = make(map[string]bool)
	}

	e.state.CreateAlert(nodeName, models.AlertSeverity(severity), condition, message)
	log.Printf("ALERT [%s] %s: %s", severity, nodeName, message)
}

// ClearRecent resets the duplicate-suppression tracker so alerts can re-fire.
func (e *AlertEngine) ClearRecent() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recentAlerts = make(map[string]bool)
}
