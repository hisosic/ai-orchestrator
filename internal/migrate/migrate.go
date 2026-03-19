// Package migrate implements the container migration controller.
//
// Migration: docker commit -> save -> transfer -> load -> run (stateful, slow)
// Move: source stop -> destination run with same image (fast)
//
// Both operations:
//   - Do NOT modify replicas (only admin can change replicas)
//   - Use reconcile_skip to prevent reconcile from interfering
//   - Remove only the specific source container, not the service
package migrate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"ai-container-go/internal/models"

	"github.com/google/uuid"
)

var logger = log.New(log.Writer(), "[migrate] ", log.LstdFlags)

// ReconcileSkipFunc is a function type for adding/removing reconcile skip entries locally.
type ReconcileSkipFunc func(serviceName string)

// LocalReconcileSkipAdd is called to add a local reconcile skip entry.
// Set this from the runtime package at init time.
var LocalReconcileSkipAdd ReconcileSkipFunc

// LocalReconcileSkipRemove is called to remove a local reconcile skip entry.
// Set this from the runtime package at init time.
var LocalReconcileSkipRemove ReconcileSkipFunc

// MigrationStateProvider is the interface for cluster state access needed by the migration controller.
type MigrationStateProvider interface {
	GetNode(name string) *models.NodeInfo
	ListNodes(status ...models.NodeStatus) []models.NodeInfo
	GetPlacements(serviceName, nodeName string) []models.ContainerPlacement
	CreateMigration(migration models.MigrationInfo) models.MigrationInfo
	UpdateMigration(id string, status models.MigrationStatus, progress int, errMsg string)
	GetMigration(id string) *models.MigrationInfo
	UpdateNodeStatus(name string, status models.NodeStatus)
}

// MigrationController orchestrates container migration between nodes.
type MigrationController struct {
	state MigrationStateProvider
}

// NewMigrationController creates a new MigrationController.
func NewMigrationController(state MigrationStateProvider) *MigrationController {
	return &MigrationController{state: state}
}

// nodeBaseURL returns the base URL for a node's agent API.
func nodeBaseURL(node *models.NodeInfo) string {
	addr := node.Address
	if strings.HasPrefix(addr, "http") {
		return addr
	}
	return "http://" + addr
}

// nodeHeaders returns the HTTP headers for requests to a node.
func nodeHeaders(node *models.NodeInfo) map[string]string {
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	if node.Token != "" {
		headers["Authorization"] = "Bearer " + node.Token
	}
	return headers
}

// forceRemoveContainer stops and deletes a specific container on a remote node.
func forceRemoveContainer(node *models.NodeInfo, containerID, containerName string) {
	baseURL := nodeBaseURL(node)
	headers := nodeHeaders(node)
	client := &http.Client{Timeout: 30 * time.Second}

	// Try to stop the container (ignore errors)
	stopReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/containers/%s/stop", baseURL, containerID), nil)
	if err == nil {
		for k, v := range headers {
			stopReq.Header.Set(k, v)
		}
		resp, err := client.Do(stopReq)
		if err == nil {
			resp.Body.Close()
		}
	}

	// Delete by container ID
	delReq, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", baseURL, containerID), nil)
	if err == nil {
		for k, v := range headers {
			delReq.Header.Set(k, v)
		}
		resp, err := client.Do(delReq)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK && containerName != "" {
				// Fallback: delete by container name
				delByName, err2 := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/containers/%s", baseURL, containerName), nil)
				if err2 == nil {
					for k, v := range headers {
						delByName.Header.Set(k, v)
					}
					resp2, err2 := client.Do(delByName)
					if err2 == nil {
						resp2.Body.Close()
					}
				}
			}
		}
	}
}

// setReconcileSkip sets the reconcile skip flag on the local node.
func setReconcileSkip(serviceName string, skip bool) {
	defer func() {
		if r := recover(); r != nil {
			// ignore panics from nil function pointers
		}
	}()
	if skip {
		if LocalReconcileSkipAdd != nil {
			LocalReconcileSkipAdd(serviceName)
		}
	} else {
		if LocalReconcileSkipRemove != nil {
			LocalReconcileSkipRemove(serviceName)
		}
	}
}

// setRemoteReconcileSkip tells a remote node to skip/resume reconcile for a service.
func setRemoteReconcileSkip(node *models.NodeInfo, serviceName string, skip bool) {
	baseURL := nodeBaseURL(node)
	headers := nodeHeaders(node)
	client := &http.Client{Timeout: 10 * time.Second}

	body, err := json.Marshal(map[string]any{
		"service_name": serviceName,
		"skip":         skip,
	})
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/agent/reconcile-skip", baseURL), bytes.NewReader(body))
	if err != nil {
		return
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// Migrate starts a container migration from source to destination node.
// It creates a migration record, validates nodes, and launches the migration in a goroutine.
func (mc *MigrationController) Migrate(containerID, sourceNode, destinationNode, containerName, serviceName string) map[string]any {
	migrationID := uuid.New().String()[:8]
	now := time.Now().UTC().Format(time.RFC3339)

	migration := models.MigrationInfo{
		ID:              migrationID,
		ContainerID:     containerID,
		ContainerName:   containerName,
		SourceNode:      sourceNode,
		DestinationNode: destinationNode,
		Status:          models.MigrationPending,
		StartedAt:       now,
		Progress:        0,
	}
	mc.state.CreateMigration(migration)

	srcNode := mc.state.GetNode(sourceNode)
	dstNode := mc.state.GetNode(destinationNode)

	if srcNode == nil {
		return mc.failMigration(migrationID, fmt.Sprintf("Source node '%s' not found", sourceNode))
	}
	if dstNode == nil {
		return mc.failMigration(migrationID, fmt.Sprintf("Destination node '%s' not found", destinationNode))
	}
	if dstNode.Status != models.NodeHealthy {
		return mc.failMigration(migrationID, fmt.Sprintf("Destination node status is %s", string(dstNode.Status)))
	}

	logger.Printf("Migration %s: START %s (%s -> %s)", migrationID, containerName, sourceNode, destinationNode)

	go mc.executeMigration(migrationID, srcNode, dstNode, containerID, containerName, serviceName)

	return map[string]any{
		"id":      migrationID,
		"status":  "pending",
		"message": fmt.Sprintf("Migration started: %s (%s -> %s)", coalesce(containerName, containerID), sourceNode, destinationNode),
	}
}

// executeMigration performs the actual migration phases:
// EXPORTING (15%) -> TRANSFERRING (40%) -> IMPORTING (60%) -> VERIFYING (80%) -> COMPLETED (100%)
func (mc *MigrationController) executeMigration(migrationID string, srcNode, dstNode *models.NodeInfo,
	containerID, containerName, serviceName string) {

	srcURL := nodeBaseURL(srcNode)
	dstURL := nodeBaseURL(dstNode)
	srcHeaders := nodeHeaders(srcNode)
	dstHeaders := nodeHeaders(dstNode)

	// Pause reconcile on both nodes for this service
	if serviceName != "" {
		setReconcileSkip(serviceName, true)
		setRemoteReconcileSkip(srcNode, serviceName, true)
		setRemoteReconcileSkip(dstNode, serviceName, true)
	}

	defer func() {
		// Resume reconcile on both nodes
		if serviceName != "" {
			setReconcileSkip(serviceName, false)
			setRemoteReconcileSkip(srcNode, serviceName, false)
			setRemoteReconcileSkip(dstNode, serviceName, false)
		}
	}()

	longClient := &http.Client{Timeout: 300 * time.Second}

	// ---- Export ----
	mc.state.UpdateMigration(migrationID, models.MigrationExporting, 15, "")
	logger.Printf("Migration %s: Exporting from %s", migrationID, srcNode.Name)

	exportResp, err := doRequest(longClient, http.MethodPost,
		fmt.Sprintf("%s/v1/agent/export/%s", srcURL, containerID), srcHeaders, nil)
	if err != nil {
		mc.failMigration(migrationID, fmt.Sprintf("Export request failed: %v", err))
		return
	}

	if exportResp.StatusCode != http.StatusOK {
		mc.failMigration(migrationID, fmt.Sprintf("Export HTTP failed: %d", exportResp.StatusCode))
		return
	}

	var exportData map[string]any
	if err := json.NewDecoder(exportResp.Body).Decode(&exportData); err != nil {
		exportResp.Body.Close()
		mc.failMigration(migrationID, fmt.Sprintf("Export response decode failed: %v", err))
		return
	}
	exportResp.Body.Close()

	success, _ := exportData["success"].(bool)
	if !success {
		errMsg, _ := exportData["error"].(string)
		if errMsg == "" {
			errMsg = "unknown"
		}
		mc.failMigration(migrationID, fmt.Sprintf("Export failed: %s", errMsg))
		return
	}

	imageDataB64, _ := exportData["image_data"].(string)
	config, _ := exportData["config"]
	if config == nil {
		config = map[string]any{}
	}
	logger.Printf("Migration %s: Export OK (%d chars)", migrationID, len(imageDataB64))

	// ---- Transfer + Import ----
	mc.state.UpdateMigration(migrationID, models.MigrationTransferring, 40, "")
	mc.state.UpdateMigration(migrationID, models.MigrationImporting, 60, "")
	logger.Printf("Migration %s: Importing on %s", migrationID, dstNode.Name)

	importPayload, err := json.Marshal(map[string]any{
		"image_data":   imageDataB64,
		"config":       config,
		"service_name": serviceName,
	})
	if err != nil {
		mc.failMigration(migrationID, fmt.Sprintf("Import payload marshal failed: %v", err))
		return
	}

	importResp, err := doRequest(longClient, http.MethodPost,
		fmt.Sprintf("%s/v1/agent/import", dstURL), dstHeaders, importPayload)
	if err != nil {
		mc.failMigration(migrationID, fmt.Sprintf("Import request failed: %v", err))
		return
	}

	if importResp.StatusCode != http.StatusOK {
		importResp.Body.Close()
		mc.failMigration(migrationID, fmt.Sprintf("Import HTTP failed: %d", importResp.StatusCode))
		return
	}

	var importResult map[string]any
	if err := json.NewDecoder(importResp.Body).Decode(&importResult); err != nil {
		importResp.Body.Close()
		mc.failMigration(migrationID, fmt.Sprintf("Import response decode failed: %v", err))
		return
	}
	importResp.Body.Close()

	importSuccess, _ := importResult["success"].(bool)
	if !importSuccess {
		errMsg, _ := importResult["error"].(string)
		if errMsg == "" {
			errMsg = "unknown"
		}
		mc.failMigration(migrationID, fmt.Sprintf("Import failed: %s", errMsg))
		return
	}

	newContainerID, _ := importResult["container_id"].(string)
	logger.Printf("Migration %s: Import OK, new=%s", migrationID, newContainerID)

	// ---- Verify ----
	mc.state.UpdateMigration(migrationID, models.MigrationVerifying, 80, "")
	time.Sleep(3 * time.Second)
	verified := mc.verifyContainer(dstURL, dstHeaders, newContainerID, containerName)

	if !verified {
		mc.failMigration(migrationID, "Container verification failed on destination node")
		return
	}

	logger.Printf("Migration %s: Verify OK", migrationID)

	// ---- Remove source container ONLY (replicas unchanged) ----
	logger.Printf("Migration %s: Removing source %s on %s", migrationID, containerName, srcNode.Name)
	forceRemoveContainer(srcNode, containerID, containerName)

	// ---- Complete ----
	mc.state.UpdateMigration(migrationID, models.MigrationCompleted, 100, "")
	logger.Printf("Migration %s: COMPLETED (%s: %s -> %s)", migrationID, containerName, srcNode.Name, dstNode.Name)
}

// verifyContainer checks if a container is running on the destination node.
func (mc *MigrationController) verifyContainer(dstURL string, dstHeaders map[string]string,
	newContainerID, containerName string) bool {

	client := &http.Client{Timeout: 15 * time.Second}

	// Try inspect first
	resp, err := doRequest(client, http.MethodGet,
		fmt.Sprintf("%s/v1/containers/%s/inspect", dstURL, newContainerID), dstHeaders, nil)
	if err == nil && resp.StatusCode == http.StatusOK {
		var data map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&data); err == nil {
			details, _ := data["details"].(map[string]any)
			if details == nil {
				details = data
			}
			attrs, _ := details["attrs"].(map[string]any)
			if attrs == nil {
				attrs = details
			}
			state, _ := attrs["State"].(map[string]any)
			if state != nil {
				running, _ := state["Running"].(bool)
				statusStr, _ := state["Status"].(string)
				if running || statusStr == "running" {
					resp.Body.Close()
					return true
				}
			}
		}
		resp.Body.Close()
	} else if resp != nil {
		resp.Body.Close()
	}

	// Fallback: container list
	resp, err = doRequest(client, http.MethodGet,
		fmt.Sprintf("%s/v1/containers", dstURL), dstHeaders, nil)
	if err == nil && resp.StatusCode == http.StatusOK {
		var listData map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&listData); err == nil {
			containers, _ := listData["containers"].([]any)
			for _, raw := range containers {
				c, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				cID, _ := c["id"].(string)
				cName, _ := c["name"].(string)
				cState, _ := c["state"].(string)
				cStatus, _ := c["status"].(string)

				if strings.HasPrefix(cID, newContainerID) || cName == containerName {
					if cState == "running" || strings.HasPrefix(cStatus, "Up") {
						resp.Body.Close()
						return true
					}
				}
			}
		}
		resp.Body.Close()
	} else if resp != nil {
		resp.Body.Close()
	}

	return false
}

// failMigration marks a migration as failed and returns an error result.
func (mc *MigrationController) failMigration(migrationID, errMsg string) map[string]any {
	logger.Printf("Migration %s FAILED: %s", migrationID, errMsg)
	mc.state.UpdateMigration(migrationID, models.MigrationFailed, 0, errMsg)
	return map[string]any{
		"id":     migrationID,
		"status": "failed",
		"error":  errMsg,
	}
}

// CancelMigration cancels an in-progress migration.
func (mc *MigrationController) CancelMigration(migrationID string) map[string]any {
	migration := mc.state.GetMigration(migrationID)
	if migration == nil {
		return map[string]any{"error": "not found"}
	}
	if migration.Status == models.MigrationCompleted ||
		migration.Status == models.MigrationFailed ||
		migration.Status == models.MigrationRolledBack {
		return map[string]any{"error": fmt.Sprintf("already %s", string(migration.Status))}
	}
	mc.state.UpdateMigration(migrationID, models.MigrationRolledBack, 0, "Cancelled")
	return map[string]any{
		"id":     migrationID,
		"status": "rolled_back",
	}
}

// DrainNode migrates all containers off a node, distributing them to healthy nodes
// or to a specific target node.
func (mc *MigrationController) DrainNode(nodeName string, targetNode string) map[string]any {
	mc.state.UpdateNodeStatus(nodeName, models.NodeDraining)

	placements := mc.state.GetPlacements("", nodeName)
	if len(placements) == 0 {
		mc.state.UpdateNodeStatus(nodeName, models.NodeCordoned)
		return map[string]any{
			"message":  fmt.Sprintf("No containers on %s", nodeName),
			"migrated": 0,
		}
	}

	nodes := mc.state.ListNodes()
	var healthy []models.NodeInfo
	for _, n := range nodes {
		if n.Status == models.NodeHealthy && n.Name != nodeName {
			healthy = append(healthy, n)
		}
	}

	if len(healthy) == 0 {
		mc.state.UpdateNodeStatus(nodeName, models.NodeHealthy)
		return map[string]any{"error": "No healthy nodes available"}
	}

	var migrations []map[string]any
	for i, p := range placements {
		dest := targetNode
		if dest == "" {
			dest = healthy[i%len(healthy)].Name
		}
		result := mc.Migrate(p.ContainerID, nodeName, dest, p.ContainerName, p.ServiceName)
		migrations = append(migrations, result)
	}

	return map[string]any{
		"message":    fmt.Sprintf("Draining %s: %d migrations started", nodeName, len(migrations)),
		"migrations": migrations,
	}
}

// doRequest is a helper to perform an HTTP request with headers and optional JSON body.
func doRequest(client *http.Client, method, url string, headers map[string]string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return client.Do(req)
}

// coalesce returns the first non-empty string.
func coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
