package clusterstate

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"ai-container-go/internal/models"

	_ "github.com/mattn/go-sqlite3"
)

// ClusterStateManager is an SQLite-based state manager for the cluster master node.
type ClusterStateManager struct {
	dbPath string
	mu     sync.Mutex
	db     *sql.DB
}

// NewClusterStateManager creates a new ClusterStateManager.
// If dbPath is empty, it uses ORCHESTRATOR_STATE_DIR env + "/cluster.db".
func NewClusterStateManager(dbPath string) *ClusterStateManager {
	if dbPath == "" {
		stateDir := os.Getenv("ORCHESTRATOR_STATE_DIR")
		if stateDir == "" {
			stateDir = "/data"
		}
		dbPath = stateDir + "/cluster.db"
	}
	m := &ClusterStateManager{dbPath: dbPath}
	m.initDB()
	return m
}

func (m *ClusterStateManager) openDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", m.dbPath)
	if err != nil {
		return nil, err
	}
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA foreign_keys=ON")
	return db, nil
}

func (m *ClusterStateManager) initDB() {
	db, err := m.openDB()
	if err != nil {
		panic(fmt.Sprintf("clusterstate: failed to open db: %v", err))
	}
	m.db = db

	schema := `
		CREATE TABLE IF NOT EXISTS nodes (
			name TEXT PRIMARY KEY,
			address TEXT NOT NULL,
			token TEXT DEFAULT '',
			status TEXT DEFAULT 'healthy',
			role TEXT DEFAULT 'worker',
			labels TEXT DEFAULT '{}',
			resources TEXT,
			last_heartbeat TEXT,
			registered_at TEXT NOT NULL,
			container_count INTEGER DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS container_placements (
			container_id TEXT PRIMARY KEY,
			container_name TEXT,
			service_name TEXT,
			image TEXT,
			node_name TEXT NOT NULL,
			status TEXT,
			cpu_percent REAL DEFAULT 0,
			memory_mb REAL DEFAULT 0,
			memory_limit_mb REAL DEFAULT 0,
			net_rx_mb REAL DEFAULT 0,
			net_tx_mb REAL DEFAULT 0,
			updated_at TEXT
		);

		CREATE TABLE IF NOT EXISTS services (
			name TEXT PRIMARY KEY,
			image TEXT NOT NULL,
			desired_replicas INTEGER DEFAULT 1,
			memory_limit TEXT,
			cpu_limit TEXT,
			environment TEXT DEFAULT '[]',
			volumes TEXT DEFAULT '[]',
			ports TEXT DEFAULT '[]',
			user_spec TEXT,
			volume_mode TEXT DEFAULT 'shared',
			schedule_constraints TEXT
		);

		CREATE TABLE IF NOT EXISTS migrations (
			id TEXT PRIMARY KEY,
			container_id TEXT,
			container_name TEXT DEFAULT '',
			source_node TEXT,
			destination_node TEXT,
			status TEXT DEFAULT 'pending',
			started_at TEXT,
			completed_at TEXT,
			error TEXT,
			progress INTEGER DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS alerts (
			id TEXT PRIMARY KEY,
			node_name TEXT,
			severity TEXT DEFAULT 'warning',
			condition TEXT,
			message TEXT,
			created_at TEXT,
			acknowledged INTEGER DEFAULT 0
		);

		CREATE INDEX IF NOT EXISTS idx_placements_node ON container_placements(node_name);
		CREATE INDEX IF NOT EXISTS idx_placements_service ON container_placements(service_name);
		CREATE INDEX IF NOT EXISTS idx_migrations_status ON migrations(status);
		CREATE INDEX IF NOT EXISTS idx_alerts_ack ON alerts(acknowledged);
	`
	_, err = m.db.Exec(schema)
	if err != nil {
		panic(fmt.Sprintf("clusterstate: failed to init schema: %v", err))
	}
}

func now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func shortUUID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%08x", b)
}

// --- Node Management ---

// RegisterNode registers or updates a node via INSERT OR REPLACE.
func (m *ClusterStateManager) RegisterNode(node models.NodeInfo) models.NodeInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	ts := now()

	labelsJSON, _ := json.Marshal(node.Labels)
	var resourcesJSON []byte
	if node.Resources != nil {
		resourcesJSON, _ = json.Marshal(node.Resources)
	}

	registeredAt := node.RegisteredAt
	if registeredAt == "" {
		registeredAt = ts
	}

	_, err := m.db.Exec(`
		INSERT INTO nodes (name, address, token, status, role, labels, resources, last_heartbeat, registered_at, container_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			address=excluded.address,
			token=excluded.token,
			status=excluded.status,
			role=excluded.role,
			labels=excluded.labels,
			resources=excluded.resources,
			last_heartbeat=excluded.last_heartbeat
	`, node.Name, node.Address, node.Token, string(node.Status),
		node.Role, string(labelsJSON), nullableString(resourcesJSON),
		ts, registeredAt, node.ContainerCount)
	if err != nil {
		fmt.Printf("clusterstate: register_node error: %v\n", err)
	}

	node.RegisteredAt = registeredAt
	node.LastHeartbeat = ts
	return node
}

// RemoveNode deletes a node by name. Returns true if a row was deleted.
func (m *ClusterStateManager) RemoveNode(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	res, err := m.db.Exec("DELETE FROM nodes WHERE name=?", name)
	if err != nil {
		return false
	}
	n, _ := res.RowsAffected()
	return n > 0
}

// GetNode retrieves a single node by name, or nil if not found.
func (m *ClusterStateManager) GetNode(name string) *models.NodeInfo {
	row := m.db.QueryRow("SELECT name, address, token, status, role, labels, resources, last_heartbeat, registered_at, container_count FROM nodes WHERE name=?", name)
	node, err := scanNode(row)
	if err != nil {
		return nil
	}
	return node
}

// ListNodes returns nodes, optionally filtered by status.
// If no status values are provided, all nodes are returned.
func (m *ClusterStateManager) ListNodes(status ...models.NodeStatus) []models.NodeInfo {
	var rows *sql.Rows
	var err error

	if len(status) > 0 && status[0] != "" {
		rows, err = m.db.Query("SELECT name, address, token, status, role, labels, resources, last_heartbeat, registered_at, container_count FROM nodes WHERE status=?", string(status[0]))
	} else {
		rows, err = m.db.Query("SELECT name, address, token, status, role, labels, resources, last_heartbeat, registered_at, container_count FROM nodes")
	}
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []models.NodeInfo
	for rows.Next() {
		node, err := scanNodeRows(rows)
		if err != nil {
			continue
		}
		result = append(result, *node)
	}
	if result == nil {
		result = []models.NodeInfo{}
	}
	return result
}

// UpdateNodeStatus updates only the status field for a node.
func (m *ClusterStateManager) UpdateNodeStatus(name string, status models.NodeStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.db.Exec("UPDATE nodes SET status=? WHERE name=?", string(status), name)
}

// --- Heartbeat ---

// ProcessHeartbeat updates node resources, sets it healthy, and replaces its container placements.
func (m *ClusterStateManager) ProcessHeartbeat(nodeName string, address string, resources models.NodeResources, containers []models.ContainerPlacement) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ts := now()
	resourcesJSON, _ := json.Marshal(resources)

	tx, err := m.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	tx.Exec(`
		INSERT INTO nodes (name, address, token, status, role, labels, resources, last_heartbeat, registered_at, container_count)
		VALUES (?, ?, '', 'healthy', 'worker', '{}', ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			status=CASE WHEN nodes.status IN ('cordoned','draining') THEN nodes.status ELSE 'healthy' END,
			address=CASE WHEN excluded.address != '' THEN excluded.address ELSE nodes.address END,
			resources=excluded.resources,
			last_heartbeat=excluded.last_heartbeat,
			container_count=excluded.container_count
	`, nodeName, address, string(resourcesJSON), ts, ts, len(containers))

	tx.Exec("DELETE FROM container_placements WHERE node_name=?", nodeName)

	for _, c := range containers {
		tx.Exec(`
			INSERT OR REPLACE INTO container_placements
				(container_id, container_name, service_name, image, node_name, status, cpu_percent, memory_mb, memory_limit_mb, net_rx_mb, net_tx_mb, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, c.ContainerID, c.ContainerName, c.ServiceName, c.Image, nodeName, c.Status,
			c.CPUPercent, c.MemoryMB, c.MemoryLimitMB, c.NetRxMB, c.NetTxMB, ts)
	}

	tx.Commit()
}

// CheckNodeHealth checks heartbeat timeouts and updates node statuses.
// Master role nodes are skipped.
func (m *ClusterStateManager) CheckNodeHealth(timeoutDegraded, timeoutOffline int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rows, err := m.db.Query(
		"SELECT name, status, role, last_heartbeat FROM nodes WHERE status NOT IN ('cordoned', 'draining')")
	if err != nil {
		return
	}

	type nodeRow struct {
		name, status, role, lastHeartbeat string
	}
	var nodes []nodeRow
	for rows.Next() {
		var n nodeRow
		var lh sql.NullString
		rows.Scan(&n.name, &n.status, &n.role, &lh)
		n.lastHeartbeat = lh.String
		nodes = append(nodes, n)
	}
	rows.Close()

	nowTime := time.Now().UTC()
	for _, n := range nodes {
		if n.role == "master" {
			continue
		}
		if n.lastHeartbeat == "" {
			continue
		}
		last, err := time.Parse(time.RFC3339, n.lastHeartbeat)
		if err != nil {
			continue
		}
		delta := nowTime.Sub(last).Seconds()
		if delta > float64(timeoutOffline) && n.status != "offline" {
			m.db.Exec("UPDATE nodes SET status='offline' WHERE name=?", n.name)
			m.createAlertInternal(n.name, "critical",
				fmt.Sprintf("heartbeat_timeout>%ds", timeoutOffline),
				fmt.Sprintf("Node %s is offline (no heartbeat for %ds)", n.name, int(delta)))
		} else if delta > float64(timeoutDegraded) && n.status != "offline" && n.status != "degraded" {
			m.db.Exec("UPDATE nodes SET status='degraded' WHERE name=?", n.name)
		}
	}
}

// --- Container Placements ---

// GetPlacements returns container placements, optionally filtered by service and/or node.
func (m *ClusterStateManager) GetPlacements(serviceName, nodeName string) []models.ContainerPlacement {
	query := "SELECT container_id, container_name, service_name, image, node_name, status, cpu_percent, memory_mb, memory_limit_mb, net_rx_mb, net_tx_mb FROM container_placements WHERE 1=1"
	var args []any

	if serviceName != "" {
		query += " AND service_name=?"
		args = append(args, serviceName)
	}
	if nodeName != "" {
		query += " AND node_name=?"
		args = append(args, nodeName)
	}

	rows, err := m.db.Query(query, args...)
	if err != nil {
		return []models.ContainerPlacement{}
	}
	defer rows.Close()

	var result []models.ContainerPlacement
	for rows.Next() {
		var c models.ContainerPlacement
		var svcName sql.NullString
		rows.Scan(&c.ContainerID, &c.ContainerName, &svcName, &c.Image, &c.NodeName,
			&c.Status, &c.CPUPercent, &c.MemoryMB, &c.MemoryLimitMB, &c.NetRxMB, &c.NetTxMB)
		c.ServiceName = svcName.String
		result = append(result, c)
	}
	if result == nil {
		result = []models.ContainerPlacement{}
	}
	return result
}

// GetServicePlacementSummary returns per-service, per-node container counts.
func (m *ClusterStateManager) GetServicePlacementSummary() map[string]any {
	rows, err := m.db.Query(`
		SELECT service_name, node_name, COUNT(*) as cnt,
		       SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) as running
		FROM container_placements
		WHERE service_name IS NOT NULL AND service_name != ''
		GROUP BY service_name, node_name
	`)
	if err != nil {
		return map[string]any{}
	}
	defer rows.Close()

	result := map[string]any{}
	for rows.Next() {
		var svc, nodeName string
		var cnt, running int
		rows.Scan(&svc, &nodeName, &cnt, &running)

		svcData, ok := result[svc]
		if !ok {
			svcData = map[string]any{"total": 0, "running": 0, "nodes": map[string]any{}}
			result[svc] = svcData
		}
		m2 := svcData.(map[string]any)
		m2["total"] = m2["total"].(int) + cnt
		m2["running"] = m2["running"].(int) + running
		nodesMap := m2["nodes"].(map[string]any)
		nodesMap[nodeName] = map[string]any{"total": cnt, "running": running}
	}
	return result
}

// --- Migrations ---

// CreateMigration inserts a new migration record.
func (m *ClusterStateManager) CreateMigration(migration models.MigrationInfo) models.MigrationInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.db.Exec(`
		INSERT INTO migrations (id, container_id, container_name, source_node, destination_node, status, started_at, progress)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, migration.ID, migration.ContainerID, migration.ContainerName,
		migration.SourceNode, migration.DestinationNode,
		string(migration.Status), migration.StartedAt, migration.Progress)

	return migration
}

// UpdateMigration updates status, progress, and error for a migration.
func (m *ClusterStateManager) UpdateMigration(id string, status models.MigrationStatus, progress int, errorMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var completedAt sql.NullString
	if status == models.MigrationCompleted || status == models.MigrationFailed || status == models.MigrationRolledBack {
		completedAt = sql.NullString{String: now(), Valid: true}
	}

	m.db.Exec(`
		UPDATE migrations SET status=?, progress=?, error=?, completed_at=?
		WHERE id=?
	`, string(status), progress, nullableStr(errorMsg), completedAt, id)
}

// GetMigration retrieves a migration by ID, or nil if not found.
func (m *ClusterStateManager) GetMigration(id string) *models.MigrationInfo {
	row := m.db.QueryRow("SELECT id, container_id, container_name, source_node, destination_node, status, started_at, completed_at, error, progress FROM migrations WHERE id=?", id)

	var mig models.MigrationInfo
	var containerName, startedAt sql.NullString
	var completedAt, errorMsg sql.NullString
	var statusStr string

	err := row.Scan(&mig.ID, &mig.ContainerID, &containerName, &mig.SourceNode,
		&mig.DestinationNode, &statusStr, &startedAt, &completedAt, &errorMsg, &mig.Progress)
	if err != nil {
		return nil
	}

	mig.ContainerName = containerName.String
	mig.Status = models.MigrationStatus(statusStr)
	mig.StartedAt = startedAt.String
	mig.CompletedAt = completedAt.String
	mig.Error = errorMsg.String
	return &mig
}

// ListMigrations returns migrations, optionally only active ones.
func (m *ClusterStateManager) ListMigrations(activeOnly bool) []models.MigrationInfo {
	var rows *sql.Rows
	var err error

	if activeOnly {
		rows, err = m.db.Query("SELECT id, container_id, container_name, source_node, destination_node, status, started_at, completed_at, error, progress FROM migrations WHERE status NOT IN ('completed', 'failed', 'rolled_back') ORDER BY started_at DESC")
	} else {
		rows, err = m.db.Query("SELECT id, container_id, container_name, source_node, destination_node, status, started_at, completed_at, error, progress FROM migrations ORDER BY started_at DESC LIMIT 50")
	}
	if err != nil {
		return []models.MigrationInfo{}
	}
	defer rows.Close()

	var result []models.MigrationInfo
	for rows.Next() {
		var mig models.MigrationInfo
		var containerName, startedAt, completedAt, errorMsg sql.NullString
		var statusStr string
		rows.Scan(&mig.ID, &mig.ContainerID, &containerName, &mig.SourceNode,
			&mig.DestinationNode, &statusStr, &startedAt, &completedAt, &errorMsg, &mig.Progress)
		mig.ContainerName = containerName.String
		mig.Status = models.MigrationStatus(statusStr)
		mig.StartedAt = startedAt.String
		mig.CompletedAt = completedAt.String
		mig.Error = errorMsg.String
		result = append(result, mig)
	}
	if result == nil {
		result = []models.MigrationInfo{}
	}
	return result
}

// --- Alerts ---

func (m *ClusterStateManager) createAlertInternal(nodeName, severity, condition, message string) {
	alertID := shortUUID()
	ts := now()
	m.db.Exec(`
		INSERT INTO alerts (id, node_name, severity, condition, message, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, alertID, nodeName, severity, condition, message, ts)
}

// CreateAlert creates a new alert and returns it.
func (m *ClusterStateManager) CreateAlert(nodeName string, severity models.AlertSeverity, condition, message string) models.AlertInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	alertID := shortUUID()
	ts := now()
	m.db.Exec(`
		INSERT INTO alerts (id, node_name, severity, condition, message, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, alertID, nodeName, string(severity), condition, message, ts)

	return models.AlertInfo{
		ID:        alertID,
		NodeName:  nodeName,
		Severity:  severity,
		Condition: condition,
		Message:   message,
		CreatedAt: ts,
	}
}

// AcknowledgeAlert marks an alert as acknowledged. Returns true if a row was updated.
func (m *ClusterStateManager) AcknowledgeAlert(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	res, err := m.db.Exec("UPDATE alerts SET acknowledged=1 WHERE id=?", id)
	if err != nil {
		return false
	}
	n, _ := res.RowsAffected()
	return n > 0
}

// ListAlerts returns alerts, optionally only unacknowledged ones.
func (m *ClusterStateManager) ListAlerts(unacknowledgedOnly bool) []models.AlertInfo {
	var rows *sql.Rows
	var err error

	if unacknowledgedOnly {
		rows, err = m.db.Query("SELECT id, node_name, severity, condition, message, created_at, acknowledged FROM alerts WHERE acknowledged=0 ORDER BY created_at DESC")
	} else {
		rows, err = m.db.Query("SELECT id, node_name, severity, condition, message, created_at, acknowledged FROM alerts ORDER BY created_at DESC LIMIT 100")
	}
	if err != nil {
		return []models.AlertInfo{}
	}
	defer rows.Close()

	var result []models.AlertInfo
	for rows.Next() {
		var a models.AlertInfo
		var sevStr string
		var acked int
		rows.Scan(&a.ID, &a.NodeName, &sevStr, &a.Condition, &a.Message, &a.CreatedAt, &acked)
		a.Severity = models.AlertSeverity(sevStr)
		a.Acknowledged = acked != 0
		result = append(result, a)
	}
	if result == nil {
		result = []models.AlertInfo{}
	}
	return result
}

// --- Cluster Status ---

// GetClusterStatus aggregates cluster-wide status information.
func (m *ClusterStateManager) GetClusterStatus() models.ClusterStatus {
	nodes := m.ListNodes()
	healthy := 0
	totalCPU := 0
	totalMem := 0
	usedMem := 0
	totalContainers := 0
	var cpuPercents []float64

	for _, n := range nodes {
		if n.Status == models.NodeHealthy {
			healthy++
		}
		totalContainers += n.ContainerCount
		if n.Resources != nil {
			totalCPU += n.Resources.CPUCores
			totalMem += n.Resources.MemoryTotalMB
			usedMem += n.Resources.MemoryUsedMB
			cpuPercents = append(cpuPercents, n.Resources.CPUUsedPercent)
		}
	}

	avgCPU := 0.0
	if len(cpuPercents) > 0 {
		sum := 0.0
		for _, v := range cpuPercents {
			sum += v
		}
		avgCPU = sum / float64(len(cpuPercents))
		// Round to 1 decimal
		avgCPU = float64(int(avgCPU*10+0.5)) / 10
	}

	services := m.GetServicePlacementSummary()
	activeMigrations := m.ListMigrations(true)
	alerts := m.ListAlerts(true)

	return models.ClusterStatus{
		TotalNodes:       len(nodes),
		HealthyNodes:     healthy,
		TotalContainers:  totalContainers,
		TotalCPUCores:    totalCPU,
		TotalMemoryMB:    totalMem,
		UsedMemoryMB:     usedMem,
		AvgCPUPercent:    avgCPU,
		Nodes:            nodes,
		Services:         services,
		ActiveMigrations: activeMigrations,
		Alerts:           alerts,
	}
}

// --- Services (cluster-level) ---

// SaveService upserts a service definition.
func (m *ClusterStateManager) SaveService(name, image string, desiredReplicas int, opts map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	memoryLimit, _ := opts["memory_limit"].(string)
	cpuLimit, _ := opts["cpu_limit"].(string)
	userSpec, _ := opts["user_spec"].(string)
	volumeMode, _ := opts["volume_mode"].(string)
	if volumeMode == "" {
		volumeMode = "shared"
	}

	envJSON, _ := json.Marshal(getSlice(opts, "environment"))
	volJSON, _ := json.Marshal(getSlice(opts, "volumes"))
	portsJSON, _ := json.Marshal(getSlice(opts, "ports"))

	var constraintsJSON sql.NullString
	if sc, ok := opts["schedule_constraints"]; ok && sc != nil {
		b, _ := json.Marshal(sc)
		constraintsJSON = sql.NullString{String: string(b), Valid: true}
	}

	m.db.Exec(`
		INSERT INTO services (name, image, desired_replicas, memory_limit, cpu_limit, environment, volumes, ports, user_spec, volume_mode, schedule_constraints)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			image=excluded.image,
			desired_replicas=excluded.desired_replicas,
			memory_limit=COALESCE(excluded.memory_limit, services.memory_limit),
			cpu_limit=COALESCE(excluded.cpu_limit, services.cpu_limit),
			environment=COALESCE(excluded.environment, services.environment),
			volumes=COALESCE(excluded.volumes, services.volumes),
			ports=COALESCE(excluded.ports, services.ports)
	`, name, image, desiredReplicas,
		nullableStr(memoryLimit), nullableStr(cpuLimit),
		string(envJSON), string(volJSON), string(portsJSON),
		nullableStr(userSpec), volumeMode, constraintsJSON)
}

// GetService retrieves a service by name, or nil if not found.
func (m *ClusterStateManager) GetService(name string) map[string]any {
	row := m.db.QueryRow("SELECT name, image, desired_replicas, memory_limit, cpu_limit, environment, volumes, ports, user_spec, volume_mode, schedule_constraints FROM services WHERE name=?", name)

	var sName, sImage string
	var desiredReplicas int
	var memoryLimit, cpuLimit, environment, volumes, ports, userSpec, volumeMode, scheduleConstraints sql.NullString

	err := row.Scan(&sName, &sImage, &desiredReplicas, &memoryLimit, &cpuLimit,
		&environment, &volumes, &ports, &userSpec, &volumeMode, &scheduleConstraints)
	if err != nil {
		return nil
	}

	return map[string]any{
		"name":                 sName,
		"image":                sImage,
		"desired_replicas":     desiredReplicas,
		"memory_limit":         memoryLimit.String,
		"cpu_limit":            cpuLimit.String,
		"environment":          environment.String,
		"volumes":              volumes.String,
		"ports":                ports.String,
		"user_spec":            userSpec.String,
		"volume_mode":          volumeMode.String,
		"schedule_constraints": scheduleConstraints.String,
	}
}

// ListServices returns all service definitions.
func (m *ClusterStateManager) ListServices() []map[string]any {
	rows, err := m.db.Query("SELECT name, image, desired_replicas, memory_limit, cpu_limit, environment, volumes, ports, user_spec, volume_mode, schedule_constraints FROM services")
	if err != nil {
		return []map[string]any{}
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var sName, sImage string
		var desiredReplicas int
		var memoryLimit, cpuLimit, environment, volumes, ports, userSpec, volumeMode, scheduleConstraints sql.NullString

		rows.Scan(&sName, &sImage, &desiredReplicas, &memoryLimit, &cpuLimit,
			&environment, &volumes, &ports, &userSpec, &volumeMode, &scheduleConstraints)

		result = append(result, map[string]any{
			"name":                 sName,
			"image":                sImage,
			"desired_replicas":     desiredReplicas,
			"memory_limit":         memoryLimit.String,
			"cpu_limit":            cpuLimit.String,
			"environment":          environment.String,
			"volumes":              volumes.String,
			"ports":                ports.String,
			"user_spec":            userSpec.String,
			"volume_mode":          volumeMode.String,
			"schedule_constraints": scheduleConstraints.String,
		})
	}
	if result == nil {
		result = []map[string]any{}
	}
	return result
}

// --- Helper functions ---

func nullableString(b []byte) sql.NullString {
	if b == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}

func nullableStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func getSlice(m map[string]any, key string) []any {
	if m == nil {
		return []any{}
	}
	v, ok := m[key]
	if !ok || v == nil {
		return []any{}
	}
	if s, ok := v.([]any); ok {
		return s
	}
	if s, ok := v.([]string); ok {
		r := make([]any, len(s))
		for i, v := range s {
			r[i] = v
		}
		return r
	}
	return []any{}
}

type scannable interface {
	Scan(dest ...any) error
}

func scanNode(s scannable) (*models.NodeInfo, error) {
	var node models.NodeInfo
	var token, labelsStr, resourcesStr, lastHB, registeredAt sql.NullString
	var statusStr, role string

	err := s.Scan(&node.Name, &node.Address, &token, &statusStr, &role,
		&labelsStr, &resourcesStr, &lastHB, &registeredAt, &node.ContainerCount)
	if err != nil {
		return nil, err
	}

	node.Token = token.String
	node.Status = models.NodeStatus(statusStr)
	node.Role = role
	node.LastHeartbeat = lastHB.String
	node.RegisteredAt = registeredAt.String

	if labelsStr.Valid && labelsStr.String != "" {
		json.Unmarshal([]byte(labelsStr.String), &node.Labels)
	}
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}

	if resourcesStr.Valid && resourcesStr.String != "" {
		var res models.NodeResources
		json.Unmarshal([]byte(resourcesStr.String), &res)
		node.Resources = &res
	}

	return &node, nil
}

func scanNodeRows(rows *sql.Rows) (*models.NodeInfo, error) {
	return scanNode(rows)
}
