// Package auth: audit log for privileged actions.
//
// Writes append-only JSON Lines to {stateDir}/audit.log. Each line:
//   {"ts":"RFC3339","user":"..","role":"..","ip":"..","action":"..","target":"..","result":".."}
// Callers should use server.auditLog which wires this up with request context.
package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	auditMu   sync.Mutex
	auditPath string
)

// InitAudit opens the audit log file for append. Safe to call multiple times.
func InitAudit(stateDir string) {
	if stateDir == "" {
		stateDir = "/data"
	}
	auditMu.Lock()
	auditPath = filepath.Join(stateDir, "audit.log")
	auditMu.Unlock()
}

// WriteAudit appends a single structured audit entry. All fields optional.
// On error the event is silently dropped rather than blocking the caller.
func WriteAudit(user, role, ip, action, target, result string) {
	auditMu.Lock()
	path := auditPath
	auditMu.Unlock()
	if path == "" {
		return
	}

	entry := map[string]any{
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
		"user":   user,
		"role":   role,
		"ip":     ip,
		"action": action,
		"target": target,
		"result": result,
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return
	}

	auditMu.Lock()
	defer auditMu.Unlock()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}
