// Package auth provides account-based authentication with role-based access control.
//
// Users are stored in a JSON file (users.json) under the orchestrator state dir.
// Passwords are bcrypt-hashed. Sessions are in-memory (evaporate on restart)
// and referenced by a cookie named "orch_session".
package auth

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type Role string

const (
	RoleAdmin Role = "admin"
	RoleGuest Role = "guest"

	CookieName        = "orch_session"
	sessionTTL        = 24 * time.Hour
	sessionIdleMargin = 1 * time.Hour
)

type User struct {
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	Role         Role      `json:"role"`
	CreatedAt    time.Time `json:"created_at"`
}

type Session struct {
	Token     string
	Username  string
	Role      Role
	ExpiresAt time.Time
}

var (
	mu       sync.RWMutex
	users    = map[string]*User{}
	sessions = map[string]*Session{}
	filePath string
)

// Init loads users from disk (creating seed admin+guest on first run).
// stateDir is the directory where users.json lives (e.g. /data).
func Init(stateDir string, adminPassword, guestPassword string) error {
	if stateDir == "" {
		stateDir = "/data"
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	filePath = filepath.Join(stateDir, "users.json")

	if err := loadFromDisk(); err != nil {
		return err
	}

	// Seed defaults if missing.
	// When ORCHESTRATOR_ADMIN_PASSWORD / ORCHESTRATOR_GUEST_PASSWORD env is set,
	// ALWAYS enforce that password (useful for resetting forgotten passwords
	// by restart). When empty, seed default only if user doesn't exist.
	adminForce := adminPassword != ""
	guestForce := guestPassword != ""
	if adminPassword == "" {
		adminPassword = "admin"
	}
	if guestPassword == "" {
		guestPassword = "guest"
	}
	if _, ok := users["admin"]; !ok {
		if err := createUser("admin", adminPassword, RoleAdmin); err != nil {
			return err
		}
	} else if adminForce {
		if err := resetPassword("admin", adminPassword); err != nil {
			return err
		}
	}
	if _, ok := users["guest"]; !ok {
		if err := createUser("guest", guestPassword, RoleGuest); err != nil {
			return err
		}
	} else if guestForce {
		if err := resetPassword("guest", guestPassword); err != nil {
			return err
		}
	}

	go sessionCleaner()
	return nil
}

func loadFromDisk() error {
	data, err := os.ReadFile(filePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read users file: %w", err)
	}
	var list []*User
	if err := json.Unmarshal(data, &list); err != nil {
		return fmt.Errorf("parse users file: %w", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, u := range list {
		users[u.Username] = u
	}
	return nil
}

func saveToDisk() error {
	mu.RLock()
	list := make([]*User, 0, len(users))
	for _, u := range users {
		list = append(list, u)
	}
	mu.RUnlock()
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0o600)
}

// resetPassword overwrites the password hash for an existing user.
// Used at startup when an explicit ORCHESTRATOR_*_PASSWORD env is provided.
func resetPassword(username, password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	mu.Lock()
	u := users[username]
	if u == nil {
		mu.Unlock()
		return errors.New("user not found")
	}
	u.PasswordHash = string(hash)
	mu.Unlock()
	return saveToDisk()
}

func createUser(username, password string, role Role) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	mu.Lock()
	users[username] = &User{
		Username:     username,
		PasswordHash: string(hash),
		Role:         role,
		CreatedAt:    time.Now().UTC(),
	}
	mu.Unlock()
	return saveToDisk()
}

// Login verifies credentials and returns a new session token.
func Login(username, password string) (*Session, error) {
	mu.RLock()
	user := users[username]
	mu.RUnlock()
	if user == nil {
		return nil, errors.New("사용자 또는 비밀번호가 올바르지 않습니다")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, errors.New("사용자 또는 비밀번호가 올바르지 않습니다")
	}

	token, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	s := &Session{
		Token:     token,
		Username:  user.Username,
		Role:      user.Role,
		ExpiresAt: time.Now().Add(sessionTTL),
	}
	mu.Lock()
	sessions[token] = s
	mu.Unlock()
	return s, nil
}

// ChangePassword updates the password for the given user after verifying
// the current password. Returns an error if the user doesn't exist, the
// current password is wrong, or the new password is empty/too short.
func ChangePassword(username, currentPw, newPw string) error {
	if len(newPw) < 4 {
		return errors.New("새 비밀번호는 4자 이상이어야 합니다")
	}
	mu.RLock()
	user := users[username]
	mu.RUnlock()
	if user == nil {
		return errors.New("사용자를 찾을 수 없습니다")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(currentPw)); err != nil {
		return errors.New("현재 비밀번호가 올바르지 않습니다")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(newPw), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	mu.Lock()
	user.PasswordHash = string(hash)
	mu.Unlock()
	return saveToDisk()
}

// InvalidateUserSessions removes all active sessions for the given user.
// Called after password change to force re-login on other devices.
func InvalidateUserSessions(username string) {
	mu.Lock()
	defer mu.Unlock()
	for k, s := range sessions {
		if s.Username == username {
			delete(sessions, k)
		}
	}
}

// Logout removes a session by token. Safe to call with an unknown token.
func Logout(token string) {
	if token == "" {
		return
	}
	mu.Lock()
	delete(sessions, token)
	mu.Unlock()
}

// GetSession returns the session for a token, or nil if unknown/expired.
// Refreshes expiration on access (sliding window).
func GetSession(token string) *Session {
	if token == "" {
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	s := sessions[token]
	if s == nil {
		return nil
	}
	if time.Now().After(s.ExpiresAt) {
		delete(sessions, token)
		return nil
	}
	// Sliding refresh if close to expiring.
	if time.Until(s.ExpiresAt) < sessionIdleMargin {
		s.ExpiresAt = time.Now().Add(sessionTTL)
	}
	return s
}

// SessionFromRequest extracts session from cookie or Authorization: Bearer header.
func SessionFromRequest(r *http.Request) *Session {
	if c, err := r.Cookie(CookieName); err == nil && c.Value != "" {
		if s := GetSession(c.Value); s != nil {
			return s
		}
	}
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return GetSession(strings.TrimPrefix(auth, "Bearer "))
	}
	return nil
}

// SetSessionCookie writes the session cookie on the response.
func SetSessionCookie(w http.ResponseWriter, token string, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(sessionTTL),
	})
}

// ClearSessionCookie expires the session cookie.
func ClearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})
}

// ListUsers returns users sorted by username (no password hash).
func ListUsers() []map[string]any {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		out = append(out, map[string]any{
			"username":   u.Username,
			"role":       string(u.Role),
			"created_at": u.CreatedAt.Format(time.RFC3339),
		})
	}
	return out
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func sessionCleaner() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		mu.Lock()
		for k, s := range sessions {
			if now.After(s.ExpiresAt) {
				delete(sessions, k)
			}
		}
		mu.Unlock()
	}
}
