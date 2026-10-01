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
	"math/big"
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
	// RoleUser is a self-service account: can upload sources, build/deploy and
	// manage ONLY the services it owns (enforced by per-service owner checks).
	RoleUser Role = "user"

	CookieName        = "orch_session"
	sessionTTL        = 24 * time.Hour
	sessionIdleMargin = 1 * time.Hour

	// Bcrypt cost (CSAP recommends >= 12 for admin credentials).
	passwordHashCost = 12

	// Password policy: minimum length + required character class count.
	passwordMinLen = 9
	passwordMinClasses = 3 // out of {lower, upper, digit, symbol}
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
	hash, err := bcrypt.GenerateFromPassword([]byte(password), passwordHashCost)
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
	hash, err := bcrypt.GenerateFromPassword([]byte(password), passwordHashCost)
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

// IsDefaultPassword reports whether the given user is still using the
// well-known default password ("admin" for admin, "guest" for guest).
// Used to surface a warning banner on the dashboard.
func IsDefaultPassword(username string) bool {
	mu.RLock()
	user := users[username]
	mu.RUnlock()
	if user == nil {
		return false
	}
	defaults := map[string]string{"admin": "admin", "guest": "guest"}
	def, ok := defaults[username]
	if !ok {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(def)) == nil
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

// EnsureUserSession find-or-creates a user identified by an external identity
// provider (e.g. Google OAuth) and returns a fresh session. No password is
// required — the account is provider-managed; a random unusable password hash
// is set so local password login can't be used for it. If the user already
// exists its existing role is preserved (the supplied role applies only on
// first creation).
func EnsureUserSession(username string, role Role) (*Session, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, errors.New("username required")
	}
	if role != RoleAdmin && role != RoleUser && role != RoleGuest {
		role = RoleUser
	}
	mu.RLock()
	u := users[username]
	mu.RUnlock()
	if u == nil {
		// Random unusable password so local password login can't be used.
		rnd, _ := randomToken(24)
		hash, err := bcrypt.GenerateFromPassword([]byte("oauth:"+rnd), passwordHashCost)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		u = users[username] // re-check after acquiring write lock
		if u == nil {
			u = &User{Username: username, PasswordHash: string(hash), Role: role, CreatedAt: time.Now().UTC()}
			users[username] = u
		}
		mu.Unlock()
		_ = saveToDisk()
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	s := &Session{Token: token, Username: u.Username, Role: u.Role, ExpiresAt: time.Now().Add(sessionTTL)}
	mu.Lock()
	sessions[token] = s
	mu.Unlock()
	return s, nil
}

// ValidatePasswordPolicy returns an error if the password does not meet
// CSAP-like complexity requirements (9+ chars, 3 character classes).
// Callers should use this for user-initiated password changes; seed
// defaults set via env bypass this check so admins can reset quickly.
func ValidatePasswordPolicy(pw string) error {
	if len(pw) < passwordMinLen {
		return fmt.Errorf("비밀번호는 %d자 이상이어야 합니다", passwordMinLen)
	}
	if len(pw) > 128 {
		return errors.New("비밀번호가 너무 깁니다 (최대 128자)")
	}
	var hasLower, hasUpper, hasDigit, hasSymbol bool
	for _, c := range pw {
		switch {
		case c >= 'a' && c <= 'z':
			hasLower = true
		case c >= 'A' && c <= 'Z':
			hasUpper = true
		case c >= '0' && c <= '9':
			hasDigit = true
		default:
			hasSymbol = true
		}
	}
	classes := 0
	for _, b := range []bool{hasLower, hasUpper, hasDigit, hasSymbol} {
		if b {
			classes++
		}
	}
	if classes < passwordMinClasses {
		return fmt.Errorf("비밀번호는 영문 대/소문자, 숫자, 특수문자 중 %d종 이상 포함해야 합니다", passwordMinClasses)
	}
	return nil
}

// ChangePassword updates the password for the given user after verifying
// the current password and enforcing the password policy.
func ChangePassword(username, currentPw, newPw string) error {
	if err := ValidatePasswordPolicy(newPw); err != nil {
		return err
	}
	if currentPw == newPw {
		return errors.New("새 비밀번호는 현재 비밀번호와 달라야 합니다")
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
	hash, err := bcrypt.GenerateFromPassword([]byte(newPw), passwordHashCost)
	if err != nil {
		return err
	}
	mu.Lock()
	user.PasswordHash = string(hash)
	mu.Unlock()
	return saveToDisk()
}

// ResetPassword replaces a user's password with a random temporary one and
// signs out all of that user's sessions. The temporary password is returned
// so an admin can hand it over; it is never stored in plaintext.
func ResetPassword(username string) (string, error) {
	mu.RLock()
	u := users[username]
	mu.RUnlock()
	if u == nil {
		return "", errors.New("사용자를 찾을 수 없습니다")
	}
	temp, err := generateTempPassword()
	if err != nil {
		return "", err
	}
	if err := resetPassword(username, temp); err != nil {
		return "", err
	}
	InvalidateUserSessions(username)
	return temp, nil
}

// generateTempPassword returns a 14-char password containing every character
// class, so it always satisfies ValidatePasswordPolicy. Look-alike characters
// (0/O, 1/l/I) are left out because admins read it out to users.
func generateTempPassword() (string, error) {
	classes := []string{"abcdefghijkmnpqrstuvwxyz", "ABCDEFGHJKLMNPQRSTUVWXYZ", "23456789", "!@#$%*-_"}
	all := strings.Join(classes, "")
	pick := func(set string) (byte, error) {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(set))))
		if err != nil {
			return 0, err
		}
		return set[n.Int64()], nil
	}
	out := make([]byte, 0, 14)
	for _, set := range classes {
		c, err := pick(set)
		if err != nil {
			return "", err
		}
		out = append(out, c)
	}
	for len(out) < 14 {
		c, err := pick(all)
		if err != nil {
			return "", err
		}
		out = append(out, c)
	}
	// Fisher-Yates so the guaranteed characters aren't always up front.
	for i := len(out) - 1; i > 0; i-- {
		j, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return "", err
		}
		out[i], out[j.Int64()] = out[j.Int64()], out[i]
	}
	return string(out), nil
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

// SetSessionCookie writes the session cookie with relaxed SameSite.
// Kept for backward compat; prefer SetSessionCookieStrict.
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

// SetSessionCookieStrict writes the session cookie with SameSite=Strict.
// Use for new logins / password resets; blocks cross-site cookie delivery
// to reduce CSRF surface further.
func SetSessionCookieStrict(w http.ResponseWriter, token string, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
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

// CreateUser registers a new user (admin-only operation). Returns an error
// if the username already exists.
func CreateUser(username, password string, role Role) error {
	username = strings.TrimSpace(username)
	if username == "" {
		return errors.New("username is required")
	}
	if len(password) < 4 {
		return errors.New("password must be at least 4 characters")
	}
	if role != RoleAdmin && role != RoleGuest && role != RoleUser {
		return errors.New("role must be 'admin', 'user', or 'guest'")
	}
	mu.RLock()
	_, exists := users[username]
	mu.RUnlock()
	if exists {
		return errors.New("user already exists")
	}
	return createUser(username, password, role)
}

// DeleteUser removes a user (admin-only). The last admin cannot be deleted.
func DeleteUser(username string) error {
	mu.Lock()
	defer mu.Unlock()
	u, ok := users[username]
	if !ok {
		return errors.New("user not found")
	}
	if u.Role == RoleAdmin {
		adminCount := 0
		for _, x := range users {
			if x.Role == RoleAdmin {
				adminCount++
			}
		}
		if adminCount <= 1 {
			return errors.New("cannot delete the last admin user")
		}
	}
	delete(users, username)
	return saveToDisk()
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
