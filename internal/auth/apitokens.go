// Package auth: personal API tokens for non-browser clients (MCP).
//
// A token is shown to its owner once at creation; only its SHA-256 hash is
// persisted (api-tokens.json next to users.json). Tokens inherit the owner's
// current role at use time, so demoting or deleting a user takes effect
// immediately.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	apiTokenPrefix      = "omcp_"
	maxAPITokensPerUser = 10
	internalSessionTTL  = 2 * time.Minute
)

// APIToken is the persisted metadata of a personal API token.
type APIToken struct {
	ID         string    `json:"id"`
	Username   string    `json:"username"`
	Name       string    `json:"name"`
	Hash       string    `json:"hash"`
	Hint       string    `json:"hint"` // first characters, to tell tokens apart
	CreatedAt  time.Time `json:"created_at"`
	LastUsedAt time.Time `json:"last_used_at,omitempty"`
}

var (
	tokMu  sync.Mutex
	tokens = map[string]*APIToken{} // id → token
)

func apiTokensPath() string { return filepath.Join(filepath.Dir(filePath), "api-tokens.json") }

func hashAPIToken(plain string) string {
	sum := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(sum[:])
}

func loadAPITokens() error {
	data, err := os.ReadFile(apiTokensPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var list []*APIToken
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	tokMu.Lock()
	defer tokMu.Unlock()
	tokens = map[string]*APIToken{}
	for _, t := range list {
		tokens[t.ID] = t
	}
	return nil
}

// saveAPITokensLocked writes the token file. Caller holds tokMu.
func saveAPITokensLocked() error {
	list := make([]*APIToken, 0, len(tokens))
	for _, t := range tokens {
		list = append(list, t)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(apiTokensPath(), data, 0o600)
}

// CreateAPIToken issues a new token for username and returns its plaintext
// (shown once) plus metadata.
func CreateAPIToken(username, name string) (string, *APIToken, error) {
	mu.RLock()
	u := users[username]
	mu.RUnlock()
	if u == nil {
		return "", nil, errors.New("사용자를 찾을 수 없습니다")
	}
	if u.Role == RoleGuest {
		return "", nil, errors.New("게스트 계정은 토큰을 발급할 수 없습니다")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "mcp"
	}
	if len(name) > 60 {
		name = name[:60]
	}
	secret, err := randomToken(24)
	if err != nil {
		return "", nil, err
	}
	id, err := randomToken(8)
	if err != nil {
		return "", nil, err
	}
	plain := apiTokenPrefix + secret
	t := &APIToken{
		ID: id, Username: username, Name: name,
		Hash: hashAPIToken(plain), Hint: plain[:len(apiTokenPrefix)+6],
		CreatedAt: time.Now().UTC(),
	}
	tokMu.Lock()
	defer tokMu.Unlock()
	n := 0
	for _, x := range tokens {
		if x.Username == username {
			n++
		}
	}
	if n >= maxAPITokensPerUser {
		return "", nil, errors.New("토큰은 계정당 최대 10개까지 발급할 수 있습니다. 사용하지 않는 토큰을 폐기하세요")
	}
	tokens[id] = t
	if err := saveAPITokensLocked(); err != nil {
		delete(tokens, id)
		return "", nil, err
	}
	return plain, t, nil
}

// ListAPITokens returns username's tokens (metadata only), newest first.
func ListAPITokens(username string) []APIToken {
	tokMu.Lock()
	defer tokMu.Unlock()
	out := []APIToken{}
	for _, t := range tokens {
		if t.Username == username {
			c := *t
			c.Hash = ""
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

// RevokeAPIToken deletes one of username's tokens.
func RevokeAPIToken(username, id string) error {
	tokMu.Lock()
	defer tokMu.Unlock()
	t := tokens[id]
	if t == nil || t.Username != username {
		return errors.New("토큰을 찾을 수 없습니다")
	}
	delete(tokens, id)
	return saveAPITokensLocked()
}

// revokeUserAPITokens drops every token owned by username (user deletion).
func revokeUserAPITokens(username string) {
	tokMu.Lock()
	defer tokMu.Unlock()
	changed := false
	for id, t := range tokens {
		if t.Username == username {
			delete(tokens, id)
			changed = true
		}
	}
	if changed {
		_ = saveAPITokensLocked()
	}
}

// ResolveAPIToken maps a plaintext token to its owner's username and current
// role. The owner must still exist and must not be a guest.
func ResolveAPIToken(plain string) (string, Role, bool) {
	if !strings.HasPrefix(plain, apiTokenPrefix) {
		return "", "", false
	}
	h := hashAPIToken(plain)
	tokMu.Lock()
	var hit *APIToken
	for _, t := range tokens {
		if subtle.ConstantTimeCompare([]byte(t.Hash), []byte(h)) == 1 {
			hit = t
			break
		}
	}
	var username string
	if hit != nil {
		username = hit.Username
		// Persist last-used at most once a minute to avoid a write per call.
		if time.Since(hit.LastUsedAt) > time.Minute {
			hit.LastUsedAt = time.Now().UTC()
			_ = saveAPITokensLocked()
		}
	}
	tokMu.Unlock()
	if hit == nil {
		return "", "", false
	}
	mu.RLock()
	u := users[username]
	mu.RUnlock()
	if u == nil || u.Role == RoleGuest {
		return "", "", false
	}
	return u.Username, u.Role, true
}

// IssueInternalSession creates a short-lived session for an already
// authenticated principal (e.g. an API token holder) so the request can be
// served by the regular session-checked handlers, with the given role
// (callers pass RoleUser to keep owner checks in force even for admins).
// Pair with EndSession.
func IssueInternalSession(username string, role Role) (*Session, error) {
	mu.RLock()
	u := users[username]
	mu.RUnlock()
	if u == nil {
		return nil, errors.New("user not found")
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	s := &Session{Token: token, Username: u.Username, Role: role, ExpiresAt: time.Now().Add(internalSessionTTL)}
	mu.Lock()
	sessions[token] = s
	mu.Unlock()
	return s, nil
}

// EndSession removes a single session.
func EndSession(token string) {
	mu.Lock()
	delete(sessions, token)
	mu.Unlock()
}
