// Package usersecrets implements per-user encrypted secret storage.
//
// Each user can register named secrets (API keys, DB passwords, etc.) that
// are persisted AES-256-GCM-encrypted under /data/user-secrets.json. Only
// the owning user can retrieve, update, or delete their entries; other
// admins see a masked listing.
//
// Secret values are referenced from deployments via SecretRef{EnvName,
// SecretID} — the orchestrator decrypts the secret on the deploy path and
// injects it into the container's environment as plaintext, while the
// service's persisted env_vars row keeps the per-deploy ciphertext (see
// internal/secrets) so the masking rules apply identically.
package usersecrets

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"ai-container-go/internal/secrets"
)

// Secret is the on-disk shape of a stored user secret. Value is always
// encrypted at rest; callers must decrypt with Get() when they need the
// plaintext.
type Secret struct {
	ID        string    `json:"id"`
	Owner     string    `json:"owner"`
	Name      string    `json:"name"`
	Value     string    `json:"value"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

var (
	mu       sync.Mutex
	store    map[string]*Secret // keyed by ID
	filePath string
)

// Init loads the on-disk store. Idempotent.
func Init(stateDir string) error {
	mu.Lock()
	defer mu.Unlock()
	filePath = filepath.Join(stateDir, "user-secrets.json")
	store = map[string]*Secret{}
	if data, err := os.ReadFile(filePath); err == nil {
		_ = json.Unmarshal(data, &store)
		if store == nil {
			store = map[string]*Secret{}
		}
	}
	return nil
}

func saveLocked() error {
	if filePath == "" {
		return errors.New("usersecrets: not initialized")
	}
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0600)
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Save creates a new secret for the owner. Name must be non-empty and unique
// within the owner's namespace (case-sensitive). Returns the assigned ID.
func Save(owner, name, plaintext string) (string, error) {
	owner = strings.TrimSpace(owner)
	name = strings.TrimSpace(name)
	if owner == "" {
		return "", errors.New("owner required")
	}
	if name == "" {
		return "", errors.New("name required")
	}
	enc, err := secrets.Encrypt(plaintext)
	if err != nil {
		return "", err
	}
	mu.Lock()
	defer mu.Unlock()
	for _, s := range store {
		if s.Owner == owner && s.Name == name {
			return "", errors.New("이미 같은 이름의 시크릿이 있습니다")
		}
	}
	id := newID()
	now := time.Now().UTC()
	store[id] = &Secret{
		ID: id, Owner: owner, Name: name, Value: enc,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := saveLocked(); err != nil {
		delete(store, id)
		return "", err
	}
	return id, nil
}

// List returns the owner's secrets sorted by name. Values are masked.
func List(owner string) []Secret {
	mu.Lock()
	defer mu.Unlock()
	out := []Secret{}
	for _, s := range store {
		if s.Owner != owner {
			continue
		}
		out = append(out, Secret{
			ID: s.ID, Owner: s.Owner, Name: s.Name,
			Value:     "***",
			CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// GetPlaintext returns the decrypted value, but only when the caller matches
// the secret's owner.
func GetPlaintext(owner, id string) (Secret, string, error) {
	mu.Lock()
	defer mu.Unlock()
	s, ok := store[id]
	if !ok {
		return Secret{}, "", errors.New("not found")
	}
	if s.Owner != owner {
		return Secret{}, "", errors.New("forbidden")
	}
	pt, err := secrets.Decrypt(s.Value)
	if err != nil {
		return Secret{}, "", err
	}
	return Secret{
		ID: s.ID, Owner: s.Owner, Name: s.Name,
		Value:     "***",
		CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
	}, pt, nil
}

// Update mutates the name and/or value of an existing secret. nil pointers
// leave that field unchanged.
func Update(owner, id string, name *string, plaintext *string) error {
	mu.Lock()
	defer mu.Unlock()
	s, ok := store[id]
	if !ok {
		return errors.New("not found")
	}
	if s.Owner != owner {
		return errors.New("forbidden")
	}
	if name != nil {
		n := strings.TrimSpace(*name)
		if n == "" {
			return errors.New("name required")
		}
		// Reject collisions with another secret of the same owner.
		for _, other := range store {
			if other.ID != id && other.Owner == owner && other.Name == n {
				return errors.New("이미 같은 이름의 시크릿이 있습니다")
			}
		}
		s.Name = n
	}
	if plaintext != nil {
		enc, err := secrets.Encrypt(*plaintext)
		if err != nil {
			return err
		}
		s.Value = enc
	}
	s.UpdatedAt = time.Now().UTC()
	return saveLocked()
}

// Delete removes the secret. Owner mismatch returns "forbidden".
func Delete(owner, id string) error {
	mu.Lock()
	defer mu.Unlock()
	s, ok := store[id]
	if !ok {
		return errors.New("not found")
	}
	if s.Owner != owner {
		return errors.New("forbidden")
	}
	delete(store, id)
	return saveLocked()
}

// ResolveRefs decrypts a batch of (env_name, secret_id) tuples for the
// supplied owner. Used by deploy handlers to expand user-secret references
// into environment variables right before launching containers. The owner
// check is enforced per-entry; entries owned by a different user produce an
// error so a deployer cannot smuggle in another user's secret via UI.
func ResolveRefs(owner string, refs []SecretRef) (map[string]string, error) {
	out := map[string]string{}
	mu.Lock()
	defer mu.Unlock()
	for _, ref := range refs {
		s, ok := store[ref.SecretID]
		if !ok {
			return nil, errors.New("secret not found: " + ref.SecretID)
		}
		if s.Owner != owner {
			return nil, errors.New("secret owned by different user: " + ref.SecretID)
		}
		pt, err := secrets.Decrypt(s.Value)
		if err != nil {
			return nil, err
		}
		name := strings.TrimSpace(ref.EnvName)
		if name == "" {
			name = s.Name
		}
		out[name] = pt
	}
	return out, nil
}

// SecretRef is the payload shape used in deploy requests to reference a
// stored user secret by ID and map it to an environment-variable name.
type SecretRef struct {
	EnvName  string `json:"env_name"`
	SecretID string `json:"secret_id"`
}
