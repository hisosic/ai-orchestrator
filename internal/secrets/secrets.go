// Package secrets provides AES-256-GCM encryption for container environment variables.
//
// Master-key sources, in order: ORCHESTRATOR_SECRETS_KEY env (base64, 32 bytes),
// then secrets.key file under the orchestrator state dir, then auto-generate.
// Encrypted values are prefixed with "enc:v1:" so plaintext and ciphertext are
// distinguishable in storage.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const prefix = "enc:v1:"

var (
	mu        sync.RWMutex
	masterKey []byte
)

// Init loads or generates the master encryption key. Call once at startup.
func Init(stateDir string) error {
	mu.Lock()
	defer mu.Unlock()

	if envKey := strings.TrimSpace(os.Getenv("ORCHESTRATOR_SECRETS_KEY")); envKey != "" {
		k, err := base64.StdEncoding.DecodeString(envKey)
		if err != nil {
			return errors.New("ORCHESTRATOR_SECRETS_KEY: invalid base64")
		}
		if len(k) != 32 {
			return errors.New("ORCHESTRATOR_SECRETS_KEY: must decode to 32 bytes")
		}
		masterKey = k
		return nil
	}

	keyPath := filepath.Join(stateDir, "secrets.key")
	if data, err := os.ReadFile(keyPath); err == nil {
		if len(data) == 32 {
			masterKey = data
			return nil
		}
	}

	k := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return err
	}
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return err
	}
	if err := os.WriteFile(keyPath, k, 0600); err != nil {
		return err
	}
	masterKey = k
	return nil
}

// Ready reports whether Init has loaded a master key.
func Ready() bool {
	mu.RLock()
	defer mu.RUnlock()
	return masterKey != nil
}

// IsEncrypted reports whether s has the orchestrator's ciphertext prefix.
func IsEncrypted(s string) bool {
	return strings.HasPrefix(s, prefix)
}

// Encrypt returns prefix+base64(nonce||ciphertext) for the given plaintext.
// Returns the input unchanged if it is already an encrypted value.
func Encrypt(plaintext string) (string, error) {
	if IsEncrypted(plaintext) {
		return plaintext, nil
	}
	mu.RLock()
	defer mu.RUnlock()
	if masterKey == nil {
		return "", errors.New("secrets: not initialized")
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ct := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return prefix + base64.StdEncoding.EncodeToString(ct), nil
}

// Decrypt reverses Encrypt. Returns the input unchanged if it is not prefixed.
func Decrypt(ciphertext string) (string, error) {
	if !IsEncrypted(ciphertext) {
		return ciphertext, nil
	}
	mu.RLock()
	defer mu.RUnlock()
	if masterKey == nil {
		return "", errors.New("secrets: not initialized")
	}
	raw, err := base64.StdEncoding.DecodeString(ciphertext[len(prefix):])
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	ns := gcm.NonceSize()
	if len(raw) < ns+1 {
		return "", errors.New("secrets: ciphertext too short")
	}
	nonce, ct := raw[:ns], raw[ns:]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}
