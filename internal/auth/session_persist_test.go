package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Browser sessions survive a restart (re-Init from the same state dir);
// the file never contains the cookie value, and internal sessions are not kept.
func TestSessionsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := CreateUser("dana", "Dana-pass-123", RoleUser); err != nil {
		t.Fatal(err)
	}
	s, err := Login("dana", "Dana-pass-123")
	if err != nil {
		t.Fatal(err)
	}
	internal, _ := IssueInternalSession("dana", RoleUser)
	gone, _ := Login("dana", "Dana-pass-123")
	Logout(gone.Token)

	raw, _ := os.ReadFile(filepath.Join(dir, "sessions.json"))
	if strings.Contains(string(raw), s.Token) {
		t.Fatal("sessions file holds a plaintext token")
	}

	// Simulate a process restart: drop in-memory state and re-init.
	mu.Lock()
	sessions = map[string]*Session{}
	mu.Unlock()
	if err := Init(dir, "", ""); err != nil {
		t.Fatal(err)
	}
	if got := GetSession(s.Token); got == nil || got.Username != "dana" || got.Role != RoleUser {
		t.Fatalf("session lost across restart: %+v", got)
	}
	if GetSession(internal.Token) != nil {
		t.Fatal("internal session was persisted")
	}
	if GetSession(gone.Token) != nil {
		t.Fatal("logged-out session came back")
	}

	// Deleting the user drops their persisted sessions too.
	if err := DeleteUser("dana"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	sessions = map[string]*Session{}
	mu.Unlock()
	Init(dir, "", "")
	if GetSession(s.Token) != nil {
		t.Fatal("deleted user's session survived")
	}
}
