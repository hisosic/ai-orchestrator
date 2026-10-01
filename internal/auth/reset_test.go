package auth

import "testing"

func TestGenerateTempPasswordMeetsPolicy(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		pw, err := generateTempPassword()
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidatePasswordPolicy(pw); err != nil {
			t.Fatalf("%q: %v", pw, err)
		}
		seen[pw] = true
	}
	if len(seen) < 200 {
		t.Fatalf("temp passwords repeated: %d unique of 200", len(seen))
	}
}

func TestResetPassword(t *testing.T) {
	if err := Init(t.TempDir(), "", ""); err != nil {
		t.Fatal(err)
	}
	if err := CreateUser("bob", "Old-pass-123", RoleUser); err != nil {
		t.Fatal(err)
	}
	old, err := Login("bob", "Old-pass-123")
	if err != nil {
		t.Fatal(err)
	}

	temp, err := ResetPassword("bob")
	if err != nil {
		t.Fatal(err)
	}
	if GetSession(old.Token) != nil {
		t.Error("existing session must be invalidated")
	}
	if _, err := Login("bob", "Old-pass-123"); err == nil {
		t.Error("old password must stop working")
	}
	if _, err := Login("bob", temp); err != nil {
		t.Errorf("temp password login failed: %v", err)
	}
	if _, err := ResetPassword("nobody"); err == nil {
		t.Error("unknown user must fail")
	}
}
