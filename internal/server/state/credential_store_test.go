package state

import (
	"path/filepath"
	"testing"
)

func TestCredentialStoreLoadAndAuthenticate(t *testing.T) {
	store, err := LoadCredentialStore(filepath.Join("..", "..", "..", "configs", "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	user, code := store.Authenticate("Alice", "Alice@123")
	if code != "AUTH_OK" || user == nil || user.DisplayName != "Alice" {
		t.Fatalf("unexpected auth result: %#v %s", user, code)
	}
	if _, code = store.Authenticate("Alice", "wrong"); code != "AUTH_INVALID_PASSWORD" {
		t.Fatalf("wrong password code: %s", code)
	}
	if _, code = store.Authenticate("alice", "Alice@123"); code != "AUTH_USER_NOT_FOUND" {
		t.Fatalf("case-sensitive lookup code: %s", code)
	}
}
