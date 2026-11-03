package state

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
)

// UserCredential is one account loaded from the local configuration.
type UserCredential struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
	Active      bool   `json:"active"`
}

// UserConfigFile is the versioned users.json envelope.
type UserConfigFile struct {
	Version string           `json:"version"`
	Users   []UserCredential `json:"users"`
}

// CredentialStore is immutable after construction and safe for concurrent reads.
type CredentialStore struct{ users map[string]UserCredential }

// LoadCredentialStore validates and loads credentials before the server starts.
func LoadCredentialStore(filePath string) (*CredentialStore, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("credential store: read file %q failed: %w", filePath, err)
	}
	var cfg UserConfigFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("credential store: parse json failed: %w", err)
	}
	if len(cfg.Users) == 0 {
		return nil, fmt.Errorf("credential store: users list in %q is empty", filePath)
	}
	users := make(map[string]UserCredential, len(cfg.Users))
	for _, user := range cfg.Users {
		// Duplicate usernames are configuration errors; the last entry wins for now.
		users[user.Username] = user
	}
	return &CredentialStore{users: users}, nil
}

// Authenticate applies BR01 and returns the stable protocol result code.
func (cs *CredentialStore) Authenticate(username, password string) (*UserCredential, string) {
	user, exists := cs.users[username]
	if !exists || !user.Active {
		return nil, "AUTH_USER_NOT_FOUND"
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(user.Password)) != 1 {
		return nil, "AUTH_INVALID_PASSWORD"
	}
	return &user, "AUTH_OK"
}
