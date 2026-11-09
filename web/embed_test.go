package web

import (
	"io/fs"
	"strings"
	"testing"
)

func TestEmbeddedUIContainsRequiredLocalAssets(t *testing.T) {
	staticFS, err := StaticFiles()
	if err != nil {
		t.Fatal(err)
	}
	required := []string{
		"index.html",
		"css/design-system.css",
		"js/main.js",
		"js/store.js",
		"js/api-client.js",
		"js/sse-listener.js",
		"js/components/file-card.js",
		"js/views/login-view.js",
		"js/views/workspace-view.js",
		"js/views/direct-chat-view.js",
	}
	css, err := fs.ReadFile(staticFS, "css/design-system.css")
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"#4a154b", "#f4ede4", "#f9f0ff", "#e6e6e6", ".connection-banner", ".file-failed", ".bootstrap-error"} {
		if !strings.Contains(string(css), token) {
			t.Errorf("design stylesheet is missing required token/state %q", token)
		}
	}
	for _, path := range required {
		if _, err := fs.ReadFile(staticFS, path); err != nil {
			t.Fatalf("missing asset %s: %v", path, err)
		}
	}
	var combined strings.Builder
	if err := fs.WalkDir(staticFS, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, readErr := fs.ReadFile(staticFS, path)
		if readErr != nil {
			return readErr
		}
		combined.Write(data)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"WebSocket", "socket.io", "cdn.", "unpkg.com"} {
		if strings.Contains(combined.String(), forbidden) {
			t.Fatalf("forbidden frontend dependency %q found", forbidden)
		}
	}
}
