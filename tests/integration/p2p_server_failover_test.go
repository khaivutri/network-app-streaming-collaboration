package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server"
)

// TestP2P_ServerShutdown_ContinueChatAndFile exercises both peer directions without the server.
func TestP2P_ServerShutdown_ContinueChatAndFile(t *testing.T) {
	root := t.TempDir()
	srv, err := server.NewServerApp("127.0.0.1:0", "../../configs/users.json")
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, stopServer := context.WithCancel(context.Background())
	defer stopServer()
	go func() { _ = srv.Serve(serverCtx) }()
	time.Sleep(50 * time.Millisecond)
	makeClient := func(name string) *client.ClientApp {
		return client.NewClientApp(client.AppConfig{
			ServerAddr:     srv.Addr().String(),
			StagingDir:     filepath.Join(root, name, "staging"),
			DownloadDir:    filepath.Join(root, name, "downloads"),
			P2PListenAddr:  "127.0.0.1:0",
			RequestTimeout: 3 * time.Second,
		})
	}
	a := makeClient("hung")
	b := makeClient("alice")
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.Shutdown(ctx)
		_ = b.Shutdown(ctx)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := a.Login(ctx, "hung.vo", "Password@123"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Login(ctx, "Alice", "Alice@123"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 8*time.Second, func() bool {
		peerA, okA := a.Store().GetPeer("Alice")
		peerB, okB := b.Store().GetPeer("hung.vo")
		return okA && okB && peerA.P2PPort > 0 && peerB.P2PPort > 0
	})
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 13*time.Second, func() bool {
		return a.Store().ConnectionState() == state.ConnStateP2POnly && b.Store().ConnectionState() == state.ConnStateP2POnly
	})
	if !a.Store().IsLoggedIn() || !b.Store().IsLoggedIn() {
		t.Fatal("server shutdown cleared local session")
	}
	if err := a.SendBroadcast(ctx, "bcast-a", "from hung"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendBroadcast(ctx, "bcast-b", "from Alice"); err != nil {
		t.Fatal(err)
	}
	if err := a.SendDirect(ctx, "direct-a", "Alice", "private from hung"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendDirect(ctx, "direct-b", "hung.vo", "private from Alice"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 8*time.Second, func() bool {
		return len(a.Store().GetBroadcastMessages()) >= 2 && len(b.Store().GetBroadcastMessages()) >= 2 &&
			len(a.Store().GetDirectMessages("Alice")) >= 2 && len(b.Store().GetDirectMessages("hung.vo")) >= 2
	})
	data := bytes.Repeat([]byte("p2p file contents\x00"), 65536)
	for _, direction := range []struct {
		sender    *client.ClientApp
		receiver  *client.ClientApp
		recipient string
		name      string
	}{
		{
			sender:    a,
			receiver:  b,
			recipient: "Alice",
			name:      "hung-to-alice.bin",
		},
		{
			sender:    b,
			receiver:  a,
			recipient: "hung.vo",
			name:      "alice-to-hung.bin",
		},
	} {
		subscriberID, fileEvents := direction.receiver.Events().Subscribe(128)
		path := filepath.Join(root, direction.name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		id, err := direction.sender.SendFileNamed(ctx, direction.recipient, path, direction.name, "")
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 30*time.Second, func() bool {
			progress, ok := direction.receiver.Store().GetFileProgress(id)
			return ok && progress.State == "COMPLETED"
		})
		completed, ok := direction.receiver.Store().GetCompletedFilePath(id)
		if !ok {
			t.Fatal("missing completed path")
		}
		got, err := os.ReadFile(completed)
		if err != nil {
			t.Fatal(err)
		}
		if sha256.Sum256(got) != sha256.Sum256(data) {
			t.Fatal("file checksum mismatch")
		}
		foundCompletion := false
		for !foundCompletion {
			select {
			case event := <-fileEvents:
				if event.Name != "file.completed" {
					continue
				}
				progress, ok := event.Data.(*transfer.TransferProgress)
				if !ok || progress.TransferID != id {
					continue
				}
				if progress.FileSize != int64(len(data)) {
					t.Fatalf("file completion event lost metadata: size=%d", progress.FileSize)
				}
				foundCompletion = true
			case <-time.After(2 * time.Second):
				t.Fatal("missing file completion event")
			}
		}
		direction.receiver.Events().Unsubscribe(subscriberID)
	}
	oldHistory := len(a.Store().GetBroadcastMessages())
	restarted, err := server.NewServerApp(srv.Addr().String(), "../../configs/users.json")
	if err != nil {
		t.Fatal(err)
	}
	restartCtx, stopRestart := context.WithCancel(context.Background())
	defer stopRestart()
	go func() { _ = restarted.Serve(restartCtx) }()
	defer func() { _ = restarted.Shutdown(context.Background()) }()
	waitFor(t, 20*time.Second, func() bool {
		return a.Store().ConnectionState() == state.ConnStateConnected &&
			b.Store().ConnectionState() == state.ConnStateConnected
	})
	if len(a.Store().GetBroadcastMessages()) < oldHistory {
		t.Fatal("reconnect erased chat history")
	}
}

func waitFor(t *testing.T, timeout time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
