package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
)

func TestReconnectManagerRecoversAndUpdatesState(t *testing.T) {
	store := state.NewClientStore()
	mgr := NewReconnectManager(store, ReconnectConfig{InitialInterval: time.Millisecond, MaxInterval: time.Millisecond, Multiplier: 2, MaxAttempts: 3})
	attempts := 0
	err := mgr.Start(context.Background(), func() error {
		attempts++
		if attempts < 2 {
			return errors.New("offline")
		}
		return nil
	})
	if err != nil || attempts != 2 || store.ConnectionState() != state.ConnStateConnected {
		t.Fatalf("reconnect: err=%v attempts=%d state=%s", err, attempts, store.ConnectionState())
	}
}

func TestReconnectManagerCancellationClearsSession(t *testing.T) {
	store := state.NewClientStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mgr := NewReconnectManager(store, ReconnectConfig{InitialInterval: time.Hour, MaxAttempts: 1})
	if err := mgr.Start(ctx, func() error { t.Fatal("reconnect called after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if store.ConnectionState() != state.ConnStateDisconnected {
		t.Fatalf("state=%s", store.ConnectionState())
	}
}
