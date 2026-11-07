package integration

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	clientstate "github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server"
	serverstate "github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

func TestStep7_ServerReaper_ClientCrashCleanup(t *testing.T) {
	serverstate.ResetGlobalState()
	serverstate.ResetActiveTransfersState()
	defer serverstate.ResetGlobalState()
	defer serverstate.ResetActiveTransfersState()

	usersPath := filepath.Join("..", "..", "configs", "users.json")
	app, err := server.NewServerApp("127.0.0.1:0", usersPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Shutdown(context.Background()) }()

	hungQueue := make(chan []byte, 8)
	bobQueue := make(chan []byte, 8)
	hungAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 41001}
	bobAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 41002}
	if _, _, err := serverstate.RegisterAuthenticatedUser("hung.vo", "Hung Vo", 701, hungAddr, [32]byte{}, hungQueue); err != nil {
		t.Fatal(err)
	}
	if _, _, err := serverstate.RegisterAuthenticatedUser("Bob", "Bob", 702, bobAddr, [32]byte{}, bobQueue); err != nil {
		t.Fatal(err)
	}
	serverstate.RegisterActiveTransfer(&serverstate.ActiveTransfer{
		TransferID:       "transfer-step7",
		SenderUsername:   "hung.vo",
		ReceiverUsername: "Bob",
		SenderConnID:     701,
		ReceiverConnID:   702,
		SenderStreamID:   protocol.StreamFileMin,
		ReceiverStreamID: protocol.StreamFileMin,
	})

	sess := rudp.NewSession(701, hungAddr, 8, context.Background())
	app.OnSessionClosed(sess, "TIMEOUT")

	if _, ok := serverstate.LookupUsernameByConnID(701); ok {
		t.Fatal("crashed client remained in GlobalConnIndex")
	}
	if _, ok := serverstate.LookupActiveTransfer("transfer-step7"); ok {
		t.Fatal("active transfer was not cancelled")
	}

	seenCancel := false
	seenLeave := false
	deadline := time.After(time.Second)
	for !seenCancel || !seenLeave {
		select {
		case raw := <-bobQueue:
			packet, err := protocol.UnmarshalPacket(raw)
			if err != nil {
				t.Fatal(err)
			}
			switch packet.Hdr.MsgType {
			case protocol.MsgFileCancel:
				cancel, err := protocol.DecodeJSONPayload[protocol.FileCancelPayload](packet.Payload)
				if err != nil {
					t.Fatal(err)
				}
				seenCancel = cancel.Reason == protocol.FileCodePeerDisconnected && cancel.TransferID == "transfer-step7"
			case protocol.MsgDiscoUpdate:
				update, err := protocol.DecodeJSONPayload[protocol.DiscoUpdatePayload](packet.Payload)
				if err != nil {
					t.Fatal(err)
				}
				seenLeave = update.Event == "LEAVE" && update.Reason == "TIMEOUT" && update.Peer.Username == "hung.vo"
			}
		case <-deadline:
			t.Fatalf("cleanup notifications missing: cancel=%t leave=%t", seenCancel, seenLeave)
		}
	}
}

func TestStep7_Client_AutoReconnect_Recovery(t *testing.T) {
	store := clientstate.NewClientStore()
	manager := client.NewReconnectManager(store, client.ReconnectConfig{
		InitialInterval: time.Millisecond,
		MaxInterval:     4 * time.Millisecond,
		Multiplier:      2,
		MaxAttempts:     5,
	})
	stateEvents := store.SubscribeConnectionState()
	attempts := 0
	err := manager.Start(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return errors.New("server unavailable")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || store.ConnectionState() != clientstate.ConnStateConnected {
		t.Fatalf("reconnect result: attempts=%d state=%s", attempts, store.ConnectionState())
	}
	var events []clientstate.ConnectionState
	for {
		select {
		case event := <-stateEvents:
			events = append(events, event)
		default:
			if len(events) > 0 {
				if events[0] != clientstate.ConnStateReconnecting || events[len(events)-1] != clientstate.ConnStateConnected {
					t.Fatalf("unexpected state events: %v", events)
				}
				return
			}
		}
	}
}
