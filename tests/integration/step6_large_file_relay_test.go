package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server"
	serverstate "github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// TestStep6_RealNetwork_LargeFileRelay_10MB verifies that a 10,093,111-byte file
// transfers end-to-end from hung.vo to Bob through ServerApp relay without freezing
// in staging, and is finalized into storage/downloads with a verified SHA-256 hash.
func TestStep6_RealNetwork_LargeFileRelay_10MB(t *testing.T) {
	serverstate.ResetGlobalState()
	defer serverstate.ResetGlobalState()

	usersPath := filepath.Join("..", "..", "configs", "users.json")
	srvApp, err := server.NewServerApp("127.0.0.1:0", usersPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = srvApp.Serve(ctx)
	}()
	time.Sleep(50 * time.Millisecond)

	serverUDP, err := net.ResolveUDPAddr("udp", srvApp.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	connectRelayClient := func(username, password string) (*net.UDPConn, *client.ChatClient, *state.ClientStore) {
		laddr, resolveErr := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		if resolveErr != nil {
			t.Fatal(resolveErr)
		}
		conn, listenErr := net.ListenUDP("udp", laddr)
		if listenErr != nil {
			t.Fatal(listenErr)
		}
		_ = conn.SetReadBuffer(4 * 1024 * 1024)
		_ = conn.SetWriteBuffer(4 * 1024 * 1024)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		syn, marshalErr := protocol.MarshalPacket(&protocol.Packet{
			Hdr: protocol.Header{
				Version:  protocol.ProtocolVersion,
				Flags:    protocol.FlagSYN,
				MsgType:  protocol.MsgCtrlHandshake,
				StreamID: protocol.StreamControl,
			},
		})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if _, writeErr := conn.WriteToUDP(syn, serverUDP); writeErr != nil {
			t.Fatal(writeErr)
		}

		buf := make([]byte, 2048)
		n, _, readErr := conn.ReadFromUDP(buf)
		if readErr != nil {
			t.Skipf("loopback UDP unavailable in this environment: %v", readErr)
		}
		welcome, unmarshalErr := protocol.UnmarshalPacket(buf[:n])
		if unmarshalErr != nil {
			t.Fatal(unmarshalErr)
		}
		ack, ackErr := protocol.MarshalPacket(&protocol.Packet{
			Hdr: protocol.Header{
				Version:      protocol.ProtocolVersion,
				Flags:        protocol.FlagACK,
				MsgType:      protocol.MsgCtrlHandshake,
				StreamID:     protocol.StreamControl,
				ConnectionID: welcome.Hdr.ConnectionID,
				AckNum:       1,
			},
		})
		if ackErr != nil {
			t.Fatal(ackErr)
		}
		if _, writeErr := conn.WriteToUDP(ack, serverUDP); writeErr != nil {
			t.Fatal(writeErr)
		}

		_ = conn.SetReadDeadline(time.Time{})
		store := state.NewClientStore()
		auth := client.NewAuthClient(conn, serverUDP, welcome.Hdr.ConnectionID, store)
		loginCtx, cancelLogin := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelLogin()
		resp, loginErr := auth.Login(loginCtx, username, password)
		if loginErr != nil || !resp.Success {
			t.Fatalf("login failed for %s: resp=%+v err=%v", username, resp, loginErr)
		}

		_ = conn.SetReadDeadline(time.Time{})
		chat := client.NewChatClient(conn, serverUDP, welcome.Hdr.ConnectionID, store)
		chat.StartHeartbeat(ctx, 2*time.Second)
		go func() {
			_ = chat.Listen(ctx)
		}()
		return conn, chat, store
	}

	bobConn, _, bobStore := connectRelayClient("Bob", "Bob@123")
	defer bobConn.Close()

	hungConn, hungChat, hungStore := connectRelayClient("hung.vo", "Password@123")
	defer hungConn.Close()

	fileName := "L01_Submission2_7MT.pdf"
	sourcePath := filepath.Join(t.TempDir(), fileName)
	payloadBytes := make([]byte, largeFileSize)
	for i := range payloadBytes {
		payloadBytes[i] = byte((i*37 + i/131) % 251)
	}
	if err := os.WriteFile(sourcePath, payloadBytes, 0644); err != nil {
		t.Fatal(err)
	}
	expectedSum := sha256.Sum256(payloadBytes)
	expectedHex := hex.EncodeToString(expectedSum[:])

	transferID, err := hungChat.SendFile("Bob", sourcePath, "Gửi báo cáo PDF 10MB")
	if err != nil {
		t.Fatalf("SendFile failed: %v", err)
	}

	stagingPath := filepath.Join("storage", "staging", transferID+".part")
	downloadedPath := filepath.Join("storage", "downloads", transferID+"_"+fileName)
	t.Cleanup(func() {
		_ = os.Remove(stagingPath)
		_ = os.Remove(downloadedPath)
	})

	deadline := time.Now().Add(30 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		hungProg, _ := hungStore.GetFileProgress(transferID)
		bobProg, _ := bobStore.GetFileProgress(transferID)
		if hungProg != nil && hungProg.State == "COMPLETED" && bobProg != nil && bobProg.State == "COMPLETED" {
			completed = true
			break
		}
		if (hungProg != nil && (hungProg.State == "ERROR" || hungProg.State == "CANCELLED")) ||
			(bobProg != nil && (bobProg.State == "ERROR" || bobProg.State == "CANCELLED")) {
			break
		}
	}

	if !completed {
		hungProg, _ := hungStore.GetFileProgress(transferID)
		bobProg, _ := bobStore.GetFileProgress(transferID)
		t.Fatalf("10MB relay transfer did not complete in time: sender=%+v receiver=%+v", hungProg, bobProg)
	}

	if _, statErr := os.Stat(stagingPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected staging file %s to be removed after finalize, statErr=%v", stagingPath, statErr)
	}

	gotBytes, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("read finalized download failed: %v", err)
	}
	actualSum := sha256.Sum256(gotBytes)
	actualHex := hex.EncodeToString(actualSum[:])
	if actualHex != expectedHex || !bytes.Equal(gotBytes, payloadBytes) {
		t.Fatalf("SHA-256 mismatch on finalized download: got %s, want %s", actualHex, expectedHex)
	}
}
