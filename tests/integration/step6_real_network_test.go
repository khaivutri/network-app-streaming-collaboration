package integration

import (
	"context"
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

func TestStep6_RealNetwork_OfflineRecipient(t *testing.T) {
	serverstate.ResetGlobalState()
	defer serverstate.ResetGlobalState()

	usersPath := filepath.Join("..", "..", "configs", "users.json")
	srvApp, err := server.NewServerApp("127.0.0.1:19000", usersPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srvApp.Serve(ctx) }()
	time.Sleep(50 * time.Millisecond)

	srvAddr := srvApp.Addr().String()
	t.Logf("Server running on %s", srvAddr)

	addr, err := net.ResolveUDPAddr("udp", srvAddr)
	if err != nil {
		t.Fatal(err)
	}
	laddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Handshake
	syn, synErr := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}})
	t.Logf("synErr=%v, len(syn)=%d", synErr, len(syn))
	wN, wErr := conn.WriteToUDP(syn, addr)
	t.Logf("conn.WriteToUDP to %v wrote %d bytes, err=%v", addr, wN, wErr)

	buf := make([]byte, 2048)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	welcome, _ := protocol.UnmarshalPacket(buf[:n])
	ack, _ := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: welcome.Hdr.ConnectionID, AckNum: 1}})
	_, _ = conn.WriteToUDP(ack, addr)

	store := state.NewClientStore()
	auth := client.NewAuthClient(conn, addr, welcome.Hdr.ConnectionID, store)
	loginCtx, cancelLogin := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelLogin()
	resp, err := auth.Login(loginCtx, "hung.vo", "Password@123")
	if err != nil || !resp.Success {
		t.Fatalf("login failed: resp=%+v err=%v", resp, err)
	}

	chat := client.NewChatClient(conn, addr, welcome.Hdr.ConnectionID, store)
	chatCtx, cancelChat := context.WithCancel(context.Background())
	defer cancelChat()
	go func() { _ = chat.Listen(chatCtx) }()

	// Create a dummy sample.txt
	tmpFile := filepath.Join(t.TempDir(), "sample.txt")
	_ = os.WriteFile(tmpFile, []byte("Hello world test 123"), 0644)

	transferID, err := chat.SendFile("Bob", tmpFile, "Gửi thử file")
	if err != nil {
		t.Fatalf("SendFile error: %v", err)
	}
	t.Logf("SendFile started: transferID=%s", transferID)

	time.Sleep(500 * time.Millisecond)

	prog, ok := store.GetFileProgress(transferID)
	t.Logf("Progress in store: ok=%v, prog=%+v", ok, prog)
	if prog == nil || prog.State != "REJECTED" {
		t.Fatalf("expected state REJECTED, got: %+v", prog)
	}
}

func TestStep6_RealNetwork_SuccessfulTransfer_BetweenTwoClients(t *testing.T) {
	serverstate.ResetGlobalState()
	defer serverstate.ResetGlobalState()

	usersPath := filepath.Join("..", "..", "configs", "users.json")
	srvApp, err := server.NewServerApp("127.0.0.1:19001", usersPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srvApp.Serve(ctx) }()
	time.Sleep(50 * time.Millisecond)

	srvAddr := srvApp.Addr().String()
	serverUDP, err := net.ResolveUDPAddr("udp", srvAddr)
	if err != nil {
		t.Fatal(err)
	}

	connectClient := func(username, password string) (*net.UDPConn, *client.ChatClient, *state.ClientStore) {
		laddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		conn, err := net.ListenUDP("udp", laddr)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		syn, _ := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}})
		_, _ = conn.WriteToUDP(syn, serverUDP)

		buf := make([]byte, 2048)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		welcome, _ := protocol.UnmarshalPacket(buf[:n])
		ack, _ := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: welcome.Hdr.ConnectionID, AckNum: 1}})
		_, _ = conn.WriteToUDP(ack, serverUDP)

		_ = conn.SetReadDeadline(time.Time{})
		store := state.NewClientStore()
		auth := client.NewAuthClient(conn, serverUDP, welcome.Hdr.ConnectionID, store)
		loginCtx, cancelLogin := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelLogin()
		resp, err := auth.Login(loginCtx, username, password)
		if err != nil || !resp.Success {
			t.Fatalf("login failed for %s: resp=%+v err=%v", username, resp, err)
		}

		_ = conn.SetReadDeadline(time.Time{})
		chat := client.NewChatClient(conn, serverUDP, welcome.Hdr.ConnectionID, store)
		go func() { _ = chat.Listen(ctx) }()
		return conn, chat, store
	}

	// 1. Start Bob (Receiver)
	bobConn, _, bobStore := connectClient("Bob", "Bob@123")
	defer bobConn.Close()

	// 2. Start hung.vo (Sender)
	hungConn, hungChat, hungStore := connectClient("hung.vo", "Password@123")
	defer hungConn.Close()

	// 3. Prepare sample file
	tmpDir := t.TempDir()
	sourceFile := filepath.Join(tmpDir, "sample.txt")
	testData := []byte("Hello from hung.vo to Bob reliable transfer over UDP! 1234567890")
	if err := os.WriteFile(sourceFile, testData, 0644); err != nil {
		t.Fatal(err)
	}

	// 4. Send file
	transferID, err := hungChat.SendFile("Bob", sourceFile, "Gửi file tài liệu")
	if err != nil {
		t.Fatalf("SendFile failed: %v", err)
	}
	t.Logf("Sent file transferID=%s", transferID)

	// Wait for transfer to complete across UDP
	deadline := time.Now().Add(4 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		hungProg, _ := hungStore.GetFileProgress(transferID)
		bobProg, _ := bobStore.GetFileProgress(transferID)
		if hungProg != nil && hungProg.State == "COMPLETED" && bobProg != nil && bobProg.State == "COMPLETED" {
			completed = true
			break
		}
	}

	if !completed {
		hungProg, _ := hungStore.GetFileProgress(transferID)
		bobProg, _ := bobStore.GetFileProgress(transferID)
		t.Fatalf("Transfer did not complete in time: hungProg=%+v, bobProg=%+v", hungProg, bobProg)
	}

	// Verify file on disk in storage/downloads/<transferID>_sample.txt
	downloadedPath := filepath.Join("storage", "downloads", transferID+"_sample.txt")
	got, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("read downloaded file failed: %v", err)
	}
	if string(got) != string(testData) {
		t.Fatalf("data mismatch: got %q, want %q", string(got), string(testData))
	}
	t.Logf("SUCCESS! File verified at %s with %d bytes", downloadedPath, len(got))
}

func TestStep6_RealNetwork_TransferFile_WithSpacesAndUnicode(t *testing.T) {
	serverstate.ResetGlobalState()
	defer serverstate.ResetGlobalState()

	usersPath := filepath.Join("..", "..", "configs", "users.json")
	srvApp, err := server.NewServerApp("127.0.0.1:19002", usersPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srvApp.Serve(ctx) }()
	time.Sleep(50 * time.Millisecond)

	srvAddr := srvApp.Addr().String()
	serverUDP, err := net.ResolveUDPAddr("udp", srvAddr)
	if err != nil {
		t.Fatal(err)
	}

	connectClient := func(username, password string) (*net.UDPConn, *client.ChatClient, *state.ClientStore) {
		laddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		conn, err := net.ListenUDP("udp", laddr)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		syn, _ := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}})
		_, _ = conn.WriteToUDP(syn, serverUDP)

		buf := make([]byte, 2048)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		welcome, _ := protocol.UnmarshalPacket(buf[:n])
		ack, _ := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: welcome.Hdr.ConnectionID, AckNum: 1}})
		_, _ = conn.WriteToUDP(ack, serverUDP)

		_ = conn.SetReadDeadline(time.Time{})
		store := state.NewClientStore()
		auth := client.NewAuthClient(conn, serverUDP, welcome.Hdr.ConnectionID, store)
		loginCtx, cancelLogin := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelLogin()
		resp, err := auth.Login(loginCtx, username, password)
		if err != nil || !resp.Success {
			t.Fatalf("login failed for %s: resp=%+v err=%v", username, resp, err)
		}

		_ = conn.SetReadDeadline(time.Time{})
		chat := client.NewChatClient(conn, serverUDP, welcome.Hdr.ConnectionID, store)
		go func() { _ = chat.Listen(ctx) }()
		return conn, chat, store
	}

	bobConn, _, bobStore := connectClient("Bob", "Bob@123")
	defer bobConn.Close()

	hungConn, hungChat, hungStore := connectClient("hung.vo", "Password@123")
	defer hungConn.Close()

	// File with space and unicode in path and filename
	subDir := filepath.Join(t.TempDir(), "Thư Mục Tài Liệu Có Dấu Cách")
	_ = os.MkdirAll(subDir, 0755)
	fileName := "Báo Cáo Tiến Độ Đồ Án 2026.docx"
	sourceFile := filepath.Join(subDir, fileName)
	testData := []byte("Nội dung tài liệu kiểm thử tiếng Việt UTF-8 có dấu cách và dấu thanh đầy đủ.")
	if err := os.WriteFile(sourceFile, testData, 0644); err != nil {
		t.Fatal(err)
	}

	transferID, err := hungChat.SendFile("Bob", sourceFile, "Gửi báo cáo tiếng Việt")
	if err != nil {
		t.Fatalf("SendFile with spaces and unicode failed: %v", err)
	}

	deadline := time.Now().Add(4 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		hungProg, _ := hungStore.GetFileProgress(transferID)
		bobProg, _ := bobStore.GetFileProgress(transferID)
		if hungProg != nil && hungProg.State == "COMPLETED" && bobProg != nil && bobProg.State == "COMPLETED" {
			completed = true
			break
		}
	}

	if !completed {
		t.Fatal("Transfer with spaces and unicode did not complete in time")
	}

	downloadedPath := filepath.Join("storage", "downloads", transferID+"_"+fileName)
	got, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("read downloaded unicode file failed: %v", err)
	}
	if string(got) != string(testData) {
		t.Fatalf("data mismatch: got %q, want %q", string(got), string(testData))
	}
	t.Logf("SUCCESS! Unicode and spaces verified at %s with %d bytes", downloadedPath, len(got))
}
