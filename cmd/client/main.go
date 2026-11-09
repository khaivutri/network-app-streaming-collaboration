package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/webbridge"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
	"strings"
	"unicode"
)

func main() {
	// use as new branch
	mode := flag.String("mode", "step2-tcp", "transport mode")
	server := flag.String("server", "127.0.0.1:9000", "server address")
	user := flag.String("user", "", "authentication username")
	pass := flag.String("pass", "", "authentication password")
	uiAddr := flag.String("ui-addr", "127.0.0.1:8081", "local HTTP UI address")
	storageDir := flag.String("storage-dir", "storage", "local client storage directory")
	maxUploadBytes := flag.Int64("max-upload-bytes", 1<<30, "maximum browser upload size")
	count := flag.Int("count", 20, "concurrent client count")
	timeout := flag.Duration("timeout", 5*time.Second, "handshake timeout")
	flag.Parse()
	if *mode == "step9-http-ui" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		returnWithError(runStep9HTTPUI(ctx, *server, *uiAddr, *storageDir, *maxUploadBytes))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var r *tcp_handshake.HandshakeResult
	var err error
	if *mode == "rudp" {
		r, err = rudp.PerformUDPHandshake(ctx, *server)
	} else if *mode == "step2-tcp" {
		r, err = tcp_handshake.ConnectAndHandshake(ctx, *server)
	} else if *mode == "step3-auth" {
		addr, resolveErr := net.ResolveUDPAddr("udp", *server)
		if resolveErr != nil {
			err = resolveErr
		} else {
			conn, dialErr := net.ListenUDP("udp", nil)
			if dialErr != nil {
				err = dialErr
			} else {
				defer conn.Close()
				store := state.NewClientStore()
				connectionID, idErr := transport.GenerateConnectionID()
				if idErr != nil {
					err = idErr
				} else {
					ac := client.NewAuthClient(conn, addr, connectionID, store)
					resp, loginErr := ac.Login(ctx, *user, *pass)
					if loginErr != nil {
						err = loginErr
					} else if !resp.Success {
						fmt.Printf("AUTH %s: %s\n", resp.Code, resp.Message)
					} else {
						fmt.Printf("AUTH %s user=%s peers=%d\n", resp.Code, resp.User.Username, len(resp.OnlinePeers))
					}
				}
			}
		}
	} else if *mode == "step4-concurrency" {
		results := client.RunConcurrentClients(ctx, *server, *count, *user, *pass)
		for _, result := range results {
			if result.Err != nil {
				fmt.Printf("client[%d] error=%v\n", result.Index, result.Err)
				continue
			}
			fmt.Printf("client[%d] conn=0x%08X auth=%s\n", result.Index, result.ConnectionID, result.AuthCode)
		}
	} else if *mode == "step5-chat" {
		returnWithError(runStep5Chat(*server, *user, *pass))
		return
	} else if *mode == "step6-file" {
		returnWithError(runStep6File(*server, *user, *pass))
		return
	} else if *mode == "step7-resilience" {
		returnWithError(runStep6File(*server, *user, *pass))
		return
	} else if *mode == "step8-security" {
		returnWithError(runStep8Security(*server, *user, *pass))
		return
	} else {
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *mode == "step3-auth" || *mode == "step4-concurrency" {
		return
	}
	fmt.Printf("Mode: %s\nLocalAddr -> ServerAddr: %s -> %s\nAssigned ConnectionID: 0x%08X (%d)\nServer Acknowledgment Message: %s\nRound-Trip Time (RTT): %d ms / %d µs\nFinal State: CONNECTED -> Gracefully Closed (FIN/FIN-ACK)\n", *mode, r.LocalAddr, r.ServerAddr, r.ConnectionID, r.ConnectionID, r.WelcomeMsg, r.RTTMillis, r.RTTMicros)
}

func runStep9HTTPUI(ctx context.Context, serverAddr, uiAddr, storageDir string, maxUploadBytes int64) error {
	app := client.NewClientApp(client.AppConfig{
		ServerAddr:       serverAddr,
		StagingDir:       filepath.Join(storageDir, "staging"),
		DownloadDir:      filepath.Join(storageDir, "downloads"),
		HandshakeTimeout: 5 * time.Second,
		RequestTimeout:   5 * time.Second,
	})
	bridge, err := webbridge.NewServer(app, webbridge.Config{
		ListenAddr:     uiAddr,
		UploadDir:      filepath.Join(storageDir, "staging", "uploads"),
		MaxUploadBytes: maxUploadBytes,
	})
	if err != nil {
		return err
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- bridge.Start(ctx) }()
	// The HTTP bridge intentionally does not auto-open a browser; this URL is
	// deterministic for multi-client demos and keeps the agent cross-platform.
	fmt.Printf("Step 9 Local UI listening on http://%s\n", uiAddr)
	select {
	case err := <-serverErr:
		_ = app.Shutdown(context.Background())
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = bridge.Shutdown(shutdownCtx)
		return app.Shutdown(shutdownCtx)
	}
}

func runStep8Security(serverAddr, username, password string) error {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	keys, err := security.GenerateX25519KeyPair()
	if err != nil {
		return err
	}
	var clientNonce [16]byte
	if _, err = rand.Read(clientNonce[:]); err != nil {
		return err
	}
	hello, err := protocol.EncodeJSONPayload(protocol.HandshakeSYNPayload{
		ClientVersion: protocol.DefaultClientVersion,
		Timestamp:     time.Now().UnixMilli(),
		ClientPubKey:  base64.StdEncoding.EncodeToString(keys.PublicKey[:]),
		ClientNonce:   base64.StdEncoding.EncodeToString(clientNonce[:]),
	})
	if err != nil {
		return err
	}
	syn, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}, Payload: hello})
	if err != nil {
		return err
	}
	if _, err = conn.WriteToUDP(syn, addr); err != nil {
		return err
	}
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return err
	}
	wire, err := protocol.UnmarshalPacket(buffer[:n])
	if err != nil {
		return err
	}
	welcome, err := protocol.DecodeJSONPayload[protocol.HandshakeSYNACKPayload](wire.Payload)
	if err != nil {
		return err
	}
	serverPubBytes, err := base64.StdEncoding.DecodeString(welcome.ServerPubKey)
	if err != nil || len(serverPubBytes) != 32 {
		return fmt.Errorf("invalid server public key")
	}
	serverNonceBytes, err := base64.StdEncoding.DecodeString(welcome.ServerNonce)
	if err != nil || len(serverNonceBytes) != 16 {
		return fmt.Errorf("invalid server nonce")
	}
	var serverPub [32]byte
	var serverNonce [16]byte
	copy(serverPub[:], serverPubBytes)
	copy(serverNonce[:], serverNonceBytes)
	writeKey, readKey, err := security.DeriveSessionKeys(keys.PrivateKey, serverPub, clientNonce, serverNonce, wire.Hdr.ConnectionID, false)
	if err != nil {
		return err
	}
	cipher, err := security.NewSessionCipher(writeKey, readKey, false)
	if err != nil {
		return err
	}
	ack, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: wire.Hdr.ConnectionID, AckNum: 1}})
	if err != nil {
		return err
	}
	if _, err = conn.WriteToUDP(ack, addr); err != nil {
		return err
	}
	store := state.NewClientStore()
	auth := client.NewAuthClient(conn, addr, wire.Hdr.ConnectionID, store)
	auth.SetCipher(cipher)
	loginCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := auth.Login(loginCtx, username, password)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("login failed: %s", resp.Message)
	}
	chat := client.NewChatClient(conn, addr, wire.Hdr.ConnectionID, store)
	chat.SetCipher(cipher)
	fmt.Printf("Step 8 security authentication succeeded for %s (connection 0x%08X)\n", username, wire.Hdr.ConnectionID)
	return nil
}

func runStep6File(serverAddr, username, password string) error {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return fmt.Errorf("file client resolve server: %w", err)
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return fmt.Errorf("file client bind socket: %w", err)
	}
	defer conn.Close()
	_ = conn.SetReadBuffer(4 * 1024 * 1024)
	_ = conn.SetWriteBuffer(4 * 1024 * 1024)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	syn, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:  protocol.ProtocolVersion,
			Flags:    protocol.FlagSYN,
			MsgType:  protocol.MsgCtrlHandshake,
			StreamID: protocol.StreamControl,
		},
	})
	if err != nil {
		return err
	}
	if _, err = conn.WriteToUDP(syn, addr); err != nil {
		return fmt.Errorf("file client send SYN: %w", err)
	}
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return fmt.Errorf("file client receive SYN-ACK: %w", err)
	}
	welcome, err := protocol.UnmarshalPacket(buffer[:n])
	if err != nil {
		return err
	}
	ack, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: welcome.Hdr.ConnectionID,
			AckNum:       1,
		},
	})
	if err != nil {
		return err
	}
	if _, err = conn.WriteToUDP(ack, addr); err != nil {
		return err
	}
	if err = conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	store := state.NewClientStore()
	auth := client.NewAuthClient(conn, addr, welcome.Hdr.ConnectionID, store)
	loginCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := auth.Login(loginCtx, username, password)
	if err != nil {
		return err
	}
	if !response.Success {
		return fmt.Errorf("login failed: %s", response.Message)
	}
	_ = conn.SetReadDeadline(time.Time{})
	chat := client.NewChatClient(conn, addr, welcome.Hdr.ConnectionID, store)
	chatCtx, cancelChat := context.WithCancel(context.Background())
	defer cancelChat()
	chat.StartHeartbeat(chatCtx, 3*time.Second)
	go func() {
		if err := chat.Listen(chatCtx); err != nil && chatCtx.Err() == nil {
			fmt.Printf("\n[Receiver Error] %v\n", err)
		}
	}()
	fmt.Println("Connected. Commands: /sendfile <recipient> <path> [caption], /cancel <transfer_id>, /status")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "/sendfile") {
			tokens := parseCommandLine(line)
			if len(tokens) < 3 {
				fmt.Println("usage: /sendfile <recipient> <path> [caption]")
				continue
			}
			recipient := tokens[1]
			filePath := tokens[2]
			caption := strings.Join(tokens[3:], " ")
			if tokens[2] == "&" && len(tokens) > 3 {
				filePath = tokens[3]
				caption = strings.Join(tokens[4:], " ")
			}
			filePath = transfer.CleanInputPath(filePath)
			if _, statErr := os.Stat(filePath); statErr != nil {
				fmt.Printf("[Lỗi File] Đường dẫn không tồn tại hoặc không thể đọc: %s (%v)\n", filePath, statErr)
				continue
			}
			id, sendErr := chat.SendFile(recipient, filePath, caption)
			fmt.Printf("transfer=%s err=%v\n", id, sendErr)
			continue
		}
		if strings.HasPrefix(line, "/cancel ") {
			if cancelErr := chat.CancelTransfer(strings.TrimSpace(strings.TrimPrefix(line, "/cancel "))); cancelErr != nil {
				fmt.Println("cancel error:", cancelErr)
			}
			continue
		}
		if line == "/status" {
			fmt.Println("Use transfer ID from /sendfile; progress is maintained in the client store.")
			continue
		}
		if line != "" {
			_, err = chat.SendBroadcast(line)
			if err != nil {
				fmt.Println("send error:", err)
			}
		}
	}
	return scanner.Err()
}

func returnWithError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runStep5Chat(serverAddr, username, password string) error {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return fmt.Errorf("chat client resolve server: %w", err)
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return fmt.Errorf("chat client bind socket: %w", err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	syn, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}})
	if err != nil {
		return err
	}
	if _, err = conn.WriteToUDP(syn, addr); err != nil {
		return fmt.Errorf("chat client send SYN: %w", err)
	}
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return fmt.Errorf("chat client receive SYN-ACK: %w", err)
	}
	welcome, err := protocol.UnmarshalPacket(buffer[:n])
	if err != nil {
		return err
	}
	ack, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: welcome.Hdr.ConnectionID, AckNum: 1}})
	if err != nil {
		return err
	}
	if _, err = conn.WriteToUDP(ack, addr); err != nil {
		return fmt.Errorf("chat client send ACK: %w", err)
	}
	if err = conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	store := state.NewClientStore()
	auth := client.NewAuthClient(conn, addr, welcome.Hdr.ConnectionID, store)
	loginCtx, cancelLogin := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLogin()
	response, err := auth.Login(loginCtx, username, password)
	if err != nil {
		return err
	}
	if !response.Success {
		return fmt.Errorf("login failed: %s", response.Message)
	}
	_ = conn.SetReadDeadline(time.Time{})
	chat := client.NewChatClient(conn, addr, welcome.Hdr.ConnectionID, store)
	chatCtx, cancelChat := context.WithCancel(context.Background())
	defer cancelChat()
	chat.StartHeartbeat(chatCtx, 3*time.Second)
	go func() {
		if err := chat.Listen(chatCtx); err != nil && chatCtx.Err() == nil {
			fmt.Printf("\n[Receiver Error] %v\n", err)
		}
	}()
	fmt.Println("Connected. Type a message, or /dm <user> <message>; Ctrl+C to exit.")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "/dm ") {
			parts := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(line, "/dm ")), " ", 2)
			if len(parts) != 2 {
				fmt.Println("usage: /dm <user> <message>")
				continue
			}
			_, err = chat.SendDirect(parts[0], parts[1])
		} else {
			_, err = chat.SendBroadcast(line)
		}
		if err != nil {
			fmt.Println("send error:", err)
		}
	}
	return scanner.Err()
}

// parseCommandLine splits a command line string into tokens respecting single and double quotes.
func parseCommandLine(input string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	var quoteChar rune

	for _, r := range input {
		switch {
		case (r == '"' || r == '\'') && !inQuotes:
			inQuotes = true
			quoteChar = r
		case inQuotes && r == quoteChar:
			inQuotes = false
			quoteChar = 0
		case unicode.IsSpace(r) && !inQuotes:
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}
