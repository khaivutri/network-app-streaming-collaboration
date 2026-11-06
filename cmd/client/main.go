package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
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
	count := flag.Int("count", 20, "concurrent client count")
	timeout := flag.Duration("timeout", 5*time.Second, "handshake timeout")
	flag.Parse()
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
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	syn, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}})
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
	ack, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: welcome.Hdr.ConnectionID, AckNum: 1}})
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
