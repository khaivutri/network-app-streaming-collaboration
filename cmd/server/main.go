package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/handlers"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	mode := flag.String("mode", "step2-tcp", "transport mode")
	addr := flag.String("addr", ":9000", "listen address")
	users := flag.String("users", "configs/users.json", "credential JSON file")
	_ = flag.String("log-level", "info", "log level")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if *mode == "rudp" {
		s, e := rudp.NewUDPHandshakeServer(*addr)
		if e != nil {
			err = e
		} else {
			fmt.Printf("RUDP Handshake Mode listening on %s\n", s.Addr())
			err = s.Serve(ctx)
		}
	} else if *mode == "step2-tcp" {
		s := tcp_handshake.NewServer(*addr, slog.Default())
		fmt.Printf("TCP Handshake Mode listening on %s\n", *addr)
		err = s.Serve(ctx)
	} else if *mode == "step3-auth" {
		err = serveAuth(ctx, *addr, *users)
	} else if *mode == "step4-concurrency" {
		srvApp, srvErr := server.NewServerApp(*addr, *users)
		if srvErr != nil {
			err = srvErr
		} else {
			fmt.Printf("Step 4 Concurrency Server listening on %s\n", srvApp.Addr())
			err = srvApp.Serve(ctx)
		}
	} else if *mode == "step5-chat" {
		srvApp, srvErr := server.NewServerApp(*addr, *users)
		if srvErr != nil {
			err = srvErr
		} else {
			fmt.Printf("Step 5 Streaming & Chat Server listening on %s\n", srvApp.Addr())
			err = srvApp.Serve(ctx)
		}
	} else if *mode == "step6-file" {
		srvApp, srvErr := server.NewServerApp(*addr, *users)
		if srvErr != nil {
			err = srvErr
		} else {
			fmt.Printf("Step 6 Reliable File Transfer Server listening on %s\n", srvApp.Addr())
			err = srvApp.Serve(ctx)
		}
	} else if *mode == "step7-resilience" {
		srvApp, srvErr := server.NewServerApp(*addr, *users)
		if srvErr != nil {
			err = srvErr
		} else {
			fmt.Printf("Step 7 Resilience Server listening on %s\n", srvApp.Addr())
			err = srvApp.Serve(ctx)
		}
	} else {
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// serveAuth runs the minimal Step 3 UDP request loop. Handshake reliability and
// retransmission remain owned by the transport package; this loop only routes
// authenticated control messages and drains per-client broadcast queues.
func serveAuth(ctx context.Context, listenAddr, usersPath string) error {
	store, err := state.LoadCredentialStore(usersPath)
	if err != nil {
		return err
	}
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	clients := make(map[uint32]chan []byte)
	send := func(raw []byte, target *net.UDPAddr) error {
		_, writeErr := conn.WriteToUDP(raw, target)
		return writeErr
	}
	handler := handlers.NewAuthHandler(store, send)
	fmt.Printf("Step3 Auth UDP listening on %s\n", conn.LocalAddr())
	go func() { <-ctx.Done(); _ = conn.Close() }()
	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	for {
		n, remote, readErr := conn.ReadFromUDP(buf)
		if readErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return readErr
		}
		pkt, parseErr := protocol.UnmarshalPacket(buf[:n])
		if parseErr != nil {
			continue
		}
		if pkt.Hdr.HasFlag(protocol.FlagSYN) {
			id, idErr := transport.GenerateConnectionID()
			if idErr != nil {
				continue
			}
			ack, _ := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN | protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: id, WindowSize: protocol.DefaultWindowSize}})
			_, _ = conn.WriteToUDP(ack, remote)
			continue
		}
		if pkt.Hdr.MsgType == protocol.MsgCtrlHandshake && pkt.Hdr.HasFlag(protocol.FlagACK) {
			continue
		}
		ch := clients[pkt.Hdr.ConnectionID]
		if ch == nil {
			ch = make(chan []byte, 32)
			clients[pkt.Hdr.ConnectionID] = ch
			clientAddr := *remote
			go func(outbound <-chan []byte, target net.UDPAddr) {
				for raw := range outbound {
					if _, writeErr := conn.WriteToUDP(raw, &target); writeErr != nil {
						slog.Error("presence update send failed", "addr", target.String(), "error", writeErr)
						return
					}
				}
			}(ch, clientAddr)
		}
		switch pkt.Hdr.MsgType {
		case protocol.MsgAuthRequest:
			if handlerErr := handler.HandleAuthRequest(remote, pkt.Hdr.ConnectionID, pkt.Payload, ch); handlerErr != nil {
				slog.Error("auth request failed", "addr", remote.String(), "error", handlerErr)
			}
		case protocol.MsgLogout:
			if handlerErr := handler.HandleLogout(remote, pkt.Hdr.ConnectionID, pkt.Payload); handlerErr != nil {
				slog.Error("logout request failed", "addr", remote.String(), "error", handlerErr)
			}
		case protocol.MsgDiscoPeers:
			if handlerErr := handler.HandleDiscoPeers(remote, pkt.Hdr.ConnectionID, pkt.Payload); handlerErr != nil {
				slog.Error("discovery request failed", "addr", remote.String(), "error", handlerErr)
			}
		}
		for {
			select {
			case raw := <-ch:
				_, _ = conn.WriteToUDP(raw, remote)
			default:
				goto drained
			}
		}
	drained:
	}
}
