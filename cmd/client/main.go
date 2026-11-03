package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
)

func main() {
	// use as new branch
	mode := flag.String("mode", "step2-tcp", "transport mode")
	server := flag.String("server", "127.0.0.1:9000", "server address")
	user := flag.String("user", "", "authentication username")
	pass := flag.String("pass", "", "authentication password")
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
	} else {
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *mode == "step3-auth" {
		return
	}
	fmt.Printf("Mode: %s\nLocalAddr -> ServerAddr: %s -> %s\nAssigned ConnectionID: 0x%08X (%d)\nServer Acknowledgment Message: %s\nRound-Trip Time (RTT): %d ms / %d µs\nFinal State: CONNECTED -> Gracefully Closed (FIN/FIN-ACK)\n", *mode, r.LocalAddr, r.ServerAddr, r.ConnectionID, r.ConnectionID, r.WelcomeMsg, r.RTTMillis, r.RTTMicros)
}
