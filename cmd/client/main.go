package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
)

func main() {
	// use as new branch
	mode := flag.String("mode", "step2-tcp", "transport mode")
	server := flag.String("server", "127.0.0.1:9000", "server address")
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
	} else {
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Mode: %s\nLocalAddr -> ServerAddr: %s -> %s\nAssigned ConnectionID: 0x%08X (%d)\nServer Acknowledgment Message: %s\nRound-Trip Time (RTT): %d ms / %d µs\nFinal State: CONNECTED -> Gracefully Closed (FIN/FIN-ACK)\n", *mode, r.LocalAddr, r.ServerAddr, r.ConnectionID, r.ConnectionID, r.WelcomeMsg, r.RTTMillis, r.RTTMicros)
}
