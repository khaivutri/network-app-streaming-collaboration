package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	mode := flag.String("mode", "step2-tcp", "transport mode")
	addr := flag.String("addr", ":9000", "listen address")
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
	} else {
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
