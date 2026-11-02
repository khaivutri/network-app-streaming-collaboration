package integration

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
)

func TestStep2TCPHandshakeSingleClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := tcp_handshake.NewServer("127.0.0.1:0", nil)
	addr, err := server.Listen()
	if err != nil {
		t.Fatalf("TCP server listen failed: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	res, err := tcp_handshake.ConnectAndHandshake(ctx, addr.String())
	if err != nil {
		t.Fatalf("ConnectAndHandshake failed: %v", err)
	}
	if res.ConnectionID == 0 || res.WelcomeMsg != protocol.DefaultWelcomeMessage || res.RTTMillis < 0 {
		t.Fatalf("invalid TCP handshake result: %#v", res)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("TCP server exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TCP server did not shut down in time")
	}
}

func TestStep2TCPHandshakeConcurrentClientsUniqueConnIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := tcp_handshake.NewServer("127.0.0.1:0", nil)
	addr, err := server.Listen()
	if err != nil {
		t.Fatalf("TCP server listen failed: %v", err)
	}
	go func() { _ = server.Serve(ctx) }()
	const clientCount = 20
	var wg sync.WaitGroup
	var idMu sync.Mutex
	uniqueIDs := make(map[uint32]struct{}, clientCount)
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientCtx, clientCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer clientCancel()
			res, err := tcp_handshake.ConnectAndHandshake(clientCtx, addr.String())
			if err != nil {
				t.Errorf("client %d failed handshake: %v", clientID, err)
				return
			}
			idMu.Lock()
			defer idMu.Unlock()
			if _, exists := uniqueIDs[res.ConnectionID]; exists {
				t.Errorf("ConnectionID collision: 0x%08X", res.ConnectionID)
			}
			uniqueIDs[res.ConnectionID] = struct{}{}
		}(i)
	}
	wg.Wait()
	if len(uniqueIDs) != clientCount {
		t.Fatalf("expected %d unique IDs, got %d", clientCount, len(uniqueIDs))
	}
}

func TestStep2UDPHandshakePacketLossRetransmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var dropOnce sync.Once
	dropHook := func(dir string, pkt *protocol.Packet) bool {
		if dir != "out" || !pkt.Hdr.HasFlag(protocol.FlagSYN) || !pkt.Hdr.HasFlag(protocol.FlagACK) {
			return false
		}
		dropped := false
		dropOnce.Do(func() { dropped = true })
		return dropped
	}
	server, err := rudp.NewUDPHandshakeServer("127.0.0.1:0", rudp.UDPConfig{PacketDropHook: dropHook})
	if err != nil {
		t.Fatalf("NewUDPHandshakeServer failed: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()
	clientCtx, clientCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer clientCancel()
	res, err := rudp.PerformUDPHandshake(clientCtx, server.Addr().String(), rudp.UDPConfig{HandshakeTimeout: 100 * time.Millisecond})
	if err != nil || res.ConnectionID == 0 {
		t.Fatalf("UDP retransmission handshake failed: %v %#v", err, res)
	}
}

func TestStep2UDPHandshakeDuplicateSYNReturnsSameConnID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := rudp.NewUDPHandshakeServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewUDPHandshakeServer failed: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()
	rAddr, err := net.ResolveUDPAddr("udp", server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.ListenUDP("udp", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	syn := &protocol.Packet{
		Hdr: protocol.Header{
			Version:    protocol.ProtocolVersion,
			Flags:      protocol.FlagSYN,
			MsgType:    protocol.MsgCtrlHandshake,
			StreamID:   protocol.StreamControl,
			WindowSize: protocol.DefaultWindowSize,
		},
	}
	raw, err := protocol.MarshalPacket(syn)
	if err != nil {
		t.Fatal(err)
	}
	readID := func() uint32 {
		buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		n, _, readErr := client.ReadFromUDP(buf)
		if readErr != nil {
			t.Fatalf("read SYN-ACK failed: %v", readErr)
		}
		resp, unmarshalErr := protocol.UnmarshalPacket(buf[:n])
		if unmarshalErr != nil {
			t.Fatalf("unmarshal SYN-ACK failed: %v", unmarshalErr)
		}
		return resp.Hdr.ConnectionID
	}
	_, _ = client.WriteToUDP(raw, rAddr)
	id1 := readID()
	_, _ = client.WriteToUDP(raw, rAddr)
	id2 := readID()
	if id1 == 0 || id1 != id2 {
		t.Fatalf("duplicate SYN IDs differ: 0x%08X vs 0x%08X", id1, id2)
	}
}

func TestStep2UDPHandshakeUnknownConnIDReceivesRST(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := rudp.NewUDPHandshakeServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewUDPHandshakeServer failed: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()
	rAddr, _ := net.ResolveUDPAddr("udp", server.Addr().String())
	client, _ := net.ListenUDP("udp", nil)
	defer client.Close()
	const bogusID uint32 = 0xDEADBEEF
	bogus := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD,
			MsgType:      protocol.MsgAuthRequest,
			StreamID:     protocol.StreamControl,
			ConnectionID: bogusID,
			SeqNum:       1,
			WindowSize:   protocol.DefaultWindowSize,
		},
	}
	raw, _ := protocol.MarshalPacket(bogus)
	_, _ = client.WriteToUDP(raw, rAddr)
	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := client.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("expected RST response: %v", err)
	}
	resp, err := protocol.UnmarshalPacket(buf[:n])
	if err != nil || !resp.Hdr.HasFlag(protocol.FlagRST) || resp.Hdr.ConnectionID != bogusID {
		t.Fatalf("invalid RST response: err=%v packet=%#v", err, resp)
	}
}
