package tcp_handshake

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// HandshakeResult reports the server acknowledgement and measured latency.
type HandshakeResult struct {
	ConnectionID uint32
	ServerAddr   string
	LocalAddr    string
	RTTMillis    int64
	RTTMicros    int64
	WelcomeMsg   string
}

// ConnectAndHandshake performs SYN, SYN-ACK, ACK and FIN, FIN-ACK.
func ConnectAndHandshake(ctx context.Context, serverAddr string) (*HandshakeResult, error) {
	// Step 1: Establish low-level TCP socket with dial deadline
	dialer := net.Dialer{Timeout: 3 * time.Second}
	rawConn, err := dialer.DialContext(ctx, "tcp", serverAddr)
	if err != nil {
		return nil, err
	}
	defer rawConn.Close()

	conn, ok := rawConn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("expected TCP connection")
	}

	// Disable Nagle's algorithm to ensure handshake packets are flushed immediately
	if err = conn.SetNoDelay(true); err != nil {
		return nil, err
	}

	// Bound overall handshake lifecycle to avoid hanging indefinitely on unresponsive servers
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Step 2: Assemble and send handshake initiation (SYN)
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	hello, err := protocol.EncodeJSONPayload(protocol.HandshakeSYNPayload{
		ClientVersion: protocol.DefaultClientVersion,
		Hostname:      host,
		Timestamp:     time.Now().UnixMilli(),
	})
	if err != nil {
		return nil, err
	}

	syn := &protocol.Packet{
		Hdr: protocol.Header{
			Version:    protocol.ProtocolVersion,
			Flags:      protocol.FlagSYN,
			MsgType:    protocol.MsgCtrlHandshake,
			StreamID:   protocol.StreamControl,
			WindowSize: protocol.DefaultWindowSize,
		},
		Payload: hello,
	}

	// Track start timestamp to compute network Round-Trip Time (RTT)
	started := time.Now()
	if err = WriteTCPFrame(conn, syn); err != nil {
		return nil, err
	}

	// Step 3: Receive and validate handshake acknowledgment (SYN-ACK)
	synAck, err := ReadTCPFrame(conn)
	if err != nil {
		return nil, err
	}
	rtt := time.Since(started)

	// Validate SYN-ACK invariants: must contain both flags, a valid connection ID, and acknowledge Seq 0
	if !synAck.Hdr.HasFlag(protocol.FlagSYN) || !synAck.Hdr.HasFlag(protocol.FlagACK) || synAck.Hdr.ConnectionID == 0 || synAck.Hdr.AckNum != 1 {
		return nil, fmt.Errorf("invalid SYN-ACK")
	}

	// Decode welcome metadata and ensure ID consistency
	welcome, err := protocol.DecodeJSONPayload[protocol.HandshakeSYNACKPayload](synAck.Payload)
	if err != nil {
		return nil, err
	}
	if welcome.Status != protocol.HandshakeStatusAcknowledged || welcome.AssignedConnID != synAck.Hdr.ConnectionID {
		return nil, fmt.Errorf("invalid welcome payload")
	}

	// Step 4: Finalize 3-way handshake (ACK)
	ack := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: synAck.Hdr.ConnectionID,
			SeqNum:       1,
			AckNum:       1,
			WindowSize:   protocol.DefaultWindowSize,
		},
	}
	if err = WriteTCPFrame(conn, ack); err != nil {
		return nil, err
	}

	// Step 5: Graceful teardown sequence (FIN / FIN-ACK)
	fin := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagFIN,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: synAck.Hdr.ConnectionID,
			SeqNum:       2,
			AckNum:       1,
			WindowSize:   protocol.DefaultWindowSize,
		},
	}
	if err = WriteTCPFrame(conn, fin); err != nil {
		return nil, err
	}

	finAck, err := ReadTCPFrame(conn)
	if err != nil {
		return nil, err
	}
	if !finAck.Hdr.HasFlag(protocol.FlagFIN) || !finAck.Hdr.HasFlag(protocol.FlagACK) || finAck.Hdr.ConnectionID != synAck.Hdr.ConnectionID {
		return nil, fmt.Errorf("invalid FIN-ACK")
	}

	// Step 6: Return benchmark and session diagnostics
	return &HandshakeResult{
		ConnectionID: synAck.Hdr.ConnectionID,
		ServerAddr:   serverAddr,
		LocalAddr:    conn.LocalAddr().String(),
		RTTMillis:    rtt.Milliseconds(),
		RTTMicros:    rtt.Microseconds(),
		WelcomeMsg:   welcome.Message,
	}, nil
}
