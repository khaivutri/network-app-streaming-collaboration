package rudp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
)

// UDPConfig controls UDP handshake timing and packet filtering.
type UDPConfig struct {
	HandshakeTimeout time.Duration
	PacketDropHook   func(dir string, pkt *protocol.Packet) bool
}

// UDPHandshakeServer serves handshakes over one UDP socket. Both indexes are protected
// by mu so session lookup and cleanup remain race-free as packet handling scales.
type UDPHandshakeServer struct {
	conn     *net.UDPConn
	addr     *net.UDPAddr
	dropHook func(dir string, pkt *protocol.Packet) bool
	mu       sync.RWMutex
	sessions map[string]*ServerStateMachine
	idToAddr map[uint32]string
}

// NewUDPHandshakeServer binds a UDP handshake server.
func NewUDPHandshakeServer(listenAddr string, cfg ...UDPConfig) (*UDPHandshakeServer, error) {
	// Resolve listening address and bind local UDP socket.
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("rudp: resolve udp addr failed: %w", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("rudp: listen udp failed: %w", err)
	}

	server := &UDPHandshakeServer{
		conn:     conn,
		addr:     conn.LocalAddr().(*net.UDPAddr),
		sessions: make(map[string]*ServerStateMachine),
		idToAddr: make(map[uint32]string),
	}

	// Apply optional packet loss simulation hook if configured.
	if len(cfg) > 0 {
		server.dropHook = cfg[0].PacketDropHook
	}

	// Apply socket buffer and platform-specific configurations.
	if err := configureUDPSocket(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rudp: configure udp socket failed: %w", err)
	}

	return server, nil
}

// Addr returns the actual bound address.
func (s *UDPHandshakeServer) Addr() net.Addr { return s.addr }

// Close releases the indexes before closing the socket, preserving the map invariant.
func (s *UDPHandshakeServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush active sessions to release state machine resources.
	for key := range s.sessions {
		delete(s.sessions, key)
	}
	for id := range s.idToAddr {
		delete(s.idToAddr, id)
	}

	return s.conn.Close()
}

// Serve handles packets until the context is cancelled.
func (s *UDPHandshakeServer) Serve(ctx context.Context) error {
	// Ensure the socket unblocks and shuts down when context cancels.
	go func() {
		<-ctx.Done()
		_ = s.conn.Close()
	}()

	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	for {
		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			// Normal shutdown via context cancellation; suppress socket closure error.
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		pkt, err := protocol.UnmarshalPacket(buf[:n])
		if err != nil {
			// Skip malformed packets silently to prevent server crash.
			continue
		}

		clientKey := remoteAddr.String()

		// Route SYN packets directly to session allocator; all other packets require valid session lookups.
		if pkt.Hdr.HasFlag(protocol.FlagSYN) {
			s.handleInboundSYN(remoteAddr, clientKey)
			continue
		}

		s.handleInboundSubsequent(remoteAddr, clientKey, pkt)
	}
}

// handleInboundSYN allocates one state machine per remote address; duplicate SYNs reuse it.
func (s *UDPHandshakeServer) handleInboundSYN(remoteAddr *net.UDPAddr, clientKey string) {
	s.mu.Lock()
	sm, exists := s.sessions[clientKey]
	if !exists {
		// New client connection attempt: allocate a unique connection ID.
		connID, err := transport.GenerateConnectionID()
		if err != nil {
			s.mu.Unlock()
			return
		}

		sm = NewServerStateMachine()
		s.sessions[clientKey] = sm
		s.idToAddr[connID] = clientKey
		_ = sm.AcceptSYN(connID)
	}
	connID := sm.ConnectionID()
	s.mu.Unlock()

	// Prepare SYN+ACK control response.
	synAck := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagSYN | protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: connID,
			AckNum:       1,
			WindowSize:   protocol.DefaultWindowSize,
		},
	}

	// Drop outbound packet early if intercept hook triggers (test simulation).
	if s.dropHook != nil && s.dropHook("out", synAck) {
		return
	}

	raw, err := protocol.MarshalPacket(synAck)
	if err == nil {
		_, _ = s.conn.WriteToUDP(raw, remoteAddr)
	}
}

// handleInboundSubsequent rejects unknown IDs and delegates valid packets to the state machine.
func (s *UDPHandshakeServer) handleInboundSubsequent(remoteAddr *net.UDPAddr, clientKey string, pkt *protocol.Packet) {
	s.mu.RLock()
	sm, exists := s.sessions[clientKey]
	valid := exists && sm.ConnectionID() == pkt.Hdr.ConnectionID
	s.mu.RUnlock()

	// Reject mismatched or unregistered connection attempts with RST.
	if !valid {
		// Suppress sending RST for RST packets to avoid infinite ping-pong loops.
		if pkt.Hdr.HasFlag(protocol.FlagRST) {
			return
		}

		rst := &protocol.Packet{
			Hdr: protocol.Header{
				Version:      protocol.ProtocolVersion,
				Flags:        protocol.FlagRST,
				MsgType:      protocol.MsgCtrlHandshake,
				StreamID:     protocol.StreamControl,
				ConnectionID: pkt.Hdr.ConnectionID,
			},
		}

		raw, err := protocol.MarshalPacket(rst)
		if err == nil {
			_, _ = s.conn.WriteToUDP(raw, remoteAddr)
		}
		return
	}

	// Feed valid packet into the session state machine and dispatch reply if emitted.
	action := sm.OnPacket(pkt)
	if action.Packet != nil {
		raw, err := protocol.MarshalPacket(action.Packet)
		if err == nil {
			_, _ = s.conn.WriteToUDP(raw, remoteAddr)
		}
	}

	// Purge closed sessions to reclaim memory once the teardown sequence completes.
	if sm.State() == transport.StateCloseReady {
		s.mu.Lock()
		delete(s.sessions, clientKey)
		delete(s.idToAddr, sm.ConnectionID())
		s.mu.Unlock()
	}
}

// PerformUDPHandshake runs the client state machine with bounded SYN retransmission.
func PerformUDPHandshake(ctx context.Context, serverAddr string, cfg ...UDPConfig) (*tcp_handshake.HandshakeResult, error) {
	// Parse optional timeout and packet drop hook configurations.
	timeout := HandshakeTimeout
	var hook func(string, *protocol.Packet) bool
	if len(cfg) > 0 {
		if cfg[0].HandshakeTimeout > 0 {
			timeout = cfg[0].HandshakeTimeout
		}
		hook = cfg[0].PacketDropHook
	}

	// Resolve destination server address.
	rAddr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("rudp client: resolve server addr failed: %w", err)
	}

	// Ephemeral local UDP socket for this handshake run.
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, fmt.Errorf("rudp client: bind local socket failed: %w", err)
	}
	defer conn.Close()

	if err := configureUDPSocket(conn); err != nil {
		return nil, fmt.Errorf("rudp client: configure socket failed: %w", err)
	}

	started := time.Now()
	client := NewClientStateMachine()
	synAction := client.Connect()

	synRaw, err := protocol.MarshalPacket(synAction.Packet)
	if err != nil {
		return nil, fmt.Errorf("rudp client: marshal syn failed: %w", err)
	}

	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	var welcome *protocol.Packet

	// Stage 1: SYN retransmission loop.
	for attempt := 0; attempt <= MaxHandshakeRetries; attempt++ {
		// Only transmit if not suppressed by the simulation hook.
		if hook == nil || !hook("out", synAction.Packet) {
			if _, err := conn.WriteToUDP(synRaw, rAddr); err != nil {
				return nil, fmt.Errorf("rudp client: send syn failed: %w", err)
			}
		}

		// Await SYN+ACK within deadline.
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		n, _, readErr := conn.ReadFromUDP(buf)
		if readErr == nil {
			parsed, parseErr := protocol.UnmarshalPacket(buf[:n])
			if parseErr == nil && parsed.Hdr.HasFlag(protocol.FlagSYN) && parsed.Hdr.HasFlag(protocol.FlagACK) {
				welcome = parsed
				break
			}
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Signal timeout to state machine prior to the next retransmission attempt.
		if attempt < MaxHandshakeRetries {
			_ = client.OnHandshakeTimeout()
		}
	}

	if welcome == nil || welcome.Hdr.ConnectionID == 0 {
		return nil, fmt.Errorf("rudp client: handshake timed out after %d retries", MaxHandshakeRetries)
	}

	// Stage 2: Complete 3-way handshake by replying with ACK.
	ackAction := client.OnPacket(welcome)
	if ackAction.Packet == nil {
		return nil, fmt.Errorf("rudp client: state machine rejected syn-ack")
	}

	ackRaw, err := protocol.MarshalPacket(ackAction.Packet)
	if err != nil {
		return nil, fmt.Errorf("rudp client: marshal ack failed: %w", err)
	}
	if _, err = conn.WriteToUDP(ackRaw, rAddr); err != nil {
		return nil, fmt.Errorf("rudp client: send ack failed: %w", err)
	}

	// Stage 3: Initiate graceful teardown (FIN -> FIN+ACK).
	finAction := client.Disconnect()
	if finAction.Packet == nil {
		return nil, fmt.Errorf("rudp client: state machine rejected disconnect")
	}

	finRaw, err := protocol.MarshalPacket(finAction.Packet)
	if err != nil {
		return nil, fmt.Errorf("rudp client: marshal fin failed: %w", err)
	}
	if _, err = conn.WriteToUDP(finRaw, rAddr); err != nil {
		return nil, fmt.Errorf("rudp client: send fin failed: %w", err)
	}

	// Await FIN+ACK confirmation from server.
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return nil, fmt.Errorf("rudp client: wait for fin-ack failed: %w", err)
	}

	finAck, err := protocol.UnmarshalPacket(buf[:n])
	if err != nil || !finAck.Hdr.HasFlag(protocol.FlagFIN) || !finAck.Hdr.HasFlag(protocol.FlagACK) {
		return nil, fmt.Errorf("rudp client: invalid fin-ack packet received")
	}
	_ = client.OnPacket(finAck)

	// Stage 4: Package benchmark and connection metrics.
	return &tcp_handshake.HandshakeResult{
		ConnectionID: welcome.Hdr.ConnectionID,
		ServerAddr:   serverAddr,
		LocalAddr:    conn.LocalAddr().String(),
		RTTMillis:    time.Since(started).Milliseconds(),
		RTTMicros:    time.Since(started).Microseconds(),
		WelcomeMsg:   protocol.DefaultWelcomeMessage,
	}, nil
}