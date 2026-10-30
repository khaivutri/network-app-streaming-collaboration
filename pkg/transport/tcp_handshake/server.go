package tcp_handshake

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
)

// Server accepts TCP application handshakes and performs graceful teardown.
type Server struct {
	listenAddr string
	listener   *net.TCPListener
	logger     *slog.Logger
	mu         sync.Mutex
	activeIDs  map[uint32]struct{}
	wg         sync.WaitGroup
}

// NewServer constructs a TCP handshake server.
func NewServer(listenAddr string, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{listenAddr: listenAddr, logger: logger, activeIDs: make(map[uint32]struct{})}
}

// Listen binds the TCP listener and returns its actual address.
func (s *Server) Listen() (net.Addr, error) {
	addr, err := net.ResolveTCPAddr("tcp", s.listenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	s.listener = listener
	return listener.Addr(), nil
}

// Addr returns the bound address, or nil before Listen.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Serve accepts connections until the context is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	if s.listener == nil {
		if _, err := s.Listen(); err != nil {
			return err
		}
	}
	go func() { <-ctx.Done(); _ = s.listener.Close() }()
	for {
		conn, err := s.listener.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				s.wg.Wait()
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handleConn(ctx, conn) }()
	}
}

// StartTCPServer runs a server bound to listenAddr until ctx ends.
func StartTCPServer(ctx context.Context, listenAddr string) error {
	return NewServer(listenAddr, nil).Serve(ctx)
}

func (s *Server) handleConn(ctx context.Context, conn *net.TCPConn) {
	defer conn.Close()

	// Enforce an overall deadline to mitigate Slowloris and hung connection attacks.
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Step 1: Inbound Handshake Request (SYN)
	pkt, err := ReadTCPFrame(conn)
	if err != nil {
		return
	}

	// Validate SYN invariants: must initiate handshake with unassigned ConnectionID and SeqNum 0.
	if pkt.Hdr.Flags != protocol.FlagSYN || pkt.Hdr.MsgType != protocol.MsgCtrlHandshake || pkt.Hdr.ConnectionID != 0 || pkt.Hdr.SeqNum != 0 {
		s.sendReset(conn, pkt.Hdr.ConnectionID)
		return
	}

	// Parse client metadata; reject malformed handshake payload immediately.
	var hello protocol.HandshakeSYNPayload
	if _, err = protocol.DecodeJSONPayload[protocol.HandshakeSYNPayload](pkt.Payload); err != nil {
		s.sendReset(conn, 0)
		return
	}

	// Step 2: Session Allocation
	// Reserve a unique non-zero ConnectionID; ensure cleanup upon handler exit.
	id, err := s.reserveID()
	if err != nil {
		s.sendReset(conn, 0)
		return
	}
	defer s.releaseID(id)

	// Step 3: Outbound Handshake Response (SYN-ACK)
	welcome := protocol.HandshakeSYNACKPayload{
		Status:         protocol.HandshakeStatusAcknowledged,
		Message:        protocol.DefaultWelcomeMessage,
		AssignedConnID: id,
		ClientAddr:     conn.RemoteAddr().String(),
		ServerTime:     time.Now().UnixMilli(),
	}
	payload, err := protocol.EncodeJSONPayload(welcome)
	if err != nil {
		return
	}

	synAck := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagSYN | protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: id,
			AckNum:       1,
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	}
	if err = WriteTCPFrame(conn, synAck); err != nil {
		return
	}

	// Step 4: Final Handshake Confirmation (ACK)
	// Verify that client acknowledged the assigned ID with expected sequence alignment.
	ack, err := ReadTCPFrame(conn)
	if err != nil || ack.Hdr.Flags != protocol.FlagACK || ack.Hdr.ConnectionID != id || ack.Hdr.SeqNum != 1 || ack.Hdr.AckNum != 1 {
		return
	}

	// Step 5: Graceful Shutdown Check
	// Abort if the server context was cancelled while completing the handshake.
	select {
	case <-ctx.Done():
		return
	default:
	}

	// Step 6: Session Teardown (FIN / FIN-ACK)
	// Await graceful termination signal from client before closing connection.
	fin, err := ReadTCPFrame(conn)
	if err != nil {
		return
	}
	if !fin.Hdr.HasFlag(protocol.FlagFIN) || fin.Hdr.ConnectionID != id {
		return
	}

	finAck := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagFIN | protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: id,
			SeqNum:       1,
			AckNum:       fin.Hdr.SeqNum + 1,
			WindowSize:   protocol.DefaultWindowSize,
		},
	}
	_ = WriteTCPFrame(conn, finAck)

	// Preserve hello variable reference if needed by future tracing or metrics.
	_ = hello
}
func (s *Server) reserveID() (uint32, error) {
	for {
		id, err := transport.GenerateConnectionID()
		if err != nil {
			return 0, err
		}
		s.mu.Lock()
		_, exists := s.activeIDs[id]
		if !exists {
			s.activeIDs[id] = struct{}{}
			s.mu.Unlock()
			return id, nil
		}
		s.mu.Unlock()
	}
}
func (s *Server) releaseID(id uint32) { s.mu.Lock(); delete(s.activeIDs, id); s.mu.Unlock() }
func (s *Server) sendReset(conn *net.TCPConn, id uint32) {
	_ = WriteTCPFrame(conn, &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagRST, MsgType: protocol.MsgCtrlHandshake, ConnectionID: id}})
}
