package rudp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// Session is the per-client actor context owned by a Mux.
type Session struct {
	id               uint32
	remoteAddr       *net.UDPAddr
	inboundCh        chan *protocol.Packet
	outboundCh       chan []byte
	closeCh          chan struct{}
	closeOnce        sync.Once
	mu               sync.RWMutex
	lastActive       time.Time
	isClosed         bool
	closeErr         string
	ctx              context.Context
	cancel           context.CancelFunc
	streamsMu        sync.RWMutex
	streams          map[uint8]*Stream
	cipherMu         sync.RWMutex
	cipher           *security.SessionCipher
	handshakeMu      sync.RWMutex
	handshakePayload []byte
}

// NewSession creates a session with bounded inbound and outbound queues.
func NewSession(id uint32, remoteAddr *net.UDPAddr, queueSize int, parentCtx context.Context) *Session {
	if queueSize < 1 {
		queueSize = 1
	}
	sessCtx, cancel := context.WithCancel(parentCtx)
	return &Session{
		id:         id,
		remoteAddr: cloneUDPAddr(remoteAddr),
		inboundCh:  make(chan *protocol.Packet, queueSize),
		outboundCh: make(chan []byte, queueSize),
		closeCh:    make(chan struct{}),
		lastActive: time.Now(),
		ctx:        sessCtx,
		cancel:     cancel,
		streams: map[uint8]*Stream{
			protocol.StreamControl:       NewStream(protocol.StreamControl, protocol.DefaultWindowSize),
			protocol.StreamBroadcastChat: NewStream(protocol.StreamBroadcastChat, protocol.DefaultWindowSize),
			protocol.StreamDirectChat:    NewStream(protocol.StreamDirectChat, protocol.DefaultWindowSize),
		},
	}
}

// ID returns the wire connection identifier.
func (s *Session) ID() uint32 { return s.id }

// RemoteAddr returns a detached copy of the current peer address.
func (s *Session) RemoteAddr() *net.UDPAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneUDPAddr(s.remoteAddr)
}

// UpdateRemoteAddr records the latest peer address and activity timestamp.
func (s *Session) UpdateRemoteAddr(addr *net.UDPAddr) {
	s.mu.Lock()
	s.remoteAddr = cloneUDPAddr(addr)
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// Touch refreshes the session activity timestamp.
func (s *Session) Touch() {
	s.mu.Lock()
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// LastActive returns the last activity time.
func (s *Session) LastActive() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastActive
}

// Context returns the session lifetime context.
func (s *Session) Context() context.Context { return s.ctx }

// SetCipher installs the authenticated encryption context for this session.
func (s *Session) SetCipher(cipher *security.SessionCipher) {
	s.cipherMu.Lock()
	s.cipher = cipher
	s.cipherMu.Unlock()
}

// Cipher returns the current authenticated encryption context, if negotiated.
func (s *Session) Cipher() *security.SessionCipher {
	s.cipherMu.RLock()
	defer s.cipherMu.RUnlock()
	return s.cipher
}

// HasCipher reports whether this session negotiated payload encryption.
func (s *Session) HasCipher() bool { return s.Cipher() != nil }

// SetHandshakePayload stores the SYN-ACK crypto metadata for idempotent retries.
func (s *Session) SetHandshakePayload(payload []byte) {
	s.handshakeMu.Lock()
	s.handshakePayload = append([]byte(nil), payload...)
	s.handshakeMu.Unlock()
}

// HandshakePayload returns a detached copy of the last SYN-ACK crypto metadata.
func (s *Session) HandshakePayload() []byte {
	s.handshakeMu.RLock()
	defer s.handshakeMu.RUnlock()
	return append([]byte(nil), s.handshakePayload...)
}

// InboundChan exposes the receive-only actor queue.
func (s *Session) InboundChan() <-chan *protocol.Packet { return s.inboundCh }

// OutboundChan exposes the receive-only egress queue.
func (s *Session) OutboundChan() <-chan []byte { return s.outboundCh }

// OutboundQueue exposes the writable queue to trusted server integrations.
func (s *Session) OutboundQueue() chan []byte { return s.outboundCh }

// CloseChan signals that the session has been closed.
func (s *Session) CloseChan() <-chan struct{} { return s.closeCh }

// EnqueueInbound adds a packet without ever blocking the ingress reader.
func (s *Session) EnqueueInbound(pkt *protocol.Packet) bool {
	s.mu.RLock()
	closed := s.isClosed
	s.mu.RUnlock()
	if closed {
		return false
	}
	select {
	case s.inboundCh <- pkt:
		s.Touch()
		return true
	default:
		return false
	}
}

// EnqueueOutbound adds raw data without allowing a slow consumer to block peers.
func (s *Session) EnqueueOutbound(rawPacket []byte) bool {
	s.mu.RLock()
	closed := s.isClosed
	s.mu.RUnlock()
	if closed {
		return false
	}
	select {
	case s.outboundCh <- rawPacket:
		return true
	default:
		return false
	}
}

// GetStream returns the lazily-created reliable stream for streamID.
func (s *Session) GetStream(streamID uint8) *Stream {
	s.streamsMu.RLock()
	stream := s.streams[streamID]
	s.streamsMu.RUnlock()
	if stream != nil {
		return stream
	}
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	if stream = s.streams[streamID]; stream == nil {
		stream = NewStream(streamID, protocol.DefaultWindowSize)
		s.streams[streamID] = stream
	}
	return stream
}

// SendReliable assigns a per-stream sequence number and queues a reliable packet.
func (s *Session) SendReliable(streamID uint8, msgType uint8, payload []byte) error {
	stream := s.GetStream(streamID)
	seq := stream.NextSeqNum()
	ackNum, ackBitmap, window := stream.ComputeAckState()
	pkt := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD | protocol.FlagACK,
			MsgType:      msgType,
			StreamID:     streamID,
			ConnectionID: s.ID(),
			SeqNum:       seq,
			AckNum:       ackNum,
			AckBitmap:    ackBitmap,
			WindowSize:   window,
		},
		Payload: append([]byte(nil), payload...),
	}
	var raw []byte
	var err error
	if cipher := s.Cipher(); cipher != nil {
		pkt.Hdr.Flags |= protocol.FlagENC
		raw, err = cipher.EncryptPacket(pkt.Hdr, pkt.Payload)
	} else {
		raw, err = protocol.MarshalPacket(pkt)
	}
	if err != nil {
		return fmt.Errorf("rudp: encode reliable packet: %w", err)
	}
	stream.RecordSent(pkt, raw)
	if !s.EnqueueOutbound(raw) {
		return fmt.Errorf("rudp: session %d outbound queue full", s.ID())
	}
	return nil
}

// SendAck queues an ACK-only packet without consuming the reliable send sequence.
func (s *Session) SendAck(streamID uint8) error {
	stream := s.GetStream(streamID)
	ackNum, ackBitmap, window := stream.ComputeAckState()
	pkt := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgAck,
			StreamID:     streamID,
			ConnectionID: s.ID(),
			AckNum:       ackNum,
			AckBitmap:    ackBitmap,
			WindowSize:   window,
		},
	}
	raw, err := protocol.MarshalPacket(pkt)
	if err != nil {
		return fmt.Errorf("rudp: marshal ACK: %w", err)
	}
	if !s.EnqueueOutbound(raw) {
		return fmt.Errorf("rudp: session %d ACK queue full", s.ID())
	}
	return nil
}

// IsClosed reports whether Close has completed its state transition.
func (s *Session) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isClosed
}

// Close marks the session closed and cancels all actor work exactly once.
func (s *Session) Close(reason string) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.isClosed = true
		s.closeErr = reason
		s.mu.Unlock()
		s.cancel()
		close(s.closeCh)
	})
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	copyAddr := *addr
	copyAddr.IP = append(net.IP(nil), addr.IP...)
	return &copyAddr
}
