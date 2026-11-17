package rudp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
)

var (
	// ErrMuxClosed means the Mux has already stopped.
	ErrMuxClosed = errors.New("rudp: mux is closed")
	// ErrMaxClientsLimit means the configured session limit was reached.
	ErrMaxClientsLimit = errors.New("rudp: max connections limit exceeded")
)

// MuxConfig configures the UDP reactor.
type MuxConfig struct {
	DirectEndpoint     bool
	ListenAddr         string
	SocketReadBuffer   int
	SocketWriteBuffer  int
	MaxConnections     int
	SessionQueueSize   int
	HandshakeTimeout   time.Duration
	ReaperInterval     time.Duration
	SessionIdleTimeout time.Duration
	NetEm              NetEmConfig
}

// DefaultMuxConfig returns production-oriented bounded defaults.
func DefaultMuxConfig(listenAddr string) MuxConfig {
	return MuxConfig{
		ListenAddr:         listenAddr,
		SocketReadBuffer:   4 * 1024 * 1024,
		SocketWriteBuffer:  4 * 1024 * 1024,
		MaxConnections:     128,
		SessionQueueSize:   2048,
		HandshakeTimeout:   3 * time.Second,
		ReaperInterval:     time.Second,
		SessionIdleTimeout: 10 * time.Second,
	}
}

// PacketHandler receives session and application events outside the table lock.
type PacketHandler interface {
	OnSessionConnected(sess *Session)
	OnMessageReceived(sess *Session, streamID uint8, msgType uint8, payload []byte)
	OnSessionClosed(sess *Session, reason string)
}

// AckHandler receives raw ACK packets before the transport consumes them.
// Applications can use this hook to relay acknowledgements for proxied streams.
type AckHandler interface {
	OnAckReceived(sess *Session, packet *protocol.Packet)
}

// Mux is a single-socket UDP reactor with per-session actor queues.
type Mux struct {
	conn       PacketConn
	cfg        MuxConfig
	handler    PacketHandler
	sessions   map[uint32]*Session
	sessionsMu sync.RWMutex
	byRemote   map[string]uint32
	bufPool    sync.Pool
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	closed     atomic.Bool
	serveOnce  sync.Once
}

// NewMux binds a UDP socket and prepares the reactor.
func NewMux(cfg MuxConfig, handler PacketHandler) (*Mux, error) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":9000"
	}
	if cfg.MaxConnections <= 0 || cfg.SessionQueueSize <= 0 {
		defaults := DefaultMuxConfig(cfg.ListenAddr)
		if cfg.MaxConnections <= 0 {
			cfg.MaxConnections = defaults.MaxConnections
		}
		if cfg.SessionQueueSize <= 0 {
			cfg.SessionQueueSize = defaults.SessionQueueSize
		}
	}
	if cfg.ReaperInterval <= 0 {
		cfg.ReaperInterval = time.Second
	}
	if cfg.SessionIdleTimeout <= 0 {
		cfg.SessionIdleTimeout = 10 * time.Second
	}
	addr, err := net.ResolveUDPAddr("udp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("rudp: resolve listen addr: %w", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("rudp: listen udp: %w", err)
	}
	if cfg.SocketReadBuffer > 0 {
		_ = conn.SetReadBuffer(cfg.SocketReadBuffer)
	}
	if cfg.SocketWriteBuffer > 0 {
		_ = conn.SetWriteBuffer(cfg.SocketWriteBuffer)
	}
	if err := configureUDPSocket(conn); err != nil {
		slog.Debug("rudp socket resilience configuration unavailable", "error", err)
	}
	packetConn, err := NewNetEmConn(conn, cfg.NetEm)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rudp: configure network emulator: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Mux{
		conn: packetConn, cfg: cfg, handler: handler, sessions: make(map[uint32]*Session),
		byRemote: make(map[string]uint32), ctx: ctx, cancel: cancel,
	}
	m.bufPool.New = func() any { b := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload); return &b }
	return m, nil
}

// Addr returns the bound local UDP address.
func (m *Mux) Addr() net.Addr { return m.conn.LocalAddr() }

// Serve starts ingress and reaper workers until cancellation or shutdown.
func (m *Mux) Serve(ctx context.Context) error {
	started := false
	m.serveOnce.Do(func() { started = true; m.wg.Add(2); go m.ingressLoop(); go m.reaperLoop() })
	if !started && m.closed.Load() {
		return nil
	}
	select {
	case <-ctx.Done():
		return m.Shutdown(context.Background())
	case <-m.ctx.Done():
		return nil
	}
}

// GetSession performs an O(1) detached table lookup.
func (m *Mux) GetSession(connID uint32) (*Session, bool) {
	m.sessionsMu.RLock()
	sess, ok := m.sessions[connID]
	m.sessionsMu.RUnlock()
	return sess, ok
}

// ActiveSessionCount returns the current transport session count.
func (m *Mux) ActiveSessionCount() int {
	m.sessionsMu.RLock()
	defer m.sessionsMu.RUnlock()
	return len(m.sessions)
}

// CloseSession removes and closes one session idempotently.
func (m *Mux) CloseSession(connID uint32, reason string) bool {
	m.sessionsMu.Lock()
	sess, ok := m.sessions[connID]
	if ok {
		delete(m.sessions, connID)
		delete(m.byRemote, remoteKey(sess.RemoteAddr()))
	}
	m.sessionsMu.Unlock()
	if !ok {
		return false
	}
	sess.Close(reason)
	if m.handler != nil {
		safeClosed(m.handler, sess, reason)
	}
	return true
}

// Shutdown closes the socket, all sessions, and waits for reactor workers.
func (m *Mux) Shutdown(ctx context.Context) error {
	if m.closed.CompareAndSwap(false, true) {
		m.cancel()
		_ = m.conn.Close()
		m.sessionsMu.Lock()
		sessions := make([]*Session, 0, len(m.sessions))
		for id, sess := range m.sessions {
			delete(m.sessions, id)
			sessions = append(sessions, sess)
		}
		clear(m.byRemote)
		m.sessionsMu.Unlock()
		for _, sess := range sessions {
			sess.Close("SERVER_SHUTDOWN")
			if m.handler != nil {
				safeClosed(m.handler, sess, "SERVER_SHUTDOWN")
			}
		}
	}
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Mux) ingressLoop() {
	defer m.wg.Done()
	for {
		bufPtr := m.bufPool.Get().(*[]byte)
		n, addr, err := m.conn.ReadFromUDP(*bufPtr)
		if err != nil {
			m.bufPool.Put(bufPtr)
			if m.closed.Load() {
				return
			}
			if isTransientSocketError(err) {
				slog.Debug("rudp transient read failure", "error", err)
				continue
			}
			slog.Debug("rudp read failed", "error", err)
			continue
		}
		raw := append([]byte(nil), (*bufPtr)[:n]...)
		m.bufPool.Put(bufPtr)
		m.dispatch(raw, addr)
	}
}

func (m *Mux) dispatch(raw []byte, addr *net.UDPAddr) {
	pkt, err := protocol.UnmarshalPacket(raw)
	if err != nil {
		return
	}
	if pkt.Hdr.HasFlag(protocol.FlagSYN) && !pkt.Hdr.HasFlag(protocol.FlagACK) {
		m.handleSYN(addr, pkt)
		return
	}
	sess, ok := m.GetSession(pkt.Hdr.ConnectionID)
	if !ok {
		return
	}
	if m.cfg.DirectEndpoint && sess.Cipher() != nil && !pkt.Hdr.HasFlag(protocol.FlagENC) {
		return
	}
	if cipher := sess.Cipher(); cipher != nil && !pkt.Hdr.HasFlag(protocol.FlagENC) {
		plaintextControl := pkt.Hdr.MsgType == protocol.MsgAck && len(pkt.Payload) == 0
		if !plaintextControl {
			return
		}
	}
	if pkt.Hdr.HasFlag(protocol.FlagENC) {
		cipher := sess.Cipher()
		if cipher == nil {
			return
		}
		plaintext, decryptErr := cipher.DecryptPacket(pkt.Hdr, raw[:protocol.HeaderSize], pkt.Payload)
		if decryptErr != nil {
			slog.Debug("rudp decrypt packet failed", "conn_id", sess.ID(), "error", decryptErr)
			return
		}
		pkt.Payload = plaintext
	}
	sess.UpdateRemoteAddr(addr)
	if pkt.Hdr.HasFlag(protocol.FlagFIN) {
		m.CloseSession(pkt.Hdr.ConnectionID, "CLIENT_FIN")
		return
	}
	if !sess.EnqueueInbound(pkt) {
		slog.Debug("rudp inbound queue full", "conn_id", sess.ID())
	}
}

func (m *Mux) handleSYN(addr *net.UDPAddr, packets ...*protocol.Packet) {
	key := remoteKey(addr)
	m.sessionsMu.Lock()
	if oldID, exists := m.byRemote[key]; exists {
		sess := m.sessions[oldID]
		m.sessionsMu.Unlock()
		m.sendSYNACK(sess.ID(), addr, sess.HandshakePayload())
		return
	}
	if len(m.sessions) >= m.cfg.MaxConnections {
		m.sessionsMu.Unlock()
		return
	}
	id, err := transport.GenerateConnectionID()
	if err != nil {
		m.sessionsMu.Unlock()
		return
	}
	sess := NewSession(id, addr, m.cfg.SessionQueueSize, m.ctx)
	m.sessions[id] = sess
	m.byRemote[key] = id
	m.sessionsMu.Unlock()
	m.startSession(sess)
	var synAckPayload []byte
	if len(packets) > 0 && packets[0] != nil {
		synAckPayload = m.negotiateCipher(sess, packets[0].Payload, id, addr)
	}
	m.sendSYNACK(id, addr, synAckPayload)
	if m.handler != nil {
		safeConnected(m.handler, sess)
	}
}

func (m *Mux) startSession(sess *Session) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("rudp session actor panic", "conn_id", sess.ID(), "panic", recovered)
				m.CloseSession(sess.ID(), "ACTOR_PANIC")
			}
		}()
		for {
			select {
			case <-sess.Context().Done():
				return
			case pkt := <-sess.InboundChan():
				if pkt == nil {
					continue
				}
				m.processSessionPacket(sess, pkt)
			}
		}
	}()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("rudp egress actor panic", "conn_id", sess.ID(), "panic", recovered)
			}
		}()
		for {
			select {
			case <-sess.Context().Done():
				return
			case raw := <-sess.OutboundChan():
				if _, err := m.conn.WriteToUDP(raw, sess.RemoteAddr()); err != nil && !m.closed.Load() {
					slog.Debug("rudp write failed", "conn_id", sess.ID(), "error", err)
				}
			}
		}
	}()
}

// processSessionPacket applies ACKs and per-stream ordering before dispatching application data.
func (m *Mux) processSessionPacket(sess *Session, pkt *protocol.Packet) {
	stream := sess.GetStream(pkt.Hdr.StreamID)
	if pkt.Hdr.HasFlag(protocol.FlagACK) || pkt.Hdr.MsgType == protocol.MsgAck {
		_, fastRetransmit := stream.ProcessAck(pkt.Hdr.AckNum, pkt.Hdr.AckBitmap, pkt.Hdr.WindowSize)
		if m.cfg.DirectEndpoint {
			for _, segment := range fastRetransmit {
				_ = sess.RetransmitSegment(segment)
			}
		}
	}
	if pkt.Hdr.MsgType == protocol.MsgAck {
		if ackHandler, ok := m.handler.(AckHandler); ok {
			ackHandler.OnAckReceived(sess, pkt)
		}
		return
	}
	// Sequence zero is retained for the pre-Step-5 auth/presence wire messages,
	// and MsgFileChunk is relayed end-to-end so receiver SACK state drives retransmission.
	if !pkt.Hdr.HasFlag(protocol.FlagREL) || pkt.Hdr.SeqNum == 0 || (pkt.Hdr.MsgType == protocol.MsgFileChunk && !m.cfg.DirectEndpoint) {
		if m.handler != nil {
			safeMessage(m.handler, sess, pkt.Hdr.StreamID, pkt.Hdr.MsgType, pkt.Payload)
		}
		return
	}
	delivered, needAck := stream.ProcessIncomingPacket(pkt)
	if needAck || m.cfg.DirectEndpoint {
		if err := sess.SendAck(pkt.Hdr.StreamID); err != nil {
			slog.Debug("rudp ACK enqueue failed", "conn_id", sess.ID(), "error", err)
		}
	}
	for _, item := range delivered {
		if m.handler != nil {
			safeMessage(m.handler, sess, item.Hdr.StreamID, item.Hdr.MsgType, item.Payload)
		}
	}
}

func (m *Mux) sendSYNACK(id uint32, addr *net.UDPAddr, payload []byte) {
	raw, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagSYN | protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: id,
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	})
	if err == nil {
		_, _ = m.conn.WriteToUDP(raw, addr)
	}
}

func (m *Mux) negotiateCipher(sess *Session, rawPayload []byte, id uint32, addr *net.UDPAddr) []byte {
	hello, err := protocol.DecodeJSONPayload[protocol.HandshakeSYNPayload](rawPayload)
	if err != nil || hello.ClientPubKey == "" || hello.ClientNonce == "" {
		return nil
	}
	pubBytes, err := base64.StdEncoding.DecodeString(hello.ClientPubKey)
	if err != nil || len(pubBytes) != 32 {
		return nil
	}
	nonceBytes, err := base64.StdEncoding.DecodeString(hello.ClientNonce)
	if err != nil || len(nonceBytes) != 16 {
		return nil
	}
	var clientPub [32]byte
	var clientNonce [16]byte
	copy(clientPub[:], pubBytes)
	copy(clientNonce[:], nonceBytes)
	serverKeys, err := security.GenerateX25519KeyPair()
	if err != nil {
		return nil
	}
	var serverNonce [16]byte
	if _, err := rand.Read(serverNonce[:]); err != nil {
		return nil
	}
	writeKey, readKey, err := security.DeriveSessionKeys(serverKeys.PrivateKey, clientPub, clientNonce, serverNonce, id, true)
	if err != nil {
		return nil
	}
	sc, err := security.NewSessionCipher(writeKey, readKey, true)
	if err != nil {
		return nil
	}
	sess.SetCipher(sc)
	welcome, err := protocol.EncodeJSONPayload(protocol.HandshakeSYNACKPayload{
		Status:         protocol.HandshakeStatusAcknowledged,
		Message:        protocol.DefaultWelcomeMessage,
		AssignedConnID: id,
		ClientAddr:     addr.String(),
		ServerTime:     time.Now().UnixMilli(),
		ServerPubKey:   base64.StdEncoding.EncodeToString(serverKeys.PublicKey[:]),
		ServerNonce:    base64.StdEncoding.EncodeToString(serverNonce[:]),
	})
	if err != nil {
		return nil
	}
	sess.SetHandshakePayload(welcome)
	return welcome
}

func (m *Mux) reaperLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case now := <-ticker.C:
			var expired []uint32
			m.sessionsMu.RLock()
			for id, sess := range m.sessions {
				if now.Sub(sess.LastActive()) > m.cfg.SessionIdleTimeout {
					expired = append(expired, id)
				}
			}
			m.sessionsMu.RUnlock()
			for _, id := range expired {
				m.CloseSession(id, "TIMEOUT")
			}
		}
	}
}

func isTransientSocketError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "10054") || strings.Contains(text, "wsarecvfrom") || strings.Contains(text, "forcibly closed") || strings.Contains(text, "connection reset") || strings.Contains(text, "timeout")
}

func remoteKey(addr *net.UDPAddr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}
func safeConnected(h PacketHandler, s *Session) { defer recover(); h.OnSessionConnected(s) }
func safeMessage(h PacketHandler, s *Session, stream, msg uint8, payload []byte) {
	defer recover()
	h.OnMessageReceived(s, stream, msg, payload)
}
func safeClosed(h PacketHandler, s *Session, reason string) {
	defer recover()
	h.OnSessionClosed(s, reason)
}
