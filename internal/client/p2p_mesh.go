package client

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/events"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// PeerLink owns one encrypted outgoing RUDP session and its socket.
type PeerLink struct {
	peer      protocol.PeerDTO
	conn      rudp.PacketConn
	session   *rudp.Session
	cancel    context.CancelFunc
	ready     chan struct{}
	readyOnce sync.Once
}

// peerUpload retains the source and stream until the receiver confirms completion.
type peerUpload struct {
	uploader *transfer.Uploader
	streamID uint8
	peer     string
}

// peerDownload retains the source peer for cancellation and cleanup.
type peerDownload struct {
	downloader *transfer.Downloader
	streamID   uint8
	peer       string
}

// PeerMesh runs a separate peer listener and outgoing encrypted peer sessions.
type PeerMesh struct {
	mux         *rudp.Mux
	store       *state.ClientStore
	bus         *events.Bus
	stagingDir  string
	downloadDir string
	mu          sync.Mutex
	username    string
	ticket      string
	verifyKey   string
	links       map[string]*PeerLink
	inbound     map[uint32]string
	uploads     map[string]*peerUpload
	downloads   map[string]*peerDownload
	streamPool  *rudp.StreamPool
	closed      bool
}

// NewPeerMesh binds the peer listener before login so its port can be advertised.
func NewPeerMesh(addr, stagingDir, downloadDir string, store *state.ClientStore, bus *events.Bus) (*PeerMesh, error) {
	if addr == "" {
		addr = "0.0.0.0:0"
	}
	m := &PeerMesh{
		store:       store,
		bus:         bus,
		stagingDir:  stagingDir,
		downloadDir: downloadDir,
		links:       make(map[string]*PeerLink),
		inbound:     make(map[uint32]string),
		uploads:     make(map[string]*peerUpload),
		downloads:   make(map[string]*peerDownload),
		streamPool:  rudp.NewStreamPool(),
	}
	cfg := rudp.DefaultMuxConfig(addr)
	cfg.DirectEndpoint = true
	mux, err := rudp.NewMux(cfg, m)
	if err != nil {
		return nil, err
	}
	m.mux = mux
	go func() { _ = mux.Serve(context.Background()) }()
	return m, nil
}

// P2PPort returns the bound UDP listener port.
func (m *PeerMesh) P2PPort() int {
	return m.mux.Addr().(*net.UDPAddr).Port
}

// SetIdentity binds this mesh to the authenticated user and server trust key.
func (m *PeerMesh) SetIdentity(username, ticket, verifyKey string) {
	m.mu.Lock()
	m.username = username
	m.ticket = ticket
	m.verifyKey = verifyKey
	m.mu.Unlock()
}

// SyncPeers eagerly connects to all advertised peers in the background.
func (m *PeerMesh) SyncPeers(peers []protocol.PeerDTO) {
	m.mu.Lock()
	username := m.username
	m.mu.Unlock()
	for _, peer := range peers {
		if peer.Username == username || username >= peer.Username || peer.P2PPort <= 0 || peer.Status != "ONLINE" {
			continue
		}
		m.mu.Lock()
		_, linked := m.links[peer.Username]
		m.mu.Unlock()
		if linked {
			continue
		}
		go func(name string) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			_, _ = m.EnsurePeerLink(ctx, name)
		}(peer.Username)
	}
}

// EnsurePeerLink establishes an encrypted outbound link to a known peer.
func (m *PeerMesh) EnsurePeerLink(ctx context.Context, username string) (*PeerLink, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		link, err := m.ensurePeerLinkOnce(ctx, username)
		if err == nil {
			return link, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, lastErr
}

func (m *PeerMesh) ensurePeerLinkOnce(ctx context.Context, username string) (*PeerLink, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("peer mesh: closed")
	}
	if link := m.links[username]; link != nil {
		m.mu.Unlock()
		return m.awaitReady(ctx, username, link)
	}
	m.mu.Unlock()
	peer, ok := m.store.GetPeer(username)
	if !ok || peer.P2PPort <= 0 || peer.Status != "ONLINE" {
		return nil, fmt.Errorf("peer mesh: %s unavailable", username)
	}
	addr := net.JoinHostPort(peer.IP, strconv.Itoa(peer.P2PPort))
	network, err := dialPersistentSession(ctx, addr, 3*time.Second, rudp.NetEmConfig{})
	if err != nil {
		return nil, err
	}
	linkCtx, cancel := context.WithCancel(context.Background())
	sess := rudp.NewSession(network.ConnectionID, network.Server, 2048, linkCtx)
	sess.SetCipher(network.Cipher)
	link := &PeerLink{
		peer:    peer,
		conn:    network.Conn,
		session: sess,
		cancel:  cancel,
		ready:   make(chan struct{}),
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		cancel()
		_ = network.Conn.Close()
		return nil, fmt.Errorf("peer mesh: closed")
	}
	if previous := m.links[username]; previous != nil {
		m.mu.Unlock()
		cancel()
		_ = network.Conn.Close()
		return m.awaitReady(ctx, username, previous)
	}
	m.links[username] = link
	local := m.username
	ticket := m.ticket
	m.mu.Unlock()
	go m.runOutgoing(link)
	hello := protocol.PeerHelloPayload{
		Username:   local,
		P2PPort:    m.P2PPort(),
		PeerTicket: ticket,
		Timestamp:  time.Now().UnixMilli(),
	}
	if err := m.send(link, protocol.StreamControl, protocol.MsgPeerHello, hello); err != nil {
		m.dropLink(username, link)
		return nil, err
	}
	return m.awaitReady(ctx, username, link)
}

// awaitReady bounds the identity handshake even when the caller has a long deadline.
func (m *PeerMesh) awaitReady(ctx context.Context, username string, link *PeerLink) (*PeerLink, error) {
	timer := time.NewTimer(4 * time.Second)
	defer timer.Stop()
	select {
	case <-link.ready:
		return link, nil
	case <-ctx.Done():
		m.dropLink(username, link)
		return nil, ctx.Err()
	case <-timer.C:
		m.dropLink(username, link)
		return nil, fmt.Errorf("peer mesh: identity handshake timed out")
	case <-link.session.Context().Done():
		m.dropLink(username, link)
		return nil, fmt.Errorf("peer mesh: link closed")
	}
}

// runOutgoing owns both socket directions and the retransmission and heartbeat timers.
func (m *PeerMesh) runOutgoing(link *PeerLink) {
	defer m.dropLink(link.peer.Username, link)
	go func() {
		for {
			select {
			case <-link.session.Context().Done():
				return
			case raw := <-link.session.OutboundChan():
				if _, err := link.conn.WriteToUDP(raw, link.session.RemoteAddr()); err != nil {
					return
				}
			}
		}
	}()
	go func() {
		retry := time.NewTicker(50 * time.Millisecond)
		ping := time.NewTicker(3 * time.Second)
		defer retry.Stop()
		defer ping.Stop()
		for {
			select {
			case <-link.session.Context().Done():
				return
			case <-retry.C:
				for _, stream := range link.session.StreamsSnapshot() {
					for _, segment := range stream.GetExpiredSegments(500 * time.Millisecond) {
						_ = link.session.RetransmitSegment(segment)
					}
				}
			case <-ping.C:
				_ = m.send(link, protocol.StreamControl, protocol.MsgPing, protocol.PingPayload{
					ClientTime: time.Now().UnixMilli(),
				})
				if time.Since(link.session.LastActive()) > 10*time.Second {
					link.cancel()
					_ = link.conn.Close()
					return
				}
			}
		}
	}()
	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	for {
		_ = link.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, remote, err := link.conn.ReadFromUDP(buf)
		if err != nil {
			if link.session.Context().Err() != nil {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		pkt, err := protocol.UnmarshalPacket(buf[:n])
		if err != nil || remote == nil || !remote.IP.Equal(link.session.RemoteAddr().IP) || remote.Port != link.session.RemoteAddr().Port || pkt.Hdr.ConnectionID != link.session.ID() || !pkt.Hdr.HasFlag(protocol.FlagENC) {
			continue
		}
		if pkt.Hdr.HasFlag(protocol.FlagENC) {
			pkt.Payload, err = link.session.Cipher().DecryptPacket(pkt.Hdr, buf[:protocol.HeaderSize], pkt.Payload)
			if err != nil {
				continue
			}
		}
		link.session.Touch()
		stream := link.session.GetStream(pkt.Hdr.StreamID)
		if pkt.Hdr.HasFlag(protocol.FlagACK) {
			_, fast := stream.ProcessAck(pkt.Hdr.AckNum, pkt.Hdr.AckBitmap, pkt.Hdr.WindowSize)
			for _, segment := range fast {
				_ = link.session.RetransmitSegment(segment)
			}
		}
		if pkt.Hdr.MsgType == protocol.MsgAck {
			continue
		}
		if pkt.Hdr.HasFlag(protocol.FlagREL) && pkt.Hdr.SeqNum > 0 {
			items, _ := stream.ProcessIncomingPacket(pkt)
			_ = link.session.SendAck(pkt.Hdr.StreamID)
			for _, item := range items {
				m.handle(link.peer.Username, link.session, item.Hdr.StreamID, item.Hdr.MsgType, item.Payload)
			}
		} else {
			m.handle(link.peer.Username, link.session, pkt.Hdr.StreamID, pkt.Hdr.MsgType, pkt.Payload)
		}
	}
}

func (m *PeerMesh) dropLink(username string, link *PeerLink) {
	m.mu.Lock()
	if m.links[username] != link {
		m.mu.Unlock()
		return
	}
	delete(m.links, username)
	m.mu.Unlock()
	link.cancel()
	_ = link.conn.Close()
}

func (m *PeerMesh) send(link *PeerLink, streamID, msgType uint8, value any) error {
	payload, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return err
	}
	return link.session.SendReliable(streamID, msgType, payload)
}

// SendDirect sends and records a peer message using the established mesh.
func (m *PeerMesh) SendDirect(ctx context.Context, id, recipient, content string) error {
	link, err := m.EnsurePeerLink(ctx, recipient)
	if err != nil {
		return err
	}
	user := m.store.CurrentUser()
	if user == nil {
		return ErrClientNotAuthenticated
	}
	message := protocol.UnicastMessageServerPayload{
		Scope:         "UNICAST",
		MsgID:         id,
		ClientMsgID:   id,
		Sender:        user.Username,
		SenderDisplay: user.DisplayName,
		Recipient:     recipient,
		Content:       content,
		ServerTime:    time.Now().UnixMilli(),
	}
	if err := m.send(link, protocol.StreamDirectChat, protocol.MsgMessUnicast, message); err != nil {
		return err
	}
	m.store.AppendDirectMessage(message)
	m.bus.Publish("chat.message_received", message)
	return nil
}

// SendBroadcast fans out one room message to every online P2P peer.
func (m *PeerMesh) SendBroadcast(ctx context.Context, id, content string) error {
	user := m.store.CurrentUser()
	if user == nil {
		return ErrClientNotAuthenticated
	}
	message := protocol.BroadcastMessageServerPayload{
		Scope:         "BROADCAST",
		MsgID:         id,
		ClientMsgID:   id,
		Sender:        user.Username,
		SenderDisplay: user.DisplayName,
		Content:       content,
		ServerTime:    time.Now().UnixMilli(),
	}
	for _, peer := range m.store.GetOnlinePeers() {
		if peer.Username == user.Username || peer.P2PPort <= 0 || peer.Status != "ONLINE" {
			continue
		}
		link, err := m.EnsurePeerLink(ctx, peer.Username)
		if err != nil {
			return err
		}
		if err := m.send(link, protocol.StreamBroadcastChat, protocol.MsgMessBroadcast, message); err != nil {
			return err
		}
	}
	m.store.AppendBroadcastMessage(message)
	m.bus.Publish("chat.message_received", message)
	return nil
}

// SendFileNamed begins a peer file upload and returns its correlation ID.
func (m *PeerMesh) SendFileNamed(ctx context.Context, recipient, filePath, displayName, caption string) (string, error) {
	link, err := m.EnsurePeerLink(ctx, recipient)
	if err != nil {
		return "", err
	}
	id, _, err := protocol.GenerateUUIDv4()
	if err != nil {
		return "", err
	}
	uploader, init, err := transfer.NewUploader(id, filePath, protocol.MaxFileChunkDataSize)
	if err != nil {
		return "", err
	}
	streamID, err := m.streamPool.Acquire()
	if err != nil {
		_ = uploader.Close()
		return "", err
	}
	user := m.store.CurrentUser()
	init.Sender = user.Username
	init.Recipient = recipient
	if displayName != "" {
		init.FileName = transfer.SanitizeFileName(displayName)
	}
	init.Caption = caption
	m.mu.Lock()
	m.uploads[id] = &peerUpload{
		uploader: uploader,
		streamID: streamID,
		peer:     recipient,
	}
	m.mu.Unlock()
	m.store.UpdateFileProgress(&transfer.TransferProgress{
		TransferID: id,
		Direction:  "UPLOAD",
		Peer:       recipient,
		FileName:   init.FileName,
		FileSize:   init.FileSize,
		State:      "INIT",
		CreatedAt:  init.CreatedAt,
	})
	if err := m.send(link, streamID, protocol.MsgFileInit, init); err != nil {
		m.finishUpload(id)
		return "", err
	}
	go m.watchFileAcceptance(id)
	return id, nil
}

// watchFileAcceptance fails an upload if its peer never accepts the request.
func (m *PeerMesh) watchFileAcceptance(id string) {
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	<-timer.C
	progress, ok := m.store.GetFileProgress(id)
	if !ok || progress.State != "INIT" {
		return
	}
	m.finishUpload(id)
	m.failFile(id, "Peer did not accept the transfer")
}

func (m *PeerMesh) failFile(id, message string) {
	if existing, ok := m.store.GetFileProgress(id); ok && (existing.State == "COMPLETED" || existing.State == "CANCELLED") {
		return
	}
	progress := &transfer.TransferProgress{
		TransferID:   id,
		State:        "FAILED",
		ErrorMessage: message,
	}
	m.publishFile("file.failed", progress)
}

// publishFile sends the merged store entry so SSE events retain file metadata.
func (m *PeerMesh) publishFile(name string, progress *transfer.TransferProgress) {
	m.store.UpdateFileProgress(progress)
	if current, ok := m.store.GetFileProgress(progress.TransferID); ok {
		m.bus.Publish(name, current)
	}
}

// CancelTransfer stops a local upload or download and informs its peer.
func (m *PeerMesh) CancelTransfer(ctx context.Context, id string) error {
	m.mu.Lock()
	upload := m.uploads[id]
	download := m.downloads[id]
	m.mu.Unlock()
	if upload == nil && download == nil {
		return fmt.Errorf("peer mesh: transfer not found")
	}
	peer := ""
	streamID := uint8(0)
	if upload != nil {
		peer = upload.peer
		streamID = upload.streamID
	} else {
		peer = download.peer
		streamID = download.streamID
	}
	link, err := m.EnsurePeerLink(ctx, peer)
	if err != nil {
		return err
	}
	err = m.send(link, streamID, protocol.MsgFileCancel, protocol.FileCancelPayload{
		TransferID: id,
		Reason:     "CANCELLED",
		Timestamp:  time.Now().UnixMilli(),
	})
	m.finishUpload(id)
	if download != nil {
		m.mu.Lock()
		delete(m.downloads, id)
		m.mu.Unlock()
		_ = download.downloader.Cleanup()
	}
	m.publishFile("file.failed", &transfer.TransferProgress{
		TransferID: id,
		State:      "CANCELLED",
	})
	return err
}

func (m *PeerMesh) finishUpload(id string) {
	m.mu.Lock()
	upload := m.uploads[id]
	delete(m.uploads, id)
	m.mu.Unlock()
	if upload != nil {
		_ = upload.uploader.Close()
		_ = m.streamPool.Release(upload.streamID)
	}
}

// OnSessionConnected waits for a verified peer hello before accepting traffic.
func (m *PeerMesh) OnSessionConnected(_ *rudp.Session) {}

// OnMessageReceived dispatches authenticated peer application messages.
func (m *PeerMesh) OnMessageReceived(sess *rudp.Session, streamID, msgType uint8, payload []byte) {
	m.mu.Lock()
	username := m.inbound[sess.ID()]
	m.mu.Unlock()
	if msgType == protocol.MsgPeerHello {
		hello, err := protocol.DecodeJSONPayload[protocol.PeerHelloPayload](payload)
		if err != nil {
			return
		}
		peer, ok := m.store.GetPeer(hello.Username)
		m.mu.Lock()
		verifyKey := m.verifyKey
		localUsername := m.username
		m.mu.Unlock()
		if localUsername == "" || !ok || peer.Status != "ONLINE" || peer.P2PPort != hello.P2PPort || peer.IP != sess.RemoteAddr().IP.String() || absMillis(time.Now().UnixMilli()-hello.Timestamp) > 30_000 || !verifyPeerTicket(peer, hello.PeerTicket, verifyKey) {
			return
		}
		m.mu.Lock()
		m.inbound[sess.ID()] = hello.Username
		m.mu.Unlock()
		_ = sess.SendReliable(protocol.StreamControl, protocol.MsgPeerHelloAck, payload)
		return
	}
	if username == "" {
		return
	}
	m.handle(username, sess, streamID, msgType, payload)
}

func absMillis(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

// verifyPeerTicket checks the signed roster metadata before accepting identity.
func verifyPeerTicket(peer protocol.PeerDTO, ticket, verifyKey string) bool {
	if verifyKey == "" {
		return ticket == peer.PeerTicket
	}
	if ticket == "" || ticket != peer.PeerTicket {
		return false
	}
	key, err := base64.StdEncoding.DecodeString(verifyKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return false
	}
	signature, err := base64.StdEncoding.DecodeString(ticket)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return false
	}
	claim := []byte(fmt.Sprintf("%s|%s|%d|%d", peer.Username, peer.IP, peer.P2PPort, peer.LoginAt))
	return ed25519.Verify(ed25519.PublicKey(key), claim, signature)
}

// OnSessionClosed updates presence when the peer listener loses a session.
func (m *PeerMesh) OnSessionClosed(sess *rudp.Session, _ string) {
	m.mu.Lock()
	username := m.inbound[sess.ID()]
	delete(m.inbound, sess.ID())
	stillConnected := false
	for _, name := range m.inbound {
		if name == username {
			stillConnected = true
			break
		}
	}
	m.mu.Unlock()
	if username != "" && !stillConnected {
		m.store.UpdatePeerStatus(username, "OFFLINE")
		m.bus.Publish("presence.updated", map[string]any{"peers": m.store.GetOnlinePeers()})
	}
}

func (m *PeerMesh) handle(username string, sess *rudp.Session, streamID, msgType uint8, payload []byte) {
	switch msgType {
	case protocol.MsgPeerHelloAck:
		m.mu.Lock()
		link := m.links[username]
		m.mu.Unlock()
		if link != nil && link.session == sess {
			link.readyOnce.Do(func() { close(link.ready) })
		}
		return
	case protocol.MsgPeerBye:
		m.store.UpdatePeerStatus(username, "OFFLINE")
		m.bus.Publish("presence.updated", map[string]any{"peers": m.store.GetOnlinePeers()})
	case protocol.MsgPing:
		ping, err := protocol.DecodeJSONPayload[protocol.PingPayload](payload)
		if err == nil {
			pong, _ := protocol.EncodeJSONPayload(protocol.PongPayload{
				ClientTime: ping.ClientTime,
				ServerTime: time.Now().UnixMilli(),
			})
			_ = sess.SendReliable(protocol.StreamControl, protocol.MsgPong, pong)
		}
	case protocol.MsgPong:
		return
	case protocol.MsgMessBroadcast:
		message, err := protocol.DecodeJSONPayload[protocol.BroadcastMessageServerPayload](payload)
		if err == nil && message.Sender == username {
			m.store.AppendBroadcastMessage(message)
			m.bus.Publish("chat.message_received", message)
		}
	case protocol.MsgMessUnicast:
		message, err := protocol.DecodeJSONPayload[protocol.UnicastMessageServerPayload](payload)
		m.mu.Lock()
		localUsername := m.username
		m.mu.Unlock()
		if err == nil && message.Sender == username && message.Recipient == localUsername {
			m.store.AppendDirectMessage(message)
			m.bus.Publish("chat.message_received", message)
		}
	case protocol.MsgFileInit:
		m.receiveFileInit(username, streamID, payload)
	case protocol.MsgFileResponse:
		m.receiveFileResponse(username, payload)
	case protocol.MsgFileChunk:
		m.receiveFileChunk(username, streamID, payload)
	case protocol.MsgFileComplete:
		complete, err := protocol.DecodeJSONPayload[protocol.FileCompletePayload](payload)
		if err == nil {
			m.mu.Lock()
			upload := m.uploads[complete.TransferID]
			m.mu.Unlock()
			if upload == nil || upload.peer != username {
				return
			}
			m.finishUpload(complete.TransferID)
			progress := &transfer.TransferProgress{
				TransferID: complete.TransferID,
				State:      "COMPLETED",
				Percentage: 100,
			}
			m.publishFile("file.completed", progress)
		}
	case protocol.MsgFileCancel:
		cancel, err := protocol.DecodeJSONPayload[protocol.FileCancelPayload](payload)
		if err == nil {
			m.mu.Lock()
			upload := m.uploads[cancel.TransferID]
			download := m.downloads[cancel.TransferID]
			m.mu.Unlock()
			if (upload == nil || upload.peer != username) && (download == nil || download.peer != username) {
				return
			}
			m.finishUpload(cancel.TransferID)
			m.mu.Lock()
			delete(m.downloads, cancel.TransferID)
			m.mu.Unlock()
			if download != nil {
				_ = download.downloader.Cleanup()
			}
			m.publishFile("file.failed", &transfer.TransferProgress{
				TransferID: cancel.TransferID,
				State:      "CANCELLED",
			})
		}
	}
}

func (m *PeerMesh) receiveFileInit(username string, streamID uint8, payload []byte) {
	init, err := protocol.DecodeJSONPayload[protocol.FileInitPayload](payload)
	m.mu.Lock()
	localUsername := m.username
	m.mu.Unlock()
	if err != nil || init.Sender != username || init.Recipient != localUsername {
		return
	}
	d, err := transfer.NewDownloader(m.stagingDir, m.downloadDir, &init)
	response := protocol.FileResponsePayload{
		TransferID: init.TransferID,
		Accepted:   err == nil,
		Timestamp:  time.Now().UnixMilli(),
	}
	if err == nil {
		m.mu.Lock()
		m.downloads[init.TransferID] = &peerDownload{
			downloader: d,
			streamID:   streamID,
			peer:       username,
		}
		m.mu.Unlock()
		progress := &transfer.TransferProgress{
			TransferID: init.TransferID,
			Direction:  "DOWNLOAD",
			Peer:       username,
			FileName:   init.FileName,
			FileSize:   init.FileSize,
			State:      "INIT",
			CreatedAt:  init.CreatedAt,
		}
		m.publishFile("file.progress", progress)
	} else {
		response.Message = err.Error()
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if link, linkErr := m.EnsurePeerLink(ctx, username); linkErr == nil {
			_ = m.send(link, streamID, protocol.MsgFileResponse, response)
		}
	}()
}

func (m *PeerMesh) receiveFileResponse(username string, payload []byte) {
	response, err := protocol.DecodeJSONPayload[protocol.FileResponsePayload](payload)
	if err != nil {
		return
	}
	m.mu.Lock()
	upload := m.uploads[response.TransferID]
	m.mu.Unlock()
	if upload == nil || upload.peer != username {
		return
	}
	if !response.Accepted {
		m.finishUpload(response.TransferID)
		m.failFile(response.TransferID, response.Message)
		return
	}
	m.store.UpdateFileProgress(&transfer.TransferProgress{
		TransferID: response.TransferID,
		State:      "TRANSFERRING",
	})
	go m.streamFile(response.TransferID, upload)
}

// streamFile limits in-flight chunks to the receiver window while ACKs drain.
func (m *PeerMesh) streamFile(id string, upload *peerUpload) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	link, err := m.EnsurePeerLink(ctx, upload.peer)
	if err != nil {
		m.finishUpload(id)
		m.failFile(id, err.Error())
		return
	}
	stream := link.session.GetStream(upload.streamID)
	for index := uint32(0); ; index++ {
		for stream.InFlightCount() >= 48 {
			if ctx.Err() != nil {
				m.finishUpload(id)
				m.failFile(id, ctx.Err().Error())
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		data, err := upload.uploader.ReadChunk(index)
		if err != nil {
			m.finishUpload(id)
			m.failFile(id, err.Error())
			return
		}
		if len(data) == 0 && index > 0 {
			break
		}
		idBytes, err := protocol.ParseUUID(id)
		if err != nil {
			m.finishUpload(id)
			m.failFile(id, err.Error())
			return
		}
		payload, err := protocol.MarshalFileChunk(&protocol.FileChunkPacket{
			TransferID: idBytes,
			ChunkIndex: index,
			Data:       data,
		})
		if err != nil || link.session.SendReliable(upload.streamID, protocol.MsgFileChunk, payload) != nil {
			m.finishUpload(id)
			m.failFile(id, "Unable to send file chunk")
			return
		}
		progress := &transfer.TransferProgress{
			TransferID:       id,
			State:            "TRANSFERRING",
			TransferredBytes: int64(index+1) * int64(protocol.MaxFileChunkDataSize),
		}
		m.store.UpdateFileProgress(progress)
		if index%64 == 0 {
			m.publishFile("file.progress", progress)
		}
	}
}

func (m *PeerMesh) receiveFileChunk(username string, streamID uint8, payload []byte) {
	chunk, err := protocol.UnmarshalFileChunk(payload)
	if err != nil {
		return
	}
	id := protocol.FormatUUID(chunk.TransferID)
	m.mu.Lock()
	download := m.downloads[id]
	m.mu.Unlock()
	if download == nil || download.peer != username {
		return
	}
	d := download.downloader
	complete, err := d.WriteChunk(chunk.ChunkIndex, chunk.Data)
	if err != nil {
		return
	}
	progress := &transfer.TransferProgress{
		TransferID:       id,
		State:            "TRANSFERRING",
		TransferredBytes: d.BytesWritten(),
		Percentage:       d.Progress(),
	}
	m.store.UpdateFileProgress(progress)
	if chunk.ChunkIndex%64 == 0 {
		m.publishFile("file.progress", progress)
	}
	if !complete {
		return
	}
	checksum, err := d.FinalizeAndVerify()
	m.mu.Lock()
	delete(m.downloads, id)
	m.mu.Unlock()
	if err != nil {
		m.publishFile("file.failed", &transfer.TransferProgress{
			TransferID: id,
			State:      "ERROR",
		})
		return
	}
	completed := &transfer.TransferProgress{
		TransferID:       id,
		State:            "COMPLETED",
		Percentage:       100,
		TransferredBytes: d.BytesWritten(),
		LocalPath:        d.DownloadPath(),
	}
	m.publishFile("file.completed", completed)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if link, linkErr := m.EnsurePeerLink(ctx, username); linkErr == nil {
			_ = m.send(link, streamID, protocol.MsgFileComplete, protocol.FileCompletePayload{
				TransferID:         id,
				VerifiedSHA256:     checksum,
				TotalBytesReceived: d.BytesWritten(),
				CompletedAt:        time.Now().UnixMilli(),
			})
		}
	}()
}

// Shutdown announces logout and releases the independent peer socket.
func (m *PeerMesh) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	username := m.username
	links := make([]*PeerLink, 0, len(m.links))
	for _, link := range m.links {
		links = append(links, link)
	}
	m.mu.Unlock()
	for _, link := range links {
		bye, err := protocol.EncodeJSONPayload(protocol.PeerByePayload{
			Username:  username,
			Reason:    "LOGOUT",
			Timestamp: time.Now().UnixMilli(),
		})
		if err == nil {
			hdr := protocol.Header{
				Version:      protocol.ProtocolVersion,
				MsgType:      protocol.MsgPeerBye,
				StreamID:     protocol.StreamControl,
				ConnectionID: link.session.ID(),
			}
			raw, sealErr := link.session.Cipher().EncryptPacket(hdr, bye)
			if sealErr == nil {
				_, _ = link.conn.WriteToUDP(raw, link.session.RemoteAddr())
			}
		}
		link.cancel()
		_ = link.conn.Close()
	}
	return m.mux.Shutdown(ctx)
}
