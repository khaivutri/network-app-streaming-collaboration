package client

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/events"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// ChatClient sends and receives Step 5 messages on one authenticated UDP socket.
type ChatClient struct {
	conn              rudp.PacketConn
	server            *net.UDPAddr
	connID            uint32
	store             *state.ClientStore
	mu                sync.Mutex
	streams           map[uint8]*rudp.Stream
	streamPool        *rudp.StreamPool
	transferMgr       *transfer.Manager
	transferStreams   map[string]uint8
	progressBars      map[string]*transfer.ProgressBar
	pingSeq           atomic.Uint32
	lastServerActive  time.Time
	rto               *rudp.AdaptiveRTO
	lastError         string
	cipherMu          sync.RWMutex
	cipher            *security.SessionCipher
	eventBus          *events.Bus
	progressPublished map[string]time.Time
	stagingDir        string
	downloadDir       string
}

// SetCipher enables directional encryption for chat and file traffic.
func (c *ChatClient) SetCipher(cipher *security.SessionCipher) {
	c.cipherMu.Lock()
	c.cipher = cipher
	c.cipherMu.Unlock()
}

func (c *ChatClient) getCipher() *security.SessionCipher {
	c.cipherMu.RLock()
	defer c.cipherMu.RUnlock()
	return c.cipher
}

const clientSocketBufferSize = 4 * 1024 * 1024
const fileAcceptanceTimeout = 10 * time.Second

// NewChatClient creates a chat facade over an existing authenticated socket.
func NewChatClient(conn rudp.PacketConn, server *net.UDPAddr, connectionID uint32, store *state.ClientStore) *ChatClient {
	return NewChatClientWithStorage(conn, server, connectionID, store, filepath.Join("storage", "staging"), filepath.Join("storage", "downloads"))
}

// NewChatClientWithStorage creates a chat facade with explicit transfer directories.
func NewChatClientWithStorage(conn rudp.PacketConn, server *net.UDPAddr, connectionID uint32, store *state.ClientStore, stagingDir, downloadDir string) *ChatClient {
	if conn != nil {
		_ = conn.SetReadBuffer(clientSocketBufferSize)
		_ = conn.SetWriteBuffer(clientSocketBufferSize)
	}
	return &ChatClient{
		conn:              conn,
		server:            server,
		connID:            connectionID,
		store:             store,
		streams:           make(map[uint8]*rudp.Stream),
		streamPool:        rudp.NewStreamPool(),
		transferMgr:       transfer.NewManager(stagingDir, downloadDir),
		transferStreams:   make(map[string]uint8),
		progressBars:      make(map[string]*transfer.ProgressBar),
		progressPublished: make(map[string]time.Time),
		rto:               rudp.NewAdaptiveRTO(rudp.DefaultRTOConfig()),
		lastServerActive:  time.Now(),
		stagingDir:        stagingDir,
		downloadDir:       downloadDir,
	}
}

// SetEventBus connects network state changes to the local UI event stream.
func (c *ChatClient) SetEventBus(bus *events.Bus) { c.eventBus = bus }

// SendFile starts an outgoing file transfer after hashing the source file.
func (c *ChatClient) SendFile(recipient, filePath, caption string) (string, error) {
	return c.SendFileNamed(recipient, filePath, "", caption)
}

// SendFileNamed starts an outgoing transfer and preserves the caller's display name.
func (c *ChatClient) SendFileNamed(recipient, filePath, displayName, caption string) (string, error) {
	id, _, err := protocol.GenerateUUIDv4()
	if err != nil {
		return "", fmt.Errorf("chat client: generate transfer id: %w", err)
	}
	streamID, err := c.streamPool.Acquire()
	if err != nil {
		return id, err
	}
	uploader, initPayload, err := transfer.NewUploader(id, filePath, protocol.MaxFileChunkDataSize)
	if err != nil {
		_ = c.streamPool.Release(streamID)
		return id, err
	}
	initPayload.Recipient = recipient
	if displayName != "" {
		initPayload.FileName = transfer.SanitizeFileName(displayName)
	}
	initPayload.Caption = caption
	c.transferMgr.RegisterUploader(id, uploader)
	c.updateProgress(&transfer.TransferProgress{
		TransferID: id,
		CreatedAt:  time.Now().UnixMilli(),
		UpdatedAt:  time.Now().UnixMilli(),
		Direction:  "UPLOAD",
		Peer:       recipient,
		FileName:   initPayload.FileName,
		FileSize:   initPayload.FileSize,
		State:      "INIT",
	})
	c.setProgressBar(id, transfer.NewProgressBar("Upload "+initPayload.FileName, initPayload.FileSize))
	if err := c.send(streamID, protocol.MsgFileInit, initPayload); err != nil {
		_ = uploader.Close()
		_ = c.streamPool.Release(streamID)
		return id, err
	}
	c.mu.Lock()
	c.transferStreams[id] = streamID
	c.mu.Unlock()
	go c.watchFileAcceptance(id, streamID)
	return id, nil
}

func (c *ChatClient) watchFileAcceptance(id string, streamID uint8) {
	timer := time.NewTimer(fileAcceptanceTimeout)
	defer timer.Stop()
	<-timer.C
	progress, ok := c.store.GetFileProgress(id)
	if !ok || progress == nil || progress.State != "INIT" {
		return
	}
	_ = c.send(streamID, protocol.MsgFileCancel, protocol.FileCancelPayload{TransferID: id, Reason: protocol.FileCodeUserCancelled, Message: "file acceptance timeout", Timestamp: time.Now().UnixMilli()})
	c.failTransfer(id, "Không nhận được phản hồi từ người nhận")
}

func (c *ChatClient) failTransfer(id, message string) {
	uploader, downloader := c.transferMgr.Remove(id)
	if uploader != nil {
		_ = uploader.Close()
	}
	if downloader != nil {
		_ = downloader.Cleanup()
	}
	c.removeProgressBar(id)
	c.updateProgress(&transfer.TransferProgress{TransferID: id, UpdatedAt: time.Now().UnixMilli(), State: "FAILED", ErrorMessage: message})
	c.releaseTransferStream(id)
}

// CancelTransfer stops a transfer and removes any partial local file.
func (c *ChatClient) CancelTransfer(id string) error {
	if err := c.sendUnsequenced(protocol.StreamControl, protocol.MsgFileCancel, protocol.FileCancelPayload{
		TransferID: id,
		Reason:     protocol.FileCodeUserCancelled,
		Timestamp:  time.Now().UnixMilli(),
	}); err != nil {
		return err
	}
	uploader, downloader := c.transferMgr.Remove(id)
	if uploader != nil {
		if err := uploader.Close(); err != nil {
			return err
		}
	}
	c.removeProgressBar(id)
	if downloader != nil {
		if err := downloader.Cleanup(); err != nil {
			return err
		}
	}
	c.updateProgress(&transfer.TransferProgress{
		TransferID: id,
		State:      "CANCELLED",
	})
	c.releaseTransferStream(id)
	return nil
}

// sendUnsequenced sends urgent file control without participating in data ordering.
func (c *ChatClient) sendUnsequenced(streamID, msgType uint8, value any) error {
	payload, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return err
	}
	stream := c.getStream(streamID)
	ack, bitmap, window := stream.ComputeAckState()
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: msgType, StreamID: streamID, ConnectionID: c.connID, AckNum: ack, AckBitmap: bitmap, WindowSize: window}
	var raw []byte
	if cipher := c.getCipher(); cipher != nil {
		hdr.Flags |= protocol.FlagENC
		raw, err = cipher.EncryptPacket(hdr, payload)
	} else {
		raw, err = protocol.MarshalPacket(&protocol.Packet{Hdr: hdr, Payload: payload})
	}
	if err != nil {
		return err
	}
	if _, err = c.conn.WriteToUDP(raw, c.server); err != nil {
		return err
	}
	return nil
}

// SendBroadcast sends a room message and returns its client correlation ID.
func (c *ChatClient) SendBroadcast(content string) (string, error) {
	id := fmt.Sprintf("cli-bcast-%d", time.Now().UnixNano())
	return id, c.SendBroadcastWithID(context.Background(), id, content)
}

// SendBroadcastWithID sends a room message using a caller-provided correlation ID.
func (c *ChatClient) SendBroadcastWithID(ctx context.Context, id, content string) error {
	if id == "" {
		id = fmt.Sprintf("cli-bcast-%d", time.Now().UnixNano())
	}
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("chat client: content is empty")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	err := c.send(protocol.StreamBroadcastChat, protocol.MsgMessBroadcast, protocol.BroadcastMessageClientPayload{
		ClientMsgID: id,
		Content:     content,
		SentAt:      time.Now().UnixMilli(),
	})
	return err
}

// SendDirect sends a one-to-one message and returns its client correlation ID.
func (c *ChatClient) SendDirect(recipient, content string) (string, error) {
	id := fmt.Sprintf("cli-dm-%d", time.Now().UnixNano())
	return id, c.SendDirectWithID(context.Background(), id, recipient, content)
}

// SendDirectWithID sends a direct message using a caller-provided correlation ID.
func (c *ChatClient) SendDirectWithID(ctx context.Context, id, recipient, content string) error {
	if id == "" {
		id = fmt.Sprintf("cli-dm-%d", time.Now().UnixNano())
	}
	if strings.TrimSpace(recipient) == "" || strings.TrimSpace(content) == "" {
		return fmt.Errorf("chat client: recipient and content are required")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	err := c.send(protocol.StreamDirectChat, protocol.MsgMessUnicast, protocol.UnicastMessageClientPayload{
		ClientMsgID: id,
		Recipient:   recipient,
		Content:     content,
		SentAt:      time.Now().UnixMilli(),
	})
	return err
}

// SendLogout sends a best-effort control request while the chat listener owns the socket.
func (c *ChatClient) SendLogout(username string) error {
	return c.send(protocol.StreamControl, protocol.MsgLogout, protocol.LogoutPayload{
		RequestID: fmt.Sprintf("logout-%d", time.Now().UnixNano()),
		Username:  username,
		Reason:    "USER_INITIATED",
		Timestamp: time.Now().UnixMilli(),
	})
}

// RefreshPeers requests a fresh server-side presence snapshot.
func (c *ChatClient) RefreshPeers(ctx context.Context) error {
	requestID := fmt.Sprintf("peers-%d", time.Now().UnixNano())
	if err := c.send(protocol.StreamControl, protocol.MsgDiscoPeers, protocol.DiscoPeersPayload{
		RequestID: requestID,
	}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// StartHeartbeat sends periodic PING messages to maintain session liveness.
func (c *ChatClient) StartHeartbeat(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.SendPing()
			}
		}
	}()
}

// SendPing transmits a control ping to refresh server session idle timer.
func (c *ChatClient) SendPing() error {
	seq := c.pingSeq.Add(1)
	payload, err := protocol.EncodeJSONPayload(protocol.PingPayload{Seq: seq, ClientTime: time.Now().UnixMilli()})
	if err != nil {
		return fmt.Errorf("chat client: encode ping: %w", err)
	}
	packet := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			MsgType:      protocol.MsgPing,
			StreamID:     protocol.StreamControl,
			ConnectionID: c.connID,
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	}
	var raw []byte
	if cipher := c.getCipher(); cipher != nil {
		packet.Hdr.Flags |= protocol.FlagENC
		raw, err = cipher.EncryptPacket(packet.Hdr, packet.Payload)
	} else {
		raw, err = protocol.MarshalPacket(packet)
	}
	if err != nil {
		return err
	}
	_, err = c.conn.WriteToUDP(raw, c.server)
	return err
}

// Listen consumes server messages until ctx is cancelled and updates the local store.
func (c *ChatClient) Listen(ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("chat client: recovered receiver panic: %v", recovered)
		}
	}()
	_ = c.conn.SetReadDeadline(time.Time{})
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	for {
		if deadline, ok := ctx.Deadline(); ok {
			if setErr := c.conn.SetReadDeadline(deadline); setErr != nil {
				return fmt.Errorf("chat client: set read deadline: %w", setErr)
			}
		} else {
			_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		}
		n, _, readErr := c.conn.ReadFromUDP(buffer)
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
				if time.Since(c.LastServerActive()) > 9*time.Second {
					return fmt.Errorf("chat client: server heartbeat timed out")
				}
				continue
			}
			return fmt.Errorf("chat client: read packet: %w", readErr)
		}
		packet, parseErr := protocol.UnmarshalPacket(buffer[:n])
		if parseErr != nil {
			continue
		}
		cipher := c.getCipher()
		if cipher != nil {
			if !packet.Hdr.HasFlag(protocol.FlagENC) {
				plaintextAck := packet.Hdr.MsgType == protocol.MsgAck && len(packet.Payload) == 0
				if !plaintextAck {
					continue
				}
			} else {
				packet.Payload, parseErr = cipher.DecryptPacket(packet.Hdr, buffer[:protocol.HeaderSize], packet.Payload)
				if parseErr != nil {
					continue
				}
			}
		} else if packet.Hdr.HasFlag(protocol.FlagENC) {
			continue
		}
		c.handlePacket(packet)
	}
}

func (c *ChatClient) send(streamID, msgType uint8, value any) error {
	payload, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return fmt.Errorf("chat client: encode payload: %w", err)
	}
	return c.sendPayload(streamID, msgType, payload)
}

func (c *ChatClient) sendPayload(streamID, msgType uint8, payload []byte) error {
	stream := c.getStream(streamID)
	seq := stream.NextSeqNum()
	return c.sendPayloadWithSeq(stream, streamID, msgType, seq, payload)
}

func (c *ChatClient) sendChunkPayload(streamID uint8, seq uint32, payload []byte) error {
	stream := c.getStream(streamID)
	return c.sendPayloadWithSeq(stream, streamID, protocol.MsgFileChunk, seq, payload)
}

func (c *ChatClient) sendPayloadWithSeq(stream *rudp.Stream, streamID, msgType uint8, seq uint32, payload []byte) error {
	ack, bitmap, window := stream.ComputeAckState()
	packet := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD | protocol.FlagACK,
			MsgType:      msgType,
			StreamID:     streamID,
			ConnectionID: c.connID,
			SeqNum:       seq,
			AckNum:       ack,
			AckBitmap:    bitmap,
			WindowSize:   window,
		},
		Payload: payload,
	}
	var raw []byte
	var err error
	if cipher := c.getCipher(); cipher != nil {
		packet.Hdr.Flags |= protocol.FlagENC
		raw, err = cipher.EncryptPacket(packet.Hdr, packet.Payload)
	} else {
		raw, err = protocol.MarshalPacket(packet)
	}
	if err != nil {
		return fmt.Errorf("chat client: marshal packet: %w", err)
	}
	stream.RecordSent(packet, raw)
	if _, err = c.conn.WriteToUDP(raw, c.server); err != nil {
		return fmt.Errorf("chat client: send packet: %w", err)
	}
	return nil
}

func (c *ChatClient) handlePacket(packet *protocol.Packet) {
	if packet.Hdr.HasFlag(protocol.FlagACK) {
		stream := c.getStream(packet.Hdr.StreamID)
		_, fastRetransmit := stream.ProcessAck(packet.Hdr.AckNum, packet.Hdr.AckBitmap, packet.Hdr.WindowSize)
		for _, segment := range fastRetransmit {
			if err := c.retransmitSegment(segment); err != nil {
				fmt.Printf("\n[Transfer Warning] Fast retransmit failed: %v\n", err)
			}
		}
	}
	if packet.Hdr.MsgType == protocol.MsgAck {
		return
	}
	if packet.Hdr.HasFlag(protocol.FlagREL) && packet.Hdr.SeqNum > 0 {
		stream := c.getStream(packet.Hdr.StreamID)
		packets, _ := stream.ProcessIncomingPacket(packet)
		c.sendAck(packet.Hdr.StreamID)
		for _, item := range packets {
			c.applyMessage(item)
		}
		return
	}
	c.applyMessage(packet)
}

func (c *ChatClient) applyMessage(packet *protocol.Packet) {
	switch packet.Hdr.MsgType {
	case protocol.MsgMessBroadcast:
		if message, err := protocol.DecodeJSONPayload[protocol.BroadcastMessageServerPayload](packet.Payload); err == nil {
			c.store.AppendBroadcastMessage(message)
			c.publish("chat.message_received", message)
		}
	case protocol.MsgMessUnicast:
		if message, err := protocol.DecodeJSONPayload[protocol.UnicastMessageServerPayload](packet.Payload); err == nil {
			c.store.AppendDirectMessage(message)
			c.publish("chat.message_received", message)
		}
	case protocol.MsgMessDelivered:
		if receipt, err := protocol.DecodeJSONPayload[protocol.MessageDeliveredPayload](packet.Payload); err == nil {
			c.store.MarkDelivered(receipt.ClientMsgID)
			c.publish("chat.delivery_status", map[string]any{
				"client_msg_id": receipt.ClientMsgID,
				"status":        receipt.Status,
				"scope":         receipt.Scope,
			})
		}
	case protocol.MsgDiscoUpdate:
		if update, err := protocol.DecodeJSONPayload[protocol.DiscoUpdatePayload](packet.Payload); err == nil {
			c.store.ApplyDiscoUpdate(update)
			c.publish("presence.updated", update)
		}
	case protocol.MsgDiscoPeersResponse:
		if response, err := protocol.DecodeJSONPayload[protocol.DiscoPeersResponsePayload](packet.Payload); err == nil {
			c.store.ReplaceOnlinePeers(response.Peers)
			c.publish("presence.updated", response)
		}
	case protocol.MsgFileInit:
		c.handleFileInit(packet)
	case protocol.MsgFileResponse:
		c.handleFileResponse(packet)
	case protocol.MsgFileChunk:
		c.handleFileChunk(packet)
	case protocol.MsgFileComplete:
		c.handleFileComplete(packet)
	case protocol.MsgFileCancel:
		c.handleFileCancel(packet)
	case protocol.MsgPong:
		pong, err := protocol.DecodeJSONPayload[protocol.PongPayload](packet.Payload)
		if err == nil {
			rtt := time.Duration(time.Now().UnixMilli()-pong.ClientTime) * time.Millisecond
			if rtt > 0 {
				c.rto.Update(rtt)
			}
			c.mu.Lock()
			c.lastServerActive = time.Now()
			c.mu.Unlock()
		}
	case protocol.MsgError:
		if message, err := protocol.DecodeJSONPayload[protocol.ErrorMessagePayload](packet.Payload); err == nil {
			if message.RefID != "" {
				c.store.MarkFailed(message.RefID, message.Code, message.Message)
				c.publish("chat.delivery_status", map[string]any{
					"client_msg_id": message.RefID,
					"status":        "FAILED",
					"error_code":    message.Code,
					"error_message": message.Message,
				})
			}
			c.mu.Lock()
			c.lastError = message.Code + ": " + message.Message
			c.mu.Unlock()
			fmt.Printf("\n[Lỗi Hệ Thống] Mã: %s - %s\n", message.Code, message.Message)
		}
	}
}

func (c *ChatClient) publish(name string, data any) {
	if c.eventBus != nil {
		c.eventBus.Publish(name, data)
	}
}

func (c *ChatClient) updateProgress(progress *transfer.TransferProgress) {
	c.store.UpdateFileProgress(progress)
	if progress == nil {
		return
	}
	terminal := progress.State == "COMPLETED" || progress.State == "ERROR" || progress.State == "FAILED" || progress.State == "CANCELLED" || progress.State == "REJECTED"
	c.mu.Lock()
	last := c.progressPublished[progress.TransferID]
	shouldPublish := progress.State != "TRANSFERRING" || time.Since(last) >= 100*time.Millisecond
	if shouldPublish {
		c.progressPublished[progress.TransferID] = time.Now()
	}
	if terminal {
		delete(c.progressPublished, progress.TransferID)
	}
	c.mu.Unlock()
	if !shouldPublish {
		return
	}
	// Publish the merged store snapshot so each event carries complete metadata.
	current, ok := c.store.GetFileProgress(progress.TransferID)
	if !ok {
		return
	}
	name := "file.progress"
	if current.State == "COMPLETED" {
		name = "file.completed"
	}
	if terminal && current.State != "COMPLETED" {
		name = "file.failed"
	}
	c.publish(name, current)
}

// LastServerActive returns the last accepted PONG timestamp.
func (c *ChatClient) LastServerActive() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastServerActive
}

// RTO returns the client's adaptive estimator.
func (c *ChatClient) RTO() *rudp.AdaptiveRTO { return c.rto }

func (c *ChatClient) handleFileInit(packet *protocol.Packet) {
	initPayload, err := protocol.DecodeJSONPayload[protocol.FileInitPayload](packet.Payload)
	if err != nil {
		return
	}
	fmt.Printf("\n[File Incoming] Nhận yêu cầu gửi file '%s' (%d bytes) từ '%s'...\n", initPayload.FileName, initPayload.FileSize, initPayload.Sender)
	downloader, err := transfer.NewDownloader(c.stagingDir, c.downloadDir, &initPayload)
	response := protocol.FileResponsePayload{
		TransferID: initPayload.TransferID,
		Accepted:   err == nil,
		Code:       protocol.FileCodeReady,
		Timestamp:  time.Now().UnixMilli(),
	}
	if err != nil {
		response.Code = protocol.FileCodeStorageError
		response.Message = err.Error()
		fmt.Printf("[File Error] Không thể tạo file tạm: %v\n", err)
	} else {
		c.getStream(packet.Hdr.StreamID).ResetRecv()
		c.transferMgr.RegisterDownloader(initPayload.TransferID, downloader)
		c.setProgressBar(initPayload.TransferID, transfer.NewProgressBar("Download "+initPayload.FileName, initPayload.FileSize))
		c.updateProgress(&transfer.TransferProgress{
			TransferID: initPayload.TransferID,
			CreatedAt:  initPayload.CreatedAt,
			UpdatedAt:  time.Now().UnixMilli(),
			Direction:  "DOWNLOAD",
			Peer:       initPayload.Sender,
			FileName:   initPayload.FileName,
			FileSize:   initPayload.FileSize,
			State:      "INIT",
		})
		fmt.Printf("[File Accepted] Đã chấp nhận và sẵn sàng nhận các chunk...\n")
	}
	if sendErr := c.send(packet.Hdr.StreamID, protocol.MsgFileResponse, response); sendErr != nil {
		fmt.Printf("[File Error] Không thể gửi phản hồi chấp nhận transfer_id=%s stream=%d: %v\n", initPayload.TransferID, packet.Hdr.StreamID, sendErr)
		c.failTransfer(initPayload.TransferID, "Không thể phản hồi người gửi")
	}
}

func (c *ChatClient) handleFileResponse(packet *protocol.Packet) {
	response, err := protocol.DecodeJSONPayload[protocol.FileResponsePayload](packet.Payload)
	if err != nil {
		return
	}
	if !response.Accepted {
		uploader, _ := c.transferMgr.Remove(response.TransferID)
		if uploader != nil {
			_ = uploader.Close()
		}
		c.updateProgress(&transfer.TransferProgress{
			TransferID: response.TransferID,
			Direction:  "UPLOAD",
			State:      "REJECTED",
		})
		c.releaseTransferStream(response.TransferID)
		c.removeProgressBar(response.TransferID)
		fmt.Printf("\n[File Transfer Failed] Người nhận từ chối hoặc ngoại tuyến: %s (code=%s)\n", response.Message, response.Code)
		return
	}
	uploader, ok := c.transferMgr.Uploader(response.TransferID)
	if !ok {
		fmt.Printf("\n[File Transfer] Bỏ qua phản hồi đến muộn cho transfer_id=%s\n", response.TransferID)
		return
	}
	fmt.Printf("\n[File Transfer] Người nhận đã chấp nhận! Đang truyền các khối dữ liệu (chunks)...\n")
	c.updateProgress(&transfer.TransferProgress{TransferID: response.TransferID, UpdatedAt: time.Now().UnixMilli(), State: "TRANSFERRING"})
	stream := c.getStream(packet.Hdr.StreamID)
	stream.ResetSend()
	go func() {
		progressBar := c.getProgressBar(response.TransferID)
		var transferredBytes int64
		completed := true
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-ticker.C:
					for _, segment := range stream.GetExpiredSegments(c.rto.RTO()) {
						if writeErr := c.retransmitSegment(segment); writeErr != nil {
							return
						}
					}
					if stream.HasDeadSegments(rudp.MaxSegmentRetries) {
						c.failTransfer(response.TransferID, "Mất kết nối với người nhận")
						return
					}
				case <-done:
					return
				}
			}
		}()
		defer func() {
			if recovered := recover(); recovered != nil {
				c.updateProgress(&transfer.TransferProgress{
					TransferID: response.TransferID,
					State:      "ERROR",
				})
			}
		}()
		for index := uint32(0); ; index++ {
			completed = false
			for stream.InFlightCount() >= int(stream.PeerWindowSize()) {
				if _, stillActive := c.transferMgr.Uploader(response.TransferID); !stillActive {
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
			data, readErr := uploader.ReadChunk(index)
			if readErr != nil || len(data) == 0 {
				completed = readErr == nil
				break
			}
			idBytes, parseErr := protocol.ParseUUID(response.TransferID)
			if parseErr != nil {
				completed = false
				break
			}
			payload, marshalErr := protocol.MarshalFileChunk(&protocol.FileChunkPacket{
				TransferID: idBytes,
				ChunkIndex: index,
				Data:       data,
			})
			if marshalErr != nil || c.sendChunkPayload(packet.Hdr.StreamID, index+1, payload) != nil {
				completed = false
				break
			}
			completed = true
			transferredBytes += int64(len(data))
			if progressBar != nil {
				if line := progressBar.Update(transferredBytes); line != "" {
					fmt.Printf("\r%s", line)
				}
			}
			c.updateProgress(&transfer.TransferProgress{
				TransferID:       response.TransferID,
				Direction:        "UPLOAD",
				State:            "TRANSFERRING",
				TransferredBytes: transferredBytes,
				Percentage:       float64(transferredBytes) * 100 / float64(uploader.FileSize()),
			})
		}
		// Keep retransmission active until all in-flight chunks are acknowledged
		// by the receiver or the transfer is finalized/cancelled.
		for completed {
			if _, stillActive := c.transferMgr.Uploader(response.TransferID); !stillActive {
				break
			}
			if stream.InFlightCount() == 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if progressBar != nil && completed {
			fmt.Printf("\r%s\n", progressBar.Complete())
		}
	}()
}

// retransmitSegment re-seals encrypted packets so each retry receives a fresh nonce.
// Reusing the original AES-GCM bytes would intentionally trigger the anti-replay
// window on the peer, even though the logical sequence number stays unchanged.
func (c *ChatClient) retransmitSegment(segment *rudp.SentSegment) error {
	if segment == nil {
		return fmt.Errorf("chat client: nil retransmission segment")
	}
	raw := append([]byte(nil), segment.Raw...)
	if cipher := c.getCipher(); cipher != nil && segment.Packet != nil {
		var err error
		raw, err = cipher.EncryptPacket(segment.Packet.Hdr, segment.Packet.Payload)
		if err != nil {
			return fmt.Errorf("chat client: re-seal retransmission: %w", err)
		}
	}
	if _, err := c.conn.WriteToUDP(raw, c.server); err != nil {
		return fmt.Errorf("chat client: retransmit packet: %w", err)
	}
	return nil
}

func (c *ChatClient) handleFileChunk(packet *protocol.Packet) {
	chunk, err := protocol.UnmarshalFileChunk(packet.Payload)
	if err != nil {
		return
	}
	id := protocol.FormatUUID(chunk.TransferID)
	downloader, ok := c.transferMgr.Downloader(id)
	if !ok {
		return
	}
	complete, err := downloader.WriteChunk(chunk.ChunkIndex, chunk.Data)
	if err != nil {
		return
	}
	if progressBar := c.getProgressBar(id); progressBar != nil {
		if line := progressBar.Update(downloader.BytesWritten()); line != "" {
			fmt.Printf("\r%s", line)
		}
	}
	c.updateProgress(&transfer.TransferProgress{
		TransferID:       id,
		Direction:        "DOWNLOAD",
		State:            "TRANSFERRING",
		TransferredBytes: downloader.BytesWritten(),
		Percentage:       downloader.Progress(),
	})
	if !complete {
		return
	}
	checksum, verifyErr := downloader.FinalizeAndVerify()
	if verifyErr != nil {
		_ = c.send(packet.Hdr.StreamID, protocol.MsgFileCancel, protocol.FileCancelPayload{
			TransferID: id,
			Reason:     protocol.FileCodeChecksumFailed,
			Timestamp:  time.Now().UnixMilli(),
		})
		c.transferMgr.Remove(id)
		c.removeProgressBar(id)
		c.updateProgress(&transfer.TransferProgress{
			TransferID:       id,
			Direction:        "DOWNLOAD",
			State:            "ERROR",
			TransferredBytes: downloader.BytesWritten(),
			Percentage:       downloader.Progress(),
		})
		fmt.Printf("\n[File Error] Mã băm SHA-256 không khớp! Đã hủy file (%v).\n", verifyErr)
		return
	}
	bytesReceived := downloader.BytesWritten()
	downloadPath := downloader.DownloadPath()
	_ = c.send(packet.Hdr.StreamID, protocol.MsgFileComplete, protocol.FileCompletePayload{
		TransferID:         id,
		VerifiedSHA256:     checksum,
		TotalBytesReceived: bytesReceived,
		CompletedAt:        time.Now().UnixMilli(),
	})
	c.transferMgr.Remove(id)
	if progressBar := c.getProgressBar(id); progressBar != nil {
		fmt.Printf("\r%s\n", progressBar.Complete())
	}
	c.removeProgressBar(id)
	c.updateProgress(&transfer.TransferProgress{
		TransferID:       id,
		Direction:        "DOWNLOAD",
		State:            "COMPLETED",
		TransferredBytes: bytesReceived,
		Percentage:       100,
		LocalPath:        downloadPath,
	})
	fmt.Printf("\n[File Received Complete] Đã nhận đủ và xác thực SHA-256 thành công! File đã lưu vào thư mục: storage/downloads/\n")
}

func (c *ChatClient) handleFileComplete(packet *protocol.Packet) {
	complete, err := protocol.DecodeJSONPayload[protocol.FileCompletePayload](packet.Payload)
	if err != nil {
		return
	}
	uploader, _ := c.transferMgr.Remove(complete.TransferID)
	if uploader != nil {
		_ = uploader.Close()
	}
	c.removeProgressBar(complete.TransferID)
	c.updateProgress(&transfer.TransferProgress{
		TransferID: complete.TransferID,
		State:      "COMPLETED",
		Percentage: 100,
	})
	c.releaseTransferStream(complete.TransferID)
	fmt.Printf("\n[File Transfer Success] Người nhận đã nhận thành công 100%% và kiểm tra SHA-256 trùng khớp! (transfer_id=%s)\n", complete.TransferID)
}

func (c *ChatClient) handleFileCancel(packet *protocol.Packet) {
	cancel, err := protocol.DecodeJSONPayload[protocol.FileCancelPayload](packet.Payload)
	if err != nil {
		return
	}
	uploader, downloader := c.transferMgr.Remove(cancel.TransferID)
	if uploader != nil {
		_ = uploader.Close()
	}
	if downloader != nil {
		_ = downloader.Cleanup()
	}
	c.removeProgressBar(cancel.TransferID)
	c.updateProgress(&transfer.TransferProgress{
		TransferID:   cancel.TransferID,
		UpdatedAt:    time.Now().UnixMilli(),
		State:        map[bool]string{true: "FAILED", false: "CANCELLED"}[cancel.Reason == protocol.FileCodePeerDisconnected],
		ErrorMessage: map[bool]string{true: "Người nhận đã ngắt kết nối", false: cancel.Message}[cancel.Reason == protocol.FileCodePeerDisconnected],
	})
	c.releaseTransferStream(cancel.TransferID)
	fmt.Printf("\n[File Transfer Cancelled] Quá trình truyền file bị hủy: %s\n", cancel.Reason)
}

func (c *ChatClient) transferStream(id string) uint8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if streamID, ok := c.transferStreams[id]; ok {
		return streamID
	}
	return protocol.StreamFileMin
}

func (c *ChatClient) releaseTransferStream(id string) {
	c.mu.Lock()
	streamID, ok := c.transferStreams[id]
	if ok {
		delete(c.transferStreams, id)
		delete(c.streams, streamID)
	}
	c.mu.Unlock()
	if ok {
		_ = c.streamPool.Release(streamID)
	}
}

// setProgressBar associates a renderer with an active transfer.
func (c *ChatClient) setProgressBar(id string, bar *transfer.ProgressBar) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.progressBars[id] = bar
}

// getProgressBar returns the renderer for an active transfer.
func (c *ChatClient) getProgressBar(id string) *transfer.ProgressBar {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.progressBars[id]
}

// removeProgressBar releases terminal progress state after transfer shutdown.
func (c *ChatClient) removeProgressBar(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.progressBars, id)
}

func (c *ChatClient) sendAck(streamID uint8) {
	stream := c.getStream(streamID)
	ack, bitmap, window := stream.ComputeAckState()
	packet := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgAck,
			StreamID:     streamID,
			ConnectionID: c.connID,
			AckNum:       ack,
			AckBitmap:    bitmap,
			WindowSize:   window,
		},
	}
	raw, err := protocol.MarshalPacket(packet)
	if err == nil {
		_, _ = c.conn.WriteToUDP(raw, c.server)
	}
}

func (c *ChatClient) getStream(streamID uint8) *rudp.Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	if stream := c.streams[streamID]; stream != nil {
		return stream
	}
	stream := rudp.NewStream(streamID, protocol.DefaultWindowSize)
	c.streams[streamID] = stream
	return stream
}

func (c *ChatClient) resetStream(streamID uint8) *rudp.Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	stream := rudp.NewStream(streamID, protocol.DefaultWindowSize)
	c.streams[streamID] = stream
	return stream
}
