package client

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// ChatClient sends and receives Step 5 messages on one authenticated UDP socket.
type ChatClient struct {
	conn            *net.UDPConn
	server          *net.UDPAddr
	connID          uint32
	store           *state.ClientStore
	mu              sync.Mutex
	streams         map[uint8]*rudp.Stream
	streamPool      *rudp.StreamPool
	transferMgr     *transfer.Manager
	transferStreams map[string]uint8
}

// NewChatClient creates a chat facade over an existing authenticated socket.
func NewChatClient(conn *net.UDPConn, server *net.UDPAddr, connectionID uint32, store *state.ClientStore) *ChatClient {
	return &ChatClient{
		conn:            conn,
		server:          server,
		connID:          connectionID,
		store:           store,
		streams:         make(map[uint8]*rudp.Stream),
		streamPool:      rudp.NewStreamPool(),
		transferMgr:     transfer.NewManager(filepath.Join("storage", "staging"), filepath.Join("storage", "downloads")),
		transferStreams: make(map[string]uint8),
	}
}

// SendFile starts an outgoing file transfer after hashing the source file.
func (c *ChatClient) SendFile(recipient, filePath, caption string) (string, error) {
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
	initPayload.Caption = caption
	c.transferMgr.RegisterUploader(id, uploader)
	c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: id, Direction: "UPLOAD", Peer: recipient, FileName: initPayload.FileName, FileSize: initPayload.FileSize, State: "INIT"})
	if err := c.send(streamID, protocol.MsgFileInit, initPayload); err != nil {
		_ = uploader.Close()
		_ = c.streamPool.Release(streamID)
		return id, err
	}
	c.mu.Lock()
	c.transferStreams[id] = streamID
	c.mu.Unlock()
	return id, nil
}

// CancelTransfer stops a transfer and removes any partial local file.
func (c *ChatClient) CancelTransfer(id string) error {
	if err := c.send(c.transferStream(id), protocol.MsgFileCancel, protocol.FileCancelPayload{TransferID: id, Reason: protocol.FileCodeUserCancelled, Timestamp: time.Now().UnixMilli()}); err != nil {
		return err
	}
	uploader, downloader := c.transferMgr.Remove(id)
	if uploader != nil {
		if err := uploader.Close(); err != nil {
			return err
		}
	}
	if downloader != nil {
		if err := downloader.Cleanup(); err != nil {
			return err
		}
	}
	c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: id, State: "CANCELLED"})
	c.releaseTransferStream(id)
	return nil
}

// SendBroadcast sends a room message and returns its client correlation ID.
func (c *ChatClient) SendBroadcast(content string) (string, error) {
	id := fmt.Sprintf("cli-bcast-%d", time.Now().UnixNano())
	if strings.TrimSpace(content) == "" {
		return id, fmt.Errorf("chat client: content is empty")
	}
	err := c.send(protocol.StreamBroadcastChat, protocol.MsgMessBroadcast, protocol.BroadcastMessageClientPayload{
		ClientMsgID: id,
		Content:     content,
		SentAt:      time.Now().UnixMilli(),
	})
	return id, err
}

// SendDirect sends a one-to-one message and returns its client correlation ID.
func (c *ChatClient) SendDirect(recipient, content string) (string, error) {
	id := fmt.Sprintf("cli-dm-%d", time.Now().UnixNano())
	if strings.TrimSpace(recipient) == "" || strings.TrimSpace(content) == "" {
		return id, fmt.Errorf("chat client: recipient and content are required")
	}
	err := c.send(protocol.StreamDirectChat, protocol.MsgMessUnicast, protocol.UnicastMessageClientPayload{
		ClientMsgID: id,
		Recipient:   recipient,
		Content:     content,
		SentAt:      time.Now().UnixMilli(),
	})
	return id, err
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
	packet := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			MsgType:      protocol.MsgPing,
			StreamID:     protocol.StreamControl,
			ConnectionID: c.connID,
			WindowSize:   protocol.DefaultWindowSize,
		},
	}
	raw, err := protocol.MarshalPacket(packet)
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
			_ = c.conn.SetReadDeadline(time.Time{})
		}
		n, _, readErr := c.conn.ReadFromUDP(buffer)
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("chat client: read packet: %w", readErr)
		}
		packet, parseErr := protocol.UnmarshalPacket(buffer[:n])
		if parseErr != nil {
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
	raw, err := protocol.MarshalPacket(packet)
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
			if _, err := c.conn.WriteToUDP(segment.Raw, c.server); err != nil {
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
		}
	case protocol.MsgMessUnicast:
		if message, err := protocol.DecodeJSONPayload[protocol.UnicastMessageServerPayload](packet.Payload); err == nil {
			c.store.AppendDirectMessage(message)
		}
	case protocol.MsgMessDelivered:
		if receipt, err := protocol.DecodeJSONPayload[protocol.MessageDeliveredPayload](packet.Payload); err == nil {
			c.store.MarkDelivered(receipt.ClientMsgID)
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
	}
}

func (c *ChatClient) handleFileInit(packet *protocol.Packet) {
	initPayload, err := protocol.DecodeJSONPayload[protocol.FileInitPayload](packet.Payload)
	if err != nil {
		return
	}
	fmt.Printf("\n[File Incoming] Nhận yêu cầu gửi file '%s' (%d bytes) từ '%s'...\n", initPayload.FileName, initPayload.FileSize, initPayload.Sender)
	downloader, err := transfer.NewDownloader(filepath.Join("storage", "staging"), filepath.Join("storage", "downloads"), &initPayload)
	response := protocol.FileResponsePayload{TransferID: initPayload.TransferID, Accepted: err == nil, Code: protocol.FileCodeReady, Timestamp: time.Now().UnixMilli()}
	if err != nil {
		response.Code = protocol.FileCodeStorageError
		response.Message = err.Error()
		fmt.Printf("[File Error] Không thể tạo file tạm: %v\n", err)
	} else {
		c.transferMgr.RegisterDownloader(initPayload.TransferID, downloader)
		c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: initPayload.TransferID, Direction: "DOWNLOAD", Peer: initPayload.Sender, FileName: initPayload.FileName, FileSize: initPayload.FileSize, State: "INIT"})
		fmt.Printf("[File Accepted] Đã chấp nhận và sẵn sàng nhận các chunk...\n")
	}
	_ = c.send(packet.Hdr.StreamID, protocol.MsgFileResponse, response)
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
		c.store.UpdateFileProgress(&transfer.TransferProgress{
			TransferID: response.TransferID,
			Direction:  "UPLOAD",
			State:      "REJECTED",
		})
		c.releaseTransferStream(response.TransferID)
		fmt.Printf("\n[File Transfer Failed] Người nhận từ chối hoặc ngoại tuyến: %s (code=%s)\n", response.Message, response.Code)
		return
	}
	fmt.Printf("\n[File Transfer] Người nhận đã chấp nhận! Đang truyền các khối dữ liệu (chunks)...\n")
	uploader, ok := c.transferMgr.Uploader(response.TransferID)
	if !ok {
		return
	}
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-ticker.C:
					for _, segment := range c.getStream(packet.Hdr.StreamID).GetExpiredSegments(150 * time.Millisecond) {
						if _, writeErr := c.conn.WriteToUDP(segment.Raw, c.server); writeErr != nil {
							return
						}
					}
				case <-done:
					return
				}
			}
		}()
		defer func() {
			if recovered := recover(); recovered != nil {
				c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: response.TransferID, State: "ERROR"})
			}
		}()
		for index := uint32(0); ; index++ {
			stream := c.getStream(packet.Hdr.StreamID)
			for stream.InFlightCount() >= int(stream.PeerWindowSize()) {
				time.Sleep(2 * time.Millisecond)
			}
			data, readErr := uploader.ReadChunk(index)
			if readErr != nil || len(data) == 0 {
				break
			}
			idBytes, parseErr := protocol.ParseUUID(response.TransferID)
			if parseErr != nil {
				break
			}
			payload, marshalErr := protocol.MarshalFileChunk(&protocol.FileChunkPacket{TransferID: idBytes, ChunkIndex: index, Data: data})
			if marshalErr != nil || c.sendPayload(packet.Hdr.StreamID, protocol.MsgFileChunk, payload) != nil {
				break
			}
			c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: response.TransferID, Direction: "UPLOAD", State: "TRANSFERRING", TransferredBytes: int64(index+1) * int64(len(data))})
		}
	}()
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
	c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: id, Direction: "DOWNLOAD", State: "TRANSFERRING", TransferredBytes: int64(chunk.ChunkIndex+1) * int64(len(chunk.Data)), Percentage: downloader.Progress()})
	if !complete {
		return
	}
	checksum, verifyErr := downloader.FinalizeAndVerify()
	if verifyErr != nil {
		_ = c.send(packet.Hdr.StreamID, protocol.MsgFileCancel, protocol.FileCancelPayload{TransferID: id, Reason: protocol.FileCodeChecksumFailed, Timestamp: time.Now().UnixMilli()})
		c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: id, State: "ERROR"})
		fmt.Printf("\n[File Error] Mã băm SHA-256 không khớp! Đã hủy file.\n")
		return
	}
	_ = c.send(packet.Hdr.StreamID, protocol.MsgFileComplete, protocol.FileCompletePayload{TransferID: id, VerifiedSHA256: checksum, TotalBytesReceived: int64(chunk.ChunkIndex+1) * int64(len(chunk.Data)), CompletedAt: time.Now().UnixMilli()})
	c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: id, State: "COMPLETED", Percentage: 100})
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
	c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: complete.TransferID, State: "COMPLETED", Percentage: 100})
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
	c.store.UpdateFileProgress(&transfer.TransferProgress{TransferID: cancel.TransferID, State: "CANCELLED"})
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
	}
	c.mu.Unlock()
	if ok {
		_ = c.streamPool.Release(streamID)
	}
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
