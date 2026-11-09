package handlers

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// FileRelayHandler routes file-control messages and streams chunk payloads without file buffering.
type FileRelayHandler struct{}

// NewFileRelayHandler creates a file relay handler.
func NewFileRelayHandler() *FileRelayHandler { return &FileRelayHandler{} }

// HandleFileInit validates, registers, and forwards a transfer request.
func (h *FileRelayHandler) HandleFileInit(sess *rudp.Session, streamID uint8, payload []byte) error {
	request, err := protocol.DecodeJSONPayload[protocol.FileInitPayload](payload)
	if err != nil {
		return fmt.Errorf("file relay: decode init: %w", err)
	}
	sender, ok := state.LookupUsernameByConnID(sess.ID())
	if !ok {
		return fmt.Errorf("file relay: unauthenticated sender %d", sess.ID())
	}
	receiver, online := state.LookupTargetByUsername(request.Recipient)
	if !online || receiver.OutboundCh == nil {
		return h.sendJSON(sess, streamID, protocol.MsgFileResponse, protocol.FileResponsePayload{
			TransferID: request.TransferID,
			Accepted:   false,
			Code:       protocol.FileCodeRejected,
			Message:    "recipient is offline",
			Timestamp:  time.Now().UnixMilli(),
		})
	}
	idBytes, err := protocol.ParseUUID(request.TransferID)
	if err != nil {
		return fmt.Errorf("file relay: parse transfer id: %w", err)
	}
	state.RegisterActiveTransfer(&state.ActiveTransfer{
		TransferID:       request.TransferID,
		TransferIDBytes:  idBytes,
		SenderUsername:   sender,
		ReceiverUsername: request.Recipient,
		SenderConnID:     sess.ID(),
		ReceiverConnID:   receiver.ConnectionID,
		SenderStreamID:   streamID,
		ReceiverStreamID: streamID,
		FileName:         request.FileName,
		FileSize:         request.FileSize,
		TotalChunks:      request.TotalChunks,
		ChunkSize:        request.ChunkSize,
		SHA256:           request.SHA256,
		StartedAt:        time.Now(),
	})
	request.Sender = sender
	encoded, err := protocol.EncodeJSONPayload(request)
	if err != nil {
		return fmt.Errorf("file relay: encode init: %w", err)
	}
	return h.Forward(receiver.OutboundCh, receiver.ConnectionID, streamID, protocol.MsgFileInit, encoded)
}

// HandleFileResponse forwards receiver acceptance to the sender.
func (h *FileRelayHandler) HandleFileResponse(_ *rudp.Session, _ uint8, payload []byte) error {
	response, err := protocol.DecodeJSONPayload[protocol.FileResponsePayload](payload)
	if err != nil {
		return fmt.Errorf("file relay: decode response: %w", err)
	}
	transfer, ok := state.LookupActiveTransfer(response.TransferID)
	if !ok {
		return fmt.Errorf("file relay: transfer %s not found", response.TransferID)
	}
	sender, ok := state.LookupTargetByUsername(transfer.SenderUsername)
	if !ok || sender.OutboundCh == nil {
		return fmt.Errorf("file relay: sender is offline")
	}
	encoded, err := protocol.EncodeJSONPayload(response)
	if err != nil {
		return fmt.Errorf("file relay: encode response: %w", err)
	}
	if sender.ReliableSession != nil {
		return sender.ReliableSession.SendReliable(transfer.SenderStreamID, protocol.MsgFileResponse, encoded)
	}
	return h.Forward(sender.OutboundCh, sender.ConnectionID, transfer.SenderStreamID, protocol.MsgFileResponse, encoded)
}

// HandleFileChunk relays one bounded binary chunk directly to the receiver queue.
// Each chunk is stamped with the deterministic sequence number ChunkIndex + 1 so
// retransmitted chunks map back to their original receiver sequence hole.
func (h *FileRelayHandler) HandleFileChunk(_ *rudp.Session, _ uint8, payload []byte) error {
	chunk, err := protocol.UnmarshalFileChunk(payload)
	if err != nil {
		return err
	}
	transfer, ok := state.LookupActiveTransferByBytes(chunk.TransferID)
	if !ok {
		return nil
	}
	receiver, ok := state.LookupTargetByUsername(transfer.ReceiverUsername)
	if !ok || receiver.OutboundCh == nil {
		return nil
	}
	seq := chunk.ChunkIndex + 1
	packet := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD | protocol.FlagACK,
			MsgType:      protocol.MsgFileChunk,
			StreamID:     transfer.ReceiverStreamID,
			ConnectionID: receiver.ConnectionID,
			SeqNum:       seq,
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	}
	var raw []byte
	if receiver.Cipher != nil {
		packet.Hdr.Flags |= protocol.FlagENC
		raw, err = receiver.Cipher.EncryptPacket(packet.Hdr, packet.Payload)
	} else {
		raw, err = protocol.MarshalPacket(packet)
	}
	if err != nil {
		return fmt.Errorf("file relay: marshal chunk: %w", err)
	}
	select {
	case receiver.OutboundCh <- raw:
		return nil
	case <-time.After(20 * time.Millisecond):
		slog.Warn("file relay: receiver queue congested, dropped chunk", "transfer_id", transfer.TransferID)
	}
	return nil
}

// RelayAck relays receiver ACK/SACK state to the original sender.
func (h *FileRelayHandler) RelayAck(sess *rudp.Session, packet *protocol.Packet) error {
	if packet == nil {
		return nil
	}
	transfer, ok := state.LookupActiveTransferByReceiver(sess.ID(), packet.Hdr.StreamID)
	if !ok {
		return nil
	}
	sender, ok := state.LookupTargetByUsername(transfer.SenderUsername)
	if !ok || sender.OutboundCh == nil {
		return nil
	}
	raw, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgAck,
			StreamID:     transfer.SenderStreamID,
			ConnectionID: sender.ConnectionID,
			AckNum:       packet.Hdr.AckNum,
			AckBitmap:    packet.Hdr.AckBitmap,
			WindowSize:   packet.Hdr.WindowSize,
		},
	})
	if err != nil {
		return fmt.Errorf("file relay: marshal ack: %w", err)
	}
	select {
	case sender.OutboundCh <- raw:
		return nil
	case <-time.After(20 * time.Millisecond):
		return fmt.Errorf("file relay: sender queue full while forwarding ack")
	}
}

// HandleFileComplete removes the route and forwards completion to the sender.
func (h *FileRelayHandler) HandleFileComplete(_ *rudp.Session, _ uint8, payload []byte) error {
	complete, err := protocol.DecodeJSONPayload[protocol.FileCompletePayload](payload)
	if err != nil {
		return fmt.Errorf("file relay: decode complete: %w", err)
	}
	transfer, ok := state.RemoveActiveTransfer(complete.TransferID)
	if !ok || transfer == nil {
		return nil
	}
	sender, ok := state.LookupTargetByUsername(transfer.SenderUsername)
	if !ok || sender.OutboundCh == nil {
		return nil
	}
	encoded, err := protocol.EncodeJSONPayload(complete)
	if err != nil {
		return fmt.Errorf("file relay: encode complete: %w", err)
	}
	return h.Forward(sender.OutboundCh, sender.ConnectionID, transfer.SenderStreamID, protocol.MsgFileComplete, encoded)
}

// HandleFileCancel removes a route and forwards cancellation to the other peer.
func (h *FileRelayHandler) HandleFileCancel(sess *rudp.Session, _ uint8, payload []byte) error {
	cancel, err := protocol.DecodeJSONPayload[protocol.FileCancelPayload](payload)
	if err != nil {
		return fmt.Errorf("file relay: decode cancel: %w", err)
	}
	transfer, ok := state.RemoveActiveTransfer(cancel.TransferID)
	if !ok || transfer == nil {
		return nil
	}
	username, _ := state.LookupUsernameByConnID(sess.ID())
	cancel.CancelledBy = username
	peerName, peerStream := transfer.ReceiverUsername, transfer.ReceiverStreamID
	if username == transfer.ReceiverUsername {
		peerName, peerStream = transfer.SenderUsername, transfer.SenderStreamID
	}
	peer, ok := state.LookupTargetByUsername(peerName)
	if !ok || peer.OutboundCh == nil {
		return nil
	}
	encoded, err := protocol.EncodeJSONPayload(cancel)
	if err != nil {
		return fmt.Errorf("file relay: encode cancel: %w", err)
	}
	return h.Forward(peer.OutboundCh, peer.ConnectionID, peerStream, protocol.MsgFileCancel, encoded)
}

func (h *FileRelayHandler) sendJSON(sess *rudp.Session, streamID, msgType uint8, value any) error {
	payload, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return fmt.Errorf("file relay: encode json: %w", err)
	}
	return sess.SendReliable(streamID, msgType, payload)
}

// Forward queues a file-control packet for a peer session.
func (h *FileRelayHandler) Forward(queue chan []byte, connID uint32, streamID, msgType uint8, payload []byte) error {
	hdr := protocol.Header{
		Version:      protocol.ProtocolVersion,
		Flags:        protocol.FlagREL | protocol.FlagORD,
		MsgType:      msgType,
		StreamID:     streamID,
		ConnectionID: connID,
		WindowSize:   protocol.DefaultWindowSize,
	}
	var raw []byte
	var err error
	if target, ok := state.LookupTargetByConnID(connID); ok && target.Cipher != nil {
		raw, err = target.Cipher.EncryptPacket(hdr, payload)
	} else {
		raw, err = protocol.MarshalPacket(&protocol.Packet{Hdr: hdr, Payload: payload})
	}
	if err != nil {
		return fmt.Errorf("file relay: marshal forward: %w", err)
	}
	select {
	case queue <- raw:
		return nil
	default:
		return fmt.Errorf("file relay: destination queue full")
	}
}
