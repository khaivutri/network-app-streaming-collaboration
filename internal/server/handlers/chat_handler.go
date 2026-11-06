package handlers

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// ChatHandler implements broadcast and direct-message routing for authenticated sessions.
type ChatHandler struct {
	msgCounter atomic.Uint64
}

// NewChatHandler creates a stateless chat router with a process-local message ID sequence.
func NewChatHandler() *ChatHandler { return &ChatHandler{} }

// HandleBroadcast validates and fans a room message to the online snapshot.
func (h *ChatHandler) HandleBroadcast(sess *rudp.Session, _ uint8, payload []byte) error {
	request, err := protocol.DecodeJSONPayload[protocol.BroadcastMessageClientPayload](payload)
	if err != nil {
		return fmt.Errorf("chat: decode broadcast: %w", err)
	}
	content := strings.TrimSpace(request.Content)
	if content == "" {
		return h.sendError(sess, protocol.ErrCodeMsgEmptyContent, "Nội dung tin nhắn không được để trống", request.ClientMsgID)
	}
	sender, ok := state.LookupUsernameByConnID(sess.ID())
	if !ok {
		return fmt.Errorf("chat: unauthenticated sender connection %d", sess.ID())
	}
	senderInfo, _ := state.LookupTargetByUsername(sender)
	now := time.Now().UnixMilli()
	msgID := fmt.Sprintf("srv-bcast-%05d", h.msgCounter.Add(1))
	message := protocol.BroadcastMessageServerPayload{
		MsgID:         msgID,
		ClientMsgID:   request.ClientMsgID,
		Sender:        sender,
		SenderDisplay: senderInfo.DisplayName,
		Content:       content,
		ServerTime:    now,
	}
	encoded, err := protocol.EncodeJSONPayload(message)
	if err != nil {
		return fmt.Errorf("chat: encode broadcast: %w", err)
	}
	// Snapshotting before I/O enforces BR05 : clients joining later never receive old room messages.
	for _, peer := range state.GetSnapshotOnlinePeers() {
		target, found := state.LookupTargetByUsername(peer.Username)
		if !found || target.OutboundCh == nil {
			continue
		}
		raw, marshalErr := chatPacket(protocol.StreamBroadcastChat, protocol.MsgMessBroadcast, target.ConnectionID, encoded)
		if marshalErr != nil {
			return marshalErr
		}
		select {
		case target.OutboundCh <- raw:
		default:
			// A slow client is isolated so one full queue cannot block other recipients.
		}
	}
	receipt := protocol.MessageDeliveredPayload{
		MsgID:       msgID,
		ClientMsgID: request.ClientMsgID,
		Scope:       "BROADCAST",
		Status:      "DELIVERED",
		DeliveredAt: now,
	}
	return h.sendReliableJSON(sess, protocol.StreamBroadcastChat, protocol.MsgMessDelivered, receipt)
}

// HandleUnicast validates and forwards a direct message to exactly one online recipient.
func (h *ChatHandler) HandleUnicast(sess *rudp.Session, _ uint8, payload []byte) error {
	request, err := protocol.DecodeJSONPayload[protocol.UnicastMessageClientPayload](payload)
	if err != nil {
		return fmt.Errorf("chat: decode unicast: %w", err)
	}
	content := strings.TrimSpace(request.Content)
	if content == "" {
		return h.sendError(sess, protocol.ErrCodeMsgEmptyContent, "Nội dung tin nhắn không được để trống", request.ClientMsgID)
	}
	sender, ok := state.LookupUsernameByConnID(sess.ID())
	if !ok {
		return fmt.Errorf("chat: unauthenticated sender connection %d", sess.ID())
	}
	if request.Recipient == sender {
		return h.sendError(sess, protocol.ErrCodeMsgSelfRecipient, "Không thể gửi tin nhắn riêng cho chính mình", request.ClientMsgID)
	}
	recipient, online := state.LookupTargetByUsername(request.Recipient)
	if !online || recipient.OutboundCh == nil {
		return h.sendError(sess, protocol.ErrCodeMsgRecipientOffline, "Người nhận hiện không trực tuyến", request.ClientMsgID)
	}
	senderInfo, _ := state.LookupTargetByUsername(sender)
	now := time.Now().UnixMilli()
	msgID := fmt.Sprintf("srv-dm-%05d", h.msgCounter.Add(1))
	message := protocol.UnicastMessageServerPayload{
		MsgID:         msgID,
		ClientMsgID:   request.ClientMsgID,
		Sender:        sender,
		SenderDisplay: senderInfo.DisplayName,
		Recipient:     request.Recipient,
		Content:       content,
		ServerTime:    now,
	}
	encoded, err := protocol.EncodeJSONPayload(message)
	if err != nil {
		return fmt.Errorf("chat: encode unicast: %w", err)
	}
	raw, err := chatPacket(protocol.StreamDirectChat, protocol.MsgMessUnicast, recipient.ConnectionID, encoded)
	if err != nil {
		return err
	}
	select {
	case recipient.OutboundCh <- raw:
	default:
		return fmt.Errorf("chat: recipient %s outbound queue full", request.Recipient)
	}
	receipt := protocol.MessageDeliveredPayload{
		MsgID:       msgID,
		ClientMsgID: request.ClientMsgID,
		Scope:       "UNICAST",
		Recipient:   request.Recipient,
		Status:      "DELIVERED",
		DeliveredAt: now,
	}
	return h.sendReliableJSON(sess, protocol.StreamDirectChat, protocol.MsgMessDelivered, receipt)
}

func (h *ChatHandler) sendError(sess *rudp.Session, code, message, refID string) error {
	return h.sendReliableJSON(sess, protocol.StreamControl, protocol.MsgError, protocol.ErrorMessagePayload{
		Code:       code,
		Message:    message,
		RefID:      refID,
		ServerTime: time.Now().UnixMilli(),
	})
}

func (h *ChatHandler) sendReliableJSON(sess *rudp.Session, streamID, msgType uint8, value any) error {
	encoded, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return fmt.Errorf("chat: encode response: %w", err)
	}
	return sess.SendReliable(streamID, msgType, encoded)
}

func chatPacket(streamID, msgType uint8, connectionID uint32, payload []byte) ([]byte, error) {
	raw, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD,
			MsgType:      msgType,
			StreamID:     streamID,
			ConnectionID: connectionID,
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	})
	if err != nil {
		return nil, fmt.Errorf("chat: marshal routed packet: %w", err)
	}
	return raw, nil
}
