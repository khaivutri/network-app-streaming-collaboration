package handlers

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
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
		return h.sendError(sess, protocol.ErrCodeMsgEmptyContent, "Nội dung tin nhắn không được để trống", request.ClientMsgID, protocol.MsgMessBroadcast, true)
	}
	sender, ok := state.LookupUsernameByConnID(sess.ID())
	if !ok {
		return fmt.Errorf("chat: unauthenticated sender connection %d", sess.ID())
	}
	senderInfo, _ := state.LookupTargetByUsername(sender)
	now := time.Now().UnixMilli()
	msgID := fmt.Sprintf("srv-bcast-%05d", h.msgCounter.Add(1))
	message := protocol.BroadcastMessageServerPayload{
		Scope:         "BROADCAST",
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
		raw, marshalErr := chatPacket(protocol.StreamBroadcastChat, protocol.MsgMessBroadcast, target.ConnectionID, target.Cipher, encoded)
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
		return h.sendError(sess, protocol.ErrCodeMsgEmptyContent, "Nội dung tin nhắn không được để trống", request.ClientMsgID, protocol.MsgMessUnicast, true)
	}
	sender, ok := state.LookupUsernameByConnID(sess.ID())
	if !ok {
		return fmt.Errorf("chat: unauthenticated sender connection %d", sess.ID())
	}
	if request.Recipient == sender {
		return h.sendError(sess, protocol.ErrCodeMsgSelfRecipient, "Không thể gửi tin nhắn riêng cho chính mình", request.ClientMsgID, protocol.MsgMessUnicast, true)
	}
	recipient, online := state.LookupTargetByUsername(request.Recipient)
	if !online || recipient.OutboundCh == nil {
		return h.sendError(sess, protocol.ErrCodeMsgRecipientOffline, "Người nhận hiện không trực tuyến", request.ClientMsgID, protocol.MsgMessUnicast, true)
	}
	senderInfo, _ := state.LookupTargetByUsername(sender)
	now := time.Now().UnixMilli()
	msgID := fmt.Sprintf("srv-dm-%05d", h.msgCounter.Add(1))
	message := protocol.UnicastMessageServerPayload{
		Scope:         "UNICAST",
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
	if recipient.ReliableSession == nil {
		return fmt.Errorf("chat: recipient session unavailable")
	}
	if err := recipient.ReliableSession.SendReliable(protocol.StreamDirectChat, protocol.MsgMessUnicast, encoded); err != nil {
		return err
	}
	// Echo the canonical server message so the sender store and browser converge.
	if senderSession, found := state.LookupTargetByUsername(sender); found && senderSession.ReliableSession != nil {
		if err := senderSession.ReliableSession.SendReliable(protocol.StreamDirectChat, protocol.MsgMessUnicast, encoded); err != nil {
			return err
		}
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

func (h *ChatHandler) sendError(sess *rudp.Session, code, message, refID string, refMsgType uint8, recoverable bool) error {
	return h.sendReliableJSON(sess, protocol.StreamControl, protocol.MsgError, protocol.ErrorMessagePayload{
		Code:        code,
		Message:     message,
		RefID:       refID,
		RefMsgType:  refMsgType,
		Recoverable: recoverable,
		ServerTime:  time.Now().UnixMilli(),
	})
}

func (h *ChatHandler) sendReliableJSON(sess *rudp.Session, streamID, msgType uint8, value any) error {
	encoded, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return fmt.Errorf("chat: encode response: %w", err)
	}
	return sess.SendReliable(streamID, msgType, encoded)
}

func chatPacket(streamID, msgType uint8, connectionID uint32, cipher *security.SessionCipher, payload []byte) ([]byte, error) {
	hdr := protocol.Header{
		Version:      protocol.ProtocolVersion,
		Flags:        protocol.FlagREL | protocol.FlagORD,
		MsgType:      msgType,
		StreamID:     streamID,
		ConnectionID: connectionID,
		WindowSize:   protocol.DefaultWindowSize,
	}
	var raw []byte
	var err error
	if cipher != nil {
		raw, err = cipher.EncryptPacket(hdr, payload)
	} else {
		raw, err = protocol.MarshalPacket(&protocol.Packet{Hdr: hdr, Payload: payload})
	}
	if err != nil {
		return nil, fmt.Errorf("chat: marshal routed packet: %w", err)
	}
	return raw, nil
}
