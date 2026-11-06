package client

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// ChatClient sends and receives Step 5 messages on one authenticated UDP socket.
type ChatClient struct {
	conn    *net.UDPConn
	server  *net.UDPAddr
	connID  uint32
	store   *state.ClientStore
	mu      sync.Mutex
	streams map[uint8]*rudp.Stream
}

// NewChatClient creates a chat facade over an existing authenticated socket.
func NewChatClient(conn *net.UDPConn, server *net.UDPAddr, connectionID uint32, store *state.ClientStore) *ChatClient {
	return &ChatClient{
		conn:    conn,
		server:  server,
		connID:  connectionID,
		store:   store,
		streams: make(map[uint8]*rudp.Stream),
	}
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

// Listen consumes server messages until ctx is cancelled and updates the local store.
func (c *ChatClient) Listen(ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("chat client: recovered receiver panic: %v", recovered)
		}
	}()
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	for {
		if deadline, ok := ctx.Deadline(); ok {
			if setErr := c.conn.SetReadDeadline(deadline); setErr != nil {
				return fmt.Errorf("chat client: set read deadline: %w", setErr)
			}
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
		c.getStream(packet.Hdr.StreamID).ProcessAck(packet.Hdr.AckNum, packet.Hdr.AckBitmap, packet.Hdr.WindowSize)
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
