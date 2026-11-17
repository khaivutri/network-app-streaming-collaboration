package client

import (
	"context"
	"fmt"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"net"
	"time"

	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
)

// AuthClient owns the UDP request/response flow for one connection.
type AuthClient struct {
	conn          rudp.PacketConn
	server        *net.UDPAddr
	connectionID  uint32
	store         *state.ClientStore
	cipher        *security.SessionCipher
	p2pPort       int
	preserveState bool
}

// SetCipher enables directional payload encryption for authentication traffic.
func (c *AuthClient) SetCipher(cipher *security.SessionCipher) { c.cipher = cipher }

// SetP2PPort advertises the separate peer listener during login.
func (c *AuthClient) SetP2PPort(port int) { c.p2pPort = port }

// PreserveState keeps local history while a server session is renewed.
func (c *AuthClient) PreserveState() { c.preserveState = true }

// NewAuthClient binds a client socket to a server endpoint and connection ID.
func NewAuthClient(conn rudp.PacketConn, server *net.UDPAddr, connectionID uint32, store *state.ClientStore) *AuthClient {
	return &AuthClient{conn: conn, server: server, connectionID: connectionID, store: store}
}

// Login commits the returned peer snapshot only after successful authentication.
func (c *AuthClient) Login(ctx context.Context, username, password string) (protocol.AuthResponsePayload, error) {
	requestID := fmt.Sprintf("auth-%d", time.Now().UnixNano())
	err := c.send(ctx, protocol.MsgAuthRequest, protocol.AuthRequestPayload{
		P2PPort:       c.p2pPort,
		RequestID:     requestID,
		Username:      username,
		Password:      password,
		ClientVersion: protocol.DefaultClientVersion,
		Timestamp:     time.Now().UnixMilli(),
	})
	if err != nil {
		return protocol.AuthResponsePayload{}, err
	}
	pkt, err := c.read(ctx, protocol.MsgAuthResponse)
	if err != nil {
		return protocol.AuthResponsePayload{}, err
	}
	resp, err := protocol.DecodeJSONPayload[protocol.AuthResponsePayload](pkt.Payload)
	if err != nil {
		return protocol.AuthResponsePayload{}, err
	}
	if resp.Success {
		if c.preserveState {
			c.store.SyncServerSession(resp.User, resp.OnlinePeers)
		} else {
			c.store.SetLoginSuccess(resp.User, resp.OnlinePeers)
		}
	}
	return resp, nil
}

// Logout always clears local state, including timeout and transport-error paths.
func (c *AuthClient) Logout(ctx context.Context, username string) (protocol.LogoutResponsePayload, error) {
	requestID := fmt.Sprintf("logout-%d", time.Now().UnixNano())
	if err := c.send(ctx, protocol.MsgLogout, protocol.LogoutPayload{
		RequestID: requestID,
		Username:  username,
		Reason:    "USER_INITIATED",
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		c.store.ClearSession()
		return protocol.LogoutResponsePayload{}, err
	}
	pkt, err := c.read(ctx, protocol.MsgLogoutResponse)
	if err != nil {
		c.store.ClearSession()
		return protocol.LogoutResponsePayload{}, err
	}
	resp, err := protocol.DecodeJSONPayload[protocol.LogoutResponsePayload](pkt.Payload)
	c.store.ClearSession()
	return resp, err
}

// send serializes one control request and honors cancellation before I/O.
func (c *AuthClient) send(ctx context.Context, msgType uint8, value any) error {
	payload, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return err
	}
	hdr := protocol.Header{
		Version:      protocol.ProtocolVersion,
		Flags:        protocol.FlagREL | protocol.FlagORD,
		MsgType:      msgType,
		StreamID:     protocol.StreamControl,
		ConnectionID: c.connectionID,
		WindowSize:   protocol.DefaultWindowSize,
	}
	var raw []byte
	if c.cipher != nil {
		raw, err = c.cipher.EncryptPacket(hdr, payload)
	} else {
		raw, err = protocol.MarshalPacket(&protocol.Packet{Hdr: hdr, Payload: payload})
	}
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	_, err = c.conn.WriteToUDP(raw, c.server)
	return err
}

// read waits for the expected message and ignores unrelated packets.
func (c *AuthClient) read(ctx context.Context, expected uint8) (*protocol.Packet, error) {
	deadline := time.Now().Add(time.Second)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		pkt, err := protocol.UnmarshalPacket(buf[:n])
		if err == nil && pkt.Hdr.HasFlag(protocol.FlagENC) && c.cipher != nil {
			pkt.Payload, err = c.cipher.DecryptPacket(pkt.Hdr, buf[:protocol.HeaderSize], pkt.Payload)
		}
		if err == nil && pkt.Hdr.MsgType == expected {
			return pkt, nil
		}
	}
}
