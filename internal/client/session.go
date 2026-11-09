package client

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"time"

	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

const defaultHandshakeTimeout = 3 * time.Second

// NetworkSession is a live client-side UDP connection after the three-way handshake.
type NetworkSession struct {
	Conn         *net.UDPConn
	Server       *net.UDPAddr
	ConnectionID uint32
	Cipher       *security.SessionCipher
}

// dialPersistentSession performs the crypto handshake without sending FIN.
// The benchmark helper in pkg/transport/rudp intentionally closes its socket,
// so the UI agent uses this lifecycle-specific implementation instead.
func dialPersistentSession(ctx context.Context, serverAddr string, timeout time.Duration) (*NetworkSession, error) {
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	server, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("client session: resolve server: %w", err)
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, fmt.Errorf("client session: bind socket: %w", err)
	}
	closeOnError := func(cause error) (*NetworkSession, error) {
		_ = conn.Close()
		return nil, cause
	}

	keys, err := security.GenerateX25519KeyPair()
	if err != nil {
		return closeOnError(fmt.Errorf("client session: generate key pair: %w", err))
	}
	var clientNonce [16]byte
	if _, err := rand.Read(clientNonce[:]); err != nil {
		return closeOnError(fmt.Errorf("client session: generate nonce: %w", err))
	}
	hello, err := protocol.EncodeJSONPayload(protocol.HandshakeSYNPayload{
		ClientVersion: protocol.DefaultClientVersion,
		Timestamp:     time.Now().UnixMilli(),
		ClientPubKey:  base64.StdEncoding.EncodeToString(keys.PublicKey[:]),
		ClientNonce:   base64.StdEncoding.EncodeToString(clientNonce[:]),
	})
	if err != nil {
		return closeOnError(fmt.Errorf("client session: encode hello: %w", err))
	}
	syn, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:  protocol.ProtocolVersion,
			Flags:    protocol.FlagSYN,
			MsgType:  protocol.MsgCtrlHandshake,
			StreamID: protocol.StreamControl,
		},
		Payload: hello,
	})
	if err != nil {
		return closeOnError(fmt.Errorf("client session: marshal SYN: %w", err))
	}
	if _, err := conn.WriteToUDP(syn, server); err != nil {
		return closeOnError(fmt.Errorf("client session: send SYN: %w", err))
	}
	deadline := time.Now().Add(timeout)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		return closeOnError(fmt.Errorf("client session: set handshake deadline: %w", err))
	}
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload+security.Overhead)
	var welcome *protocol.Packet
	for {
		n, remote, readErr := conn.ReadFromUDP(buffer)
		if readErr != nil {
			if ctx.Err() != nil {
				return closeOnError(ctx.Err())
			}
			return closeOnError(fmt.Errorf("client session: receive SYN-ACK: %w", readErr))
		}
		if remote == nil || remote.String() != server.String() {
			continue
		}
		candidate, parseErr := protocol.UnmarshalPacket(buffer[:n])
		if parseErr == nil && candidate.Hdr.HasFlag(protocol.FlagSYN) && candidate.Hdr.HasFlag(protocol.FlagACK) {
			welcome = candidate
			break
		}
	}
	if welcome.Hdr.ConnectionID == 0 {
		return closeOnError(fmt.Errorf("client session: server returned empty connection ID"))
	}

	var sessionCipher *security.SessionCipher
	welcomePayload, decodeErr := protocol.DecodeJSONPayload[protocol.HandshakeSYNACKPayload](welcome.Payload)
	if decodeErr == nil && welcomePayload.ServerPubKey != "" && welcomePayload.ServerNonce != "" {
		serverPubBytes, pubErr := base64.StdEncoding.DecodeString(welcomePayload.ServerPubKey)
		serverNonceBytes, nonceErr := base64.StdEncoding.DecodeString(welcomePayload.ServerNonce)
		if pubErr != nil || len(serverPubBytes) != 32 || nonceErr != nil || len(serverNonceBytes) != 16 {
			return closeOnError(fmt.Errorf("client session: invalid server crypto metadata"))
		}
		var serverPub [32]byte
		var serverNonce [16]byte
		copy(serverPub[:], serverPubBytes)
		copy(serverNonce[:], serverNonceBytes)
		writeKey, readKey, deriveErr := security.DeriveSessionKeys(keys.PrivateKey, serverPub, clientNonce, serverNonce, welcome.Hdr.ConnectionID, false)
		if deriveErr != nil {
			return closeOnError(fmt.Errorf("client session: derive keys: %w", deriveErr))
		}
		sessionCipher, err = security.NewSessionCipher(writeKey, readKey, false)
		if err != nil {
			return closeOnError(fmt.Errorf("client session: create cipher: %w", err))
		}
	} else {
		if decodeErr != nil {
			return closeOnError(fmt.Errorf("client session: decode server crypto metadata: %w", decodeErr))
		}
		return closeOnError(fmt.Errorf("client session: server omitted required crypto metadata"))
	}
	ack, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			StreamID:     protocol.StreamControl,
			ConnectionID: welcome.Hdr.ConnectionID,
			SeqNum:       1,
			AckNum:       1,
			WindowSize:   protocol.DefaultWindowSize,
		},
	})
	if err != nil {
		return closeOnError(fmt.Errorf("client session: marshal ACK: %w", err))
	}
	if _, err := conn.WriteToUDP(ack, server); err != nil {
		return closeOnError(fmt.Errorf("client session: send ACK: %w", err))
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return closeOnError(fmt.Errorf("client session: clear read deadline: %w", err))
	}
	return &NetworkSession{
		Conn:         conn,
		Server:       server,
		ConnectionID: welcome.Hdr.ConnectionID,
		Cipher:       sessionCipher,
	}, nil
}
