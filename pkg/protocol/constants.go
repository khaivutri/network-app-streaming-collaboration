// Package protocol defines the shared wire format for the application protocol.
package protocol

import "strings"

const (
	ProtocolVersion   uint8  = 0x01
	HeaderSize        int    = 20
	MaxUDPPayload     int    = 1380
	DefaultWindowSize uint16 = 64
)

const (
	FlagSYN  uint8 = 0x01
	FlagACK  uint8 = 0x02
	FlagFIN  uint8 = 0x04
	FlagRST  uint8 = 0x08
	FlagREL  uint8 = 0x10
	FlagORD  uint8 = 0x20
	FlagFRAG uint8 = 0x40
	FlagENC  uint8 = 0x80
)

const (
	StreamControl       uint8 = 0x00
	StreamBroadcastChat uint8 = 0x01
	StreamDirectChat    uint8 = 0x02
	StreamFileMin       uint8 = 0x03
	StreamFileMax       uint8 = 0xFE
)

const (
	MsgCtrlHandshake      uint8 = 0x00
	MsgAuthRequest        uint8 = 0x10
	MsgAuthResponse       uint8 = 0x11
	MsgLogout             uint8 = 0x12
	MsgLogoutResponse     uint8 = 0x13
	MsgDiscoPeers         uint8 = 0x20
	MsgDiscoPeersResponse uint8 = 0x21
	MsgDiscoUpdate        uint8 = 0x22
	MsgMessBroadcast      uint8 = 0x30
	MsgMessUnicast        uint8 = 0x31
	MsgMessDelivered      uint8 = 0x32
	MsgFileInit           uint8 = 0x40
	MsgFileResponse       uint8 = 0x41
	MsgFileChunk          uint8 = 0x42
	MsgFileComplete       uint8 = 0x43
	MsgFileCancel         uint8 = 0x44
	MsgAck                uint8 = 0x50
	MsgPing               uint8 = 0x51
	MsgPong               uint8 = 0x52
	MsgWindowUpdate       uint8 = 0x53
	MsgError              uint8 = 0x54
	MsgPeerHello          uint8 = 0x60
	MsgPeerHelloAck       uint8 = 0x61
	MsgPeerBye            uint8 = 0x62
)

// FormatFlags returns a stable human-readable representation of a flag mask.
func FormatFlags(flags uint8) string {
	if flags == 0 {
		return "NONE"
	}
	names := make([]string, 0, 8)
	for _, item := range []struct {
		name string
		flag uint8
	}{{"SYN", FlagSYN}, {"ACK", FlagACK}, {"FIN", FlagFIN}, {"RST", FlagRST}, {"REL", FlagREL}, {"ORD", FlagORD}, {"FRAG", FlagFRAG}, {"ENC", FlagENC}} {
		if flags&item.flag != 0 {
			names = append(names, item.name)
		}
	}
	return strings.Join(names, "|")
}

// MsgTypeName returns a readable name for a message type.
func MsgTypeName(msgType uint8) string {
	names := map[uint8]string{MsgCtrlHandshake: "CTRL_HANDSHAKE", MsgAuthRequest: "AUTH_REQUEST", MsgAuthResponse: "AUTH_RESPONSE", MsgLogout: "LOGOUT", MsgLogoutResponse: "LOGOUT_RESPONSE", MsgDiscoPeers: "DISCO_PEERS", MsgDiscoPeersResponse: "DISCO_PEERS_RESPONSE", MsgDiscoUpdate: "DISCO_UPDATE", MsgMessBroadcast: "MESS_BROADCAST", MsgMessUnicast: "MESS_UNICAST", MsgMessDelivered: "MESS_DELIVERED", MsgFileInit: "FILE_INIT", MsgFileResponse: "FILE_RESPONSE", MsgFileChunk: "FILE_CHUNK", MsgFileComplete: "FILE_COMPLETE", MsgFileCancel: "FILE_CANCEL", MsgAck: "ACK", MsgPing: "PING", MsgPong: "PONG", MsgWindowUpdate: "WINDOW_UPDATE", MsgError: "ERROR", MsgPeerHello: "PEER_HELLO", MsgPeerHelloAck: "PEER_HELLO_ACK", MsgPeerBye: "PEER_BYE"}
	if name, ok := names[msgType]; ok {
		return name
	}
	return "UNKNOWN"
}
