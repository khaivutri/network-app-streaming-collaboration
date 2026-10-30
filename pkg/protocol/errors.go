package protocol

import "errors"

var (
	ErrBufferTooShort     = errors.New("protocol: buffer shorter than 20-byte header")
	ErrUnsupportedVersion = errors.New("protocol: unsupported protocol version")
	ErrPayloadTooLarge    = errors.New("protocol: payload exceeds max MTU safety limit")
	ErrNilPacket          = errors.New("protocol: packet cannot be nil")
	ErrInvalidTCPFrame    = errors.New("protocol: invalid TCP frame length")
)
