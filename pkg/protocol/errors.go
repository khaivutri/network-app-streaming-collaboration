package protocol

import "errors"

var (
	ErrBufferTooShort     = errors.New("protocol: buffer shorter than 20-byte header")
	ErrUnsupportedVersion = errors.New("protocol: unsupported protocol version")
	ErrPayloadTooLarge    = errors.New("protocol: payload exceeds max MTU safety limit")
	ErrNilPacket          = errors.New("protocol: packet cannot be nil")
	ErrInvalidTCPFrame    = errors.New("protocol: invalid TCP frame length")
)

const (
	FileCodeReady            = "FILE_READY"
	FileCodeStorageError     = "FILE_STORAGE_ERROR"
	FileCodeRejected         = "FILE_REJECTED"
	FileCodeChecksumMismatch = "FILE_CHECKSUM_MISMATCH"
	FileCodeUserCancelled    = "USER_CANCELLED"
	FileCodePeerDisconnected = "PEER_DISCONNECTED"
	FileCodeChecksumFailed   = "CHECKSUM_FAILED"

	// ErrCodeMsgEmptyContent identifies a blank chat message.
	ErrCodeMsgEmptyContent = "MSG_EMPTY_CONTENT"
	// ErrCodeMsgSelfRecipient identifies an attempted direct message to oneself.
	ErrCodeMsgSelfRecipient = "MSG_SELF_RECIPIENT"
	// ErrCodeMsgRecipientOffline identifies an unavailable direct-message target.
	ErrCodeMsgRecipientOffline = "MSG_RECIPIENT_OFFLINE"
)

// ErrorMessagePayload is the standard JSON error response for an application message.
type ErrorMessagePayload struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	RefID      string `json:"ref_id,omitempty"`
	ServerTime int64  `json:"server_time"`
}
