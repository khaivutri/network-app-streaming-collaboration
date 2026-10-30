package protocol

import "encoding/json"

const (
	HandshakeStatusAcknowledged = "ACKNOWLEDGED"
	DefaultClientVersion        = "1.0.0"
	DefaultWelcomeMessage       = "Kết nối máy chủ điều phối thành công"
)

// HandshakeSYNPayload is the JSON hello sent by a client.
type HandshakeSYNPayload struct {
	ClientVersion string `json:"client_version"`
	Hostname      string `json:"hostname"`
	Timestamp     int64  `json:"timestamp"`
	ClientPubKey  string `json:"client_pub_key,omitempty"`
	ClientNonce   string `json:"client_nonce,omitempty"`
}

// HandshakeSYNACKPayload is the JSON welcome returned by a server.
type HandshakeSYNACKPayload struct {
	Status         string `json:"status"`
	Message        string `json:"message"`
	AssignedConnID uint32 `json:"assigned_conn_id"`
	ClientAddr     string `json:"client_addr"`
	ServerTime     int64  `json:"server_time"`
	ServerPubKey   string `json:"server_pub_key,omitempty"`
	ServerNonce    string `json:"server_nonce,omitempty"`
}

// EncodeJSONPayload serializes a handshake DTO.
func EncodeJSONPayload(v any) ([]byte, error) { return json.Marshal(v) }

// DecodeJSONPayload deserializes a handshake DTO.
func DecodeJSONPayload[T any](data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}
