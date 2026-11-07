package protocol

// PingPayload carries a client heartbeat sequence and send timestamp.
type PingPayload struct {
	Seq        uint32 `json:"seq"`
	ClientTime int64  `json:"client_time"`
}

// PongPayload acknowledges a heartbeat and adds the server timestamp.
type PongPayload struct {
	Seq        uint32 `json:"seq"`
	ClientTime int64  `json:"client_time"`
	ServerTime int64  `json:"server_time"`
}
