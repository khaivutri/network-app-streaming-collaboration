package protocol

// PeerHelloPayload binds a peer session to an advertised account.
type PeerHelloPayload struct {
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	P2PPort     int    `json:"p2p_port"`
	PeerTicket  string `json:"peer_ticket,omitempty"`
	Timestamp   int64  `json:"timestamp"`
}

// PeerByePayload announces an intentional peer disconnect.
type PeerByePayload struct {
	Username  string `json:"username"`
	Reason    string `json:"reason"`
	Timestamp int64  `json:"timestamp"`
}
