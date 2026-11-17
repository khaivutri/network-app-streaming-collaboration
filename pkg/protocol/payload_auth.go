package protocol

const (
	AuthCodeOK              = "AUTH_OK"
	AuthCodeUserNotFound    = "AUTH_USER_NOT_FOUND"
	AuthCodeInvalidPassword = "AUTH_INVALID_PASSWORD"
	AuthCodeAlreadyOnline   = "AUTH_ALREADY_ONLINE"
)

// AuthRequestPayload is the JSON body of AUTH_REQUEST (0x10).
type AuthRequestPayload struct {
	P2PPort       int    `json:"p2p_port,omitempty"`
	PeerTicket    string `json:"peer_ticket,omitempty"`
	RequestID     string `json:"request_id"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	ClientVersion string `json:"client_version"`
	Timestamp     int64  `json:"timestamp"`
}

// UserInfoDTO describes the authenticated client returned by the server.
type UserInfoDTO struct {
	P2PPort     int    `json:"p2p_port,omitempty"`
	PeerTicket  string `json:"peer_ticket,omitempty"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	LoginAt     int64  `json:"login_at"`
}

// PeerDTO is the wire representation of one online or offline peer.
type PeerDTO struct {
	P2PPort     int    `json:"p2p_port,omitempty"`
	PeerTicket  string `json:"peer_ticket,omitempty"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Status      string `json:"status"`
	LoginAt     int64  `json:"login_at"`
}

// AuthResponsePayload is the JSON body of AUTH_RESPONSE (0x11).
type AuthResponsePayload struct {
	ClusterVerifyKey string       `json:"cluster_verify_key,omitempty"`
	RequestID        string       `json:"request_id"`
	Success          bool         `json:"success"`
	Code             string       `json:"code"`
	Message          string       `json:"message"`
	User             *UserInfoDTO `json:"user,omitempty"`
	OnlinePeers      []PeerDTO    `json:"online_peers,omitempty"`
	ServerTime       int64        `json:"server_time"`
}

// DiscoUpdatePayload broadcasts one JOIN or LEAVE event.
type DiscoUpdatePayload struct {
	Event       string  `json:"event"`
	Reason      string  `json:"reason"`
	Peer        PeerDTO `json:"peer"`
	OnlineCount int     `json:"online_count"`
	ServerTime  int64   `json:"server_time"`
}

// DiscoPeersPayload requests a fresh online snapshot.
type DiscoPeersPayload struct {
	RequestID string `json:"request_id"`
}

// DiscoPeersResponsePayload returns the requested online snapshot.
type DiscoPeersResponsePayload struct {
	RequestID   string    `json:"request_id"`
	OnlineCount int       `json:"online_count"`
	Peers       []PeerDTO `json:"peers"`
	ServerTime  int64     `json:"server_time"`
}

// LogoutPayload is the JSON body of LOGOUT (0x12).
type LogoutPayload struct {
	RequestID string `json:"request_id"`
	Username  string `json:"username"`
	Reason    string `json:"reason"`
	Timestamp int64  `json:"timestamp"`
}

// LogoutResponsePayload confirms whether the server released the session.
type LogoutResponsePayload struct {
	RequestID  string `json:"request_id"`
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	ServerTime int64  `json:"server_time"`
}
