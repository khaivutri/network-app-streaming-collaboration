package protocol

// BroadcastMessageClientPayload is a room-message request from a client.
type BroadcastMessageClientPayload struct {
	ClientMsgID string `json:"client_msg_id"`
	Content     string `json:"content"`
	SentAt      int64  `json:"sent_at"`
}

// BroadcastMessageServerPayload is a room message authenticated and fanned out by the server.
type BroadcastMessageServerPayload struct {
	Scope         string `json:"scope"`
	MsgID         string `json:"msg_id"`
	ClientMsgID   string `json:"client_msg_id"`
	Sender        string `json:"sender"`
	SenderDisplay string `json:"sender_display"`
	Content       string `json:"content"`
	ServerTime    int64  `json:"server_time"`
}

// UnicastMessageClientPayload is a direct-message request from a client.
type UnicastMessageClientPayload struct {
	ClientMsgID string `json:"client_msg_id"`
	Recipient   string `json:"recipient"`
	Content     string `json:"content"`
	SentAt      int64  `json:"sent_at"`
}

// UnicastMessageServerPayload is a direct message addressed to one recipient.
type UnicastMessageServerPayload struct {
	Scope         string `json:"scope"`
	MsgID         string `json:"msg_id"`
	ClientMsgID   string `json:"client_msg_id"`
	Sender        string `json:"sender"`
	SenderDisplay string `json:"sender_display"`
	Recipient     string `json:"recipient"`
	Content       string `json:"content"`
	ServerTime    int64  `json:"server_time"`
}

// MessageDeliveredPayload confirms server-side delivery of a chat message.
type MessageDeliveredPayload struct {
	MsgID       string `json:"msg_id"`
	ClientMsgID string `json:"client_msg_id"`
	Scope       string `json:"scope"`
	Recipient   string `json:"recipient"`
	Status      string `json:"status"`
	DeliveredAt int64  `json:"delivered_at"`
}
