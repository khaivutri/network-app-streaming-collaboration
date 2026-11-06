package protocol

// FileInitPayload describes a file transfer request.
type FileInitPayload struct {
	TransferID  string `json:"transfer_id"`
	Sender      string `json:"sender"`
	Recipient   string `json:"recipient"`
	FileName    string `json:"file_name"`
	MimeType    string `json:"mime_type"`
	FileSize    int64  `json:"file_size"`
	ChunkSize   int    `json:"chunk_size"`
	TotalChunks uint32 `json:"total_chunks"`
	SHA256      string `json:"sha256"`
	Caption     string `json:"caption,omitempty"`
	CreatedAt   int64  `json:"created_at"`
}

// FileResponsePayload reports whether a receiver accepted a transfer.
type FileResponsePayload struct {
	TransferID      string `json:"transfer_id"`
	Accepted        bool   `json:"accepted"`
	Code            string `json:"code"`
	MaxWindowChunks uint16 `json:"max_window_chunks"`
	Message         string `json:"message"`
	Timestamp       int64  `json:"timestamp"`
}

// FileCompletePayload confirms verified transfer completion.
type FileCompletePayload struct {
	TransferID         string `json:"transfer_id"`
	VerifiedSHA256     string `json:"verified_sha256"`
	TotalBytesReceived int64  `json:"total_bytes_received"`
	CompletedAt        int64  `json:"completed_at"`
}

// FileCancelPayload reports cancellation and cleanup reason.
type FileCancelPayload struct {
	TransferID  string `json:"transfer_id"`
	CancelledBy string `json:"cancelled_by"`
	Reason      string `json:"reason"`
	Message     string `json:"message"`
	Timestamp   int64  `json:"timestamp"`
}
