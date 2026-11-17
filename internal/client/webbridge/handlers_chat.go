package webbridge

import (
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

type broadcastRequest struct {
	ClientMsgID string `json:"client_msg_id"`
	Content     string `json:"content"`
}

type unicastRequest struct {
	ClientMsgID string `json:"client_msg_id"`
	Recipient   string `json:"recipient"`
	Content     string `json:"content"`
}

// handlePeers returns the current detached presence snapshot.
func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"online_peers": s.service.Snapshot().OnlinePeers,
	})
}

// handlePeersRefresh enqueues a fresh server presence request.
func (s *Server) handlePeersRefresh(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	ctx, cancel := requestContext(r.Context(), 5*time.Second)
	defer cancel()
	if s.service.Snapshot().ConnectionState == "P2P_ONLY" {
		writeJSON(w, http.StatusOK, map[string]any{"status": "P2P_ONLY", "online_peers": s.service.Snapshot().OnlinePeers})
		return
	}
	if err := s.service.RefreshPeers(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "ERR_PEERS_REFRESH", "Không thể làm mới danh sách thành viên")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "REFRESHING"})
}

// handleBroadcast queues one room message and returns its correlation ID.
func (s *Server) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	var request broadcastRequest
	if err := decodeJSONBody(w, r, &request, 16<<10); err != nil {
		return
	}
	if err := validateMessage(request.ClientMsgID, request.Content); err != nil {
		writeError(w, http.StatusBadRequest, "MSG_INVALID_REQUEST", err.Error())
		return
	}
	ctx, cancel := requestContext(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.service.SendBroadcast(ctx, request.ClientMsgID, request.Content); err != nil {
		writeError(w, http.StatusServiceUnavailable, "MSG_SEND_FAILED", "Không thể đưa tin nhắn vào hàng đợi")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"client_msg_id": request.ClientMsgID,
		"status":        "SENDING",
		"queued_at":     time.Now().UnixMilli(),
	})
}

// handleUnicast queues one direct message.
func (s *Server) handleUnicast(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	var request unicastRequest
	if err := decodeJSONBody(w, r, &request, 16<<10); err != nil {
		return
	}
	if err := validateMessage(request.ClientMsgID, request.Content); err != nil || strings.TrimSpace(request.Recipient) == "" {
		writeError(w, http.StatusBadRequest, "MSG_INVALID_REQUEST", "Người nhận và nội dung là bắt buộc")
		return
	}
	if current := s.service.Snapshot().CurrentUser; current != nil && strings.EqualFold(current.Username, strings.TrimSpace(request.Recipient)) {
		writeError(w, http.StatusBadRequest, "MSG_SELF_RECIPIENT", "Không thể tự gửi tin nhắn cho chính mình")
		return
	}
	online := false
	for _, peer := range s.service.Snapshot().OnlinePeers {
		if peer.Username == request.Recipient && peer.Status == "ONLINE" {
			online = true
			break
		}
	}
	if !online {
		writeError(w, http.StatusConflict, "MSG_RECIPIENT_OFFLINE", "Người nhận hiện đang ngoại tuyến")
		return
	}
	ctx, cancel := requestContext(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.service.SendDirect(ctx, request.ClientMsgID, request.Recipient, request.Content); err != nil {
		writeError(w, http.StatusServiceUnavailable, "MSG_SEND_FAILED", "Không thể đưa tin nhắn vào hàng đợi")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"client_msg_id": request.ClientMsgID,
		"status":        "SENDING",
		"queued_at":     time.Now().UnixMilli(),
	})
}

func validateMessage(clientMsgID, content string) error {
	if strings.TrimSpace(clientMsgID) == "" || utf8.RuneCountInString(clientMsgID) > 128 {
		return fmt.Errorf("mã tin nhắn không hợp lệ")
	}
	if strings.TrimSpace(content) == "" || len([]byte(content)) > 1024 {
		return fmt.Errorf("nội dung tin nhắn trống hoặc vượt giới hạn")
	}
	return nil
}
