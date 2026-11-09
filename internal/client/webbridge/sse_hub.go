package webbridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/events"
)

// handleEvents streams domain events over standard HTTP/1.1 Server-Sent Events.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "SSE_UNSUPPORTED", "Máy chủ không hỗ trợ luồng sự kiện")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	subscriptionID, channel := s.service.Events().Subscribe(64)
	defer s.service.Events().Unsubscribe(subscriptionID)
	ready := map[string]any{
		"connection_state": string(s.service.Snapshot().ConnectionState),
		"server_time":      time.Now().UnixMilli(),
	}
	if err := writeSSE(w, events.Event{
		ID:   0,
		Name: "ready",
		Data: ready,
	}); err != nil {
		return
	}
	flusher.Flush()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, open := <-channel:
			if !open {
				return
			}
			if err := writeSSE(w, event); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event events.Event) error {
	data, err := json.Marshal(event.Data)
	if err != nil {
		return fmt.Errorf("webbridge: encode sse data: %w", err)
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.ID, event.Name, data); err != nil {
		return fmt.Errorf("webbridge: write sse event: %w", err)
	}
	return nil
}
