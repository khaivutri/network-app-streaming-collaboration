package webbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// handleSession returns the browser bootstrap snapshot and process-local CSRF token.
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	snapshot := s.service.Snapshot()
	store := s.service.Store()
	writeJSON(w, http.StatusOK, SessionResponse{
		CSRFToken:         s.csrfToken,
		Authenticated:     snapshot.Authenticated,
		ConnectionState:   string(snapshot.ConnectionState),
		CurrentUser:       snapshot.CurrentUser,
		OnlinePeers:       nonNilSlice(snapshot.OnlinePeers),
		BroadcastMessages: nonNilSlice(store.GetBroadcastMessages()),
		DirectMessages:    nonNilMap(store.GetAllDirectMessages()),
		DeliveryStatus:    nonNilMap(store.GetDeliveryStatuses()),
		FileTransfers:     nonNilSlice(store.GetFileProgresses()),
	})
}

func nonNilSlice[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

func nonNilMap[K comparable, V any](values map[K]V) map[K]V {
	if values == nil {
		return map[K]V{}
	}
	return values
}

// handleLogin validates credentials and maps protocol auth codes to HTTP status.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var request loginRequest
	if err := decodeJSONBody(w, r, &request, 8<<10); err != nil {
		return
	}
	request.Username = strings.TrimSpace(request.Username)
	if request.Username == "" || utf8.RuneCountInString(request.Username) > 64 || request.Password == "" {
		writeError(w, http.StatusBadRequest, "AUTH_INVALID_REQUEST", "Tài khoản và mật khẩu là bắt buộc")
		return
	}
	ctx, cancel := requestContext(r.Context(), 10*time.Second)
	defer cancel()
	snapshot, err := s.service.Login(ctx, request.Username, request.Password)
	if err != nil {
		var authErr *client.AuthError
		if errors.As(err, &authErr) {
			status := http.StatusUnauthorized
			if authErr.Code == "AUTH_ALREADY_ONLINE" {
				status = http.StatusConflict
			}
			writeError(w, status, authErr.Code, authErr.Message)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeError(w, http.StatusServiceUnavailable, "ERR_SERVER_UNREACHABLE", "Không thể kết nối đến hệ thống")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "ERR_SERVER_UNREACHABLE", "Không thể kết nối đến hệ thống")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user":         snapshot.CurrentUser,
		"online_peers": snapshot.OnlinePeers,
	})
}

// handleLogout clears the local session even when UDP logout cannot reach server.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	ctx, cancel := requestContext(r.Context(), 1500*time.Millisecond)
	defer cancel()
	if err := s.service.Logout(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusServiceUnavailable, "ERR_LOGOUT_FAILED", "Không thể hoàn tất đăng xuất local")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, target any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "Dữ liệu JSON không hợp lệ")
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "Chỉ chấp nhận một JSON object")
		return fmt.Errorf("webbridge: trailing json")
	} else if !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "Dữ liệu JSON có phần dư không hợp lệ")
		return err
	}
	return nil
}

func requestContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
