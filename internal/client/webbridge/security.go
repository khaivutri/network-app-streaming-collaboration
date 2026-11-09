package webbridge

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const csrfTokenBytes = 16

func generateCSRFToken() (string, error) {
	bytes := make([]byte, csrfTokenBytes)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("webbridge: generate csrf token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func (s *Server) localRequestAllowed(r *http.Request) bool {
	host := r.Host
	if host == "" {
		return false
	}
	hostname := host
	if parsed, err := url.Parse("http://" + host); err == nil && parsed.Hostname() != "" {
		hostname = parsed.Hostname()
	}
	if hostname != "127.0.0.1" && hostname != "localhost" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsedOrigin, err := url.Parse(origin)
	if err != nil || parsedOrigin.Hostname() != hostname {
		return false
	}
	return parsedOrigin.Scheme == "http"
}

func (s *Server) csrfAllowed(r *http.Request) bool {
	provided := r.Header.Get("X-Client-CSRF-Token")
	if provided == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(s.csrfToken)) == 1
}

func securityHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
}

func (s *Server) securityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		securityHeaders(w)
		if !s.localRequestAllowed(r) {
			writeError(w, http.StatusForbidden, "LOCAL_ORIGIN_REJECTED", "Chỉ cho phép truy cập từ local agent")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/") && r.Method != http.MethodGet && !s.csrfAllowed(r) {
			writeError(w, http.StatusForbidden, "CSRF_TOKEN_REQUIRED", "Thiếu hoặc sai mã bảo vệ phiên local")
			return
		}
		next.ServeHTTP(w, r)
	})
}
