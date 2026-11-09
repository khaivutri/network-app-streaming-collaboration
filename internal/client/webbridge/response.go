package webbridge

import (
	"encoding/json"
	"net/http"
)

// APIError is the stable error body exposed to the browser.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Envelope is the common JSON response shape for all REST endpoints.
type Envelope struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data"`
	Error *APIError `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Envelope{
		OK:    status < http.StatusBadRequest,
		Data:  data,
		Error: nil,
	})
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Envelope{
		OK:   false,
		Data: nil,
		Error: &APIError{
			Code:    code,
			Message: message,
		},
	})
}
