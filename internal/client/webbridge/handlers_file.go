package webbridge

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
)

// handleFileSend streams one multipart upload to an agent-owned temporary file.
func (s *Server) handleFileSend(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxUploadBytes+1<<20)
	reader, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "FILE_INVALID_MULTIPART", "Dữ liệu multipart không hợp lệ")
		return
	}
	var recipient, caption, displayName string
	var tempPath string
	var tempFile *os.File
	var fileSeen bool
	cleanup := func() {
		if tempFile != nil {
			_ = tempFile.Close()
		}
		if tempPath != "" {
			_ = os.Remove(tempPath)
		}
	}
	defer func() {
		if tempFile != nil {
			_ = tempFile.Close()
		}
	}()
	if err := os.MkdirAll(s.config.UploadDir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "FILE_STORAGE_ERROR", "Không thể tạo vùng tạm cho tệp")
		return
	}
	for {
		part, nextErr := reader.NextPart()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			cleanup()
			writeError(w, http.StatusBadRequest, "FILE_INVALID_MULTIPART", "Không thể đọc phần tệp")
			return
		}
		fieldName := part.FormName()
		switch fieldName {
		case "recipient", "caption":
			value, readErr := readFormField(part, 1024)
			if readErr != nil {
				cleanup()
				writeError(w, http.StatusBadRequest, "FILE_INVALID_FIELD", "Trường biểu mẫu không hợp lệ")
				return
			}
			if fieldName == "recipient" {
				recipient = strings.TrimSpace(value)
			} else {
				caption = value
			}
		case "file":
			if fileSeen || part.FileName() == "" {
				cleanup()
				writeError(w, http.StatusBadRequest, "FILE_INVALID_SOURCE", "Cần đúng một tệp hợp lệ")
				return
			}
			fileSeen = true
			displayName = transfer.SanitizeFileName(part.FileName())
			tempFile, err = os.CreateTemp(s.config.UploadDir, "ui-upload-*")
			if err != nil {
				cleanup()
				writeError(w, http.StatusInternalServerError, "FILE_STORAGE_ERROR", "Không thể tạo tệp tạm")
				return
			}
			tempPath = tempFile.Name()
			written, copyErr := io.Copy(tempFile, io.LimitReader(part, s.config.MaxUploadBytes+1))
			if copyErr != nil {
				cleanup()
				writeError(w, http.StatusBadRequest, "FILE_UPLOAD_FAILED", "Không thể đọc tệp tải lên")
				return
			}
			if written > s.config.MaxUploadBytes {
				cleanup()
				writeError(w, http.StatusRequestEntityTooLarge, "FILE_TOO_LARGE", "Tệp vượt quá giới hạn cho phép")
				return
			}
		default:
			cleanup()
			writeError(w, http.StatusBadRequest, "FILE_UNKNOWN_FIELD", "Trường multipart không được hỗ trợ")
			return
		}
	}
	if recipient == "" || !fileSeen || displayName == "" {
		cleanup()
		writeError(w, http.StatusBadRequest, "FILE_INVALID_REQUEST", "Người nhận và tệp là bắt buộc")
		return
	}
	if err := tempFile.Close(); err != nil {
		cleanup()
		writeError(w, http.StatusInternalServerError, "FILE_STORAGE_ERROR", "Không thể đóng tệp tạm")
		return
	}
	tempFile = nil
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	transferID, err := s.service.SendFileNamed(ctx, recipient, tempPath, displayName, caption)
	if err != nil {
		cleanup()
		writeError(w, http.StatusConflict, "FILE_SEND_FAILED", "Không thể khởi tạo truyền tệp")
		return
	}
	go s.cleanupUploadAfterTerminalState(transferID, tempPath)
	s.uploadMu.Lock()
	s.uploadSources[transferID] = tempPath
	s.uploadMu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"transfer_id": transferID,
		"file_name":   displayName,
		"recipient":   recipient,
		"state":       "INIT",
	})
}

// handleFileRetry starts a new attempt from the retained browser upload source.
func (s *Server) handleFileRetry(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	oldID := r.PathValue("transfer_id")
	s.uploadMu.Lock()
	sourcePath := s.uploadSources[oldID]
	s.uploadMu.Unlock()
	if sourcePath == "" {
		writeError(w, http.StatusNotFound, "FILE_RETRY_UNAVAILABLE", "Không còn tệp nguồn để thử lại")
		return
	}
	progress, ok := s.service.Store().GetFileProgress(oldID)
	if !ok || progress == nil || progress.Peer == "" {
		writeError(w, http.StatusConflict, "FILE_RETRY_INVALID_STATE", "Trạng thái truyền tệp không cho phép thử lại")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	newID, err := s.service.SendFileNamed(ctx, progress.Peer, sourcePath, progress.FileName, "")
	if err != nil {
		writeError(w, http.StatusConflict, "FILE_RETRY_FAILED", "Không thể khởi tạo lần truyền lại")
		return
	}
	s.uploadMu.Lock()
	s.uploadSources[newID] = sourcePath
	s.uploadMu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"transfer_id": newID,
		"file_name":   progress.FileName,
		"recipient":   progress.Peer,
		"state":       "INIT",
	})
}

// handleFileCancel requests transfer cancellation by opaque transfer ID.
func (s *Server) handleFileCancel(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	id := r.PathValue("transfer_id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "FILE_INVALID_ID", "Mã truyền tệp không hợp lệ")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.service.CancelTransfer(ctx, id); err != nil {
		writeError(w, http.StatusConflict, "FILE_CANCEL_FAILED", "Không thể hủy truyền tệp")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"transfer_id": id, "state": "CANCELLED"})
}

// handleFileDownload streams a verified completed incoming file by transfer ID.
func (s *Server) handleFileDownload(w http.ResponseWriter, r *http.Request) {
	if !s.service.Snapshot().Authenticated {
		writeError(w, http.StatusUnauthorized, "ERR_UNAUTHORIZED", "Vui lòng đăng nhập")
		return
	}
	id := r.PathValue("transfer_id")
	file, progress, err := s.service.OpenCompletedFile(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "FILE_NOT_FOUND", "Không tìm thấy tệp hoàn chỉnh")
		return
	}
	defer file.Close()
	fileName := transfer.SanitizeFileName(progress.FileName)
	if fileName == "" {
		fileName = "download.bin"
	}
	contentType := "application/octet-stream"
	if value := mime.TypeByExtension(filepath.Ext(fileName)); value != "" {
		contentType = value
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": fileName}))
	stat, statErr := file.Stat()
	if statErr == nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()))
	}
	_, _ = io.Copy(w, file)
}

func readFormField(reader io.Reader, maxBytes int64) (string, error) {
	value, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(value)) > maxBytes {
		return "", fmt.Errorf("webbridge: multipart field too large")
	}
	return string(value), nil
}

func (s *Server) cleanupUploadAfterTerminalState(id, path string) {
	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) {
		progress, ok := s.service.Store().GetFileProgress(id)
		if ok && progress != nil {
			switch progress.State {
			case "COMPLETED":
				_ = os.Remove(path)
				s.uploadMu.Lock()
				delete(s.uploadSources, id)
				s.uploadMu.Unlock()
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = os.Remove(path)
}
