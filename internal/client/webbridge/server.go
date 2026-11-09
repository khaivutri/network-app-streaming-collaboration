package webbridge

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/web"
)

// Config controls the loopback HTTP server and upload safety limit.
type Config struct {
	ListenAddr        string
	UploadDir         string
	MaxUploadBytes    int64
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
}

// Server serves the embedded UI and REST/SSE bridge for one ClientApp.
type Server struct {
	service       ClientService
	config        Config
	csrfToken     string
	mux           *http.ServeMux
	http          *http.Server
	listener      net.Listener
	closeOnce     sync.Once
	uploadMu      sync.Mutex
	uploadSources map[string]string
}

// NewServer creates a bridge without binding a port.
func NewServer(service ClientService, config Config) (*Server, error) {
	if service == nil {
		return nil, fmt.Errorf("webbridge: client service is required")
	}
	if config.ListenAddr == "" {
		config.ListenAddr = "127.0.0.1:8081"
	}
	if config.MaxUploadBytes <= 0 {
		config.MaxUploadBytes = 1 << 30
	}
	if config.UploadDir == "" {
		config.UploadDir = "storage/staging/uploads"
	}
	if config.ReadHeaderTimeout <= 0 {
		config.ReadHeaderTimeout = 5 * time.Second
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = 60 * time.Second
	}
	token, err := generateCSRFToken()
	if err != nil {
		return nil, err
	}
	staticFS, err := web.StaticFiles()
	if err != nil {
		return nil, fmt.Errorf("webbridge: load embedded static files: %w", err)
	}
	server := &Server{
		service:       service,
		config:        config,
		csrfToken:     token,
		mux:           http.NewServeMux(),
		uploadSources: make(map[string]string),
	}
	server.registerRoutes(staticFS)
	server.http = &http.Server{
		Handler:           server.securityMiddleware(server.mux),
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		IdleTimeout:       config.IdleTimeout,
	}
	return server, nil
}

// CSRFToken returns the in-process token for tests and bootstrap wiring.
func (s *Server) CSRFToken() string { return s.csrfToken }

// Handler returns the fully protected HTTP handler for httptest or embedding.
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Start binds loopback and serves until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	if !strings.HasPrefix(s.config.ListenAddr, "127.0.0.1:") {
		return fmt.Errorf("webbridge: listen address must bind 127.0.0.1")
	}
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("webbridge: listen: %w", err)
	}
	s.listener = listener
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.http.Serve(listener) }()
	go func() {
		<-ctx.Done()
		_ = s.Shutdown(context.Background())
	}()
	err = <-serveErr
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Addr returns the actual bound address, or nil before Start.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Shutdown gracefully stops HTTP and closes the listener.
func (s *Server) Shutdown(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() { err = s.http.Shutdown(ctx) })
	return err
}

func (s *Server) registerRoutes(staticFS fs.FS) {
	staticHandler := http.StripPrefix("/static/", http.FileServer(http.FS(staticFS)))
	s.mux.Handle("GET /static/", staticHandler)
	s.mux.HandleFunc("GET /", s.handleIndex)
	s.mux.HandleFunc("GET /api/v1/session", s.handleSession)
	s.mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
	s.mux.HandleFunc("POST /api/v1/auth/logout", s.handleLogout)
	s.mux.HandleFunc("GET /api/v1/peers", s.handlePeers)
	s.mux.HandleFunc("POST /api/v1/peers/refresh", s.handlePeersRefresh)
	s.mux.HandleFunc("POST /api/v1/messages/broadcast", s.handleBroadcast)
	s.mux.HandleFunc("POST /api/v1/messages/unicast", s.handleUnicast)
	s.mux.HandleFunc("GET /api/v1/events", s.handleEvents)
	s.mux.HandleFunc("POST /api/v1/files/send", s.handleFileSend)
	s.mux.HandleFunc("POST /api/v1/files/{transfer_id}/cancel", s.handleFileCancel)
	s.mux.HandleFunc("POST /api/v1/files/{transfer_id}/retry", s.handleFileRetry)
	s.mux.HandleFunc("GET /api/v1/files/{transfer_id}/download", s.handleFileDownload)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Không tìm thấy tài nguyên")
		return
	}
	data, err := fs.ReadFile(mustStaticFS(), "index.html")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "STATIC_UNAVAILABLE", "Không thể tải giao diện")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func mustStaticFS() fs.FS {
	staticFS, _ := web.StaticFiles()
	return staticFS
}
