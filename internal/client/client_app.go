package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/events"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

var (
	// ErrClientAlreadyAuthenticated prevents competing login lifecycles.
	ErrClientAlreadyAuthenticated = errors.New("client app: already authenticated")
	// ErrClientNotAuthenticated identifies a use case that needs a live session.
	ErrClientNotAuthenticated = errors.New("client app: authentication required")
)

// AuthError carries a stable protocol code without exposing transport internals.
type AuthError struct {
	Code    string
	Message string
}

// Error implements error for failed authentication responses.
func (e *AuthError) Error() string { return e.Code + ": " + e.Message }

// AppConfig contains dependencies and lifecycle limits for the local agent.
type AppConfig struct {
	ServerAddr       string
	StagingDir       string
	DownloadDir      string
	HandshakeTimeout time.Duration
	RequestTimeout   time.Duration
}

// SessionSnapshot is the minimal network-facing result returned by login.
type SessionSnapshot struct {
	Authenticated   bool
	ConnectionState state.ConnectionState
	CurrentUser     *protocol.UserInfoDTO
	OnlinePeers     []protocol.PeerDTO
}

// ClientApp owns one persistent encrypted UDP session and its client use cases.
type ClientApp struct {
	config          AppConfig
	store           *state.ClientStore
	eventBus        *events.Bus
	lifecycleMu     sync.Mutex
	conn            *net.UDPConn
	server          *net.UDPAddr
	connectionID    uint32
	cipher          *security.SessionCipher
	auth            *AuthClient
	chat            *ChatClient
	sessionCtx      context.Context
	sessionCancel   context.CancelFunc
	listenerDone    chan struct{}
	reconnectMu     sync.Mutex
	reconnecting    bool
	reconnectCancel context.CancelFunc
	username        string
	password        string
	closed          bool
}

// NewClientApp creates an unauthenticated local client facade.
func NewClientApp(config AppConfig) *ClientApp {
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 5 * time.Second
	}
	return &ClientApp{
		config:   config,
		store:    state.NewClientStore(),
		eventBus: events.NewBus(),
	}
}

// Store returns the thread-safe local state projection.
func (a *ClientApp) Store() *state.ClientStore { return a.store }

// Events returns the domain event bus used by the local UI bridge.
func (a *ClientApp) Events() *events.Bus { return a.eventBus }

// Login performs a persistent secure handshake and starts the single reader.
func (a *ClientApp) Login(ctx context.Context, username, password string) (SessionSnapshot, error) {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.closed {
		return SessionSnapshot{}, fmt.Errorf("client app: %w", context.Canceled)
	}
	if a.chat != nil || a.store.IsLoggedIn() {
		return SessionSnapshot{}, ErrClientAlreadyAuthenticated
	}
	session, err := dialPersistentSession(ctx, a.config.ServerAddr, a.config.HandshakeTimeout)
	if err != nil {
		return SessionSnapshot{}, err
	}
	store := a.store
	auth := NewAuthClient(session.Conn, session.Server, session.ConnectionID, store)
	auth.SetCipher(session.Cipher)
	loginCtx := ctx
	if _, ok := ctx.Deadline(); !ok && a.config.RequestTimeout > 0 {
		var cancel context.CancelFunc
		loginCtx, cancel = context.WithTimeout(ctx, a.config.RequestTimeout)
		defer cancel()
	}
	response, err := auth.Login(loginCtx, username, password)
	if err != nil {
		_ = session.Conn.Close()
		return SessionSnapshot{}, err
	}
	if !response.Success {
		_ = session.Conn.Close()
		return SessionSnapshot{}, &AuthError{
			Code:    response.Code,
			Message: response.Message,
		}
	}
	chat := NewChatClientWithStorage(session.Conn, session.Server, session.ConnectionID, store, a.config.StagingDir, a.config.DownloadDir)
	chat.SetCipher(session.Cipher)
	chat.SetEventBus(a.eventBus)
	a.sessionCtx, a.sessionCancel = context.WithCancel(context.Background())
	a.conn = session.Conn
	a.server = session.Server
	a.connectionID = session.ConnectionID
	a.cipher = session.Cipher
	a.auth = auth
	a.chat = chat
	a.username = username
	a.password = password
	a.listenerDone = make(chan struct{})
	store.SetConnectionState(state.ConnStateConnected)
	go a.runListener(chat, a.sessionCtx, a.listenerDone)
	chat.StartHeartbeat(a.sessionCtx, 3*time.Second)
	return a.Snapshot(), nil
}

// Snapshot returns a detached current session summary for HTTP bootstrap.
func (a *ClientApp) Snapshot() SessionSnapshot {
	return SessionSnapshot{
		Authenticated:   a.store.IsLoggedIn(),
		ConnectionState: a.store.ConnectionState(),
		CurrentUser:     a.store.CurrentUser(),
		OnlinePeers:     a.store.GetOnlinePeers(),
	}
}

// Logout clears local state and closes the live session within the caller deadline.
func (a *ClientApp) Logout(ctx context.Context) error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.chat == nil {
		a.store.ClearSession()
		return nil
	}
	username := a.username
	chat := a.chat
	if username != "" {
		// Logout is best effort: UC02 requires local cleanup even when the server is unreachable.
		_ = chat.SendLogout(username)
	}
	if a.sessionCancel != nil {
		a.sessionCancel()
	}
	a.reconnectMu.Lock()
	if a.reconnectCancel != nil {
		a.reconnectCancel()
		a.reconnectCancel = nil
	}
	a.reconnecting = false
	a.reconnectMu.Unlock()
	if a.conn != nil {
		_ = a.conn.Close()
	}
	if a.listenerDone != nil {
		select {
		case <-a.listenerDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	a.chat = nil
	a.auth = nil
	a.conn = nil
	a.server = nil
	a.connectionID = 0
	a.cipher = nil
	a.username = ""
	a.password = ""
	a.sessionCancel = nil
	a.listenerDone = nil
	a.store.ClearSession()
	return nil
}

// SendBroadcast enqueues one room message using the caller's correlation ID.
func (a *ClientApp) SendBroadcast(ctx context.Context, clientMsgID, content string) error {
	chat, err := a.currentChat()
	if err != nil {
		return err
	}
	return chat.SendBroadcastWithID(ctx, clientMsgID, content)
}

// SendDirect enqueues one direct message using the caller's correlation ID.
func (a *ClientApp) SendDirect(ctx context.Context, clientMsgID, recipient, content string) error {
	chat, err := a.currentChat()
	if err != nil {
		return err
	}
	return chat.SendDirectWithID(ctx, clientMsgID, recipient, content)
}

// SendFile starts one upload from an agent-owned staged source path.
func (a *ClientApp) SendFile(ctx context.Context, recipient, filePath, caption string) (string, error) {
	return a.SendFileNamed(ctx, recipient, filePath, "", caption)
}

// SendFileNamed starts one upload while preserving a browser-selected display name.
func (a *ClientApp) SendFileNamed(ctx context.Context, recipient, filePath, displayName, caption string) (string, error) {
	chat, err := a.currentChat()
	if err != nil {
		return "", err
	}
	return chat.SendFileNamed(recipient, filePath, displayName, caption)
}

// CancelTransfer cancels one active transfer and removes partial state.
func (a *ClientApp) CancelTransfer(ctx context.Context, transferID string) error {
	chat, err := a.currentChat()
	if err != nil {
		return err
	}
	return chat.CancelTransfer(transferID)
}

// RefreshPeers asks the authenticated server for a fresh presence snapshot.
func (a *ClientApp) RefreshPeers(ctx context.Context) error {
	chat, err := a.currentChat()
	if err != nil {
		return err
	}
	return chat.RefreshPeers(ctx)
}

// OpenCompletedFile opens a verified completed file by transfer ID.
func (a *ClientApp) OpenCompletedFile(transferID string) (*os.File, transfer.TransferProgress, error) {
	progress, ok := a.store.GetFileProgress(transferID)
	if !ok || progress == nil || progress.State != "COMPLETED" || progress.Direction != "DOWNLOAD" {
		return nil, transfer.TransferProgress{}, fmt.Errorf("client app: completed download not found")
	}
	path, ok := a.store.GetCompletedFilePath(transferID)
	if !ok {
		return nil, transfer.TransferProgress{}, fmt.Errorf("client app: completed path not found")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, transfer.TransferProgress{}, fmt.Errorf("client app: open completed file: %w", err)
	}
	return file, *progress, nil
}

// Shutdown stops the session and event bus, making subsequent use invalid.
func (a *ClientApp) Shutdown(ctx context.Context) error {
	a.lifecycleMu.Lock()
	if a.closed {
		a.lifecycleMu.Unlock()
		return nil
	}
	a.closed = true
	a.lifecycleMu.Unlock()
	if err := a.Logout(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	a.eventBus.Close()
	return nil
}

func (a *ClientApp) currentChat() (*ChatClient, error) {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.chat == nil {
		return nil, ErrClientNotAuthenticated
	}
	return a.chat, nil
}

func (a *ClientApp) runListener(chat *ChatClient, ctx context.Context, done chan struct{}) {
	defer close(done)
	if err := chat.Listen(ctx); err != nil && ctx.Err() == nil {
		a.store.SetConnectionState(state.ConnStateReconnecting)
		a.eventBus.Publish("connection.status", map[string]any{
			"state":   string(state.ConnStateReconnecting),
			"message": "Mất kết nối. Một số tính năng không khả dụng.",
		})
		a.startReconnect()
	}
}

func (a *ClientApp) startReconnect() {
	a.reconnectMu.Lock()
	if a.reconnecting || a.closed {
		a.reconnectMu.Unlock()
		return
	}
	a.reconnecting = true
	ctx, cancel := context.WithCancel(context.Background())
	a.reconnectCancel = cancel
	a.reconnectMu.Unlock()
	go func() {
		manager := NewReconnectManager(a.store, ReconnectConfig{
			InitialInterval: time.Second,
			MaxInterval:     16 * time.Second,
			Multiplier:      2,
			MaxAttempts:     5,
		})
		_ = manager.Start(ctx, a.reconnectOnce)
		a.reconnectMu.Lock()
		a.reconnecting = false
		a.reconnectCancel = nil
		a.reconnectMu.Unlock()
	}()
}

func (a *ClientApp) reconnectOnce() error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.closed || a.username == "" {
		return ErrClientNotAuthenticated
	}
	if a.conn != nil {
		_ = a.conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.config.RequestTimeout)
	defer cancel()
	session, err := dialPersistentSession(ctx, a.config.ServerAddr, a.config.HandshakeTimeout)
	if err != nil {
		return err
	}
	store := a.store
	auth := NewAuthClient(session.Conn, session.Server, session.ConnectionID, store)
	auth.SetCipher(session.Cipher)
	response, err := auth.Login(ctx, a.username, a.password)
	if err != nil {
		_ = session.Conn.Close()
		return err
	}
	if !response.Success {
		_ = session.Conn.Close()
		return &AuthError{
			Code:    response.Code,
			Message: response.Message,
		}
	}
	if a.sessionCancel != nil {
		a.sessionCancel()
	}
	chat := NewChatClientWithStorage(session.Conn, session.Server, session.ConnectionID, store, a.config.StagingDir, a.config.DownloadDir)
	chat.SetCipher(session.Cipher)
	chat.SetEventBus(a.eventBus)
	a.sessionCtx, a.sessionCancel = context.WithCancel(context.Background())
	a.conn = session.Conn
	a.server = session.Server
	a.connectionID = session.ConnectionID
	a.cipher = session.Cipher
	a.auth = auth
	a.chat = chat
	a.listenerDone = make(chan struct{})
	store.SetLoginSuccess(response.User, response.OnlinePeers)
	store.SetConnectionState(state.ConnStateConnected)
	go a.runListener(chat, a.sessionCtx, a.listenerDone)
	chat.StartHeartbeat(a.sessionCtx, 3*time.Second)
	return nil
}
