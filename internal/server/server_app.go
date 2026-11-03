package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/dispatcher"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// ServerApp wires the concurrent transport, dispatcher, credentials, and presence state.
type ServerApp struct {
	mux       *rudp.Mux
	router    *dispatcher.Router
	credStore *state.CredentialStore
}

// NewServerApp creates a Step 4 server bound to listenAddr.
func NewServerApp(listenAddr, usersPath string) (*ServerApp, error) {
	credStore, err := state.LoadCredentialStore(usersPath)
	if err != nil {
		return nil, fmt.Errorf("server: load users: %w", err)
	}
	app := &ServerApp{
		router:    dispatcher.NewRouter(),
		credStore: credStore,
	}
	mux, err := rudp.NewMux(rudp.DefaultMuxConfig(listenAddr), app)
	if err != nil {
		return nil, fmt.Errorf("server: create mux: %w", err)
	}
	app.mux = mux
	app.registerRoutes()
	return app, nil
}

func (app *ServerApp) registerRoutes() {
	app.router.Register(protocol.MsgAuthRequest, app.handleAuth)
	app.router.Register(protocol.MsgLogout, app.handleLogout)
	app.router.Register(protocol.MsgDiscoPeers, app.handlePeers)
	app.router.Register(protocol.MsgPing, app.handlePing)
}

func (app *ServerApp) handleAuth(sess *rudp.Session, _ uint8, payload []byte) error {
	req, err := protocol.DecodeJSONPayload[protocol.AuthRequestPayload](payload)
	if err != nil {
		return fmt.Errorf("server auth decode: %w", err)
	}
	user, code := app.credStore.Authenticate(req.Username, req.Password)
	if code != protocol.AuthCodeOK {
		return app.sendJSON(sess, protocol.MsgAuthResponse, protocol.AuthResponsePayload{
			RequestID:  req.RequestID,
			Success:    false,
			Code:       code,
			Message:    "Xác thực thất bại",
			ServerTime: time.Now().UnixMilli(),
		})
	}
	addr := sess.RemoteAddr()
	peers, targets, err := state.RegisterAuthenticatedUser(user.Username, user.DisplayName, sess.ID(), addr, [32]byte{}, sess.OutboundQueue())
	if err != nil {
		return app.sendJSON(sess, protocol.MsgAuthResponse, protocol.AuthResponsePayload{
			RequestID:  req.RequestID,
			Success:    false,
			Code:       protocol.AuthCodeAlreadyOnline,
			Message:    "Tài khoản đang có phiên hoạt động",
			ServerTime: time.Now().UnixMilli(),
		})
	}
	now := time.Now().UnixMilli()
	peer := protocol.PeerDTO{
		Username:    user.Username,
		DisplayName: user.DisplayName,
		IP:          addr.IP.String(),
		Port:        addr.Port,
		Status:      "ONLINE",
		LoginAt:     now,
	}
	if err := app.sendJSON(sess, protocol.MsgAuthResponse, protocol.AuthResponsePayload{
		RequestID: req.RequestID,
		Success:   true,
		Code:      protocol.AuthCodeOK,
		Message:   "Đăng nhập thành công",
		User: &protocol.UserInfoDTO{
			Username:    user.Username,
			DisplayName: user.DisplayName,
			IP:          addr.IP.String(),
			Port:        addr.Port,
			LoginAt:     now,
		},
		OnlinePeers: peers,
		ServerTime:  now,
	}); err != nil {
		return err
	}
	app.broadcast(targets, protocol.DiscoUpdatePayload{
		Event:       "JOIN",
		Reason:      "LOGIN",
		Peer:        peer,
		OnlineCount: len(peers),
		ServerTime:  now,
	})
	return nil
}

func (app *ServerApp) handleLogout(sess *rudp.Session, _ uint8, payload []byte) error {
	req, err := protocol.DecodeJSONPayload[protocol.LogoutPayload](payload)
	if err != nil {
		return fmt.Errorf("server logout decode: %w", err)
	}
	username, ok := state.LookupUsernameByConnID(sess.ID())
	if !ok || username != req.Username {
		return app.sendJSON(sess, protocol.MsgLogoutResponse, protocol.LogoutResponsePayload{RequestID: req.RequestID, Message: "Phiên đăng nhập không hợp lệ", ServerTime: time.Now().UnixMilli()})
	}
	removed, targets, existed := state.RemoveUserByConnID(sess.ID())
	if existed && removed != nil {
		now := time.Now().UnixMilli()
		app.broadcast(targets, protocol.DiscoUpdatePayload{Event: "LEAVE", Reason: "LOGOUT", Peer: *removed, OnlineCount: len(state.GetSnapshotOnlinePeers()), ServerTime: now})
	}
	return app.sendJSON(sess, protocol.MsgLogoutResponse, protocol.LogoutResponsePayload{RequestID: req.RequestID, Success: true, Message: "Đã đăng xuất", ServerTime: time.Now().UnixMilli()})
}

func (app *ServerApp) handlePeers(sess *rudp.Session, _ uint8, payload []byte) error {
	req, err := protocol.DecodeJSONPayload[protocol.DiscoPeersPayload](payload)
	if err != nil {
		return fmt.Errorf("server peers decode: %w", err)
	}
	peers := state.GetSnapshotOnlinePeers()
	return app.sendJSON(sess, protocol.MsgDiscoPeersResponse, protocol.DiscoPeersResponsePayload{RequestID: req.RequestID, OnlineCount: len(peers), Peers: peers, ServerTime: time.Now().UnixMilli()})
}

func (app *ServerApp) handlePing(sess *rudp.Session, _ uint8, _ []byte) error {
	state.TouchHeartbeat(sess.ID(), sess.RemoteAddr())
	return app.sendJSON(sess, protocol.MsgPong, struct{}{})
}

// OnSessionConnected records a transport-level session event.
func (app *ServerApp) OnSessionConnected(sess *rudp.Session) {
	slog.Debug("server session connected", "conn_id", sess.ID())
}

// OnMessageReceived dispatches a complete application message.
func (app *ServerApp) OnMessageReceived(sess *rudp.Session, streamID, msgType uint8, payload []byte) {
	app.router.Dispatch(sess, streamID, msgType, payload)
}

// OnSessionClosed removes authenticated presence after transport closure.
func (app *ServerApp) OnSessionClosed(sess *rudp.Session, reason string) {
	removed, targets, existed := state.RemoveUserByConnID(sess.ID())
	if !existed || removed == nil {
		return
	}
	now := time.Now().UnixMilli()
	app.broadcast(targets, protocol.DiscoUpdatePayload{Event: "LEAVE", Reason: reason, Peer: *removed, OnlineCount: len(state.GetSnapshotOnlinePeers()), ServerTime: now})
}

// Serve runs the reactor until ctx is cancelled.
func (app *ServerApp) Serve(ctx context.Context) error { return app.mux.Serve(ctx) }

// Addr returns the server's bound UDP address.
func (app *ServerApp) Addr() net.Addr { return app.mux.Addr() }

// ActiveConnections returns the current transport session count.
func (app *ServerApp) ActiveConnections() int { return app.mux.ActiveSessionCount() }

// Shutdown closes the reactor and waits for its workers.
func (app *ServerApp) Shutdown(ctx context.Context) error { return app.mux.Shutdown(ctx) }

func (app *ServerApp) sendJSON(sess *rudp.Session, msgType uint8, value any) error {
	payload, err := protocol.EncodeJSONPayload(value)
	if err != nil {
		return fmt.Errorf("server encode response: %w", err)
	}
	raw, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD,
			MsgType:      msgType,
			StreamID:     protocol.StreamControl,
			ConnectionID: sess.ID(),
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("server marshal response: %w", err)
	}
	if !sess.EnqueueOutbound(raw) {
		return fmt.Errorf("server enqueue response: session %d queue full", sess.ID())
	}
	return nil
}

func (app *ServerApp) broadcast(targets []chan []byte, update protocol.DiscoUpdatePayload) {
	payload, err := protocol.EncodeJSONPayload(update)
	if err != nil {
		return
	}
	raw, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:    protocol.ProtocolVersion,
			Flags:      protocol.FlagREL | protocol.FlagORD,
			MsgType:    protocol.MsgDiscoUpdate,
			StreamID:   protocol.StreamControl,
			WindowSize: protocol.DefaultWindowSize,
		}, Payload: payload})
	if err != nil {
		return
	}
	for _, target := range targets {
		select {
		case target <- raw:
		default:
			slog.Warn("server dropped update for slow consumer")
		}
	}
}
