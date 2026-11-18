package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/dispatcher"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/handlers"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// ServerApp wires the concurrent transport, dispatcher, credentials, and presence state.
type ServerApp struct {
	verifyKey  ed25519.PublicKey
	signingKey ed25519.PrivateKey
	mux        *rudp.Mux
	router     *dispatcher.Router
	credStore  *state.CredentialStore
	chatHdl    *handlers.ChatHandler
	fileHdl    *handlers.FileRelayHandler
}

// NewServerApp creates a Step 4 server bound to listenAddr.
func NewServerApp(listenAddr, usersPath string) (*ServerApp, error) {
	return NewServerAppWithNetEm(listenAddr, usersPath, rudp.NetEmConfig{})
}

// NewServerAppWithNetEm creates the server with outbound UDP impairment enabled.
func NewServerAppWithNetEm(listenAddr, usersPath string, netEm rudp.NetEmConfig) (*ServerApp, error) {
	verifyKey, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("server: generate peer signing key: %w", err)
	}
	credStore, err := state.LoadCredentialStore(usersPath)
	if err != nil {
		return nil, fmt.Errorf("server: load users: %w", err)
	}
	app := &ServerApp{
		verifyKey:  verifyKey,
		signingKey: signingKey,
		router:     dispatcher.NewRouter(),
		credStore:  credStore,
		chatHdl:    handlers.NewChatHandler(),
		fileHdl:    handlers.NewFileRelayHandler(),
	}
	app.router.SetPanicHandler(func(sess *rudp.Session, msgType uint8, panicErr error) {
		_ = app.sendJSON(sess, protocol.MsgError, protocol.ErrorMessagePayload{Code: protocol.ErrCodeInternalServer, Message: "Hệ thống bận, vui lòng thử lại", RefMsgType: msgType, Recoverable: true, ServerTime: time.Now().UnixMilli()})
	})
	muxConfig := rudp.DefaultMuxConfig(listenAddr)
	muxConfig.NetEm = netEm
	mux, err := rudp.NewMux(muxConfig, app)
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
	app.router.Register(protocol.MsgMessBroadcast, app.chatHdl.HandleBroadcast)
	app.router.Register(protocol.MsgMessUnicast, app.chatHdl.HandleUnicast)
	app.router.Register(protocol.MsgFileInit, app.fileHdl.HandleFileInit)
	app.router.Register(protocol.MsgFileResponse, app.fileHdl.HandleFileResponse)
	app.router.Register(protocol.MsgFileChunk, app.fileHdl.HandleFileChunk)
	app.router.Register(protocol.MsgFileComplete, app.fileHdl.HandleFileComplete)
	app.router.Register(protocol.MsgFileCancel, app.fileHdl.HandleFileCancel)
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
	peers, targets, err := state.RegisterReliableUser(user.Username, user.DisplayName, sess)
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
	registered, _ := state.LookupTargetByUsername(user.Username)
	claim := []byte(fmt.Sprintf("%s|%s|%d|%d", user.Username, addr.IP.String(), req.P2PPort, registered.LoginAt.UnixMilli()))
	ticket := base64.StdEncoding.EncodeToString(ed25519.Sign(app.signingKey, claim))
	peers = state.SetP2PMetadata(user.Username, req.P2PPort, ticket)
	peer := protocol.PeerDTO{
		P2PPort:     req.P2PPort,
		PeerTicket:  ticket,
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
			P2PPort:     req.P2PPort,
			PeerTicket:  ticket,
			Username:    user.Username,
			DisplayName: user.DisplayName,
			IP:          addr.IP.String(),
			Port:        addr.Port,
			LoginAt:     now,
		},
		OnlinePeers:      peers,
		ClusterVerifyKey: base64.StdEncoding.EncodeToString(app.verifyKey),
		ServerTime:       now,
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
	removed, targets, existed := state.RemoveReliableUserByConnID(sess.ID())
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

func (app *ServerApp) handlePing(sess *rudp.Session, _ uint8, payload []byte) error {
	ping, err := protocol.DecodeJSONPayload[protocol.PingPayload](payload)
	if err != nil {
		return fmt.Errorf("server ping decode: %w", err)
	}
	state.TouchHeartbeat(sess.ID(), sess.RemoteAddr())
	return app.sendJSON(sess, protocol.MsgPong, protocol.PongPayload{Seq: ping.Seq, ClientTime: ping.ClientTime, ServerTime: time.Now().UnixMilli()})
}

// OnSessionConnected records a transport-level session event.
func (app *ServerApp) OnSessionConnected(sess *rudp.Session) {
	slog.Debug("server session connected", "conn_id", sess.ID())
}

// OnMessageReceived dispatches a complete application message.
func (app *ServerApp) OnMessageReceived(sess *rudp.Session, streamID, msgType uint8, payload []byte) {
	app.router.Dispatch(sess, streamID, msgType, payload)
}

// OnAckReceived forwards receiver SACK state to the sender of a relayed file.
func (app *ServerApp) OnAckReceived(sess *rudp.Session, packet *protocol.Packet) {
	if err := app.fileHdl.RelayAck(sess, packet); err != nil {
		slog.Debug("server file ACK relay failed", "error", err)
	}
}

// OnSessionClosed removes authenticated presence after transport closure.
func (app *ServerApp) OnSessionClosed(sess *rudp.Session, reason string) {
	if reason == "IDLE_TIMEOUT" {
		reason = "TIMEOUT"
	}
	if username, ok := state.LookupUsernameByConnID(sess.ID()); ok {
		for _, transfer := range state.CancelTransfersForUser(username) {
			peerName := transfer.ReceiverUsername
			streamID := transfer.ReceiverStreamID
			if username == transfer.ReceiverUsername {
				peerName = transfer.SenderUsername
				streamID = transfer.SenderStreamID
			}
			peer, online := state.LookupTargetByUsername(peerName)
			if !online || peer.OutboundCh == nil {
				continue
			}
			payload, encodeErr := protocol.EncodeJSONPayload(protocol.FileCancelPayload{TransferID: transfer.TransferID, CancelledBy: username, Reason: protocol.FileCodePeerDisconnected, Message: "peer disconnected", Timestamp: time.Now().UnixMilli()})
			if encodeErr != nil {
				continue
			}
			if forwardErr := app.fileHdl.Forward(peer.OutboundCh, peer.ConnectionID, streamID, protocol.MsgFileCancel, payload); forwardErr != nil {
				slog.Debug("server file cancel forwarding failed", "error", forwardErr)
			}
		}
	}
	removed, targets, existed := state.RemoveReliableUserByConnID(sess.ID())
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
	flags := uint8(protocol.FlagREL | protocol.FlagORD)
	if msgType == protocol.MsgPong {
		flags = protocol.FlagACK
	}
	hdr := protocol.Header{
		Version:      protocol.ProtocolVersion,
		Flags:        flags,
		MsgType:      msgType,
		StreamID:     protocol.StreamControl,
		ConnectionID: sess.ID(),
		WindowSize:   protocol.DefaultWindowSize,
	}
	var raw []byte
	if cipher := sess.Cipher(); cipher != nil {
		raw, err = cipher.EncryptPacket(hdr, payload)
	} else {
		raw, err = protocol.MarshalPacket(&protocol.Packet{Hdr: hdr, Payload: payload})
	}
	if err != nil {
		return fmt.Errorf("server marshal response: %w", err)
	}
	if !sess.EnqueueOutbound(raw) {
		return fmt.Errorf("server enqueue response: session %d queue full", sess.ID())
	}
	return nil
}

// broadcast seals each presence update for the recipient's authenticated session.
func (app *ServerApp) broadcast(targets []state.BroadcastTarget, update protocol.DiscoUpdatePayload) {
	payload, err := protocol.EncodeJSONPayload(update)
	if err != nil {
		return
	}
	for _, target := range targets {
		hdr := protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD,
			MsgType:      protocol.MsgDiscoUpdate,
			StreamID:     protocol.StreamControl,
			ConnectionID: target.ConnectionID,
			WindowSize:   protocol.DefaultWindowSize,
		}
		var raw []byte
		if target.Cipher != nil {
			raw, err = target.Cipher.EncryptPacket(hdr, payload)
		} else {
			raw, err = protocol.MarshalPacket(&protocol.Packet{Hdr: hdr, Payload: payload})
		}
		if err != nil {
			slog.Warn("server failed to encode presence update", "conn_id", target.ConnectionID, "error", err)
			continue
		}
		select {
		case target.OutboundCh <- raw:
		default:
			slog.Warn("server dropped update for slow consumer", "conn_id", target.ConnectionID)
		}
	}
}
