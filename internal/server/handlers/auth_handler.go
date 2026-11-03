package handlers

import (
	"errors"
	"fmt"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"net"
	"time"
)

// AuthHandler translates authentication and presence requests into wire packets.
type AuthHandler struct {
	credStore *state.CredentialStore
	sendFn    func(rawPacket []byte, addr *net.UDPAddr) error
}

// NewAuthHandler wires credential lookup to the caller-owned UDP sender.
func NewAuthHandler(credStore *state.CredentialStore, sendFn func([]byte, *net.UDPAddr) error) *AuthHandler {
	return &AuthHandler{credStore: credStore, sendFn: sendFn}
}

// HandleAuthRequest applies BR01/BR02 and sends the initial peer snapshot.
func (h *AuthHandler) HandleAuthRequest(remoteAddr *net.UDPAddr, connID uint32, payload []byte, clientOutboundCh chan []byte) error {
	req, err := protocol.DecodeJSONPayload[protocol.AuthRequestPayload](payload)
	if err != nil {
		return fmt.Errorf("auth handler: decode request failed: %w", err)
	}
	user, code := h.credStore.Authenticate(req.Username, req.Password)
	if code != protocol.AuthCodeOK {
		return h.sendResponse(remoteAddr, connID, protocol.MsgAuthResponse, protocol.AuthResponsePayload{
			RequestID:  req.RequestID,
			Success:    false,
			Code:       code,
			Message:    authCodeMessage(code),
			ServerTime: time.Now().UnixMilli(),
		})
	}
	peers, targets, err := state.RegisterAuthenticatedUser(user.Username, user.DisplayName, connID, remoteAddr, [32]byte{}, clientOutboundCh)
	if err != nil {
		if !errors.Is(err, state.ErrUserAlreadyOnline) {
			return fmt.Errorf("auth handler: register session failed: %w", err)
		}
		return h.sendResponse(remoteAddr, connID, protocol.MsgAuthResponse, protocol.AuthResponsePayload{
			RequestID:  req.RequestID,
			Success:    false,
			Code:       protocol.AuthCodeAlreadyOnline,
			Message:    authCodeMessage(protocol.AuthCodeAlreadyOnline),
			ServerTime: time.Now().UnixMilli(),
		})
	}
	now := time.Now().UnixMilli()
	peer := protocol.PeerDTO{
		Username:    user.Username,
		DisplayName: user.DisplayName,
		IP:          remoteAddr.IP.String(),
		Port:        remoteAddr.Port,
		Status:      "ONLINE",
		LoginAt:     now,
	}
	if err := h.sendResponse(remoteAddr, connID, protocol.MsgAuthResponse, protocol.AuthResponsePayload{
		RequestID: req.RequestID,
		Success:   true,
		Code:      protocol.AuthCodeOK,
		Message:   "Đăng nhập thành công",
		User: &protocol.UserInfoDTO{
			Username:    user.Username,
			DisplayName: user.DisplayName,
			IP:          peer.IP,
			Port:        peer.Port,
			LoginAt:     now,
		},
		OnlinePeers: peers,
		ServerTime:  now,
	}); err != nil {
		return err
	}
	h.broadcastDiscoUpdate(targets, protocol.DiscoUpdatePayload{
		Event:       "JOIN",
		Reason:      "LOGIN",
		Peer:        peer,
		OnlineCount: len(peers),
		ServerTime:  now,
	})
	return nil
}

// HandleLogout validates connection ownership before releasing a session.
func (h *AuthHandler) HandleLogout(remoteAddr *net.UDPAddr, connID uint32, payload []byte) error {
	req, err := protocol.DecodeJSONPayload[protocol.LogoutPayload](payload)
	if err != nil {
		return fmt.Errorf("auth handler: decode logout payload failed: %w", err)
	}
	// ConnectionID is authoritative; a client cannot log out another user's session by changing JSON.
	username, ok := state.LookupUsernameByConnID(connID)
	if !ok || username != req.Username {
		return h.sendLogoutResponse(remoteAddr, connID, req.RequestID, false, "Phiên đăng nhập không hợp lệ")
	}
	removed, targets, existed := state.RemoveUserByConnID(connID)
	if existed && removed != nil {
		h.broadcastDiscoUpdate(targets, protocol.DiscoUpdatePayload{
			Event:       "LEAVE",
			Reason:      "LOGOUT",
			Peer:        *removed,
			OnlineCount: len(state.GetSnapshotOnlinePeers()),
			ServerTime:  time.Now().UnixMilli(),
		})
	}
	return h.sendLogoutResponse(remoteAddr, connID, req.RequestID, true, "Đã đăng xuất khỏi hệ thống")
}

func (h *AuthHandler) sendLogoutResponse(addr *net.UDPAddr, connID uint32, requestID string, success bool, message string) error {
	return h.sendResponse(addr, connID, protocol.MsgLogoutResponse, protocol.LogoutResponsePayload{
		RequestID:  requestID,
		Success:    success,
		Message:    message,
		ServerTime: time.Now().UnixMilli(),
	})
}

// HandleDiscoPeers returns a lock-free snapshot of online users.
func (h *AuthHandler) HandleDiscoPeers(remoteAddr *net.UDPAddr, connID uint32, payload []byte) error {
	req, err := protocol.DecodeJSONPayload[protocol.DiscoPeersPayload](payload)
	if err != nil {
		return fmt.Errorf("auth handler: decode discovery request failed: %w", err)
	}
	peers := state.GetSnapshotOnlinePeers()
	return h.sendResponse(remoteAddr, connID, protocol.MsgDiscoPeersResponse, protocol.DiscoPeersResponsePayload{
		RequestID:   req.RequestID,
		OnlineCount: len(peers),
		Peers:       peers,
		ServerTime:  time.Now().UnixMilli(),
	})
}

// sendResponse serializes a DTO and wraps it in the control-stream header.
func (h *AuthHandler) sendResponse(addr *net.UDPAddr, connID uint32, msgType uint8, data any) error {
	payload, err := protocol.EncodeJSONPayload(data)
	if err != nil {
		return err
	}
	raw, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD,
			MsgType:      msgType,
			StreamID:     protocol.StreamControl,
			ConnectionID: connID,
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	})
	if err != nil {
		return err
	}
	return h.sendFn(raw, addr)
}

// broadcastDiscoUpdate uses bounded non-blocking queues so a slow peer cannot
// stall authentication for every other peer.
func (h *AuthHandler) broadcastDiscoUpdate(targets []chan []byte, update protocol.DiscoUpdatePayload) {
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
		},
		Payload: payload,
	})
	if err != nil {
		return
	}
	for _, target := range targets {
		select {
		case target <- raw:
		default:
		}
	}
}

func authCodeMessage(code string) string {
	switch code {
	case protocol.AuthCodeUserNotFound:
		return "Tài khoản không hợp lệ hoặc không có quyền truy cập"
	case protocol.AuthCodeInvalidPassword:
		return "Mật khẩu không đúng, vui lòng thử lại"
	case protocol.AuthCodeAlreadyOnline:
		return "Tài khoản này hiện đang hoạt động ở một phiên khác"
	default:
		return "Xác thực không thành công"
	}
}
