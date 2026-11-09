package webbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/events"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

type fakeService struct {
	store *state.ClientStore
	bus   *events.Bus
	login func(string, string) (client.SessionSnapshot, error)
}

func newFakeService() *fakeService {
	return &fakeService{
		store: state.NewClientStore(),
		bus:   events.NewBus(),
	}
}

func (f *fakeService) Snapshot() client.SessionSnapshot {
	return client.SessionSnapshot{
		Authenticated:   f.store.IsLoggedIn(),
		ConnectionState: f.store.ConnectionState(),
		CurrentUser:     f.store.CurrentUser(),
		OnlinePeers:     f.store.GetOnlinePeers(),
	}
}

func (f *fakeService) Store() *state.ClientStore { return f.store }
func (f *fakeService) Events() *events.Bus       { return f.bus }
func (f *fakeService) Login(_ context.Context, username, password string) (client.SessionSnapshot, error) {
	if f.login != nil {
		return f.login(username, password)
	}
	f.store.SetLoginSuccess(&protocol.UserInfoDTO{Username: username}, nil)
	return f.Snapshot(), nil
}
func (f *fakeService) Logout(context.Context) error                             { f.store.ClearSession(); return nil }
func (f *fakeService) SendBroadcast(context.Context, string, string) error      { return nil }
func (f *fakeService) SendDirect(context.Context, string, string, string) error { return nil }
func (f *fakeService) RefreshPeers(context.Context) error                       { return nil }
func (f *fakeService) SendFileNamed(context.Context, string, string, string, string) (string, error) {
	return "transfer-id", nil
}
func (f *fakeService) CancelTransfer(context.Context, string) error { return nil }
func (f *fakeService) OpenCompletedFile(string) (*os.File, transfer.TransferProgress, error) {
	return nil, transfer.TransferProgress{}, errors.New("not found")
}

func newTestServer(t *testing.T, service *fakeService) *Server {
	t.Helper()
	server, err := NewServer(service, Config{
		ListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func requestWithHost(method, path string) *http.Request {
	request := httptest.NewRequest(method, "http://127.0.0.1"+path, nil)
	request.Host = "127.0.0.1"
	return request
}

func TestServerBootstrapAndCSRFGuard(t *testing.T) {
	service := newFakeService()
	server := newTestServer(t, service)
	get := httptest.NewRecorder()
	server.Handler().ServeHTTP(get, requestWithHost(http.MethodGet, "/api/v1/session"))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), "csrf_token") {
		t.Fatalf("bootstrap status=%d body=%s", get.Code, get.Body.String())
	}
	post := httptest.NewRecorder()
	server.Handler().ServeHTTP(post, requestWithHost(http.MethodPost, "/api/v1/auth/login"))
	if post.Code != http.StatusForbidden {
		t.Fatalf("missing csrf status=%d", post.Code)
	}
}

func TestSessionCollectionsRemainNonNullAfterSuccessfulLogin(t *testing.T) {
	service := newFakeService()
	server := newTestServer(t, service)
	login := requestWithHost(http.MethodPost, "/api/v1/auth/login")
	login.Body = http.NoBody
	login.Header.Set("X-Client-CSRF-Token", server.CSRFToken())
	// Drive the same empty-history state produced by a successful new login.
	service.store.SetLoginSuccess(&protocol.UserInfoDTO{Username: "Alice"}, nil)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, requestWithHost(http.MethodGet, "/api/v1/session"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var envelope struct {
		Data struct {
			OnlinePeers       []protocol.PeerDTO                                `json:"online_peers"`
			BroadcastMessages []protocol.BroadcastMessageServerPayload          `json:"broadcast_messages"`
			DirectMessages    map[string][]protocol.UnicastMessageServerPayload `json:"direct_messages"`
			DeliveryStatus    map[string]string                                 `json:"delivery_status"`
			FileTransfers     []transfer.TransferProgress                       `json:"file_transfers"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data.OnlinePeers == nil || envelope.Data.BroadcastMessages == nil || envelope.Data.DirectMessages == nil || envelope.Data.DeliveryStatus == nil || envelope.Data.FileTransfers == nil {
		t.Fatalf("session collections must be non-null: %s", recorder.Body.String())
	}
}

func TestServerRejectsNonLocalHostAndServesEmbeddedIndex(t *testing.T) {
	service := newFakeService()
	server := newTestServer(t, service)
	bad := requestWithHost(http.MethodGet, "/")
	bad.Host = "192.168.1.20"
	badRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(badRecorder, bad)
	if badRecorder.Code != http.StatusForbidden {
		t.Fatalf("bad host status=%d", badRecorder.Code)
	}
	goodRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(goodRecorder, requestWithHost(http.MethodGet, "/"))
	if goodRecorder.Code != http.StatusOK || !strings.Contains(goodRecorder.Body.String(), "Hệ thống Cộng tác Nội bộ") {
		t.Fatalf("index status=%d body=%s", goodRecorder.Code, goodRecorder.Body.String())
	}
}

func TestChatHandlerValidatesCSRFAndReturnsAcceptedEnvelope(t *testing.T) {
	service := newFakeService()
	service.store.SetLoginSuccess(&protocol.UserInfoDTO{
		Username: "Alice",
	}, []protocol.PeerDTO{
		{
			Username: "Bob",
			Status:   "ONLINE",
		},
	})
	server := newTestServer(t, service)
	body := bytes.NewBufferString(`{"client_msg_id":"m-1","content":"hello"}`)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/messages/broadcast", body)
	request.Host = "127.0.0.1"
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Client-CSRF-Token", server.CSRFToken())
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"client_msg_id":"m-1"`) {
		t.Fatalf("broadcast status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestUnicastHandlerRejectsSelfAndOfflinePeers(t *testing.T) {
	service := newFakeService()
	service.store.SetLoginSuccess(&protocol.UserInfoDTO{
		Username: "Alice",
	}, []protocol.PeerDTO{
		{
			Username: "Bob",
			Status:   "OFFLINE",
		},
	})
	server := newTestServer(t, service)
	for _, recipient := range []string{"Alice", "Bob"} {
		body := bytes.NewBufferString(`{"client_msg_id":"m-1","recipient":"` + recipient + `","content":"hello"}`)
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/messages/unicast", body)
		request.Host = "127.0.0.1"
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Client-CSRF-Token", server.CSRFToken())
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest && recorder.Code != http.StatusConflict {
			t.Fatalf("recipient=%s status=%d body=%s", recipient, recorder.Code, recorder.Body.String())
		}
	}
}
